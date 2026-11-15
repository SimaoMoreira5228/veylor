use crate::h1_peer::*;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::Duration;
use veylor_server::{
    MAX_RESPONSE_BODY, MAX_STATIC_BODY_BYTES, NativeRequestView, NativeResponseOut, Server,
    ServerConfig, ServerError,
};

#[test]
fn native_304_drops_stray_body_and_keeps_alive() {
    // NOTE: H1 mirror of h2_native_304_drops_handler_body: a native 304 with a stray
    // body must shed it (no content-length, no DATA) and keep the connection alive.
    let server = native_server();
    let mut stream = connect(&server);
    get(&mut stream, "/stray");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 304);
    assert!(response.body.is_empty(), "body: {:?}", response.body);
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| name != "content-length"),
        "headers: {:?}",
        response.headers
    );
    get(&mut stream, "/fixed");
    let sibling = read_response(&mut stream);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"native-hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_fixed_bytes_served_with_keep_alive() {
    let server = native_server();
    let mut stream = connect(&server);
    get(&mut stream, "/fixed");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"native-hello");
    stream
        .write_all(b"HEAD /fixed HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write HEAD");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
    }
    let text = String::from_utf8(head).expect("head utf8");
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(text.contains("Content-Length: 12"), "length kept: {text}");
    get(&mut stream, "/fixed");
    let third = read_response(&mut stream);
    assert_eq!(third.status, 200);
    assert_eq!(third.body, b"native-hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_panic_fails_the_request_closed() {
    let server = native_server();
    let mut stream = connect(&server);
    get(&mut stream, "/boom");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.response_overflow, 1, "counts: {counts:?}");
}

#[test]
fn native_reentrant_registration_rejected_while_outer_succeeds() {
    NATIVE_REENTER_REJECTED.store(false, Ordering::SeqCst);
    let server = native_server();
    let mut stream = connect(&server);
    get(&mut stream, "/reenter");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"reentered");
    assert!(
        NATIVE_REENTER_REJECTED.load(Ordering::SeqCst),
        "nested server call must fail while a handler runs"
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_large_body_within_caps_drains_then_reusable() {
    let server = native_server();
    let mut stream = connect(&server);
    let payload = vec![b'p'; 1024];
    let head = format!(
        "POST /large HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        payload.len()
    );
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(&payload).expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body.len(), MAX_RESPONSE_BODY);
    assert!(response.body.iter().all(|byte| *byte == b'N'));
    get(&mut stream, "/fixed");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"native-hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_echo_returns_staged_body_then_reusable() {
    let server = native_server();
    let mut stream = connect(&server);
    let request = "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 11\r\nConnection: keep-alive\r\n\r\nhello world";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello world");
    get(&mut stream, "/fixed");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"native-hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_zero_length_body_echoes_empty() {
    let server = native_server();
    let mut stream = connect(&server);
    let request = "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_partial_tail_invokes_framing_only_then_drains() {
    NATIVE_RECORD_BODY_NULL.store(false, Ordering::SeqCst);
    NATIVE_RECORD_HAS_BODY.store(false, Ordering::SeqCst);
    NATIVE_RECORD_BODY_LEN.store(0, Ordering::SeqCst);
    let server = native_server();
    let mut stream = connect(&server);
    let head = "POST /record HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\n";
    stream.write_all(head.as_bytes()).expect("write head");
    // NOTE: the pump invokes at head-parse time, so a withheld tail guarantees the handler runs
    // framing-only; the sleep only lets that turn happen before the body arrives.
    std::thread::sleep(Duration::from_millis(300));
    stream.write_all(b"hello").expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"recorded");
    assert!(
        NATIVE_RECORD_BODY_NULL.load(Ordering::SeqCst),
        "withheld tail must invoke with null body"
    );
    assert!(NATIVE_RECORD_HAS_BODY.load(Ordering::SeqCst));
    assert_eq!(NATIVE_RECORD_BODY_LEN.load(Ordering::SeqCst), 5);
    get(&mut stream, "/fixed");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"native-hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_registration_rejects_duplicates_and_null() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/dup",
            200,
            b"OK",
            b"static",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register static");
    let duplicate = server.register_native(b"GET", b"/dup", Some(native_fixed), 0);
    assert!(
        matches!(duplicate, Err(ServerError::DuplicateRoute)),
        "native over static must be Duplicate, got {duplicate:?}"
    );
    let null = server.register_native(b"GET", b"/null", None, 0);
    assert!(
        matches!(null, Err(ServerError::InvalidRoute)),
        "null handler must be Invalid, got {null:?}"
    );
    server
        .register_native(b"GET", b"/ok", Some(native_fixed), 0)
        .expect("register native");
    let back = server.register_route(b"GET", b"/ok", 200, b"OK", b"static", MAX_STATIC_BODY_BYTES);
    assert!(
        matches!(back, Err(ServerError::DuplicateRoute)),
        "static over native must be Duplicate, got {back:?}"
    );
}

static NATIVE_CTX_SENTINEL: usize = 0x5EED_C0DE;

#[allow(unsafe_code)]
unsafe extern "C" fn native_ctx(
    user_data: *mut std::ffi::c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if user_data as usize != NATIVE_CTX_SENTINEL || request.is_null() || response.is_null() {
        return 1;
    }

    let out = unsafe { &mut *response };
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = b"ctx-ok".as_ptr();
    out.body_len = 6;
    0
}

#[test]
fn native_user_data_passes_through_verbatim() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_native(b"GET", b"/ctx", Some(native_ctx), NATIVE_CTX_SENTINEL)
        .expect("register ctx");
    let server = server.start().expect("start server");
    let mut stream = connect(&server);
    get(&mut stream, "/ctx");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"ctx-ok");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}
