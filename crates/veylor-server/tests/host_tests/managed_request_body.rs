use crate::host_peer::*;
use std::io::Write;
use std::net::SocketAddr;
use std::time::Instant;
use veylor_server::{CancelOutcome, MAX_STATIC_BODY_BYTES, RespondOutcome, Server, ServerConfig};

#[test]
fn managed_body_expect_pulls_offsets_and_reuses() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 11\r\nExpect: 100-continue\r\nConnection: keep-alive\r\n\r\n")
        .expect("write head");
    let (interim, _) = read_head(&mut stream);
    assert_eq!(
        interim, 100,
        "admitted body emits interim before body bytes flow"
    );
    let (event, method, path) = poll_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    stream.write_all(b"hello").expect("write first chunk");
    let mut collected = Vec::new();
    let deadline = Instant::now() + DEADLINE;
    while collected.len() < 5 {
        assert!(Instant::now() < deadline, "first body chunk stalls");
        let mut buf = [0; 16];
        let (outcome, n, complete) = host.pull_body(event.token, &mut buf);
        assert_eq!(outcome, RespondOutcome::Ok);
        assert!(!complete, "end-of-body before declared length");
        collected.extend_from_slice(&buf[..n]);
        if n == 0 {
            std::thread::yield_now();
        }
    }
    assert_eq!(collected, b"hello");
    stream.write_all(b" world").expect("write second chunk");
    let deadline = Instant::now() + DEADLINE;
    while collected.len() < 11 {
        assert!(Instant::now() < deadline, "second body chunk stalls");
        let mut buf = [0; 16];
        let (outcome, n, complete) = host.pull_body(event.token, &mut buf);
        assert_eq!(outcome, RespondOutcome::Ok);
        collected.extend_from_slice(&buf[..n]);
        assert_eq!(
            complete,
            collected.len() == 11,
            "completion tracks pulled offsets"
        );
        if n == 0 {
            std::thread::yield_now();
        }
    }
    assert_eq!(collected, b"hello world");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_over_cap_rejects_without_interim_or_read() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    let request = format!(
        "POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nExpect: 100-continue\r\nConnection: keep-alive\r\n\r\n",
        MAX_STATIC_BODY_BYTES + 1
    );
    stream.write_all(request.as_bytes()).expect("write head");
    let (status, length) = read_head(&mut stream);
    assert_eq!(status, 413, "over-cap sends no interim and parks nothing");
    assert_eq!(read_exact_body(&mut stream, length), b"Payload Too Large");
    let (_, _, _, report) = poll_batch(&host, 0, 8);
    assert_eq!(report.copied, 0, "no host event for rejected bodies");
    expect_eof(&mut stream);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_chunked_split_size_line_assembles_and_reuses() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\nConnection: keep-alive\r\n\r\n")
        .expect("write head");
    let (interim, _) = read_head(&mut stream);
    assert_eq!(
        interim, 100,
        "admitted chunked body emits interim before framing bytes flow"
    );
    let (event, method, path) = poll_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    stream.write_all(b"5\r").expect("split chunk-size line");
    stream
        .write_all(b"\nhello\r\n")
        .expect("size line rest and data");
    stream
        .write_all(b"6;ext=v\r\n world\r\n")
        .expect("chunk with extension");
    stream.write_all(b"0\r\n\r\n").expect("terminal chunk");
    let mut collected = Vec::new();
    let mut complete = false;
    let deadline = Instant::now() + DEADLINE;
    while !complete {
        assert!(Instant::now() < deadline, "chunked body stalls");
        let mut buf = [0; 16];
        let (outcome, n, done) = host.pull_body(event.token, &mut buf);
        assert_eq!(outcome, RespondOutcome::Ok);
        collected.extend_from_slice(&buf[..n]);
        complete = done;
        if n == 0 && !done {
            std::thread::yield_now();
        }
    }
    assert_eq!(collected, b"hello world");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_chunked_bad_size_closes() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\nZZ\r\nnope\r\n0\r\n\r\n")
        .expect("write malformed chunks");
    let (event, _, _) = poll_request(&host);
    expect_eof(&mut stream);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone,
        "malformed framing fails the connection closed"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_chunked_disallowed_trailer_closes() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n5\r\nhello\r\n0\r\nX-Note: hi\r\n\r\n")
        .expect("write trailer chunks");
    let (event, _, _) = poll_request(&host);
    expect_eof(&mut stream);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone,
        "empty route allowlist rejects every trailer"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_chunked_over_cap_closes_mid_stream() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\nConnection: keep-alive\r\n\r\n")
        .expect("write head");
    let (interim, _) = read_head(&mut stream);
    assert_eq!(
        interim, 100,
        "chunked admission sends interim even without a declared length"
    );
    let (event, _, _) = poll_request(&host);
    let chunk = [b'a'; 1024];
    for _ in 0..65 {
        stream.write_all(b"400\r\n").expect("chunk size");
        stream.write_all(&chunk).expect("chunk data");
        stream.write_all(b"\r\n").expect("chunk end");
    }
    expect_eof(&mut stream);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone,
        "over-cap aggregate closes mid-stream instead of 413"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_cancel_mid_body_closes_sibling_unaffected() {
    let server = default_managed_server();
    let host = server.host();
    let mut first = connect(&server);
    first
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nabc")
        .expect("write partial body");
    let (upload, method, path) = poll_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    let deadline = Instant::now() + DEADLINE;
    let mut staged = Vec::new();
    while staged.len() < 3 {
        assert!(Instant::now() < deadline, "staged body stalls");
        let mut scratch = [0; 16];
        let (outcome, n, complete) = host.pull_body(upload.token, &mut scratch);
        assert_eq!(outcome, RespondOutcome::Ok);
        assert!(!complete);
        staged.extend_from_slice(&scratch[..n]);
        if n == 0 {
            std::thread::yield_now();
        }
    }
    assert_eq!(staged, b"abc");
    let mut second = connect(&server);
    get(&mut second, "/work");
    let (work, _, _) = poll_request(&host);
    assert_eq!(host.cancel(upload.token), CancelOutcome::Ok);
    expect_eof(&mut first);
    assert!(
        spin_until(|| host.parked_count() == 1),
        "cancel unparks exactly one"
    );
    let mut scratch = [0; 16];
    assert_eq!(
        host.pull_body(upload.token, &mut scratch).0,
        RespondOutcome::Stale,
        "cancelled body tomb reports stale"
    );
    assert_eq!(
        host.respond(work.token, 200, b"OK", b"live"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut second);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"live");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.cancelled, 1, "counts: {counts:?}");
}

#[test]
fn managed_body_unread_respond_drains_within_cap() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nabc")
        .expect("write partial body");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"ok"),
        RespondOutcome::Ok
    );
    stream.write_all(b"defghij").expect("write rest of body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"ok");
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn zero_cap_route_rejects_chunked_without_park_or_interim() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut builder = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    builder
        .register_managed(b"POST", b"/void", 0)
        .expect("register zero-cap");
    let server = builder.start().expect("start server");
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /void HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\nConnection: keep-alive\r\n\r\n")
        .expect("write head");
    let (first_status, first_len) = read_head(&mut stream);
    assert_eq!(
        first_status, 413,
        "zero-cap chunked rejects at admission with no interim"
    );
    let rejected = read_exact_body(&mut stream, first_len);
    assert_eq!(rejected, b"Payload Too Large");
    expect_eof(&mut stream);
    assert_eq!(host.parked_count(), 0, "rejected chunked never parks");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_unread_respond_repeated_sequential() {
    // NOTE: regression test for the park-time race where a host respond staged between
    // try_park and the buffered-body push killed the connection with TransportError instead
    // of draining (~3% per exchange, ~60% per 50-exchange run pre-fix). Repeating the
    // respond-then-write exchange exercises both orders across runs; every order must reuse.
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    for _ in 0..25 {
        stream
            .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nabc")
            .expect("write head and partial body");
        let (event, _, _) = poll_request(&host);
        assert_eq!(
            host.respond(event.token, 200, b"OK", b"ok"),
            RespondOutcome::Ok
        );
        stream.write_all(b"defghij").expect("write rest of body");
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"ok");
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}
