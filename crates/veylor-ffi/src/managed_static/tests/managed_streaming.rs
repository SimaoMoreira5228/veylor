use super::super::*;
use super::ffi_server::*;
use super::managed_peer::*;
use crate::abi::{VylBytes, VylResult};
use std::io::{Read, Write};

#[test]
fn stream_begin_writes_end_exact_framing() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"early")) },
        VylResult::BadState,
        "write with no open stream rejects"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Duplicate,
        "second begin loses to the open stream"
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"hello ")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"world")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Ok
    );
    let (status, raw, body) = read_streamed(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello world");
    let (head, tail) = split_head(&raw);
    head_with_date(
        head,
        b"HTTP/1.1 200 OK",
        b"\r\nTransfer-Encoding: chunked\r\n\r\n",
    );
    assert_eq!(tail, b"6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n");
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Gone,
        "post-end ops observe Gone after unpark"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"late")) },
        VylResult::Gone
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Gone
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_pacing_rejects_unarmed_then_admits() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"")) },
        VylResult::InvalidArgument,
        "empty chunk rejects"
    );
    let oversized = vec![b'x'; 16_384 + 1];
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(&oversized)) },
        VylResult::BodyTooLong,
        "chunk past 16 KiB rejects"
    );
    let unframeable = vec![b'x'; 16_384];
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(&unframeable)) },
        VylResult::BodyTooLong,
        "chunk whose frame cannot fit the output rejects at stage"
    );
    let unknown = VylManagedStaticToken {
        request_seq: 9999,
        ..token
    };
    assert_eq!(
        unsafe { is_writable(server, unknown) },
        (VylResult::Ok, 0),
        "unknown-but-wellformed token answers not-writable"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, unknown, bytes(b"early")) },
        VylResult::BadState,
        "write with no token rejects"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"late")) },
        VylResult::Ok
    );
    // NOTE: the queued window is transient by design, so the test loops until the queue is
    // observed: every iteration is either an admission or the expected rejection, and admitted
    // bytes are tracked so the wire body stays exact.
    let mut agains = 0u32;
    loop {
        match unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"again")) } {
            VylResult::Ok => agains += 1,
            VylResult::WouldBlock | VylResult::BadState => break,
            other => panic!("unexpected write outcome: {other:?}"),
        }
        assert!(agains < 1000, "worker outran the stage loop");
    }
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"!")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Ok
    );
    let (status, _, body) = read_streamed(&mut stream);
    assert_eq!(status, 200);
    let mut expected = b"late".to_vec();
    for _ in 0..agains {
        expected.extend_from_slice(b"again");
    }
    expected.extend_from_slice(b"!");
    assert_eq!(body, expected);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_end_twice_reports_duplicate() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Ok
    );
    // NOTE: the queued end applies within microseconds, so the immediate second end wins the race
    // in practice: a queued end reports duplicate, an applied one gone. The terminal-chunk read
    // below proves the apply, pinning the later gone assertions either way.
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Duplicate,
        "queued end reports duplicate"
    );
    let (status, raw, body) = read_streamed(&mut stream);
    assert_eq!(status, 200);
    assert!(body.is_empty());
    assert!(
        raw.ends_with(b"0\r\n\r\n"),
        "stream ends with terminal chunk"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Gone
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"late")) },
        VylResult::Gone
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_cancel_mid_stream_closes() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"partial")) },
        VylResult::Ok
    );
    let head = read_stream_head(&mut stream);
    let text = String::from_utf8(head).expect("head utf8");
    assert!(
        text.starts_with("HTTP/1.1 200 OK"),
        "stream head sent: {text}"
    );
    assert_eq!(
        read_stream_frame(&mut stream),
        Some(b"partial".to_vec()),
        "first chunk flushes before cancel"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_cancel(server, token) },
        VylResult::Ok
    );
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).expect("read close");
    assert!(rest.is_empty(), "cancel must close without tail: {rest:?}");
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"late")) },
        VylResult::Gone,
        "cancelled stream tombs Gone"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Gone
    );
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_begin_rejects_no_body_status_without_staging() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 204, bytes(b"No Content")) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 304, bytes(b"Not Modified")) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"bad\r\nreason"))
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"")) },
        VylResult::InvalidArgument,
        "empty reason rejects"
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"fallback"))
        },
        VylResult::Ok,
        "rejected begins stage nothing"
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"fallback");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_rejects_bad_state_and_null_inputs() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    let zero_token = VylManagedStaticToken::default();
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, zero_token, 200, bytes(b"OK")) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, zero_token, bytes(b"x")) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, zero_token) },
        VylResult::BadState
    );
    let mut writable = 0u32;
    assert_eq!(
        unsafe { vyl_managed_static_host_is_writable(server, zero_token, &mut writable) },
        VylResult::BadState
    );
    assert_eq!(writable, 0);
    assert_eq!(
        unsafe {
            vyl_managed_static_host_begin_stream(
                std::ptr::null_mut(),
                zero_token,
                200,
                bytes(b"OK"),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_write_stream(std::ptr::null_mut(), zero_token, bytes(b"x"))
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(std::ptr::null_mut(), zero_token) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_is_writable(std::ptr::null_mut(), zero_token, &mut writable)
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_begin_stream(
                server,
                zero_token,
                200,
                VylBytes {
                    data: std::ptr::null(),
                    length: 1,
                },
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_write_stream(
                server,
                zero_token,
                VylBytes {
                    data: std::ptr::null(),
                    length: 1,
                },
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_is_writable(server, zero_token, std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { vyl_managed_static_host_is_writable(server, zero_token, &mut writable) },
        VylResult::BadState,
        "zero-generation token is a shape problem, never Ok(false)"
    );
    assert_eq!(writable, 0);
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, zero_token, 200, bytes(b"OK")) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, zero_token, bytes(b"x")) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, zero_token) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_is_writable(server, zero_token, &mut writable) },
        VylResult::BadState
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}
