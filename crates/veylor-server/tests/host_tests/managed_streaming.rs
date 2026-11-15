use crate::host_peer::*;
use veylor_runtime::ConnectionToken;
use veylor_server::{CancelOutcome, RespondOutcome};

#[test]
fn managed_stream_begin_writes_end_exact_and_reuses() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(
        host.write_stream(event.token, b"hello "),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"world"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (status, raw, body) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello world");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("head end")
        + 4;
    stream_head_exact(&raw[..split], "HTTP/1.1 200 OK");
    assert_eq!(&raw[split..], b"6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n");
    assert_eq!(
        host.end_stream(event.token),
        RespondOutcome::Gone,
        "post-end ops observe Gone after unpark"
    );
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
fn managed_stream_write_without_writable_rejected_then_admitted() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.write_stream(event.token, &[]),
        RespondOutcome::Invalid,
        "empty chunk rejects"
    );
    assert_eq!(
        host.write_stream(event.token, &[b'x'; 16_384 + 1]),
        RespondOutcome::BodyTooLarge,
        "chunk past MAX_STREAM_WRITE rejects"
    );
    assert_eq!(
        host.write_stream(event.token, &[b'x'; 16_384]),
        RespondOutcome::BodyTooLarge,
        "chunk whose frame cannot fit the output rejects at stage"
    );
    assert_eq!(
        host.write_stream(event.token, b"early"),
        RespondOutcome::NotWritable,
        "write with no stream rejects"
    );
    let unknown = ConnectionToken {
        request_seq: 9999,
        ..event.token
    };
    assert!(!host.is_writable(unknown), "unknown token never writable");
    assert_eq!(
        host.write_stream(unknown, b"early"),
        RespondOutcome::NotWritable,
        "write with no token rejects"
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"late"), RespondOutcome::Ok);
    // NOTE: the queued window is transient by design (the worker applies within microseconds),
    // so the test loops until the queue is observed: every iteration is either an admission
    // (a full worker cycle completed first) or the expected rejection, and the admitted bytes
    // are tracked so the wire body stays exact. A wedged worker breaks the loop immediately
    // instead of hanging the suite.
    let mut agains = 0u32;
    loop {
        match host.write_stream(event.token, b"again") {
            RespondOutcome::Ok => agains += 1,
            RespondOutcome::WouldBlock | RespondOutcome::NotWritable => break,
            other => panic!("unexpected write outcome: {other:?}"),
        }
        assert!(agains < 1000, "worker outran the stage loop");
    }
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"!"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (_, _, body) = read_chunked(&mut stream);
    let mut expected = b"late".to_vec();
    for _ in 0..agains {
        expected.extend_from_slice(b"again");
    }
    expected.extend_from_slice(b"!");
    assert_eq!(body, expected);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_end_twice_fails_closed() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    // NOTE: the queued end applies within microseconds, so a single second end races between
    // Duplicate (queued) and Gone (applied): loop until the apply is observed. Every iteration
    // is one of the two legal outcomes, so the test cannot flake either way.
    let mut spins = 0u32;
    loop {
        match host.end_stream(event.token) {
            RespondOutcome::Duplicate => spins += 1,
            RespondOutcome::Gone => break,
            other => panic!("unexpected end outcome: {other:?}"),
        }
        assert!(spins < 1_000_000, "staged end never applied");
    }
    let (status, raw, body) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    assert!(body.is_empty());
    assert!(
        raw.ends_with(b"0\r\n\r\n"),
        "stream ends with terminal chunk"
    );
    assert_eq!(host.end_stream(event.token), RespondOutcome::Gone);
    assert_eq!(
        host.write_stream(event.token, b"late"),
        RespondOutcome::Gone
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Gone
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_cancel_mid_stream_closes() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(
        host.write_stream(event.token, b"partial"),
        RespondOutcome::Ok
    );
    let head = read_raw_head(&mut stream);
    let text = String::from_utf8(head).expect("head utf8");
    assert!(
        text.starts_with("HTTP/1.1 200 OK"),
        "stream head sent: {text}"
    );
    assert_eq!(
        read_chunk_frame(&mut stream),
        Some(b"partial".to_vec()),
        "first chunk flushes before cancel"
    );
    assert_eq!(host.cancel(event.token), CancelOutcome::Ok);
    expect_eof(&mut stream);
    assert_eq!(
        host.write_stream(event.token, b"late"),
        RespondOutcome::Gone,
        "cancelled stream tombs Gone"
    );
    assert_eq!(host.end_stream(event.token), RespondOutcome::Gone);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.cancelled, 1, "counts: {counts:?}");
}

#[test]
fn managed_stream_begin_no_body_status_rejected() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 204, b"No Content"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.begin_stream(event.token, 304, b"Not Modified"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.begin_stream(event.token, 100, b"Continue"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"bad\r\nreason"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"fallback"),
        RespondOutcome::Ok,
        "rejected begins stage nothing"
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"fallback");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}
