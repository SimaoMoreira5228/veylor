use crate::host_peer::*;
use std::io::{Read, Write};
use std::time::Instant;
use veylor_server::{CancelOutcome, HostEventKind, RespondOutcome, ServerConfig};

#[test]
fn managed_park_poll_respond_keep_alive_reusable() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    stream
        .set_read_timeout(Some(PARKED_QUIET))
        .expect("quiet timeout");
    let mut byte = [0; 1];
    assert!(
        stream.read(&mut byte).is_err(),
        "parked request sends no bytes"
    );
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    let (event, method, path) = poll_request(&host);
    assert_eq!(event.kind, HostEventKind::Request);
    assert_eq!(method, b"GET");
    assert_eq!(path, b"/work");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"done");
    get(&mut stream, "/work");
    let (second_event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second_event.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"again");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
}

#[test]
fn managed_parked_http10_keep_alive_echoes_and_reuses() {
    // NOTE: drift alarm for the converged keep-alive verdict: an HTTP/1.0 request with
    // Connection: keep-alive must echo Connection: keep-alive on every H1 path (static,
    // parked fixed, streaming Begin). The static path emits the shared
    // connection_header_for_response verdict directly; the parked paths must carry that same
    // verdict, not a bool-erased Omit.
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"GET /hello HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write static 1.0");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    assert!(
        first
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("connection")
                && value.eq_ignore_ascii_case("keep-alive")),
        "static echoes keep-alive: {:?}",
        first.headers
    );
    stream
        .write_all(b"GET /work HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write parked 1.0");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"done");
    assert!(
        second
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("connection")
                && value.eq_ignore_ascii_case("keep-alive")),
        "parked fixed echoes keep-alive: {:?}",
        second.headers
    );
    stream
        .write_all(b"GET /work HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write reuse 1.0");
    let (third, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(third.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let again = read_response(&mut stream);
    assert_eq!(again.status, 200);
    assert_eq!(again.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_http10_uses_close_delimited_response() {
    // NOTE: HTTP/1.0 has no chunked framing, so streamed responses use close-delimited bodies.
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"GET /work HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write stream 1.0");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b""), RespondOutcome::Invalid);
    assert_eq!(host.write_stream(event.token, b"body"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let head = read_raw_head(&mut stream);
    let text = String::from_utf8(head).expect("head utf8");
    assert!(
        text.starts_with("HTTP/1.0 200 OK\r\n"),
        "stream Begin uses HTTP/1.0: {text}"
    );
    assert!(text.to_ascii_lowercase().contains("connection: close"));
    assert!(!text.to_ascii_lowercase().contains("transfer-encoding"));
    assert!(!text.to_ascii_lowercase().contains("content-length"));
    let mut body = [0; 4];
    stream.read_exact(&mut body).expect("body");
    assert_eq!(&body, b"body");
    let mut tail = [0; 1];
    assert_eq!(stream.read(&mut tail).expect("close-delimited eof"), 0);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_date_present_and_shaped_one_shot_stream_and_reuse() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"done");
    date_shape(&first);
    get(&mut stream, "/work");
    let (missing, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(missing.token, 404, b"Not Found", b"nope"),
        RespondOutcome::Ok
    );
    let not_found = read_response(&mut stream);
    assert_eq!(not_found.status, 404);
    assert_eq!(not_found.body, b"nope");
    date_shape(&not_found);
    get(&mut stream, "/work");
    let (begun, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(begun.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, begun.token);
    assert_eq!(host.end_stream(begun.token), RespondOutcome::Ok);
    let head = read_raw_head(&mut stream);
    stream_head_exact(&head, "HTTP/1.1 200 OK");
    let mut terminal = [0; 5];
    stream.read_exact(&mut terminal).expect("terminal");
    assert_eq!(&terminal, b"0\r\n\r\n", "bare end carries no trailer block");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_cancel_closes_without_response() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(host.cancel(event.token), CancelOutcome::Ok);
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Err(_) => {}
        Ok(_) => panic!("cancel must send no response bytes"),
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.cancelled, 1, "counts: {counts:?}");
}

#[test]
fn managed_duplicate_respond_and_stale_cancel_fail_closed() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (first, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(first.token, 200, b"OK", b"one"),
        RespondOutcome::Ok
    );
    assert_eq!(
        host.respond(first.token, 200, b"OK", b"one"),
        RespondOutcome::Duplicate
    );
    assert_eq!(host.cancel(first.token), CancelOutcome::Stale);
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"one");
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(first.token, 200, b"OK", b"hijack"),
        RespondOutcome::Duplicate
    );
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"two"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"two");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_poll_reports_required_capacities() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let mut events = vec![empty_slot(); 1];
    let mut paths = vec![0; 4096];
    let report = host.poll(2_000_000_000, &mut events, &mut [], &mut paths);
    assert_eq!(report.copied, 0);
    assert_eq!(report.method_required, 3);
    let (event, method, path) = poll_request(&host);
    assert_eq!(method, b"GET");
    assert_eq!(path, b"/work");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.body, b"done");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_parked_timeout_releases_slot() {
    let server = tight_managed_server(100_000_000, 5_000_000_000);
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let mut byte = [0xFF; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        other => panic!("expected EOF after parked timeout, got {other:?}"),
    }
    assert!(spin_until(|| host.parked_count() == 0), "park expires");
    let (stale, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(stale.token, 200, b"OK", b"late"),
        RespondOutcome::Gone
    );

    let mut again = connect(&server);
    get(&mut again, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"reused"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut again);
    assert_eq!(response.body, b"reused");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.parked_timeout, 1, "counts: {counts:?}");
}

#[test]
fn managed_queue_full_fails_closed_server_healthy() {
    let config = ServerConfig {
        shutdown_grace_ns: 300_000_000,
        ..ServerConfig::default()
    };
    let server = managed_server(config);
    let host = server.host();
    let mut clients = Vec::new();
    for _ in 0..65 {
        let mut stream = connect(&server);
        get(&mut stream, "/work");
        stream.set_nonblocking(true).expect("nonblocking");
        clients.push(stream);
    }
    let mut parked = 0;
    // NOTE: no polling until the queue saturates: draining events early would free room
    // and let the 65th head park, so the test waits for the fail-closed attempt first.
    assert!(
        spin_until(|| host.counters().queue_full == 1),
        "one park attempt fails closed while full"
    );
    let deadline = Instant::now() + DEADLINE;
    while parked < 64 && Instant::now() < deadline {
        let (events, _, _, _) = poll_batch(&host, 100_000_000, 8);
        parked += events
            .iter()
            .filter(|event| event.kind == HostEventKind::Request)
            .count();
        std::thread::yield_now();
    }
    assert_eq!(parked, 64, "queue holds exactly one depth");
    let (_, _, _, empty) = poll_batch(&host, 100_000_000, 8);
    assert_eq!(empty.copied, 0, "no 65th event escapes the full queue");
    let closed = spin_until(|| {
        let mut closed = 0;
        for stream in clients.iter_mut() {
            if is_closed(stream) {
                closed += 1;
            }
        }
        closed == 1 && host.counters().queue_full == 1
    });
    assert!(closed, "one connection fails closed with queue-full");
    let mut closed_count = 0;
    let mut pending_count = 0;
    for stream in clients.iter_mut() {
        match stream.read(&mut [0; 1]) {
            Ok(0) => closed_count += 1,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => pending_count += 1,
            Err(_) => closed_count += 1,
            Ok(_) => panic!("parked connection must send no bytes"),
        }
    }
    assert_eq!(closed_count, 1, "exactly one connection closed");
    assert_eq!(pending_count, 64, "the rest stay parked with no bytes");
    let mut healthy = connect(&server);
    get(&mut healthy, "/hello");
    let response = read_response(&mut healthy);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.queue_full, 1, "counts: {counts:?}");
}

#[test]
fn managed_shutdown_emits_one_event_per_worker() {
    let config = ServerConfig {
        worker_count: 2,
        shutdown_grace_ns: 300_000_000,
        ..ServerConfig::default()
    };
    let server = managed_server(config);
    let host = server.host();
    server.stop_accepting();
    let mut shutdowns = 0;
    let deadline = Instant::now() + DEADLINE;
    while shutdowns < 2 && Instant::now() < deadline {
        let (events, _, _, _) = poll_batch(&host, 100_000_000, 8);
        shutdowns += events
            .iter()
            .filter(|event| event.kind == HostEventKind::Shutdown)
            .count();
    }
    assert_eq!(shutdowns, 2, "one shutdown event per worker");
    let _ = server.join().expect("join server");
}

#[test]
fn managed_respond_after_disconnect_is_gone() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    drop(stream);
    assert!(
        spin_until(|| host.parked_count() == 0),
        "peer close unparks"
    );
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone
    );
    assert_eq!(host.counters().responds_gone, 1);
    let mut again = connect(&server);
    get(&mut again, "/work");
    let (fresh, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(fresh.token, 200, b"OK", b"live"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut again);
    assert_eq!(response.body, b"live");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_oversized_target_answers_414_with_close() {
    let server = default_managed_server();
    let mut stream = connect(&server);
    let target = format!("/work?x={}", "a".repeat(3000));
    let request =
        format!("GET {target} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .expect("write long target");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 414);
    assert_eq!(response.body, b"URI Too Long");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    let mut tail = Vec::new();
    stream.read_to_end(&mut tail).expect("read close tail");
    assert!(tail.is_empty(), "tail: {tail:?}");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn tomb_overflow_evicts_oldest_and_counts_it() {
    const CANCELS: usize = 70;
    const DEPTH: usize = 64;
    let server = default_managed_server();
    let host = server.host();
    for _ in 0..CANCELS {
        let mut stream = connect(&server);
        get(&mut stream, "/work");
        let (event, _, _) = poll_request(&host);
        assert_eq!(host.cancel(event.token), CancelOutcome::Ok);
        expect_eof(&mut stream);
    }
    assert!(
        spin_until(|| host.parked_count() == 0),
        "all cancels unparked"
    );
    // NOTE: 70 cancelled parks over one worker exceed the 64-deep tomb queue, so the
    // oldest 6 evict; the counter pins the loss instead of leaving it silent.
    assert_eq!(host.counters().tombs_evicted, (CANCELS - DEPTH) as u64);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}
