use crate::h1_peer::*;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::time::Instant;
use veylor_runtime::ConnectionToken;
use veylor_server::{
    EventSlot, HostEventKind, MAX_STATIC_BODY_BYTES, RespondOutcome, Server, ServerConfig,
};

#[test]
fn malformed_request_line_closes_with_400() {
    let server = test_server();
    let mut stream = connect(&server);
    stream
        .write_all(b"BAD REQUEST\r\n\r\n")
        .expect("write malformed");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 400);
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn oversized_header_section_closes_with_431() {
    let server = test_server();
    let mut stream = connect(&server);
    let mut request = String::from("GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n");
    for index in 0..120 {
        request.push_str(&format!("X-Pad-{index:03}: abcdefgh\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).expect("write headers");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 431);
    assert_eq!(response.body, b"Request Header Fields Too Large");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn oversized_request_target_answers_414() {
    let server = test_server();
    let mut stream = connect(&server);
    let target = format!("/{}", "a".repeat(9000));
    let request = format!("GET {target} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .expect("write long target");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 414);
    assert_eq!(response.body, b"URI Too Long");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn oversized_request_method_stays_400() {
    let server = test_server();
    let mut stream = connect(&server);
    let method = "A".repeat(9000);
    let request = format!("{method} /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .expect("write long method");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 400);
    assert_eq!(response.body, b"Bad Request");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn oversized_headers_with_bad_expect_answers_431() {
    // NOTE: drift alarm for H1-6 (error precedence), managed half: with both the section cap
    // breached and Expect unsupported, the shared head parser fails on the section overflow
    // first (request_head/parser.rs scans line order; the 101st header trips before the Expect
    // line), so the 431 arm wins over the 417 arm. The conn crate inherits the same decoder
    // verdict and collapses it to ProtocolError (request_read/tests
    // head_error_corpus_matches_managed_status_table pins the mirror row); the 417-vs-431 order
    // itself is decoder truth, not a mapper choice — the parse_exchange arms are disjoint.
    let server = test_server();
    let mut stream = connect(&server);
    let mut request = String::from("GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n");
    for index in 0..120 {
        request.push_str(&format!("X-Pad-{index:03}: abcdefgh\r\n"));
    }
    request.push_str("Expect: magic\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write headers");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 431);
    assert_eq!(response.body, b"Request Header Fields Too Large");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn full_buffer_without_head_answers_400() {
    // NOTE: drift alarm for H1-6 (error precedence), transport-split row: a full INPUT_CAP with
    // no complete head and no newline attributes 400 (no target side to blame 414 on). The
    // host-driven conn crate deliberately diverges here — the host owns its buffers, so the same
    // bytes read NeedMore (request_read/tests head_error_corpus_matches_managed_status_table
    // pins that half). Sharing the attribution helper would drag pump buffer semantics into the
    // host API, so the split is documented, not converged.
    let server = test_server();
    let mut stream = connect(&server);
    stream
        .write_all(&vec![b'A'; 8192])
        .expect("write full buffer");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 400);
    assert_eq!(response.body, b"Bad Request");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn request_body_on_static_route_closes_after_response() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\n\r\nhello";
    stream
        .write_all(request.as_bytes())
        .expect("write body request");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 405, "static POST on GET-only is 405");
    assert_eq!(response.body, b"Method Not Allowed");
    assert_eq!(header(&response, "allow"), "GET");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn small_get_body_drained_then_keep_alive_reusable() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\n\r\nhello";
    stream
        .write_all(request.as_bytes())
        .expect("write GET with body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    get(&mut stream, "/empty");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert!(second.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn post_small_body_drained_then_keep_alive_reusable() {
    let server = test_server();
    let mut stream = connect(&server);
    let request =
        "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 11\r\n\r\nhello world";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn post_large_body_drains_across_pumps_then_reusable() {
    let server = test_server();
    let mut stream = connect(&server);
    let body = vec![b'a'; 20_000];
    let head = format!(
        "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(&body).expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn oversized_body_rejected_without_reading() {
    let server = test_server();
    let mut stream = connect(&server);
    let head = format!(
        "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\n\r\n",
        MAX_STATIC_BODY_BYTES + 1024
    );
    stream.write_all(head.as_bytes()).expect("write head");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 413);
    assert_eq!(response.body, b"Payload Too Large");
    let chunk = vec![b'x'; 1024];
    let _ = stream.write_all(&chunk);
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn chunked_request_rejected_without_reading() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write head");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 413);
    assert_eq!(response.body, b"Payload Too Large");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn expect_continue_oversized_rejected_without_interim() {
    let server = test_server();
    let mut stream = connect(&server);
    let head = format!(
        "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nExpect: 100-continue\r\n\r\n",
        MAX_STATIC_BODY_BYTES + 1024
    );
    stream.write_all(head.as_bytes()).expect("write head");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 413);
    assert_eq!(response.body, b"Payload Too Large");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn expect_continue_withheld_body_gets_interim_then_final() {
    let server = test_server();
    let mut stream = connect(&server);
    let head = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n";
    stream.write_all(head.as_bytes()).expect("write head");
    let interim = read_response(&mut stream);
    assert_eq!(interim.status, 100);
    assert!(interim.body.is_empty());
    stream.write_all(b"hello").expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn expect_continue_pipelined_body_gets_interim_before_final() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\nhello";
    stream.write_all(request.as_bytes()).expect("write POST");
    let interim = read_response(&mut stream);
    assert_eq!(interim.status, 100);
    assert!(interim.body.is_empty());
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn expect_continue_chunked_rejected_without_interim() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write head");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 413);
    assert_eq!(response.body, b"Payload Too Large");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn unsupported_expectation_rejected_with_417_then_close() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nExpect: chocolate\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write head");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 417);
    assert_eq!(response.body, b"Expectation Failed");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn http10_expect_ignored_without_interim() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /submit HTTP/1.0\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\nhello";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    read_eof(&mut stream);
    let mut other = connect(&server);
    let bogus = "POST /submit HTTP/1.0\r\nContent-Length: 5\r\nExpect: chocolate\r\n\r\nhello";
    other.write_all(bogus.as_bytes()).expect("write bogus");
    let second = read_response(&mut other);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"submitted");
    read_eof(&mut other);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 2, "counts: {counts:?}");
}

fn post_head(stream: &mut TcpStream, path: &str, length: usize) {
    let head =
        format!("POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {length}\r\n\r\n");
    stream.write_all(head.as_bytes()).expect("write head");
}

#[test]
fn tight_cap_drains_at_cap_and_rejects_one_over() {
    let server = capped_server();
    let mut stream = connect(&server);
    post_head(&mut stream, "/tight", 16);
    stream.write_all(&[b'a'; 16]).expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"tight");
    post_head(&mut stream, "/tight", 0);
    let reuse = read_response(&mut stream);
    assert_eq!(reuse.status, 200);
    let mut over = connect(&server);
    post_head(&mut over, "/tight", 17);
    let rejected = read_response(&mut over);
    assert_eq!(rejected.status, 413);
    read_closed(&mut over);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn zero_cap_admits_empty_body_and_rejects_one_byte() {
    let server = capped_server();
    let mut stream = connect(&server);
    post_head(&mut stream, "/zero", 0);
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"zero");
    post_head(&mut stream, "/zero", 1);
    let rejected = read_response(&mut stream);
    assert_eq!(rejected.status, 413);
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn uncapped_route_drains_20kib() {
    let server = capped_server();
    let mut stream = connect(&server);
    let body = vec![b'b'; 20_000];
    post_head(&mut stream, "/huge", body.len());
    stream.write_all(&body).expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"huge");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn managed_cap_rejects_over_cap_and_parks_under_cap() {
    let server = capped_server();
    let host = server.host();
    let mut over = connect(&server);
    post_head(&mut over, "/mupload", 17);
    let rejected = read_response(&mut over);
    assert_eq!(rejected.status, 413);
    read_closed(&mut over);
    // NOTE: the 413 above proves the worker consumed the head, so an empty nonblocking
    // poll proves the rejection parked nothing instead of racing a late event.
    let report = host.poll(0, &mut [], &mut [], &mut []);
    assert_eq!(report.copied, 0);
    assert_eq!(report.events_required, 0);
    let mut under = connect(&server);
    post_head(&mut under, "/mupload", 5);
    under.write_all(b"hello").expect("write body");
    let token = ConnectionToken {
        worker_id: 0,
        slot: 0,
        request_seq: 0,
        stream_id: 0,
        worker_generation: 0,
        generation: 0,
    };
    let mut events = [EventSlot {
        kind: HostEventKind::Shutdown,
        token,
        route_id: 0,
        route_token: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    }];
    let mut methods = [0; 64];
    let mut paths = [0; 4096];
    let report = host.poll(5_000_000_000, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, HostEventKind::Request);
    let mut staged = [0; 16];
    // NOTE: the head and body travel in separate segments, so the park event can win the race
    // against the body pump; retry the pull until the staged bytes land or the test times out.
    let pulled = {
        let start = Instant::now();
        loop {
            let pulled = host.pull_body(events[0].token, &mut staged);
            if pulled == (RespondOutcome::Ok, 5, true) || start.elapsed() >= DEADLINE {
                break pulled;
            }
            std::thread::yield_now();
        }
    };
    assert_eq!(pulled, (RespondOutcome::Ok, 5, true));
    assert_eq!(&staged[..5], b"hello");
    assert_eq!(
        host.respond(events[0].token, 200, b"OK", b"m-ok"),
        RespondOutcome::Ok
    );
    let done = read_response(&mut under);
    assert_eq!(done.status, 200);
    assert_eq!(done.body, b"m-ok");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn managed_fixed_body_echo_round_trip_reuses_keep_alive() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut builder = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    builder
        .register_managed(b"POST", b"/echo", MAX_STATIC_BODY_BYTES)
        .expect("register echo");
    let server = builder.start().expect("start server");
    let host = server.host();
    let mut stream = connect(&server);

    for body in [b"hello echo".as_slice(), b"second pass".as_slice()] {
        let head = format!(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).expect("write head");
        stream.write_all(body).expect("write body");
        let token = ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 0,
            generation: 0,
        };
        let mut events = [EventSlot {
            kind: HostEventKind::Shutdown,
            token,
            route_id: 0,
            route_token: 0,
            method_len: 0,
            path_len: 0,
            method_offset: 0,
            path_offset: 0,
        }];
        let mut methods = [0; 64];
        let mut paths = [0; 4096];
        let report = host.poll(5_000_000_000, &mut events, &mut methods, &mut paths);
        assert_eq!(report.copied, 1);
        assert_eq!(events[0].kind, HostEventKind::Request);
        // NOTE: the head and body travel in separate segments, so the park event can win
        // the race against the body pump; retry the pull until the staged bytes complete
        // or the test times out.
        let mut collected = Vec::new();
        let start = Instant::now();
        loop {
            let mut staged = [0; 16];
            let (outcome, n, complete) = host.pull_body(events[0].token, &mut staged);
            assert_eq!(outcome, RespondOutcome::Ok);
            collected.extend_from_slice(&staged[..n]);
            if complete {
                break;
            }
            assert!(start.elapsed() < DEADLINE, "echo body stalls");
            if n == 0 {
                std::thread::yield_now();
            }
        }
        assert_eq!(collected, body);
        assert_eq!(
            host.respond(events[0].token, 200, b"OK", &collected),
            RespondOutcome::Ok
        );
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, body);
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}
