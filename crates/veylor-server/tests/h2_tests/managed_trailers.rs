use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::net::SocketAddr;
use std::time::Instant;
use veylor_http2::{ErrorCode, FrameType, decode_rst_stream_payload};
use veylor_runtime::ConnectionToken;
use veylor_server::{
    CancelOutcome, EventSlot, HostEventKind, HostHandle, MAX_STATIC_BODY_BYTES, RespondOutcome,
    Server, ServerConfig,
};

#[test]
fn h2_managed_request_trailers_stage_and_pull() {
    // NOTE: one-predicate allowlist enforcement: /upload registers no trailer names, so the
    // x-checksum/x-count set RSTs with ProtocolError (policy violation, not RefusedStream)
    // while the conn survives; the sibling proves multiplexing continues.
    let server = managed_h2_body_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    post_upload_open(&mut stream, 1, b"data");
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    assert_eq!(event.token.stream_id, 1);
    send_trailers(
        &mut stream,
        1,
        &[(b"x-checksum", b"abc"), (b"x-count", b"2")],
    );
    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 1);
    assert_eq!(code, ErrorCode::ProtocolError);
    post_upload(&mut stream, 3, b"again");
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(event.token.stream_id, 3);
    assert_eq!(pull_all(&host, event.token), b"again");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"second"),
        RespondOutcome::Ok
    );
    let (_, response) = read_response_with_grants(&mut stream, 3);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"second");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_prohibited_trailer_rsts_stream_sibling_fine() {
    // NOTE: stream ids ascend here: a lower id opened after a higher one is implicitly
    // closed per RFC 9113 section 5.1.1, which would RST the sibling for protocol reasons.
    let server = managed_h2_body_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    post_upload_open(&mut stream, 1, b"data");
    poll_h2_request(&host);
    send_trailers(&mut stream, 1, &[(b"connection", b"close")]);
    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 1);
    assert_eq!(code, ErrorCode::ProtocolError);
    post_upload_open(&mut stream, 3, b"data");
    poll_h2_request(&host);
    send_trailers(&mut stream, 3, &[(b":method", b"GET")]);
    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 3);
    assert_eq!(code, ErrorCode::ProtocolError);
    send_headers(&mut stream, 5, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 5);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_refused, 2, "counts: {counts:?}");
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_stream_end_with_trailers_exact_bytes() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(
        host.write_stream(event.token, b"chunk-one"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(
        host.write_stream(event.token, b"chunk-two"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(
        host.end_stream_with_trailers(
            event.token,
            &[(b"x-resp-trailer", b"trailer-value"), (b"x-count", b"2")]
        ),
        RespondOutcome::Ok
    );
    let (response, trailers) = read_response_with_trailers(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"chunk-onechunk-two");
    assert_eq!(
        trailers,
        vec![
            (b"x-resp-trailer".to_vec(), b"trailer-value".to_vec()),
            (b"x-count".to_vec(), b"2".to_vec()),
        ]
    );
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

#[test]
fn h2_managed_write_after_trailers_fail_closed() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(
        host.end_stream_with_trailers(event.token, &[(b"x-end", b"1")]),
        RespondOutcome::Ok
    );
    let (response, trailers) = read_response_with_trailers(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    assert_eq!(trailers, vec![(b"x-end".to_vec(), b"1".to_vec())]);
    assert_eq!(
        host.write_stream(event.token, b"late"),
        RespondOutcome::Gone
    );
    assert_eq!(host.end_stream(event.token), RespondOutcome::Gone);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Gone
    );
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

fn wait_h2_writable(host: &HostHandle, token: ConnectionToken) -> EventSlot {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let mut events = [host_slot(); 8];
        let mut methods = [0; 64];
        let mut paths = [0; 4096];
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::Writable && event.token == token {
                return *event;
            }
        }
        assert!(Instant::now() < deadline, "writable signal stalls");
    }
}

// NOTE: H2 WRITABLE emission pin: the applied head and each consumed write re-arm exactly one
// coalesced signal, and each signal admits one write. poll_h2_request keeps skipping advisory
// signals; only this waiter keeps them, so batch shape never matters.
#[test]
fn h2_managed_stream_writable_event_per_release() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    let first = wait_h2_writable(&host, event.token);
    assert_eq!(first.kind, HostEventKind::Writable);
    assert_eq!(first.token, event.token);
    assert_eq!(first.route_id, 0);
    assert_eq!((first.method_len, first.path_len), (0, 0));
    assert_eq!((first.method_offset, first.path_offset), (0, 0));
    assert_eq!(
        host.write_stream(event.token, b"chunk-one"),
        RespondOutcome::Ok
    );
    let second = wait_h2_writable(&host, event.token);
    assert_eq!(second.token, event.token);
    assert_eq!(second.route_id, 0);
    assert_eq!(
        host.write_stream(event.token, b"chunk-two"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (response, trailers) = read_response_with_trailers(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"chunk-onechunk-two");
    assert!(trailers.is_empty());
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

// NOTE: H2 BODY_AVAILABLE pin: staged DATA carries ready bytes with the end flag in the reused
// length fields like H1, so the host observes the full window before pulling.
#[test]
fn h2_managed_body_available_signals_ready_and_end() {
    let server = managed_h2_body_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    post_upload(&mut stream, 1, b"hello");
    let deadline = Instant::now() + DEADLINE;
    let mut kept = Vec::new();
    let request = loop {
        let mut events = [host_slot(); 8];
        let mut methods = [0; 64];
        let mut paths = [0; 4096];
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        let mut found = None;
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::Request {
                found = Some(*event);
            } else if event.kind == HostEventKind::BodyAvailable {
                kept.push(*event);
            }
        }
        if let Some(found) = found {
            break found;
        }
        assert!(Instant::now() < deadline, "timed out waiting for request");
    };
    assert_eq!(request.token.stream_id, 1);
    let deadline = Instant::now() + DEADLINE;
    let signal = loop {
        if let Some(done) = kept.iter().find(|event| {
            event.kind == HostEventKind::BodyAvailable
                && event.token == request.token
                && event.path_len == 1
        }) {
            break *done;
        }
        let mut events = [host_slot(); 8];
        let mut methods = [0; 64];
        let mut paths = [0; 4096];
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        kept.extend(events.iter().take(report.copied).filter(|event| {
            event.kind == HostEventKind::BodyAvailable && event.token == request.token
        }));
        assert!(Instant::now() < deadline, "terminal body signal stalls");
    };
    assert_eq!(signal.token, request.token);
    assert_eq!(signal.route_id, 0);
    assert_eq!(signal.method_len, 5);
    assert_eq!((signal.method_offset, signal.path_offset), (0, 0));
    assert_eq!(pull_all(&host, request.token), b"hello");
    assert_eq!(
        host.respond(request.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let (grants, response) = read_response_with_grants(&mut stream, 1);
    assert_eq!(grants, vec![(0, 5), (1, 5)]);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_stream_trailers_isolated_per_stream() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    send_headers(&mut stream, 3, b"GET", b"/work", true);
    let requests = poll_h2_requests(&host, 2);
    assert_eq!(requests[0].0.token.stream_id, 1);
    assert_eq!(requests[1].0.token.stream_id, 3);
    let first = requests[0].0.token;
    let second = requests[1].0.token;
    assert_eq!(host.begin_stream(first, 200, b"OK"), RespondOutcome::Ok);
    assert_eq!(host.begin_stream(second, 200, b"OK"), RespondOutcome::Ok);
    spin_until(|| host.is_writable(first));
    assert_eq!(host.write_stream(first, b"body-one"), RespondOutcome::Ok);
    spin_until(|| host.is_writable(second));
    assert_eq!(host.write_stream(second, b"body-three"), RespondOutcome::Ok);
    spin_until(|| host.is_writable(first));
    assert_eq!(
        host.end_stream_with_trailers(first, &[(b"x-one", b"1")]),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(second));
    assert_eq!(
        host.end_stream_with_trailers(second, &[(b"x-three", b"3")]),
        RespondOutcome::Ok
    );
    spin_until(|| host.parked_count() == 0);
    let [(one, one_trailers), (three, three_trailers)] = read_trailer_pair(&mut stream, [1, 3]);
    assert_eq!(one.status, 200);
    assert_eq!(one.body, b"body-one");
    assert_eq!(one_trailers, vec![(b"x-one".to_vec(), b"1".to_vec())]);
    assert_eq!(three.status, 200);
    assert_eq!(three.body, b"body-three");
    assert_eq!(three_trailers, vec![(b"x-three".to_vec(), b"3".to_vec())]);
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

#[test]
fn h2_managed_stream_trailer_without_begin_rejected() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.end_stream_with_trailers(event.token, &[(b"x-early", b"1")]),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(
        host.write_stream(event.token, b"late-body"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (response, trailers) = read_response_with_trailers(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"late-body");
    assert!(trailers.is_empty());
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_stream_trailer_after_terminal_gone() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(
        host.end_stream_with_trailers(event.token, &[(b"x-end", b"1")]),
        RespondOutcome::Ok
    );
    let (response, trailers) = read_response_with_trailers(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    assert_eq!(trailers, vec![(b"x-end".to_vec(), b"1".to_vec())]);
    assert_eq!(
        host.end_stream_with_trailers(event.token, &[(b"x-late", b"2")]),
        RespondOutcome::Gone
    );
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_stream_trailer_after_cancel_stale() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(host.cancel(event.token), CancelOutcome::Ok);
    spin_until(|| host.parked_count() == 0);
    assert_eq!(
        host.end_stream_with_trailers(event.token, &[(b"x-late", b"2")]),
        RespondOutcome::Stale
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Stale
    );
    let (header, payload) = read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::RstStream);
    assert_eq!(header.stream_id, 1);
    assert_eq!(
        decode_rst_stream_payload(header, &payload).expect("decode rst"),
        ErrorCode::Cancel
    );
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
}

// NOTE: HEAD-park Begin parity pin: a host-staged Begin on a HEAD park RSTs InternalError on
// its stream only (H1 fails the connection closed instead); the connection stays up for
// siblings and the tomb reads Gone for the exact token.
#[test]
fn h2_managed_head_park_begin_aborts_stream_conn_survives() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"HEAD", b"/work", true);
    let (event, method, _) = poll_h2_request(&host);
    assert_eq!(method, b"HEAD");
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    spin_until(|| host.parked_count() == 0);
    let (header, payload) = read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::RstStream);
    assert_eq!(header.stream_id, 1);
    assert_eq!(
        decode_rst_stream_payload(header, &payload).expect("decode rst"),
        ErrorCode::InternalError
    );
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Gone
    );
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
}

// NOTE: HEAD-park trailers parity pin: EndWithTrailers stages only on a begun stream, so a
// never-begun HEAD park rejects Invalid at stage; the staged Begin then aborts with the same
// stream-only RST InternalError and no HEADERS/DATA precedes it on that stream.
#[test]
fn h2_managed_head_park_end_with_trailers_aborts() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"HEAD", b"/work", true);
    let (event, method, _) = poll_h2_request(&host);
    assert_eq!(method, b"HEAD");
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(
        host.end_stream_with_trailers(event.token, &[(b"x-head-trailer", b"1")]),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    spin_until(|| host.parked_count() == 0);
    let deadline = Instant::now() + DEADLINE;
    loop {
        assert!(Instant::now() < deadline, "no RST for aborted HEAD park");
        let (header, payload) = read_frame(&mut stream);
        if header.stream_id == 0 {
            continue;
        }
        assert_eq!(header.stream_id, 1);
        match header.frame_type {
            FrameType::RstStream => {
                assert_eq!(
                    decode_rst_stream_payload(header, &payload).expect("decode rst"),
                    ErrorCode::InternalError
                );
                break;
            }
            FrameType::Headers | FrameType::Continuation | FrameType::Data => {
                panic!("aborted HEAD park emitted {:?}", header.frame_type);
            }
            _ => {}
        }
    }
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Gone
    );
    assert_eq!(
        host.end_stream_with_trailers(event.token, &[(b"x-head-trailer", b"1")]),
        RespondOutcome::Gone
    );
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
}

// NOTE: former gap pin h2c_managed_with_trailers_route_unprojected_documents_gap, now positive
// coverage: allowlist-registered managed routes mirror to H2C and the park resolves the list;
// an unlisted trailer RSTs with ProtocolError while the conn survives, and a listed trailer
// stages for a whole-set pull.
#[test]
fn h2c_managed_with_trailers_mirror_serves_and_enforces() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_managed_with_trailers(b"POST", b"/t", MAX_STATIC_BODY_BYTES, &[b"x-sum"])
        .expect("register trailer route");
    let server = server.start().expect("start server");
    let host = server.host();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"POST", b"/t", true);
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/t");
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"mirrored"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"mirrored");
    send_headers_len(&mut client, 3, b"POST", b"/t", Some(4), false);
    write_frame(&mut client, FrameType::Data, 0, 3, b"data");
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(event.token.stream_id, 3);
    send_trailers(&mut client, 3, &[(b"x-evil", b"1")]);
    let (id, code) = read_rst(&mut client);
    assert_eq!(id, 3);
    assert_eq!(code, ErrorCode::ProtocolError);
    send_headers_len(&mut client, 5, b"POST", b"/t", Some(4), false);
    write_frame(&mut client, FrameType::Data, 0, 5, b"data");
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(event.token.stream_id, 5);
    send_trailers(&mut client, 5, &[(b"x-sum", b"9f")]);
    assert_eq!(pull_all(&host, event.token), b"data");
    let (names, values) = pull_trailers_ready(&host, event.token);
    assert_eq!(names, b"x-sum");
    assert_eq!(values, b"9f");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"staged"),
        RespondOutcome::Ok
    );
    let (_, response) = read_response_with_grants(&mut client, 5);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"staged");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
}
