use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::net::SocketAddr;
use std::time::Duration;
use veylor_http2::{DATA_FLAG_END_STREAM, ErrorCode, FrameType, SETTINGS_ACK};
use veylor_runtime::ConnectionTimeouts;
use veylor_server::{H2_BODY_CAP, MAX_STATIC_BODY_BYTES, RespondOutcome, Server, ServerConfig};

#[test]
fn h2_static_response_waits_for_stream_window_update() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    write_frame(&mut stream, FrameType::Settings, 0, 0, &[0, 4, 0, 0, 0, 0]);
    let (header, _) = read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_ne!(header.flags & SETTINGS_ACK, 0);
    send_headers(&mut stream, 1, b"GET", b"/hello", true);

    let (head, ended) = read_response_headers(&mut stream, 1);
    assert_eq!(head.status, 200);
    assert_eq!(head.content_length, 5);
    assert!(!ended);

    send_window_update(&mut stream, 1, 5);
    assert_eq!(read_response_body(&mut stream, 1), b"hello");

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
}

#[test]
fn h2_stalled_fixed_response_resets_only_its_stream_after_write_timeout() {
    let server_address = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        timeouts: ConnectionTimeouts {
            response_write_ns: 50_000_000,
            ..ConnectionTimeouts::production()
        },
        ..ServerConfig::default()
    };
    let mut server = Server::bind(server_address, config).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/body",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register response");
    server
        .register_route(b"GET", b"/empty", 200, b"OK", b"", MAX_STATIC_BODY_BYTES)
        .expect("register empty response");
    let server = server.start().expect("start server");
    let mut stream = h2_connect(&server);
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    handshake(&mut stream);

    write_frame(&mut stream, FrameType::Settings, 0, 0, &[0, 4, 0, 0, 0, 0]);
    let (settings, _) = read_frame(&mut stream);
    assert_eq!(settings.frame_type, FrameType::Settings);
    assert_ne!(settings.flags & SETTINGS_ACK, 0);

    send_headers(&mut stream, 1, b"GET", b"/body", true);
    let (response, ended) = read_response_headers(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert!(!ended);

    let (stream_id, code) = read_rst(&mut stream);
    assert_eq!(stream_id, 1);
    assert_eq!(code, ErrorCode::Cancel);

    send_headers(&mut stream, 3, b"GET", b"/empty", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert!(sibling.body.is_empty());

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
}

#[test]
fn h2_fixed_responses_resume_after_connection_window_update() {
    let mut server = Server::bind(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        ServerConfig {
            h2c_port: Some(0),
            ..ServerConfig::default()
        },
    )
    .expect("bind loopback");
    let body = [b'x'; 4096];
    server
        .register_route(b"GET", b"/body", 200, b"OK", &body, MAX_STATIC_BODY_BYTES)
        .expect("register body");
    let server = server.start().expect("start server");
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    for id in (1..=29).step_by(2) {
        send_headers(&mut stream, id, b"GET", b"/body", true);
        let response = read_response(&mut stream, id);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, body);
    }

    send_headers(&mut stream, 31, b"GET", b"/body", true);
    let (sixteenth, ended) = read_response_headers(&mut stream, 31);
    assert_eq!(sixteenth.status, 200);
    assert_eq!(sixteenth.content_length, body.len() as u64);
    assert!(!ended);

    let (header, first_payload) = read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(header.stream_id, 31);
    assert_eq!(header.flags & DATA_FLAG_END_STREAM, 0);
    assert_eq!(first_payload.len(), 4095);
    assert!(first_payload.iter().all(|byte| *byte == b'x'));

    send_headers(&mut stream, 33, b"GET", b"/body", true);
    let (seventeenth, ended) = read_response_headers(&mut stream, 33);
    assert_eq!(seventeenth.status, 200);
    assert!(!ended);

    send_window_update(&mut stream, 0, 8192);
    send_window_update(&mut stream, 31, 1);
    send_window_update(&mut stream, 33, 4096);

    let bodies = read_response_bodies(&mut stream, &[31, 33]);
    assert_eq!(bodies[&31].as_slice(), b"x");
    assert_eq!(bodies[&33].as_slice(), &body);

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 17, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
}

#[test]
fn h2_staged_responses_on_siblings_resume_independently() {
    let server = managed_h2_body_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    write_frame(&mut stream, FrameType::Settings, 0, 0, &[0, 4, 0, 0, 0, 0]);
    let (header, _) = read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_ne!(header.flags & SETTINGS_ACK, 0);

    send_headers_len(&mut stream, 1, b"POST", b"/upload", Some(4), false);
    write_frame(
        &mut stream,
        FrameType::Data,
        DATA_FLAG_END_STREAM,
        1,
        b"data",
    );
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Ok
    );

    send_headers_len(&mut stream, 3, b"POST", b"/upload", Some(4), false);
    write_frame(
        &mut stream,
        FrameType::Data,
        DATA_FLAG_END_STREAM,
        3,
        b"data",
    );
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Ok
    );

    let (first, first_end) = read_response_headers(&mut stream, 1);
    let (second, second_end) = read_response_headers(&mut stream, 3);
    assert_eq!(first.status, 200);
    assert_eq!(second.status, 200);
    assert_eq!(first.content_length, 4);
    assert_eq!(second.content_length, 4);
    assert!(!first_end);
    assert!(!second_end);

    send_window_update(&mut stream, 3, 4);
    assert_eq!(read_response_body(&mut stream, 3), b"late");

    send_window_update(&mut stream, 1, 4);
    assert_eq!(read_response_body(&mut stream, 1), b"late");

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
}

#[test]
fn h2_managed_stream_write_survives_a_zero_window_until_credit_arrives() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    write_frame(&mut stream, FrameType::Settings, 0, 0, &[0, 4, 0, 0, 0, 0]);
    let (settings, _) = read_frame(&mut stream);
    assert_eq!(settings.frame_type, FrameType::Settings);
    assert_ne!(settings.flags & SETTINGS_ACK, 0);

    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(
        host.write_stream(event.token, b"retained"),
        RespondOutcome::Ok
    );

    send_window_update(&mut stream, 1, 8);
    spin_until(|| host.is_writable(event.token));

    assert_eq!(
        host.end_stream_with_trailers(event.token, &[]),
        RespondOutcome::Ok
    );
    spin_until(|| !host.is_writable(event.token));

    let (response, trailers) = read_response_with_trailers(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"retained");
    assert!(trailers.is_empty());

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
}

// NOTE: RFC 9113 section 6.9 connection accounting (every flow-controlled octet counts even
// in error) with section 5.4.1 teardown: two max-body uploads total 65536 octets against the
// 65535-octet connection window, so the second upload trips a connection FLOW_CONTROL_ERROR.
#[test]
fn h2_adversarial_conn_window_exceeded_goes_away() {
    let server = managed_h2_body_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    let first: Vec<u8> = (0..H2_BODY_CAP as u32)
        .map(|value| (value % 251) as u8)
        .collect();
    post_upload(&mut stream, 1, &first);
    let second: Vec<u8> = (0..H2_BODY_CAP as u32)
        .map(|value| (value * 3 % 251) as u8)
        .collect();
    post_upload(&mut stream, 3, &second);
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(code, ErrorCode::FlowControlError);
    assert_eq!(last, 3);
    read_eof(&mut stream);
    stop_join(server);
}

// NOTE: RFC 9113 section 6.9.1 (stream window past 2^31-1 RSTs FLOW_CONTROL_ERROR): the parked
// upload holds stream 1 open with a 65535-octet send window, so one maximal increment trips
// only that stream while the sibling serves on the surviving connection.
#[test]
fn h2_adversarial_window_update_overflow_stream_resets() {
    let server = managed_h2_body_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers_len(&mut stream, 1, b"POST", b"/upload", Some(4), false);
    write_frame(
        &mut stream,
        FrameType::WindowUpdate,
        0,
        1,
        &[0x7F, 0xFF, 0xFF, 0xFF],
    );
    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 1);
    assert_eq!(code, ErrorCode::FlowControlError);
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

// NOTE: RFC 9113 section 6.9.1 (connection window past 2^31-1 GOAWAYs FLOW_CONTROL_ERROR):
// one maximal increment on stream 0 tears the connection down with no half-served state.
#[test]
fn h2_adversarial_window_update_overflow_conn_goes_away() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    write_frame(
        &mut stream,
        FrameType::WindowUpdate,
        0,
        0,
        &[0x7F, 0xFF, 0xFF, 0xFF],
    );
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(code, ErrorCode::FlowControlError);
    assert_eq!(last, 0);
    read_eof(&mut stream);
    stop_join(server);
}

// NOTE: RFC 9113 section 5.1 idle (any frame but HEADERS/PRIORITY is a PROTOCOL_ERROR
// connection error): WINDOW_UPDATE on never-opened stream 9 GOAWAYs with last 0, proving no
// stream was half-created by the violation.
#[test]
fn h2_adversarial_window_update_idle_goes_away() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    write_frame(&mut stream, FrameType::WindowUpdate, 0, 9, &[0, 0, 0, 100]);
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(code, ErrorCode::ProtocolError);
    assert_eq!(last, 0);
    read_eof(&mut stream);
    stop_join(server);
}

// NOTE: RFC 9113 section 6.9 (WINDOW_UPDATE on closed MUST NOT be an error): the grant for
// the answered stream is silently absorbed, the sibling serves, and graceful shutdown still
// reports NoError with the right last-stream id.
#[test]
fn h2_adversarial_window_update_closed_ignored() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/hello", true);
    let first = read_response(&mut stream, 1);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    write_frame(&mut stream, FrameType::WindowUpdate, 0, 1, &[0, 0, 0, 100]);
    send_headers(&mut stream, 3, b"GET", b"/empty", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert!(sibling.body.is_empty());
    server.stop_accepting();
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(code, ErrorCode::NoError);
    assert_eq!(last, 3);
    read_eof(&mut stream);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 2);
    assert_eq!(counts.shutdown, 1);
}
