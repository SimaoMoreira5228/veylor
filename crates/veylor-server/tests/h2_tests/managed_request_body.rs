use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::net::SocketAddr;
use std::time::Duration;
use veylor_http2::{DATA_FLAG_END_STREAM, ErrorCode, FrameType};
use veylor_server::{H2_BODY_CAP, MAX_STATIC_BODY_BYTES, RespondOutcome, Server, ServerConfig};

#[test]
fn h2_managed_post_stages_pullable_body() {
    let server = managed_h2_body_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    let sent: Vec<u8> = (0..3000u32).map(|value| (value % 251) as u8).collect();
    post_upload(&mut stream, 1, &sent);
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    let body = pull_all(&host, event.token);
    assert_eq!(body, sent);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let (grants, response) = read_response_with_grants(&mut stream, 1);
    assert_eq!(grants, vec![(0, 3000), (1, 3000)]);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    let second: Vec<u8> = (0..17u32).map(|value| (value * 7 % 251) as u8).collect();
    post_upload(&mut stream, 3, &second);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(pull_all(&host, event.token), second);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let (grants, response) = read_response_with_grants(&mut stream, 3);
    assert_eq!(grants, vec![(0, 17), (3, 17)]);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"again");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
}

#[test]
fn h2_managed_window_reopens_after_drain() {
    let server = managed_h2_body_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    let first: Vec<u8> = (0..H2_BODY_CAP as u32)
        .map(|value| (value % 251) as u8)
        .collect();
    post_upload(&mut stream, 1, &first);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(pull_all(&host, event.token), first);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"one"),
        RespondOutcome::Ok
    );
    let (grants, response) = read_response_with_grants(&mut stream, 1);
    assert_eq!(
        grants,
        vec![(0, H2_BODY_CAP as u32), (1, H2_BODY_CAP as u32)]
    );
    assert_eq!(response.status, 200);
    // NOTE: both max bodies total 65536 bytes against the 65535-byte connection window:
    // the second body only arrives because the first drain re-granted its window share.
    let second: Vec<u8> = (0..H2_BODY_CAP as u32)
        .map(|value| (value * 3 % 251) as u8)
        .collect();
    post_upload(&mut stream, 3, &second);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(pull_all(&host, event.token), second);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"two"),
        RespondOutcome::Ok
    );
    let (grants, response) = read_response_with_grants(&mut stream, 3);
    assert_eq!(
        grants,
        vec![(0, H2_BODY_CAP as u32), (3, H2_BODY_CAP as u32)]
    );
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"two");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

#[test]
fn h2_managed_trailers_refused_stream_survives() {
    let server = managed_h2_body_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers_len(&mut stream, 1, b"POST", b"/upload", Some(4), false);
    let (event, _, _) = poll_h2_request(&host);
    write_frame(
        &mut stream,
        FrameType::Data,
        DATA_FLAG_END_STREAM,
        1,
        b"data",
    );
    assert_eq!(pull_all(&host, event.token), b"data");
    // NOTE: trailers ride a later batch here on purpose: a same-batch trailer closes the
    // stream at feed time, so the HEADERS event finds it closed and never parks (a refused
    // request stays invisible to the host, like any other refusal). Post-park trailers hit
    // the live park and refuse at the stream level instead. The RST read synchronizes the
    // respond past trailer processing: answering first would serve a full 200 instead.
    send_trailers(&mut stream, 1, &[(b"x-checksum", b"yes")]);
    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 1);
    assert_eq!(code, ErrorCode::StreamClosed);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Ok
    );
    let (header, payload) = read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::WindowUpdate);
    assert_eq!(header.stream_id, 0);
    assert_eq!(payload.len(), 4);
    let increment = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(increment, 4);
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    stop_join(server);
}

#[test]
fn h2_managed_nonfinal_trailer_headers_reset_protocol_error() {
    // NOTE: a trailer block without END_STREAM is malformed framing (RFC 9113 §8.1),
    // not a slow trailer set: the server must RST the stream with PROTOCOL_ERROR
    // instead of staging trailers before content-end. The allowlisted name matters:
    // unlisted names already RST at the allowlist gate, which would mask this path.
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
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    send_headers_len(&mut stream, 1, b"POST", b"/t", Some(4), false);
    write_frame(&mut stream, FrameType::Data, 0, 1, b"data");
    let (_event, _, _) = poll_h2_request(&host);
    send_open_trailers(&mut stream, 1, &[(b"x-sum", b"1")]);
    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 1);
    assert_eq!(code, ErrorCode::ProtocolError);
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 0, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_short_body_reset_stream_survives() {
    let server = managed_h2_body_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers_len(&mut stream, 1, b"POST", b"/upload", Some(10), false);
    write_frame(
        &mut stream,
        FrameType::Data,
        DATA_FLAG_END_STREAM,
        1,
        b"shor",
    );
    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 1);
    assert_eq!(code, ErrorCode::ProtocolError);
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}
