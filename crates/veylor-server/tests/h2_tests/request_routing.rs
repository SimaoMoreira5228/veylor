use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::io::Write;
use std::net::SocketAddr;
use veylor_http2::{
    DATA_FLAG_END_STREAM, ErrorCode, FrameType, HEADERS_FLAG_END_STREAM, decode_goaway_payload,
    decode_rst_stream_payload,
};
use veylor_server::{MAX_STATIC_BODY_BYTES, Server, ServerConfig};

#[test]
fn get_static_route_returns_200_with_exact_body() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_length, 5);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let (last, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    assert_eq!(last, 1);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 1);
}

#[test]
fn head_falls_back_to_get_without_body() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"HEAD", b"/hello", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_length, 5);
    assert!(response.body.is_empty());
    server.stop_accepting();
    let (_, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 1);
}

#[test]
fn unknown_path_returns_404() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/missing", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 404);
    assert_eq!(response.content_length, 9);
    assert_eq!(response.body, b"Not Found");
    server.stop_accepting();
    let (_, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 1);
}

// NOTE: over-long :path 414 pin (H1 oversized_request_target_answers_414 mirror): a 9 KiB
// :path decodes fine through the 16 KiB header-block cap but answers 414 with Date first,
// never the 404 the exact-match miss used to give; the conn survives and a sibling still
// serves behind a clean drain. One over-long :path per connection: decoded literals share
// the 16 KiB cumulative decoder scratch, so the HEAD sibling-probe below runs on its own
// connection rather than behind this block.
#[test]
fn long_path_answers_414_and_connection_survives() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut long = vec![b'a'; 9000];
    long.insert(0, b'/');
    send_long_headers(&mut client, 1, b"GET", &long, true);
    let denied = read_response(&mut client, 1);
    assert_eq!(denied.status, 414);
    assert_eq!(denied.content_length, 12);
    assert_eq!(denied.body, b"URI Too Long");
    date_shape(&denied);
    assert_date_first(&denied.order, true);
    send_headers(&mut client, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut client, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    server.stop_accepting();
    let (last, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    assert_eq!(last, 3);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 2);
    assert_eq!(counts.shutdown, 1);
}

// NOTE: HEAD over-long :path stays headers-only with the 414 length (the 404 HEAD shape):
// END_STREAM on HEADERS, no DATA, Date first — on a fresh connection for the scratch
// budget reason above.
#[test]
fn long_path_head_answers_414_headers_only() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut long = vec![b'a'; 9000];
    long.insert(0, b'/');
    send_long_headers(&mut client, 1, b"HEAD", &long, true);
    let head = read_response(&mut client, 1);
    assert_eq!(head.status, 414);
    assert_eq!(head.content_length, 12);
    assert!(head.body.is_empty());
    date_shape(&head);
    assert_date_first(&head.order, true);
    server.stop_accepting();
    let (last, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    assert_eq!(last, 1);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 1);
}

// NOTE: target-limit boundary pin: the limit itself (8 KiB, one number with the H1
// request-line cap) still routes, while one byte more answers 414. Each block fits the
// 16 KiB header-block cap and a single HEADERS frame, but the two blocks together exceed
// the cumulative decoder scratch — so the probes run on separate connections, and the
// limit is reachable live with no other HPACK-cap caveat.
#[test]
fn target_limit_boundary_routes_then_414s() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    let mut at_limit = vec![b'a'; 8191];
    at_limit.insert(0, b'/');
    assert_eq!(at_limit.len(), 8192);
    server
        .register_route(
            b"GET",
            &at_limit,
            200,
            b"OK",
            b"edge",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register edge");
    let server = server.start().expect("start server");
    let mut first = h2_connect(&server);
    handshake(&mut first);
    send_long_headers(&mut first, 1, b"GET", &at_limit, true);
    let edge = read_response(&mut first, 1);
    assert_eq!(edge.status, 200);
    assert_eq!(edge.body, b"edge");
    let mut over = vec![b'a'; 8192];
    over.insert(0, b'/');
    assert_eq!(over.len(), 8193);
    let mut second = h2_connect(&server);
    handshake(&mut second);
    send_long_headers(&mut second, 1, b"GET", &over, true);
    let denied = read_response(&mut second, 1);
    assert_eq!(denied.status, 414);
    assert_eq!(denied.content_length, 12);
    assert_eq!(denied.body, b"URI Too Long");
    date_shape(&denied);
    assert_date_first(&denied.order, true);
    server.stop_accepting();
    for client in [&mut first, &mut second] {
        let (_, code) = read_goaway(client);
        assert_eq!(code, ErrorCode::NoError);
        read_eof(client);
    }
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 2);
    assert_eq!(counts.shutdown, 2);
}

// NOTE: open-stream 414 refusal mirror of request_body_is_refused_and_connection_closes:
// HEADERS without END_STREAM still answers 414 first (HEADERS-open, no DATA), then RSTs
// RefusedStream and closes exactly like the 404 tail — never a GOAWAY for this verdict.
#[test]
fn long_path_with_open_stream_rsts_and_closes() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut long = vec![b'a'; 9000];
    long.insert(0, b'/');
    send_long_headers(&mut client, 1, b"GET", &long, false);
    let (header, payload) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, 1);
    assert_eq!(header.flags & HEADERS_FLAG_END_STREAM, 0);
    let response = decode_response(&payload);
    assert_eq!(response.status, 414);
    assert_eq!(response.content_length, 12);
    date_shape(&response);
    let (header, payload) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::RstStream);
    assert_eq!(
        decode_rst_stream_payload(header, &payload).expect("decode rst"),
        ErrorCode::RefusedStream
    );
    read_eof(&mut client);
    let counts = stop_join(server);
    assert_eq!(counts.h2_refused, 1);
    assert_eq!(counts.completed, 1);
}

#[test]
fn bad_magic_goes_away_and_closes() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    client
        .write_all(b"NOT-H2C-MAGIC-NOT-H2C-MAGIC!!!")
        .expect("write garbage");
    let (header, _) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::Settings);
    let (header, payload) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::GoAway);
    let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
    assert_eq!(goaway.error_code, ErrorCode::ProtocolError);
    read_eof(&mut client);
    let counts = stop_join(server);
    assert_eq!(counts.protocol_error, 1);
    assert_eq!(counts.h2_served, 0);
}

#[test]
fn request_body_is_refused_and_connection_closes() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", false);
    write_frame(
        &mut client,
        FrameType::Data,
        DATA_FLAG_END_STREAM,
        1,
        b"junk",
    );
    let (header, payload) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.flags & HEADERS_FLAG_END_STREAM, 0);
    let response = decode_response(&payload);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_length, 5);
    let (header, payload) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::RstStream);
    assert_eq!(
        decode_rst_stream_payload(header, &payload).expect("decode rst"),
        ErrorCode::RefusedStream
    );
    read_eof(&mut client);
    let counts = stop_join(server);
    assert_eq!(counts.h2_refused, 1);
    assert_eq!(counts.completed, 1);
}

#[test]
fn second_stream_reuses_connection() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let first = read_response(&mut client, 1);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    send_headers(&mut client, 3, b"GET", b"/empty", true);
    let second = read_response(&mut client, 3);
    assert_eq!(second.status, 200);
    assert_eq!(second.content_length, 0);
    assert!(second.body.is_empty());
    server.stop_accepting();
    let (last, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    assert_eq!(last, 3);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 2);
    assert_eq!(counts.shutdown, 1);
}
