use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM,
    DEFAULT_MAX_FRAME_SIZE, ErrorCode, FRAME_HEADER_LEN, FrameHeader, FrameType, H2Role,
    HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackFieldToEncode,
    HpackHeader, HpackRepresentation, HpackTable, SETTINGS_ACK, StreamSlot, StreamTable,
    decode_frame_header, decode_goaway_payload, decode_header_block, decode_rst_stream_payload,
    encode_frame_header, encode_header_block,
};
use veylor_runtime::ConnectionToken;
use veylor_server::{
    ACK_SLOTS, CancelOutcome, CloseCounts, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES,
    ENCODE_ENTRIES, EventSlot, FrozenRoute, FrozenRoutesView, H2_BODY_CAP, H2Conn, H2HostCtx,
    HostEventKind, HostHandle, HostShared, INPUT_STAGING_CAP, MAX_ROUTES, MAX_STATIC_BODY_BYTES,
    RespondOutcome, RouteBuildError, RunningServer, Server, ServerConfig,
    h2::{HEADER_BLOCK_CAP, HEADER_SLOTS, STREAM_SLOTS},
};

const DEADLINE: Duration = Duration::from_secs(5);

static ROUTES: [FrozenRoute<'static>; 2] = [
    FrozenRoute {
        method: b"GET",
        path: b"/hello",
        status: 200,
        body: b"hello",
        managed: true,
    },
    FrozenRoute {
        method: b"GET",
        path: b"/empty",
        status: 200,
        body: b"",
        managed: false,
    },
];

fn h2_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register hello");
    server
        .register_route(b"GET", b"/empty", 200, b"OK", b"", MAX_STATIC_BODY_BYTES)
        .expect("register empty");
    server.start().expect("start server")
}

fn h2_connect(server: &RunningServer) -> TcpStream {
    let addr = server.h2c_addr().expect("h2c enabled");
    let stream = TcpStream::connect(addr).expect("connect h2c loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
}

fn h1_connect(server: &RunningServer) -> TcpStream {
    let stream = TcpStream::connect(server.addr()).expect("connect h1 loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
}

fn stop_join(server: RunningServer) -> CloseCounts {
    server.stop_accepting();
    server.join().expect("join server")
}

fn field(name: &'static [u8], value: &'static [u8]) -> HpackFieldToEncode<'static> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

fn write_frame(stream: &mut TcpStream, frame_type: FrameType, flags: u8, id: u32, payload: &[u8]) {
    let header = FrameHeader {
        payload_length: payload.len() as u32,
        frame_type,
        flags,
        stream_id: id,
    };
    let mut encoded = [0; FRAME_HEADER_LEN];
    encode_frame_header(header, &mut encoded, DEFAULT_MAX_FRAME_SIZE).expect("encode header");
    stream.write_all(&encoded).expect("write header");
    stream.write_all(payload).expect("write payload");
}

fn read_frame(stream: &mut TcpStream) -> (FrameHeader, Vec<u8>) {
    let mut encoded = [0; FRAME_HEADER_LEN];
    stream.read_exact(&mut encoded).expect("read header");
    let header = decode_frame_header(&encoded, DEFAULT_MAX_FRAME_SIZE).expect("decode header");
    let mut payload = vec![0; header.payload_length as usize];
    stream.read_exact(&mut payload).expect("read payload");
    (header, payload)
}

fn handshake(stream: &mut TcpStream) {
    stream
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write magic");
    write_frame(stream, FrameType::Settings, 0, 0, &[]);
    let (header, _) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.stream_id, 0);
    write_frame(stream, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);
}

fn send_headers(
    stream: &mut TcpStream,
    id: u32,
    method: &'static [u8],
    path: &'static [u8],
    end: bool,
) {
    let fields = [
        field(b":method", method),
        field(b":scheme", b"http"),
        field(b":path", path),
        field(b":authority", b"localhost"),
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    let mut flags = HEADERS_FLAG_END_HEADERS;
    if end {
        flags |= HEADERS_FLAG_END_STREAM;
    }
    write_frame(stream, FrameType::Headers, flags, id, &block[..len]);
}

struct Response {
    status: u16,
    content_length: u64,
    body: Vec<u8>,
}

fn read_response(stream: &mut TcpStream, id: u32) -> Response {
    let mut block = Vec::new();
    let mut headers_ended_stream = false;
    loop {
        let (header, payload) = read_frame(stream);
        assert_eq!(header.stream_id, id);
        match header.frame_type {
            FrameType::Headers => {
                headers_ended_stream = header.flags & HEADERS_FLAG_END_STREAM != 0;
                block.extend_from_slice(&payload);
                if header.flags & HEADERS_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            FrameType::Continuation => {
                block.extend_from_slice(&payload);
                if header.flags & CONTINUATION_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            found => panic!("expected HEADERS, got {found:?}"),
        }
    }
    let mut response = decode_response(&block);
    let mut body = Vec::new();
    if !headers_ended_stream {
        loop {
            let (header, payload) = read_frame(stream);
            assert_eq!(header.stream_id, id);
            assert_eq!(header.frame_type, FrameType::Data);
            body.extend_from_slice(&payload);
            if header.flags & DATA_FLAG_END_STREAM != 0 {
                break;
            }
        }
    }
    response.body = body;
    response
}

fn decode_response(block: &[u8]) -> Response {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (count, _) = decode_header_block(block, &mut decoder, &mut decoded).expect("decode block");
    let mut status = 0;
    let mut content_length = 0;
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        } else if name == b"content-length" {
            content_length = std::str::from_utf8(value)
                .expect("length utf8")
                .parse()
                .expect("length int");
        }
    }
    assert_ne!(status, 0, "response carries :status");
    Response {
        status,
        content_length,
        body: Vec::new(),
    }
}

fn read_eof(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        other => panic!("expected EOF, got {other:?}"),
    }
}

// NOTE: a force close races in-flight bytes, so the kernel may deliver RST
// instead of FIN; either proves the connection is dead.
fn read_closed(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Err(_) => {}
        Ok(_) => panic!("expected close, got data"),
    }
}

fn read_goaway(stream: &mut TcpStream) -> (u32, ErrorCode) {
    let (header, payload) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::GoAway);
    let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
    (goaway.last_stream_id, goaway.error_code)
}

fn read_h1_body(stream: &mut TcpStream) -> (u16, Vec<u8>) {
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
        assert!(head.len() < 65536, "head overflow");
    }
    let text = String::from_utf8(head).expect("head utf8");
    let mut lines = text.split("\r\n");
    let status = lines
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status int");
    let mut content_length = 0;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').expect("header colon");
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().expect("content length int");
        }
    }
    let mut body = vec![0; content_length];
    stream.read_exact(&mut body).expect("read body");
    (status, body)
}

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

// NOTE: policy-adoption golden: :status resolves to the static-table indexed form while
// content-length stays a 5-byte literal, so the repeat response costs the same 5 bytes.
#[test]
fn h2_policy_indexed_status_wire_golden() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let (header, first) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(first, vec![0x88, 0x0f, 0x0d, 0x01, b'5']);
    let response = decode_response(&first);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_length, 5);
    let (header, body) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(body, b"hello");
    send_headers(&mut client, 3, b"GET", b"/empty", true);
    let (header, second) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(second, vec![0x88, 0x0f, 0x0d, 0x01, b'0']);
    let response = decode_response(&second);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_length, 0);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 2);
    assert_eq!(counts.shutdown, 1);
}

// NOTE: explicit kinds bypass the policy: the same :status value encodes indexed under
// Default but stays a 5-byte literal under WithoutIndexing (the pre-adoption wire form).
#[test]
fn h2_explicit_literal_bypasses_policy() {
    let legacy = [field(b":status", b"200"), field(b"content-length", b"5")];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&legacy, &mut encoder, &mut block).expect("encode legacy");
    assert_eq!(len, 9);
    assert_eq!(
        &block[..len],
        &[0x08, 0x03, b'2', b'0', b'0', 0x0f, 0x0d, 0x01, b'5']
    );
    let policy = [
        HpackFieldToEncode {
            name: b":status",
            value: b"200",
            representation: HpackRepresentation::Default,
            huffman_name: false,
            huffman_value: false,
        },
        HpackFieldToEncode {
            name: b"content-length",
            value: b"5",
            representation: HpackRepresentation::Default,
            huffman_name: false,
            huffman_value: false,
        },
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&policy, &mut encoder, &mut block).expect("encode policy");
    assert_eq!(len, 5);
    assert_eq!(&block[..len], &[0x88, 0x0f, 0x0d, 0x01, b'5']);
}

#[test]
fn graceful_drain_sends_goaway() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
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
fn concurrent_h1_and_h2_traffic_on_both_ports() {
    let server = h2_server();
    let mut h1 = h1_connect(&server);
    let mut h2 = h2_connect(&server);
    h1.write_all(b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write h1 request");
    handshake(&mut h2);
    send_headers(&mut h2, 1, b"GET", b"/hello", true);
    let (status, body) = read_h1_body(&mut h1);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    let response = read_response(&mut h2, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let (_, code) = read_goaway(&mut h2);
    assert_eq!(code, ErrorCode::NoError);
    read_eof(&mut h2);
    read_eof(&mut h1);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 2);
}

#[test]
fn forced_stop_closes_promptly() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    server.stop_force();
    let counts = server.join().expect("join server");
    read_closed(&mut client);
    assert_eq!(counts.shutdown, 1);
    assert_eq!(counts.h2_served, 0);
}

#[test]
fn multi_worker_h2c_fans_out_and_joins() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        worker_count: 2,
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register hello");
    let server = server.start().expect("start server");
    let mut clients = Vec::new();
    for id in [1, 3, 5, 7] {
        let mut client = h2_connect(&server);
        handshake(&mut client);
        send_headers(&mut client, id, b"GET", b"/hello", true);
        let response = read_response(&mut client, id);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
        clients.push(client);
    }
    server.stop_accepting();
    for client in clients.iter_mut() {
        let (_, code) = read_goaway(client);
        assert_eq!(code, ErrorCode::NoError);
        read_eof(client);
    }
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 4);
    assert_eq!(counts.shutdown, 4);
}

#[test]
fn slot_reuse_across_ten_sequential_conns() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    config.timeouts.keep_alive_idle_ns = 100_000_000;
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register hello");
    let server = server.start().expect("start server");
    // NOTE: 10 sequential conns over 8 slots forces reuse; each generation is
    // synchronized by the server-sent GOAWAY+EOF (idle timeout), so the slot is
    // provably free server-side before the next connect. No sleeps: blocking
    // reads wait out the 100 ms idle bound.
    for _ in 0..10 {
        let mut client = h2_connect(&server);
        handshake(&mut client);
        send_headers(&mut client, 1, b"GET", b"/hello", true);
        let response = read_response(&mut client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
        let (_, code) = read_goaway(&mut client);
        assert_eq!(code, ErrorCode::NoError);
        read_eof(&mut client);
    }
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 10);
    assert_eq!(counts.timeout, 10);
}

#[test]
fn step_driver_rejects_bad_magic_direct() {
    let mut staging = [0u8; INPUT_STAGING_CAP];
    let mut slots = [StreamSlot::EMPTY; STREAM_SLOTS];
    let mut header_block = [0u8; HEADER_BLOCK_CAP];
    let mut streams = StreamTable::new(
        &mut slots,
        H2Role::Server,
        &mut header_block,
        HEADER_BLOCK_CAP as u32,
    );
    let mut decoder_entries = [HpackEntrySlot::EMPTY; DECODE_ENTRIES];
    let mut decoder_scratch = [0u8; DECODE_BYTES];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; ENCODE_ENTRIES];
    let mut encoder_scratch = [0u8; ENCODE_BYTES];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut ack_limits = [0u32; ACK_SLOTS];
    let mut headers = [HpackHeader::EMPTY; HEADER_SLOTS];
    let mut conn = H2Conn::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut staging,
        &mut ack_limits,
        &mut headers,
    );
    conn.push_input(b"NOT-H2C-MAGIC-NOT-H2C-MAGIC!!!")
        .expect("stage garbage");
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    let mut output = [0u8; 1024];
    let outcome = conn.step(&mut output, &view);
    assert!(outcome.goaway);
    assert!(outcome.closed);
    assert!(outcome.written > 0);
    assert!(conn.is_closed());
}

#[test]
fn route_view_rejects_bad_input() {
    let bad_status = FrozenRoute {
        method: b"GET",
        path: b"/a",
        status: 204,
        body: b"",
        managed: false,
    };
    assert_eq!(
        FrozenRoutesView::new(&[bad_status]).unwrap_err(),
        RouteBuildError::Invalid
    );
    let bad_method = FrozenRoute {
        method: b"GE T",
        path: b"/a",
        status: 200,
        body: b"x",
        managed: false,
    };
    assert_eq!(
        FrozenRoutesView::new(&[bad_method]).unwrap_err(),
        RouteBuildError::Invalid
    );
    let big_body = FrozenRoute {
        method: b"GET",
        path: b"/a",
        status: 200,
        body: &[0; 4097],
        managed: false,
    };
    assert_eq!(
        FrozenRoutesView::new(&[big_body]).unwrap_err(),
        RouteBuildError::BodyTooLarge
    );
    let first = FrozenRoute {
        method: b"GET",
        path: b"/a",
        status: 200,
        body: b"a",
        managed: false,
    };
    let dupe = FrozenRoute {
        method: b"GET",
        path: b"/a",
        status: 200,
        body: b"b",
        managed: false,
    };
    assert_eq!(
        FrozenRoutesView::new(&[first, dupe]).unwrap_err(),
        RouteBuildError::Duplicate
    );
    let mut paths = Vec::new();
    for index in 0..MAX_ROUTES + 1 {
        paths.push(format!("/r{index}").into_bytes());
    }
    let full: Vec<FrozenRoute<'_>> = paths
        .iter()
        .map(|path| FrozenRoute {
            method: b"GET",
            path,
            status: 200,
            body: b"x",
            managed: false,
        })
        .collect();
    assert_eq!(
        FrozenRoutesView::new(&full).unwrap_err(),
        RouteBuildError::Full
    );
}

// NOTE: host-dispatch rig for the H2 stream mapping: a loopback socketpair carries real H2C
// bytes, the test thread pumps the transport-neutral driver on the server side and drives the
// host handle directly (park/poll/respond/cancel), then applies staged work through the H2
// writers. No worker thread, no sleeps: every blocking read consumes already-sent bytes and
// every client read follows a server flush. Streams park into stream-keyed subslots, so
// siblings multiplex while one stream awaits the host.

fn host_base() -> ConnectionToken {
    ConnectionToken {
        worker_id: 0,
        slot: 0,
        request_seq: 0,
        stream_id: 0,
        worker_generation: 1,
        generation: 1,
    }
}

fn socketpair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind rig");
    let addr = listener.local_addr().expect("rig addr");
    let client = TcpStream::connect(addr).expect("rig connect");
    let (server, _) = listener.accept().expect("rig accept");
    client
        .set_read_timeout(Some(DEADLINE))
        .expect("client timeout");
    client
        .set_write_timeout(Some(DEADLINE))
        .expect("client timeout");
    server
        .set_read_timeout(Some(DEADLINE))
        .expect("server timeout");
    server
        .set_write_timeout(Some(DEADLINE))
        .expect("server timeout");
    (client, server)
}

fn with_rig(body: impl FnOnce(&mut H2Conn<'_>, &HostShared, &mut TcpStream, &mut TcpStream)) {
    let mut staging = [0u8; INPUT_STAGING_CAP];
    let mut slots = [StreamSlot::EMPTY; STREAM_SLOTS];
    let mut header_block = [0u8; HEADER_BLOCK_CAP];
    let mut streams = StreamTable::new(
        &mut slots,
        H2Role::Server,
        &mut header_block,
        HEADER_BLOCK_CAP as u32,
    );
    let mut decoder_entries = [HpackEntrySlot::EMPTY; DECODE_ENTRIES];
    let mut decoder_scratch = [0u8; DECODE_BYTES];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; ENCODE_ENTRIES];
    let mut encoder_scratch = [0u8; ENCODE_BYTES];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut ack_limits = [0u32; ACK_SLOTS];
    let mut headers = [HpackHeader::EMPTY; HEADER_SLOTS];
    let mut conn = H2Conn::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut staging,
        &mut ack_limits,
        &mut headers,
    );
    let shared = HostShared::new(1);
    let (mut client, mut server) = socketpair();
    body(&mut conn, &shared, &mut client, &mut server)
}

fn server_pump(
    conn: &mut H2Conn<'_>,
    server: &mut TcpStream,
    routes: &FrozenRoutesView<'_>,
    ctx: Option<H2HostCtx<'_>>,
) -> veylor_server::StepOutcome {
    let mut chunk = [0u8; 4096];
    let read = server.read(&mut chunk).expect("rig read");
    assert!(read > 0, "rig eof");
    conn.push_input(&chunk[..read]).expect("rig stage");
    let mut output = [0u8; 16384];
    let outcome = match ctx {
        Some(ctx) => conn.step_host(&mut output, routes, ctx),
        None => conn.step(&mut output, routes),
    };
    server
        .write_all(&output[..outcome.written])
        .expect("rig flush");
    if outcome.closed {
        server
            .shutdown(std::net::Shutdown::Both)
            .expect("rig shutdown");
    }
    outcome
}

fn pump_until(
    conn: &mut H2Conn<'_>,
    server: &mut TcpStream,
    routes: &FrozenRoutesView<'_>,
    ctx: Option<H2HostCtx<'_>>,
    mut done: impl FnMut(&veylor_server::StepOutcome) -> bool,
) -> veylor_server::StepOutcome {
    for _ in 0..64 {
        let outcome = server_pump(conn, server, routes, ctx);
        if done(&outcome) {
            return outcome;
        }
    }
    panic!("rig pump stalled");
}

fn rig_handshake(
    conn: &mut H2Conn<'_>,
    client: &mut TcpStream,
    server: &mut TcpStream,
    routes: &FrozenRoutesView<'_>,
) {
    client
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write magic");
    write_frame(client, FrameType::Settings, 0, 0, &[]);
    pump_until(conn, server, routes, None, |outcome| outcome.written > 0);
    let (header, _) = read_frame(client);
    assert_eq!(header.frame_type, FrameType::Settings);
    write_frame(client, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = read_frame(client);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);
    for _ in 0..64 {
        server_pump(conn, server, routes, None);
        if conn.handshake_complete() {
            break;
        }
    }
    assert!(conn.handshake_complete());
}

fn host_slot() -> EventSlot {
    EventSlot {
        kind: HostEventKind::Shutdown,
        token: host_base(),
        route_id: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    }
}

fn poll_request_event(shared: &HostShared) -> (EventSlot, Vec<u8>, Vec<u8>) {
    let mut events = [host_slot(); 8];
    let mut methods = [0; 64];
    let mut paths = [0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    let event = events[0];
    assert_eq!(event.kind, HostEventKind::Request);
    let method = methods[event.method_offset..event.method_offset + event.method_len].to_vec();
    let path = paths[event.path_offset..event.path_offset + event.path_len].to_vec();
    (event, method, path)
}

#[test]
fn host_park_poll_respond_round_trip() {
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/hello", true);
        let outcome = pump_until(conn, server, &view, Some(ctx), |outcome| outcome.parked > 0);
        assert_eq!(outcome.written, 0);
        assert_eq!(outcome.parked, 1);
        let (event, method, path) = poll_request_event(shared);
        assert_eq!(method, b"GET");
        assert_eq!(path, b"/hello");
        assert_eq!(event.token.stream_id, 1);
        assert_eq!(event.route_id, 0);
        assert_eq!(
            shared.stage_respond(event.token, 200, b"OK", b"host-hello"),
            RespondOutcome::Ok
        );
        let staged = shared.take_staged(0);
        assert_eq!(staged.len(), 1);
        assert!(!staged[0].cancel);
        let mut output = [0u8; 16384];
        let applied = conn.apply_staged(&mut output, &staged[0]);
        assert_eq!(applied.served, 1);
        server
            .write_all(&output[..applied.written])
            .expect("flush staged");
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"host-hello");
    });
}

#[test]
fn host_cancel_mid_park_rsts_sibling_unaffected() {
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/hello", true);
        send_headers(client, 3, b"GET", b"/missing", true);
        pump_until(conn, server, &view, Some(ctx), |outcome| {
            outcome.parked > 0 && outcome.served > 0
        });
        let (event, _, _) = poll_request_event(shared);
        assert_eq!(event.token.stream_id, 1);
        assert_eq!(shared.stage_cancel(event.token), CancelOutcome::Ok);
        let staged = shared.take_staged(0);
        assert_eq!(staged.len(), 1);
        assert!(staged[0].cancel);
        let mut output = [0u8; 16384];
        let applied = conn.apply_staged(&mut output, &staged[0]);
        server
            .write_all(&output[..applied.written])
            .expect("flush cancel");
        let sibling = read_response(client, 3);
        assert_eq!(sibling.status, 404);
        assert_eq!(sibling.body, b"Not Found");
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::RstStream);
        assert_eq!(header.stream_id, 1);
        assert_eq!(
            decode_rst_stream_payload(header, &payload).expect("decode rst"),
            ErrorCode::Cancel
        );
    });
}

#[test]
fn host_stale_replay_and_duplicate_fail_closed() {
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/hello", true);
        pump_until(conn, server, &view, Some(ctx), |outcome| outcome.parked > 0);
        let (event, _, _) = poll_request_event(shared);
        let stale = ConnectionToken {
            request_seq: event.token.request_seq.wrapping_add(99),
            ..event.token
        };
        assert_eq!(
            shared.stage_respond(stale, 200, b"OK", b"hijack"),
            RespondOutcome::Stale
        );
        assert_eq!(
            shared.stage_respond(event.token, 200, b"OK", b"live"),
            RespondOutcome::Ok
        );
        let staged = shared.take_staged(0);
        assert_eq!(staged.len(), 1);
        let mut output = [0u8; 16384];
        let applied = conn.apply_staged(&mut output, &staged[0]);
        server
            .write_all(&output[..applied.written])
            .expect("flush staged");
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"live");
        assert_eq!(
            shared.stage_respond(event.token, 200, b"OK", b"again"),
            RespondOutcome::Duplicate
        );
    });
}

#[test]
fn host_unknown_path_stays_sync_404() {
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/missing", true);
        let outcome = pump_until(conn, server, &view, Some(ctx), |outcome| outcome.served > 0);
        assert_eq!(outcome.parked, 0);
        assert_eq!(shared.parked_count(), 0);
        let response = read_response(client, 1);
        assert_eq!(response.status, 404);
        assert_eq!(response.body, b"Not Found");
    });
}

#[test]
fn host_request_body_refused_without_staging() {
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/hello", false);
        write_frame(client, FrameType::Data, DATA_FLAG_END_STREAM, 1, b"junk");
        let outcome = pump_until(conn, server, &view, Some(ctx), |outcome| outcome.closed);
        assert_eq!(outcome.parked, 0);
        assert_eq!(outcome.refused, 1);
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::Headers);
        assert_eq!(header.flags & HEADERS_FLAG_END_STREAM, 0);
        assert_eq!(decode_response(&payload).status, 200);
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::RstStream);
        assert_eq!(
            decode_rst_stream_payload(header, &payload).expect("decode rst"),
            ErrorCode::RefusedStream
        );
        read_eof(client);
    });
}
// NOTE: managed-turn host tests: loopback H2C through the worker turn (the turn drives the
// pumps; no test-owned pumps on these paths). A managed route parks into the host mapping,
// the test thread polls/responds/cancels via HostHandle, and the turn applies staged work
// back onto the streams. Bounded event waits only (100ms polls under a 5s deadline), no sleeps.

fn managed_h2_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register hello");
    server
        .register_managed(b"GET", b"/work", MAX_STATIC_BODY_BYTES)
        .expect("register managed");
    server.start().expect("start server")
}

fn poll_h2_request(host: &HostHandle) -> (EventSlot, Vec<u8>, Vec<u8>) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let mut events = [host_slot(); 8];
        let mut methods = [0; 64];
        let mut paths = [0; 4096];
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::Request {
                let method =
                    methods[event.method_offset..event.method_offset + event.method_len].to_vec();
                let path = paths[event.path_offset..event.path_offset + event.path_len].to_vec();
                return (*event, method, path);
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting for request");
    }
}

fn poll_h2_requests(host: &HostHandle, count: usize) -> Vec<(EventSlot, Vec<u8>, Vec<u8>)> {
    let deadline = Instant::now() + DEADLINE;
    let mut out = Vec::new();
    while out.len() < count {
        let mut events = [host_slot(); 8];
        let mut methods = [0; 64];
        let mut paths = [0; 4096];
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::Request {
                let method =
                    methods[event.method_offset..event.method_offset + event.method_len].to_vec();
                let path = paths[event.path_offset..event.path_offset + event.path_len].to_vec();
                out.push((*event, method, path));
                if out.len() == count {
                    break;
                }
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting for requests");
    }
    out
}

#[test]
fn h2_managed_park_respond_serves_host_body() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"GET");
    assert_eq!(path, b"/work");
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"managed-work"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"managed-work");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_concurrent_parks_share_conn() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    send_headers(&mut stream, 3, b"GET", b"/work", true);
    let requests = poll_h2_requests(&host, 2);
    let (first, _, _) = &requests[0];
    let (second, _, _) = &requests[1];
    assert_eq!(first.token.stream_id, 1);
    assert_eq!(second.token.stream_id, 3);
    assert_eq!(host.parked_count(), 2);
    assert_eq!(
        host.respond(first.token, 200, b"OK", b"one"),
        RespondOutcome::Ok
    );
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"two"),
        RespondOutcome::Ok
    );
    let one = read_response(&mut stream, 1);
    assert_eq!(one.status, 200);
    assert_eq!(one.body, b"one");
    let three = read_response(&mut stream, 3);
    assert_eq!(three.status, 200);
    assert_eq!(three.body, b"two");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

#[test]
fn h2_managed_cancel_rsts_only_its_stream() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(host.cancel(event.token), CancelOutcome::Ok);
    let (header, payload) = read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::RstStream);
    assert_eq!(header.stream_id, 1);
    assert_eq!(
        decode_rst_stream_payload(header, &payload).expect("decode rst"),
        ErrorCode::Cancel
    );
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_teardown_unparks_parked_stream() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(host.parked_count(), 1);
    // NOTE: shut_slot unparks before the fd dies, so the client-observed EOF below implies
    // the host mapping already released the stream slot: no strand window, no sleep. The
    // half-close (Write only) keeps the read side open so EOF genuinely proves server close.
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown client");
    read_eof(&mut stream);
    assert_eq!(host.parked_count(), 0);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone
    );
    stop_join(server);
}
// NOTE: managed H2 request-body tests: fixed-length POST bodies stage per parked stream,
// the host pulls them with pull-observed completion, and drained bytes reopen both flow
// windows (grant-on-consume). H2 has no chunked coding: END_STREAM on DATA frames the
// body, so unknown-length bodies stay refused and trailers are stream errors.

fn managed_h2_body_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register hello");
    server
        .register_managed(b"POST", b"/upload", MAX_STATIC_BODY_BYTES)
        .expect("register upload");
    server.start().expect("start server")
}

fn send_headers_len(
    stream: &mut TcpStream,
    id: u32,
    method: &'static [u8],
    path: &'static [u8],
    length: Option<u64>,
    end: bool,
) {
    let length_text = length.map(|value| value.to_string());
    let mut fields = vec![
        field(b":method", method),
        field(b":scheme", b"http"),
        field(b":path", path),
        field(b":authority", b"localhost"),
    ];
    if let Some(text) = length_text.as_ref() {
        fields.push(HpackFieldToEncode {
            name: b"content-length",
            value: text.as_bytes(),
            representation: HpackRepresentation::WithoutIndexing,
            huffman_name: false,
            huffman_value: false,
        });
    }
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    let mut flags = HEADERS_FLAG_END_HEADERS;
    if end {
        flags |= HEADERS_FLAG_END_STREAM;
    }
    write_frame(stream, FrameType::Headers, flags, id, &block[..len]);
}

fn send_trailers(stream: &mut TcpStream, id: u32, fields: &[(&'static [u8], &'static [u8])]) {
    let encoded: Vec<HpackFieldToEncode<'_>> = fields
        .iter()
        .map(|(name, value)| field(name, value))
        .collect();
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&encoded, &mut encoder, &mut block).expect("encode block");
    let flags = HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM;
    write_frame(stream, FrameType::Headers, flags, id, &block[..len]);
}

fn pull_all(host: &HostHandle, token: ConnectionToken) -> Vec<u8> {
    let deadline = Instant::now() + DEADLINE;
    let mut body = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        let (outcome, copied, done) = host.pull_body(token, &mut chunk);
        assert_eq!(outcome, RespondOutcome::Ok);
        body.extend_from_slice(&chunk[..copied]);
        if done {
            return body;
        }
        assert!(Instant::now() < deadline, "staged body never completed");
    }
}

fn read_response_with_grants(stream: &mut TcpStream, id: u32) -> (Vec<(u32, u32)>, Response) {
    let mut grants = Vec::new();
    let mut block = Vec::new();
    let mut headers_ended_stream = false;
    loop {
        let (header, payload) = read_frame(stream);
        match header.frame_type {
            FrameType::WindowUpdate => {
                assert_eq!(payload.len(), 4);
                let increment =
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
                        & 0x7FFF_FFFF;
                grants.push((header.stream_id, increment));
            }
            FrameType::Headers => {
                assert_eq!(header.stream_id, id);
                headers_ended_stream = header.flags & HEADERS_FLAG_END_STREAM != 0;
                block.extend_from_slice(&payload);
                if header.flags & HEADERS_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            FrameType::Continuation => {
                block.extend_from_slice(&payload);
                if header.flags & CONTINUATION_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            found => panic!("expected HEADERS/WINDOW_UPDATE, got {found:?}"),
        }
    }
    let mut response = decode_response(&block);
    let mut body = Vec::new();
    if !headers_ended_stream {
        loop {
            let (header, payload) = read_frame(stream);
            assert_eq!(header.stream_id, id);
            assert_eq!(header.frame_type, FrameType::Data);
            body.extend_from_slice(&payload);
            if header.flags & DATA_FLAG_END_STREAM != 0 {
                break;
            }
        }
    }
    response.body = body;
    (grants, response)
}

fn read_rst(stream: &mut TcpStream) -> (u32, ErrorCode) {
    let (header, payload) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::RstStream);
    let code = decode_rst_stream_payload(header, &payload).expect("decode rst");
    (header.stream_id, code)
}

fn post_upload(stream: &mut TcpStream, id: u32, body: &[u8]) {
    send_headers_len(
        stream,
        id,
        b"POST",
        b"/upload",
        Some(body.len() as u64),
        false,
    );
    let mut offset = 0;
    while offset < body.len() {
        let take = (body.len() - offset).min(4096);
        let last = offset + take == body.len();
        let flags = if last { DATA_FLAG_END_STREAM } else { 0 };
        write_frame(
            stream,
            FrameType::Data,
            flags,
            id,
            &body[offset..offset + take],
        );
        offset += take;
    }
}

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
fn h2_managed_over_cap_body_refused_stream_survives() {
    let server = managed_h2_body_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers_len(
        &mut stream,
        1,
        b"POST",
        b"/upload",
        Some(H2_BODY_CAP + 1),
        false,
    );
    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 1);
    assert_eq!(code, ErrorCode::RefusedStream);
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
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
