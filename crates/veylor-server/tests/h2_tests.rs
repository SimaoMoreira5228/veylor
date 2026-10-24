use std::ffi::c_void;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Mutex;
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

type TrailerFields = Vec<(Vec<u8>, Vec<u8>)>;
use veylor_server::{
    ACK_SLOTS, CancelOutcome, CloseCounts, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES,
    ENCODE_ENTRIES, EventSlot, FrozenRoute, FrozenRoutesView, H2_BODY_CAP, H2Conn, H2HostCtx,
    HostEventKind, HostHandle, HostShared, INPUT_STAGING_CAP, MAX_ROUTES, MAX_STATIC_BODY_BYTES,
    NativeRequestView, NativeResponseOut, RespondOutcome, RouteBuildError, RunningServer, Server,
    ServerConfig,
    h2::{FrozenNativeRoute, HEADER_BLOCK_CAP, HEADER_SLOTS, STREAM_SLOTS},
};

const DEADLINE: Duration = Duration::from_secs(5);

static ROUTES: [FrozenRoute<'static>; 2] = [
    FrozenRoute {
        method: b"GET",
        path: b"/hello",
        status: 200,
        body: b"hello",
        managed: true,
        file_root: None,
    },
    FrozenRoute {
        method: b"GET",
        path: b"/empty",
        status: 200,
        body: b"",
        managed: false,
        file_root: None,
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

fn field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
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
    send_headers_extra(stream, id, method, path, &[], end);
}

fn send_headers_extra(
    stream: &mut TcpStream,
    id: u32,
    method: &[u8],
    path: &[u8],
    extra: &[(&[u8], &[u8])],
    end: bool,
) {
    let mut fields = vec![
        field(b":method", method),
        field(b":scheme", b"http"),
        field(b":path", path),
        field(b":authority", b"localhost"),
    ];
    for (name, value) in extra {
        fields.push(field(name, value));
    }
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 2048];
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
    content_type: Vec<u8>,
    last_modified: Vec<u8>,
    content_range: Vec<u8>,
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
    decode_with(&mut decoder, block)
}

// NOTE: stateful HPACK decode for connections whose responses use the dynamic table (file
// headers index content-type incrementally): pass one table per connection, since a fresh
// table per response wipes the entries and desyncs after the first insertion.
fn decode_with(decoder: &mut HpackTable<'_>, block: &[u8]) -> Response {
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (count, _) = decode_header_block(block, decoder, &mut decoded).expect("decode block");
    let mut status = 0;
    let mut content_length = 0;
    let mut content_type = Vec::new();
    let mut last_modified = Vec::new();
    let mut content_range = Vec::new();
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
        } else if name == b"content-type" {
            content_type = value.to_vec();
        } else if name == b"last-modified" {
            last_modified = value.to_vec();
        } else if name == b"content-range" {
            content_range = value.to_vec();
        }
    }
    assert_ne!(status, 0, "response carries :status");
    Response {
        status,
        content_length,
        content_type,
        last_modified,
        content_range,
        body: Vec::new(),
    }
}

fn read_response_stateful(
    stream: &mut TcpStream,
    id: u32,
    decoder: &mut HpackTable<'_>,
) -> Response {
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
    let mut response = decode_with(decoder, &block);
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
        file_root: None,
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
        file_root: None,
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
        file_root: None,
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
        file_root: None,
    };
    let dupe = FrozenRoute {
        method: b"GET",
        path: b"/a",
        status: 200,
        body: b"b",
        managed: false,
        file_root: None,
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
            file_root: None,
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

// NOTE: HEAD-park mapping lock: HEAD /work parks through the GET fallback with its method
// intact, and an empty host respond serves HEADERS-only. Streaming Begin/EndWithTrailers on
// this park stays unenforced until the ParkedEntry method seam lands (see serve.rs Begin arm).
#[test]
fn h2_managed_head_park_responds_without_body() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"HEAD", b"/work", true);
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"HEAD");
    assert_eq!(path, b"/work");
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b""),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
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

fn spin_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + DEADLINE;
    while !ready() {
        assert!(Instant::now() < deadline, "spun waiting for host state");
        std::thread::yield_now();
    }
}

fn post_upload_open(stream: &mut TcpStream, id: u32, body: &[u8]) {
    send_headers_len(
        stream,
        id,
        b"POST",
        b"/upload",
        Some(body.len() as u64),
        false,
    );
    write_frame(stream, FrameType::Data, 0, id, body);
}

fn pull_trailers_ready(host: &HostHandle, token: ConnectionToken) -> (Vec<u8>, Vec<u8>) {
    let deadline = Instant::now() + DEADLINE;
    let mut names = vec![0; 256];
    let mut values = vec![0; 1024];
    loop {
        let report = host.pull_trailers(token, &mut names, &mut values);
        match report.outcome {
            veylor_server::TrailerOutcome::Ok => {
                names.truncate(report.names_copied);
                values.truncate(report.values_copied);
                return (names, values);
            }
            veylor_server::TrailerOutcome::NotReady | veylor_server::TrailerOutcome::Stale => {}
            outcome => panic!("unexpected trailer outcome: {outcome:?}"),
        }
        assert!(Instant::now() < deadline, "trailers never staged");
        std::thread::yield_now();
    }
}

fn read_block(stream: &mut TcpStream, id: u32, first: (FrameHeader, Vec<u8>)) -> (Vec<u8>, bool) {
    let (header, mut block) = first;
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, id);
    let mut ended = header.flags & HEADERS_FLAG_END_STREAM != 0;
    let mut flags = header.flags;
    while flags & HEADERS_FLAG_END_HEADERS == 0 {
        let (next, payload) = read_frame(stream);
        assert_eq!(next.frame_type, FrameType::Continuation);
        assert_eq!(next.stream_id, id);
        flags = next.flags;
        block.extend_from_slice(&payload);
        ended = ended || flags & HEADERS_FLAG_END_STREAM != 0;
        if flags & CONTINUATION_FLAG_END_HEADERS != 0 {
            break;
        }
    }
    (block, ended)
}

fn decode_fields(block: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (count, _) = decode_header_block(block, &mut decoder, &mut decoded).expect("decode block");
    decoded
        .iter()
        .take(count)
        .map(|header| {
            let name = decoder.resolve(header.field.name).expect("name").to_vec();
            let value = decoder.resolve(header.field.value).expect("value").to_vec();
            (name, value)
        })
        .collect()
}

fn read_response_with_trailers(stream: &mut TcpStream, id: u32) -> (Response, TrailerFields) {
    let (block, mut ended) = loop {
        let frame = read_frame(stream);
        if frame.0.frame_type == FrameType::WindowUpdate {
            continue;
        }
        break read_block(stream, id, frame);
    };
    let mut response = decode_response(&block);
    let mut body = Vec::new();
    let mut trailers = Vec::new();
    while !ended {
        let (header, payload) = read_frame(stream);
        assert_eq!(header.stream_id, id);
        match header.frame_type {
            FrameType::Data => {
                body.extend_from_slice(&payload);
                ended = header.flags & DATA_FLAG_END_STREAM != 0;
            }
            FrameType::Headers => {
                let (block, end) = read_block(stream, id, (header, payload));
                ended = end;
                trailers.extend(decode_fields(&block));
            }
            FrameType::WindowUpdate => {}
            found => panic!("unexpected frame: {found:?}"),
        }
    }
    response.body = body;
    (response, trailers)
}

#[test]
fn h2_managed_request_trailers_stage_and_pull() {
    let server = managed_h2_body_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    post_upload_open(&mut stream, 1, b"data");
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    send_trailers(
        &mut stream,
        1,
        &[(b"x-checksum", b"abc"), (b"x-count", b"2")],
    );
    assert_eq!(pull_all(&host, event.token), b"data");
    let (names, values) = pull_trailers_ready(&host, event.token);
    assert_eq!(names, b"x-checksum\x00x-count");
    assert_eq!(values, b"abc\x002");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let (grants, response) = read_response_with_grants(&mut stream, 1);
    assert_eq!(grants, vec![(0, 4), (1, 4)]);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    post_upload(&mut stream, 3, b"again");
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(pull_all(&host, event.token), b"again");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"second"),
        RespondOutcome::Ok
    );
    let (_, response) = read_response_with_grants(&mut stream, 3);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"second");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
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
    let response = read_response(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"chunk-onechunk-two");
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

// NOTE: paired trailer reader: concurrent trailered streams interleave DATA/HEADERS frame by
// frame (one stream command applies per turn), so sequential per-stream readers would misroute
// siblings. This demuxes both streams in one loop until each sees its terminal HEADERS.
fn read_trailer_pair(stream: &mut TcpStream, ids: [u32; 2]) -> [(Response, TrailerFields); 2] {
    let mut heads: [Option<Response>; 2] = [None, None];
    let mut bodies: [Vec<u8>; 2] = [Vec::new(), Vec::new()];
    let mut trailers: [TrailerFields; 2] = [Vec::new(), Vec::new()];
    let mut blocks: [Vec<u8>; 2] = [Vec::new(), Vec::new()];
    let mut ended = [false, false];
    let deadline = Instant::now() + DEADLINE;
    while ended != [true, true] {
        assert!(Instant::now() < deadline, "trailer pair never completed");
        let (header, payload) = read_frame(stream);
        if header.frame_type == FrameType::WindowUpdate {
            continue;
        }
        let index = ids
            .iter()
            .position(|id| *id == header.stream_id)
            .expect("pair stream");
        match header.frame_type {
            FrameType::Headers | FrameType::Continuation => {
                blocks[index].extend_from_slice(&payload);
                let complete = if header.frame_type == FrameType::Headers {
                    header.flags & HEADERS_FLAG_END_HEADERS != 0
                } else {
                    header.flags & CONTINUATION_FLAG_END_HEADERS != 0
                };
                if !complete {
                    continue;
                }
                let end = header.flags & HEADERS_FLAG_END_STREAM != 0;
                let block = std::mem::take(&mut blocks[index]);
                if heads[index].is_none() {
                    assert!(!end, "response HEADERS must not end a trailered stream");
                    heads[index] = Some(decode_response(&block));
                } else {
                    trailers[index].extend(decode_fields(&block));
                    ended[index] = end;
                }
            }
            FrameType::Data => {
                bodies[index].extend_from_slice(&payload);
                ended[index] = header.flags & DATA_FLAG_END_STREAM != 0;
            }
            found => panic!("unexpected frame: {found:?}"),
        }
    }
    let [first, second] = heads;
    let mut first = first.expect("first head");
    first.body = std::mem::take(&mut bodies[0]);
    let mut second = second.expect("second head");
    second.body = std::mem::take(&mut bodies[1]);
    [
        (first, std::mem::take(&mut trailers[0])),
        (second, std::mem::take(&mut trailers[1])),
    ]
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
// NOTE: H2 native-handler tests: FrozenNativeRoute entries ride the frozen view beside
// static/managed kinds, and HEADERS-time dispatch invokes them through the shared
// transport-neutral invoke (same guard/panic/validation contract as H1). The rig pumps
// the transport-neutral driver directly, so no worker thread or host is involved.

static H2N_OK: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/native",
    handler: h2n_ok,
    user_data: 0,
}];

static H2N_ECHO: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"POST",
    path: b"/echo",
    handler: h2n_record,
    user_data: 0,
}];

static H2N_END_PROBE: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"POST",
    path: b"/echo",
    handler: h2n_probe_end,
    user_data: 0,
}];

static H2N_CAP_PROBE: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"POST",
    path: b"/echo",
    handler: h2n_probe_cap,
    user_data: 0,
}];

static H2N_FAIL: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_fail,
    user_data: 0,
}];

static H2N_BAD_STATUS: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_bad_status,
    user_data: 0,
}];

static H2N_BIG_BODY: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_big_body,
    user_data: 0,
}];

static H2N_BAD_REASON: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_bad_reason,
    user_data: 0,
}];

static H2N_204: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_204,
    user_data: 0,
}];

static H2N_SELECT: [FrozenNativeRoute<'static>; 2] = [
    FrozenNativeRoute {
        method: b"GET",
        path: b"/u1",
        handler: h2n_select,
        user_data: 1,
    },
    FrozenNativeRoute {
        method: b"GET",
        path: b"/u2",
        handler: h2n_select,
        user_data: 2,
    },
];

static H2N_PRE_ROUTES: [FrozenRoute<'static>; 1] = [FrozenRoute {
    method: b"HEAD",
    path: b"/x",
    status: 200,
    body: b"static-head",
    managed: false,
    file_root: None,
}];

static H2N_PRE_NATIVES: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/x",
    handler: h2n_ok,
    user_data: 0,
}];

static H2N_BIG: [u8; 4097] = [b'x'; 4097];
static H2N_BAD_REASON_BYTES: [u8; 3] = *b"B\nd";
static H2N_END_HIT: Mutex<bool> = Mutex::new(false);
static H2N_CAP_HIT: Mutex<bool> = Mutex::new(false);

struct H2nObserved {
    method: Vec<u8>,
    path: Vec<u8>,
    has_body: u8,
    body_len: u64,
    null_body: bool,
}

static H2N_OBSERVED: Mutex<Option<H2nObserved>> = Mutex::new(None);

fn h2n_view(natives: &'static [FrozenNativeRoute<'static>]) -> FrozenRoutesView<'static> {
    FrozenRoutesView::new(&ROUTES)
        .expect("frozen routes")
        .with_natives(natives)
        .expect("natives")
}

#[allow(unsafe_code)]
fn h2n_respond(
    response: *mut NativeResponseOut,
    status: i32,
    reason: &'static [u8],
    body: &'static [u8],
) -> i32 {
    if response.is_null() {
        return 1;
    }
    
    let out = unsafe { &mut *response };
    out.status = status;
    out.reason = reason.as_ptr();
    out.reason_len = reason.len();
    out.body = body.as_ptr();
    out.body_len = body.len();
    0
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_ok(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 200, b"OK", b"native-hello")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_record(
    _user_data: *mut c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() {
        return 1;
    }
    
    let view = unsafe { &*request };
    let method = unsafe { std::slice::from_raw_parts(view.method, view.method_len) }.to_vec();
    let path = unsafe { std::slice::from_raw_parts(view.path, view.path_len) }.to_vec();
    *H2N_OBSERVED.lock().expect("observed lock") = Some(H2nObserved {
        method,
        path,
        has_body: view.has_body,
        body_len: view.body_len,
        null_body: view.body.is_null(),
    });
    h2n_respond(response, 200, b"OK", b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_probe_end(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    *H2N_END_HIT.lock().expect("probe lock") = true;
    h2n_respond(response, 200, b"OK", b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_probe_cap(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    *H2N_CAP_HIT.lock().expect("probe lock") = true;
    h2n_respond(response, 200, b"OK", b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_fail(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    _response: *mut NativeResponseOut,
) -> i32 {
    1
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_bad_status(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 199, b"OK", b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_big_body(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 200, b"OK", &H2N_BIG)
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_bad_reason(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 200, &H2N_BAD_REASON_BYTES, b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_204(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 204, b"No Content", b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_select(
    user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if user_data as usize == 1 {
        h2n_respond(response, 200, b"OK", b"one")
    } else {
        h2n_respond(response, 200, b"OK", b"two")
    }
}

#[test]
fn h2_native_get_serves_handler_body() {
    let view = h2n_view(&H2N_OK);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"GET", b"/native", true);
        let outcome = pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        assert_eq!(outcome.served, 1);
        assert_eq!(outcome.refused, 0);
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.content_length, 12);
        assert_eq!(response.body, b"native-hello");
    });
}

#[test]
fn h2_native_head_falls_back_to_get_without_body() {
    let view = h2n_view(&H2N_OK);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"HEAD", b"/native", true);
        let outcome = pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        assert_eq!(outcome.served, 1);
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.content_length, 12);
        assert!(response.body.is_empty());
    });
}

#[test]
fn h2_native_user_data_selects_body() {
    let view = h2n_view(&H2N_SELECT);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"GET", b"/u1", true);
        pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        let first = read_response(client, 1);
        assert_eq!(first.status, 200);
        assert_eq!(first.body, b"one");
        send_headers(client, 3, b"GET", b"/u2", true);
        pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        let second = read_response(client, 3);
        assert_eq!(second.status, 200);
        assert_eq!(second.body, b"two");
    });
}

#[test]
fn h2_native_open_tail_invokes_framing_only_then_refuses() {
    let view = h2n_view(&H2N_ECHO);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        *H2N_OBSERVED.lock().expect("observed lock") = None;
        send_headers_len(client, 1, b"POST", b"/echo", Some(5), false);
        let outcome = pump_until(conn, server, &view, None, |outcome| outcome.closed);
        assert_eq!(outcome.refused, 1);
        let seen = H2N_OBSERVED
            .lock()
            .expect("observed lock")
            .take()
            .expect("handler invoked");
        assert_eq!(seen.method, b"POST");
        assert_eq!(seen.path, b"/echo");
        assert_eq!(seen.has_body, 1);
        assert_eq!(seen.body_len, 5);
        assert!(seen.null_body);
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::Headers);
        assert_eq!(header.stream_id, 1);
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

#[test]
fn h2_native_end_with_declared_body_is_protocol_error() {
    let view = h2n_view(&H2N_END_PROBE);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        *H2N_END_HIT.lock().expect("probe lock") = false;
        send_headers_len(client, 1, b"POST", b"/echo", Some(5), true);
        pump_until(conn, server, &view, None, |outcome| outcome.refused > 0);
        assert!(!*H2N_END_HIT.lock().expect("probe lock"));
        let (id, code) = read_rst(client);
        assert_eq!(id, 1);
        assert_eq!(code, ErrorCode::ProtocolError);
    });
}

#[test]
fn h2_native_over_cap_refuses_before_invoke() {
    let view = h2n_view(&H2N_CAP_PROBE);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        *H2N_CAP_HIT.lock().expect("probe lock") = false;
        send_headers_len(
            client,
            1,
            b"POST",
            b"/echo",
            Some(MAX_STATIC_BODY_BYTES + 1),
            false,
        );
        pump_until(conn, server, &view, None, |outcome| outcome.refused > 0);
        assert!(!*H2N_CAP_HIT.lock().expect("probe lock"));
        let (id, code) = read_rst(client);
        assert_eq!(id, 1);
        assert_eq!(code, ErrorCode::RefusedStream);
        send_headers(client, 3, b"GET", b"/empty", true);
        pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        let sibling = read_response(client, 3);
        assert_eq!(sibling.status, 200);
        assert!(sibling.body.is_empty());
    });
}

fn h2n_failure_rsts(natives: &'static [FrozenNativeRoute<'static>]) {
    let view = h2n_view(natives);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"GET", b"/fail", true);
        pump_until(conn, server, &view, None, |outcome| outcome.refused > 0);
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::RstStream);
        assert_eq!(header.stream_id, 1);
        assert_eq!(
            decode_rst_stream_payload(header, &payload).expect("decode rst"),
            ErrorCode::InternalError
        );
    });
}

#[test]
fn h2_native_handler_failures_rst_without_headers() {
    h2n_failure_rsts(&H2N_FAIL);
    h2n_failure_rsts(&H2N_BAD_STATUS);
    h2n_failure_rsts(&H2N_BIG_BODY);
    h2n_failure_rsts(&H2N_BAD_REASON);
    h2n_failure_rsts(&H2N_204);
}

#[test]
fn h2_native_wins_over_head_static_on_fallback() {
    let view = FrozenRoutesView::new(&H2N_PRE_ROUTES)
        .expect("frozen routes")
        .with_natives(&H2N_PRE_NATIVES)
        .expect("natives");
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"HEAD", b"/x", true);
        pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.content_length, 12);
        assert!(response.body.is_empty());
    });
}

#[test]
fn h2_native_view_rejects_overlaps_and_bad_input() {
    static OVER_STATIC: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
        method: b"GET",
        path: b"/empty",
        handler: h2n_ok,
        user_data: 0,
    }];
    assert_eq!(
        FrozenRoutesView::new(&ROUTES)
            .expect("routes")
            .with_natives(&OVER_STATIC)
            .unwrap_err(),
        RouteBuildError::Duplicate
    );
    static OVER_MANAGED: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
        method: b"GET",
        path: b"/hello",
        handler: h2n_ok,
        user_data: 0,
    }];
    assert_eq!(
        FrozenRoutesView::new(&ROUTES)
            .expect("routes")
            .with_natives(&OVER_MANAGED)
            .unwrap_err(),
        RouteBuildError::Duplicate
    );
    static INTERNAL_DUPE: [FrozenNativeRoute<'static>; 2] = [
        FrozenNativeRoute {
            method: b"GET",
            path: b"/a",
            handler: h2n_ok,
            user_data: 0,
        },
        FrozenNativeRoute {
            method: b"GET",
            path: b"/a",
            handler: h2n_ok,
            user_data: 1,
        },
    ];
    assert_eq!(
        FrozenRoutesView::new(&ROUTES)
            .expect("routes")
            .with_natives(&INTERNAL_DUPE)
            .unwrap_err(),
        RouteBuildError::Duplicate
    );
    static BAD_METHOD: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
        method: b"GE T",
        path: b"/a",
        handler: h2n_ok,
        user_data: 0,
    }];
    assert_eq!(
        FrozenRoutesView::new(&ROUTES)
            .expect("routes")
            .with_natives(&BAD_METHOD)
            .unwrap_err(),
        RouteBuildError::Invalid
    );
    static EMPTY_PATH: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
        method: b"GET",
        path: b"",
        handler: h2n_ok,
        user_data: 0,
    }];
    assert_eq!(
        FrozenRoutesView::new(&ROUTES)
            .expect("routes")
            .with_natives(&EMPTY_PATH)
            .unwrap_err(),
        RouteBuildError::Invalid
    );
}

#[allow(unsafe_code)]
unsafe extern "C" fn live_native_hello(
    _user_data: *mut c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }
    
    let out = unsafe { &mut *response };
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = b"h2-native".as_ptr();
    out.body_len = 9;
    0
}

#[test]
fn h2c_serves_native_routes_live() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_native(b"GET", b"/native", Some(live_native_hello), 0)
        .expect("register native");
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
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/native", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"h2-native");
    send_headers(&mut client, 3, b"GET", b"/hello", true);
    let static_response = read_response(&mut client, 3);
    assert_eq!(static_response.status, 200);
    assert_eq!(static_response.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2);
}
// NOTE: H2C file-mount proof through a live Server::bind plus real H2C bytes (never
// driver-only, so projection wiring is covered): full GET/HEAD 200s with Last-Modified,
// 304 revalidation, single-range 206, and fail-closed 404/413/416 mappings over one
// multiplexed connection. Multipart ranges stay OUT (RST RefusedStream, follow-up).

fn file_mount_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("veylor-h2f-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("mount root");
    root
}

fn file_mount_server(root: &std::path::Path) -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/assets/exact",
            200,
            b"OK",
            b"static-wins",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register exact");
    server
        .register_file_mount(b"/assets/", root)
        .expect("register mount");
    server.start().expect("start server")
}

#[test]
fn h2_file_mount_get_head_last_modified_304() {
    let root = file_mount_root("basic");
    std::fs::write(root.join("hello.txt"), b"file-content").expect("mount file");
    std::fs::write(root.join("exact"), b"file-loses").expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);
    send_headers(&mut client, 1, b"GET", b"/assets/hello.txt", true);
    let got = read_response_stateful(&mut client, 1, &mut dec);
    assert_eq!(got.status, 200);
    assert_eq!(got.content_length, 12);
    assert_eq!(got.body, b"file-content");
    assert_eq!(got.content_type, b"text/plain");
    assert!(!got.last_modified.is_empty());
    send_headers(&mut client, 3, b"HEAD", b"/assets/hello.txt", true);
    let head = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(head.status, 200);
    assert_eq!(head.content_length, 12);
    assert!(head.body.is_empty());
    assert_eq!(head.last_modified, got.last_modified);
    let since: &[u8] = &got.last_modified;
    send_headers_extra(
        &mut client,
        5,
        b"GET",
        b"/assets/hello.txt",
        &[(b"if-modified-since".as_slice(), since)],
        true,
    );
    let fresh = read_response_stateful(&mut client, 5, &mut dec);
    assert_eq!(fresh.status, 304);
    assert_eq!(fresh.content_length, 0);
    assert!(fresh.body.is_empty());
    assert_eq!(fresh.last_modified, got.last_modified);
    send_headers_extra(
        &mut client,
        7,
        b"GET",
        b"/assets/hello.txt",
        &[(
            b"if-modified-since".as_slice(),
            b"Thu, 01 Jan 1970 00:00:00 GMT".as_slice(),
        )],
        true,
    );
    let stale = read_response_stateful(&mut client, 7, &mut dec);
    assert_eq!(stale.status, 200);
    assert_eq!(stale.body, b"file-content");
    send_headers(&mut client, 9, b"GET", b"/assets/missing.txt", true);
    let missing = read_response_stateful(&mut client, 9, &mut dec);
    assert_eq!(missing.status, 404);
    assert_eq!(missing.body, b"Not Found");
    send_headers(&mut client, 11, b"GET", b"/assets/exact", true);
    let exact = read_response_stateful(&mut client, 11, &mut dec);
    assert_eq!(exact.status, 200);
    assert_eq!(exact.body, b"static-wins");
    send_headers(&mut client, 13, b"GET", b"/assets/../hello.txt", true);
    let traversal = read_response_stateful(&mut client, 13, &mut dec);
    assert_eq!(traversal.status, 404);
    assert_eq!(traversal.body, b"Not Found");
    send_headers(&mut client, 15, b"POST", b"/assets/hello.txt", true);
    let wrong_method = read_response_stateful(&mut client, 15, &mut dec);
    assert_eq!(wrong_method.status, 404);
    send_headers_extra(
        &mut client,
        17,
        b"HEAD",
        b"/assets/hello.txt",
        &[(b"range".as_slice(), b"bytes=0-3".as_slice())],
        true,
    );
    let head_range = read_response_stateful(&mut client, 17, &mut dec);
    assert_eq!(head_range.status, 200);
    assert_eq!(head_range.content_length, 12);
    assert!(head_range.body.is_empty());
    send_headers(&mut client, 19, b"GET", b"/assets/hello.txt?v=2", true);
    let query = read_response_stateful(&mut client, 19, &mut dec);
    assert_eq!(query.status, 200);
    assert_eq!(query.body, b"file-content");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 10, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn h2_file_mount_streams_large_file_across_turns() {
    let root = file_mount_root("large");
    let mut bytes = vec![0u8; 20_000];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    std::fs::write(root.join("big.bin"), &bytes).expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);
    send_headers(&mut client, 1, b"GET", b"/assets/big.bin", true);
    let got = read_response_stateful(&mut client, 1, &mut dec);
    assert_eq!(got.status, 200);
    assert_eq!(got.content_length, 20_000);
    assert_eq!(got.body, bytes);
    assert_eq!(got.content_type, b"application/octet-stream");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn h2_file_mount_single_range_206_and_fail_closed() {
    let root = file_mount_root("range");
    let mut bytes = vec![0u8; 1000];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    std::fs::write(root.join("data.bin"), &bytes).expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);
    send_headers_extra(
        &mut client,
        1,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=0-99".as_slice())],
        true,
    );
    let ranged = read_response_stateful(&mut client, 1, &mut dec);
    assert_eq!(ranged.status, 206);
    assert_eq!(ranged.content_length, 100);
    assert_eq!(ranged.body, &bytes[..100]);
    assert_eq!(ranged.content_range, b"bytes 0-99/1000");
    send_headers_extra(
        &mut client,
        3,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=-10".as_slice())],
        true,
    );
    let suffix = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(suffix.status, 206);
    assert_eq!(suffix.body, &bytes[990..]);
    assert_eq!(suffix.content_range, b"bytes 990-999/1000");
    send_headers_extra(
        &mut client,
        5,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=5000-6000".as_slice())],
        true,
    );
    let bad = read_response_stateful(&mut client, 5, &mut dec);
    assert_eq!(bad.status, 416);
    assert!(bad.body.is_empty());
    assert_eq!(bad.content_range, b"bytes */1000");
    send_headers_extra(
        &mut client,
        7,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=0-1, 3-4".as_slice())],
        true,
    );
    // NOTE: a satisfiable multi-range set is not unsatisfiable, so it RST-refuses with no
    // HEADERS first (multipart 206 framing stays a follow-up); the connection survives.
    let (rst_stream, rst_code) = read_rst(&mut client);
    assert_eq!(rst_stream, 7);
    assert_eq!(rst_code, ErrorCode::RefusedStream);
    send_headers(&mut client, 9, b"GET", b"/assets/data.bin", true);
    let sibling = read_response_stateful(&mut client, 9, &mut dec);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, bytes);
    send_headers_len(
        &mut client,
        11,
        b"GET",
        b"/assets/data.bin",
        Some(1_000_000),
        true,
    );
    let big = read_response_stateful(&mut client, 11, &mut dec);
    assert_eq!(big.status, 413);
    assert_eq!(big.body, b"Payload Too Large");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 5, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}
