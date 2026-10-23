use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM,
    DEFAULT_MAX_FRAME_SIZE, ErrorCode, FRAME_HEADER_LEN, FrameHeader, FrameType, H2Role,
    HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackFieldToEncode,
    HpackHeader, HpackRepresentation, HpackTable, SETTINGS_ACK, StreamSlot, StreamTable,
    decode_frame_header, decode_goaway_payload, decode_header_block, decode_rst_stream_payload,
    encode_frame_header, encode_header_block,
};
use veylor_server::{
    ACK_SLOTS, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES, ENCODE_ENTRIES, FrozenRoute,
    FrozenRoutesView, H2Conn, INPUT_STAGING_CAP, MAX_ROUTES, RouteBuildError,
    h2::{HEADER_BLOCK_CAP, HEADER_SLOTS, STREAM_SLOTS},
};

const DEADLINE: Duration = Duration::from_secs(5);
const PUMP_IDLE: Duration = Duration::from_millis(100);

static ROUTES: [FrozenRoute<'static>; 2] = [
    FrozenRoute {
        method: b"GET",
        path: b"/hello",
        status: 200,
        body: b"hello",
    },
    FrozenRoute {
        method: b"GET",
        path: b"/empty",
        status: 200,
        body: b"",
    },
];

struct PumpReport {
    served: u16,
    refused: u16,
    closed: bool,
}

fn spawn_pump(server: TcpStream, drain: Arc<AtomicBool>) -> JoinHandle<PumpReport> {
    std::thread::spawn(move || pump_loop(server, &drain))
}

fn pump_loop(mut server: TcpStream, drain: &AtomicBool) -> PumpReport {
    server
        .set_read_timeout(Some(PUMP_IDLE))
        .expect("pump timeout");
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
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    let mut output = [0u8; 16_384 + 512];
    let mut served = 0;
    let mut refused = 0;
    let mut chunk = [0u8; 4_096];
    loop {
        if drain.load(Ordering::Acquire) && !conn.is_closed() {
            let outcome = conn.drain(&mut output);
            if outcome.written > 0 && server.write_all(&output[..outcome.written]).is_err() {
                break;
            }
            break;
        }
        if conn.is_closed() {
            break;
        }
        match server.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                if conn.push_input(&chunk[..read]).is_err() {
                    break;
                }
                let outcome = conn.step(&mut output, &view);
                served += outcome.served;
                refused += outcome.refused;
                if outcome.written > 0 && server.write_all(&output[..outcome.written]).is_err() {
                    break;
                }
                if outcome.closed {
                    break;
                }
            }
            Err(error)
                if error.kind() == ErrorKind::TimedOut || error.kind() == ErrorKind::WouldBlock => {
            }
            Err(_) => break,
        }
    }
    PumpReport {
        served,
        refused,
        closed: conn.is_closed(),
    }
}

fn test_pump() -> (TcpStream, Arc<AtomicBool>, JoinHandle<PumpReport>) {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("bind loopback");
    let addr = listener.local_addr().expect("loopback addr");
    let client = TcpStream::connect(addr).expect("connect loopback");
    client
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    client
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    let (server, _) = listener.accept().expect("accept loopback");
    let drain = Arc::new(AtomicBool::new(false));
    let handle = spawn_pump(server, Arc::clone(&drain));
    (client, drain, handle)
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

#[test]
fn get_static_route_returns_200_with_exact_body() {
    let (mut client, _drain, pump) = test_pump();
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_length, 5);
    assert_eq!(response.body, b"hello");
    drop(client);
    let report = pump.join().expect("pump join");
    assert_eq!(report.served, 1);
    assert!(!report.closed);
}

#[test]
fn head_falls_back_to_get_without_body() {
    let (mut client, _drain, pump) = test_pump();
    handshake(&mut client);
    send_headers(&mut client, 1, b"HEAD", b"/hello", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_length, 5);
    assert!(response.body.is_empty());
    drop(client);
    let report = pump.join().expect("pump join");
    assert_eq!(report.served, 1);
}

#[test]
fn unknown_path_returns_404() {
    let (mut client, _drain, pump) = test_pump();
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/missing", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 404);
    assert_eq!(response.body, b"Not Found");
    drop(client);
    let report = pump.join().expect("pump join");
    assert_eq!(report.served, 1);
}

#[test]
fn bad_magic_goes_away_and_closes() {
    let (mut client, _drain, pump) = test_pump();
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
    let report = pump.join().expect("pump join");
    assert!(report.closed);
}

#[test]
fn request_body_is_refused_and_connection_closes() {
    let (mut client, _drain, pump) = test_pump();
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
    let report = pump.join().expect("pump join");
    assert_eq!(report.refused, 1);
    assert!(report.closed);
}

#[test]
fn second_stream_reuses_connection() {
    let (mut client, _drain, pump) = test_pump();
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
    drop(client);
    let report = pump.join().expect("pump join");
    assert_eq!(report.served, 2);
    assert!(!report.closed);
}

#[test]
fn graceful_drain_sends_goaway() {
    let (mut client, drain, pump) = test_pump();
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    drain.store(true, Ordering::Release);
    let (header, payload) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::GoAway);
    let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
    assert_eq!(goaway.error_code, ErrorCode::NoError);
    assert_eq!(goaway.last_stream_id, 1);
    read_eof(&mut client);
    let report = pump.join().expect("pump join");
    assert!(report.closed);
}

#[test]
fn route_view_rejects_bad_input() {
    let bad_status = FrozenRoute {
        method: b"GET",
        path: b"/a",
        status: 204,
        body: b"",
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
    };
    let dupe = FrozenRoute {
        method: b"GET",
        path: b"/a",
        status: 200,
        body: b"b",
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
        })
        .collect();
    assert_eq!(
        FrozenRoutesView::new(&full).unwrap_err(),
        RouteBuildError::Full
    );
}
