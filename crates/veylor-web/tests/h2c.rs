use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM,
    DEFAULT_MAX_FRAME_SIZE, FRAME_HEADER_LEN, FrameHeader, FrameType, HEADERS_FLAG_END_HEADERS,
    HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackFieldToEncode, HpackHeader, HpackRepresentation,
    HpackTable, SETTINGS_ACK, decode_frame_header, decode_header_block, encode_frame_header,
    encode_header_block,
};
use veylor_server::{Server, ServerConfig};
use veylor_web::{Path, Query, Router, ServeExt, SseEvent, SseSender};

const DEADLINE: Duration = Duration::from_secs(5);

fn statement(Path(account): Path<i64>, Query(query): Query<'_>) -> String {
    format!("{account}:{}", query.parse::<i64>("m").unwrap_or(1))
}

fn move_funds(Path(account): Path<i64>) -> String {
    format!("moved:{account}")
}

fn close_account(Path(account): Path<i64>) -> String {
    format!("closed:{account}")
}

fn events(sender: SseSender) {
    sender.send(SseEvent::data("h2"));
    sender.close();
}

#[test]
fn h2c_serves_the_same_router() {
    let config = ServerConfig {
        h2c_port: Some(0),
        worker_count: 1,
        ..ServerConfig::default()
    };
    let server = Server::bind(([127, 0, 0, 1], 0).into(), config).expect("bind");
    let running = server
        .serve(
            Router::new()
                .get("/accounts/:account/statement", statement)
                .post("/accounts/:account/move", move_funds)
                .post("/accounts/:account/close", close_account)
                .sse("/events", events),
        )
        .expect("serve");

    let addr = running.h2c_addr().expect("h2c address");
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.set_read_timeout(Some(DEADLINE)).expect("timeout");
    handshake(&mut stream);

    send_request(&mut stream, 1, b"GET", b"/accounts/42/statement?m=3", true);
    let (status, body) = read_response(&mut stream, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"42:3");

    send_request(&mut stream, 3, b"POST", b"/accounts/42/move", false);
    write_data(&mut stream, 3, &[]);
    let (status, body) = read_response(&mut stream, 3);
    assert_eq!(status, 200);
    assert_eq!(body, b"moved:42");

    send_request(&mut stream, 5, b"POST", b"/accounts/42/close", false);
    write_data(&mut stream, 5, &[]);
    let (status, body) = read_response(&mut stream, 5);
    assert_eq!(status, 200);
    assert_eq!(body, b"closed:42");

    send_request(&mut stream, 7, b"GET", b"/events", true);
    let (status, body) = read_response(&mut stream, 7);
    assert_eq!(status, 200);
    assert_eq!(body, b"data: h2\n\n");

    running.join().expect("join");
}

fn handshake(stream: &mut TcpStream) {
    stream
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("preface");
    write_frame(stream, FrameType::Settings, 0, 0, &[]);
    let (header, _) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    write_frame(stream, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = read_frame(stream);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);
}

fn send_request(stream: &mut TcpStream, id: u32, method: &[u8], path: &[u8], end: bool) {
    let fields = [
        field(b":method", method),
        field(b":scheme", b"http"),
        field(b":path", path),
        field(b":authority", b"localhost"),
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 2048];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode");
    let mut flags = HEADERS_FLAG_END_HEADERS;
    if end {
        flags |= HEADERS_FLAG_END_STREAM;
    }
    write_frame(stream, FrameType::Headers, flags, id, &block[..len]);
}

fn write_data(stream: &mut TcpStream, id: u32, body: &[u8]) {
    write_frame(stream, FrameType::Data, DATA_FLAG_END_STREAM, id, body);
}

fn read_response(stream: &mut TcpStream, id: u32) -> (u16, Vec<u8>) {
    let mut block = Vec::new();
    let mut last_flags = 0u8;
    loop {
        let (header, payload) = read_frame(stream);
        if matches!(
            header.frame_type,
            FrameType::Settings | FrameType::WindowUpdate
        ) {
            continue;
        }
        assert_eq!(header.stream_id, id);
        match header.frame_type {
            FrameType::Headers => {
                last_flags = header.flags;
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

    let ended = last_flags & HEADERS_FLAG_END_STREAM != 0;
    let status = decode_status(&block);
    let mut body = Vec::new();
    if !ended {
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
    (status, body)
}

fn decode_status(block: &[u8]) -> u16 {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (count, _) = decode_header_block(block, &mut decoder, &mut decoded).expect("decode");
    for header in decoded.iter().take(count) {
        if decoder.resolve(header.field.name).expect("name") == b":status" {
            let value = decoder.resolve(header.field.value).expect("value");
            return std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    0
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

fn write_frame(stream: &mut TcpStream, kind: FrameType, flags: u8, id: u32, payload: &[u8]) {
    let header = FrameHeader {
        payload_length: payload.len() as u32,
        frame_type: kind,
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
