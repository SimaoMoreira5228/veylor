use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use veylor_http2::{
    DEFAULT_MAX_FRAME_SIZE, ErrorCode, FRAME_HEADER_LEN, FrameHeader, FrameType,
    HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackTable,
    encode_frame_header, encode_goaway_payload, encode_header_block,
};
use veylor_server::MAX_H2_CONNS;

const CLEAN_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const CHURN_PER_WAVE: usize = 8;

// NOTE: one segment carries the last request, the peer's GOAWAY and then its FIN, which is
// the shape h2load leaves behind: the data read drains the socket and latches the EOF.
fn final_segment() -> Vec<u8> {
    let fields = [
        field(b":method", b"GET"),
        field(b":scheme", b"http"),
        field(b":path", b"/hello"),
        field(b":authority", b"localhost"),
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    let header = FrameHeader {
        payload_length: len as u32,
        frame_type: FrameType::Headers,
        flags: HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        stream_id: 1,
    };
    let mut encoded = [0; FRAME_HEADER_LEN];
    encode_frame_header(header, &mut encoded, 16_384).expect("encode header");
    let mut wire = Vec::with_capacity(FRAME_HEADER_LEN + len + FRAME_HEADER_LEN + 8);
    wire.extend_from_slice(&encoded);
    wire.extend_from_slice(&block[..len]);

    let mut payload = [0; 8];
    let payload_len =
        encode_goaway_payload(0, ErrorCode::NoError, &[], &mut payload).expect("encode goaway");
    let goaway = FrameHeader {
        payload_length: payload_len as u32,
        frame_type: FrameType::GoAway,
        flags: 0,
        stream_id: 0,
    };
    let mut encoded = [0; FRAME_HEADER_LEN];
    encode_frame_header(goaway, &mut encoded, DEFAULT_MAX_FRAME_SIZE)
        .expect("encode goaway header");
    wire.extend_from_slice(&encoded);
    wire.extend_from_slice(&payload[..payload_len]);
    wire
}

fn write_goaway_and_half_close(stream: &mut TcpStream) {
    stream
        .set_read_timeout(Some(CLEAN_CLOSE_TIMEOUT))
        .expect("clean close timeout");
    stream
        .write_all(&final_segment())
        .expect("write final segment");
    stream.flush().expect("flush final segment");
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("half close");
}

fn read_to_eof(stream: &mut TcpStream, port: u16) {
    let mut byte = [0; 1];
    loop {
        match stream.read(&mut byte) {
            Ok(0) => return,
            Ok(_) => continue,
            Err(error) => panic!(
                "expected the server to close, got {error:?}; CLOSE-WAIT: {:?}",
                close_wait_sockets(port)
            ),
        }
    }
}

// NOTE: an unread FIN is the only difference between a slot that is freed and one that waits
// out its idle deadline, so the assertion has to read the kernel table, not the counters.
fn close_wait_sockets(port: u16) -> Vec<String> {
    let table = std::fs::read_to_string("/proc/net/tcp").expect("read proc net tcp");
    table
        .lines()
        .skip(1)
        .filter(|line| {
            let mut fields = line.split_whitespace();
            let _slot = fields.next();
            let Some(local) = fields.next() else {
                return false;
            };
            let _remote = fields.next();
            let Some(state) = fields.next() else {
                return false;
            };
            if state != "08" {
                return false;
            }
            let Some((_, hex)) = local.rsplit_once(':') else {
                return false;
            };
            u16::from_str_radix(hex, 16).is_ok_and(|found| found == port)
        })
        .map(str::to_string)
        .collect()
}

fn assert_no_close_wait(port: u16) {
    let stuck = close_wait_sockets(port);
    assert!(
        stuck.is_empty(),
        "server left CLOSE-WAIT sockets on port {port}: {stuck:?}"
    );
}

#[test]
fn a_peer_that_sends_goaway_and_fin_with_its_last_request_frees_the_slot() {
    let server = h2_server();
    let port = server.h2c_addr().expect("h2c enabled").port();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    write_goaway_and_half_close(&mut stream);
    let response = read_response(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    read_to_eof(&mut stream, port);
    assert_no_close_wait(port);

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.timeout, 0, "slot waited out a deadline: {counts:?}");
}

#[test]
fn churn_of_clean_closes_keeps_the_worker_accepting() {
    let server = h2_server();
    let port = server.h2c_addr().expect("h2c enabled").port();
    let waves = MAX_H2_CONNS / CHURN_PER_WAVE + 4;

    for wave in 0..waves {
        let mut streams: Vec<TcpStream> =
            (0..CHURN_PER_WAVE).map(|_| h2_connect(&server)).collect();
        for stream in streams.iter_mut() {
            handshake(stream);
        }
        for stream in streams.iter_mut() {
            write_goaway_and_half_close(stream);
        }
        for stream in streams.iter_mut() {
            let response = read_response(stream, 1);
            assert_eq!(response.status, 200, "wave {wave}");
            assert_eq!(response.body, b"hello", "wave {wave}");
            read_to_eof(stream, port);
        }
        assert_no_close_wait(port);
    }

    let mut probe = h2_connect(&server);
    handshake(&mut probe);
    send_headers(&mut probe, 1, b"GET", b"/hello", true);
    let response = read_response(&mut probe, 1);
    assert_eq!(response.body, b"hello", "worker stopped accepting");

    let counts = stop_join(server);
    assert_eq!(counts.transport_error, 0, "counts: {counts:?}");
    assert_eq!(counts.timeout, 0, "counts: {counts:?}");
    assert_eq!(
        counts.h2_served,
        (waves * CHURN_PER_WAVE + 1) as u64,
        "counts: {counts:?}"
    );
}
