use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::net::TcpStream;
use veylor_http2::{DATA_FLAG_END_STREAM, ErrorCode, FrameType};

const CHUNK: usize = 16_384;
const FLOOD_FRAMES: usize = 40;

struct Drained {
    credited: u64,
    answered: bool,
}

fn rst_payload(code: ErrorCode) -> [u8; 4] {
    let mut payload = [0u8; 4];
    veylor_http2::encode_rst_stream_payload(code, &mut payload).expect("encode rst");
    payload
}

fn flood(client: &mut TcpStream, id: u32) -> u64 {
    let chunk = vec![b'x'; CHUNK];
    for _ in 0..FLOOD_FRAMES {
        write_frame(client, FrameType::Data, 0, id, &chunk);
    }
    (CHUNK * FLOOD_FRAMES) as u64
}

fn drain_until_answered(client: &mut TcpStream, id: u32) -> Drained {
    let mut credited = 0u64;
    let mut answered = false;
    loop {
        let (header, payload) = read_frame(client);
        match header.frame_type {
            FrameType::WindowUpdate if header.stream_id == 0 => {
                let value = u32::from_be_bytes(payload[..4].try_into().expect("window bytes"));
                credited += u64::from(value & 0x7fff_ffff);
            }
            FrameType::GoAway => panic!("connection torn down by the flood"),
            FrameType::Data | FrameType::Headers if header.stream_id == id => {
                answered = header.flags & DATA_FLAG_END_STREAM != 0;
            }
            _ => {}
        }
        if answered {
            return Drained { credited, answered };
        }
    }
}

#[test]
fn data_flood_on_closed_stream_credits_connection_window() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let first = read_response(&mut client, 1);
    assert_eq!(first.status, 200);

    let sent = flood(&mut client, 1);
    send_headers(&mut client, 3, b"GET", b"/hello", true);
    let drained = drain_until_answered(&mut client, 3);

    assert!(drained.answered);
    assert_eq!(drained.credited, sent);
    send_headers(&mut client, 5, b"GET", b"/hello", true);
    let third = read_response(&mut client, 5);
    assert_eq!(third.body, b"hello");
    stop_join(server);
}

#[test]
fn data_flood_on_reset_stream_credits_connection_window() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", false);
    write_frame(
        &mut client,
        FrameType::RstStream,
        0,
        1,
        &rst_payload(ErrorCode::Cancel),
    );

    let sent = flood(&mut client, 1);
    send_headers(&mut client, 3, b"GET", b"/hello", true);
    let drained = drain_until_answered(&mut client, 3);

    assert!(drained.answered);
    assert_eq!(drained.credited, sent);
    send_headers(&mut client, 5, b"GET", b"/hello", true);
    let third = read_response(&mut client, 5);
    assert_eq!(third.body, b"hello");
    stop_join(server);
}
