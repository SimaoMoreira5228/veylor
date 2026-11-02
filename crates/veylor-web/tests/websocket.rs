use std::io::{Read, Write};
use std::net::TcpStream;

use veylor_server::{Server, ServerConfig};
use veylor_web::{Router, ServeExt};

const HANDSHAKE: &str = "GET /ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

#[test]
fn websocket_echo_route_upgrades_and_echoes() {
    let server = Server::bind(
        ([127, 0, 0, 1], 0).into(),
        ServerConfig {
            worker_count: 1,
            ..ServerConfig::default()
        },
    )
    .expect("bind");
    let app = Router::new().ws_echo("/ws");

    let running = server.serve(app).expect("serve");
    let mut stream = TcpStream::connect(running.addr()).expect("connect");
    stream.write_all(HANDSHAKE.as_bytes()).expect("handshake");

    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read head");
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    assert!(
        head.starts_with(b"HTTP/1.1 101 "),
        "{}",
        String::from_utf8_lossy(&head)
    );

    write_text_frame(&mut stream, b"hello");
    let payload = read_frame(&mut stream);
    assert_eq!(payload, b"hello");

    drop(stream);
    running.stop_accepting();
    running.join().expect("join");
}

fn write_text_frame(stream: &mut TcpStream, payload: &[u8]) {
    let mut frame = Vec::with_capacity(payload.len() + 6);
    frame.push(0x81);
    frame.push(0x80 | payload.len() as u8);

    let mask = [0x11u8, 0x22, 0x33, 0x44];
    frame.extend_from_slice(&mask);
    for (index, byte) in payload.iter().enumerate() {
        frame.push(byte ^ mask[index % 4]);
    }
    stream.write_all(&frame).expect("frame");
}

fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 2];
    stream.read_exact(&mut header).expect("frame header");
    assert_eq!(header[0], 0x81, "unmasked text frame");
    assert_eq!(header[1] & 0x80, 0, "server frames are unmasked");

    let mut payload = vec![0u8; usize::from(header[1] & 0x7f)];
    stream.read_exact(&mut payload).expect("frame body");
    payload
}
