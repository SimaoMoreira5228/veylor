use std::time::Duration;

use crate::support::timeout;
use smol::io::{AsyncReadExt, AsyncWriteExt};
use smol::net::{TcpListener, TcpStream};

pub const DATA: u8 = 0;
pub const HEADERS: u8 = 1;
pub const RST_STREAM: u8 = 3;
pub const SETTINGS: u8 = 4;
pub const PING: u8 = 6;
pub const GOAWAY: u8 = 7;
pub const WINDOW_UPDATE: u8 = 8;

pub const END_STREAM: u8 = 1;
pub const ACK: u8 = 1;
pub const END_HEADERS: u8 = 4;

pub const SETTING_MAX_CONCURRENT_STREAMS: u16 = 3;
pub const SETTING_INITIAL_WINDOW_SIZE: u16 = 4;

const STEP: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct Frame {
    pub kind: u8,
    pub flags: u8,
    pub stream: u32,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn end_stream(&self) -> bool {
        self.flags & END_STREAM != 0
    }

    pub fn increment(&self) -> u32 {
        u32::from_be_bytes(self.payload[..4].try_into().unwrap()) & 0x7FFF_FFFF
    }

    pub fn code(&self) -> u32 {
        u32::from_be_bytes(self.payload[..4].try_into().unwrap())
    }
}

pub struct Peer {
    socket: TcpStream,
    buffer: Vec<u8>,
}

pub async fn listen() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    (listener, addr)
}

impl Peer {
    pub async fn accept(listener: &TcpListener) -> Self {
        let (socket, _) = timeout(STEP, listener.accept())
            .await
            .expect("accept deadline")
            .expect("accept");
        let mut peer = Self {
            socket,
            buffer: Vec::new(),
        };
        let mut preface = [0u8; 24];
        timeout(STEP, peer.socket.read_exact(&mut preface))
            .await
            .expect("preface deadline")
            .expect("preface");
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        peer
    }

    pub async fn handshake(&mut self, settings: &[(u16, u32)]) -> Vec<u8> {
        let mut payload = Vec::new();
        for (id, value) in settings {
            payload.extend_from_slice(&id.to_be_bytes());
            payload.extend_from_slice(&value.to_be_bytes());
        }
        self.send(SETTINGS, 0, 0, &payload).await;

        let mut client_settings = Vec::new();
        let mut acked = false;
        let mut seen = false;
        while !(acked && seen) {
            let frame = self.next().await;
            match (frame.kind, frame.flags & ACK != 0) {
                (SETTINGS, false) => {
                    client_settings = frame.payload;
                    seen = true;
                    self.send(SETTINGS, ACK, 0, &[]).await;
                }
                (SETTINGS, true) => acked = true,
                (WINDOW_UPDATE, _) => {}
                other => panic!("unexpected frame during handshake: {other:?}"),
            }
        }
        client_settings
    }

    pub async fn next(&mut self) -> Frame {
        loop {
            if let Some(frame) = self.take_frame() {
                return frame;
            }
            let mut chunk = [0u8; 16 * 1024];
            let count = timeout(STEP, self.socket.read(&mut chunk))
                .await
                .expect("frame deadline")
                .expect("read");
            assert!(count > 0, "client closed the connection");
            self.buffer.extend_from_slice(&chunk[..count]);
        }
    }

    pub async fn next_non_update(&mut self) -> Frame {
        loop {
            let frame = self.next().await;
            if frame.kind != WINDOW_UPDATE {
                return frame;
            }
        }
    }

    fn take_frame(&mut self) -> Option<Frame> {
        if self.buffer.len() < 9 {
            return None;
        }
        let length = (usize::from(self.buffer[0]) << 16)
            | (usize::from(self.buffer[1]) << 8)
            | usize::from(self.buffer[2]);
        if self.buffer.len() < 9 + length {
            return None;
        }
        let frame = Frame {
            kind: self.buffer[3],
            flags: self.buffer[4],
            stream: u32::from_be_bytes(self.buffer[5..9].try_into().unwrap()) & 0x7FFF_FFFF,
            payload: self.buffer[9..9 + length].to_vec(),
        };
        self.buffer.drain(..9 + length);
        Some(frame)
    }

    pub async fn send(&mut self, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
        let mut frame = Vec::with_capacity(9 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
        frame.push(kind);
        frame.push(flags);
        frame.extend_from_slice(&stream.to_be_bytes());
        frame.extend_from_slice(payload);
        timeout(STEP, self.socket.write_all(&frame))
            .await
            .expect("write deadline")
            .expect("write");
    }

    pub async fn send_headers(&mut self, stream: u32, end: bool, fields: &[(&str, &str)]) {
        let mut block = Vec::new();
        for (name, value) in fields {
            block.push(0);
            block.push(name.len() as u8);
            block.extend_from_slice(name.as_bytes());
            block.push(value.len() as u8);
            block.extend_from_slice(value.as_bytes());
        }
        let flags = END_HEADERS | if end { END_STREAM } else { 0 };
        self.send(HEADERS, flags, stream, &block).await;
    }

    pub async fn respond(&mut self, stream: u32, status: &str, body: &[u8]) {
        self.send_headers(stream, body.is_empty(), &[(":status", status)])
            .await;
        if !body.is_empty() {
            self.send(DATA, END_STREAM, stream, body).await;
        }
    }

    pub async fn send_goaway(&mut self, last_stream: u32, code: u32) {
        let mut payload = last_stream.to_be_bytes().to_vec();
        payload.extend_from_slice(&code.to_be_bytes());
        self.send(GOAWAY, 0, 0, &payload).await;
    }

    pub async fn send_rst(&mut self, stream: u32, code: u32) {
        self.send(RST_STREAM, 0, stream, &code.to_be_bytes()).await;
    }

    pub async fn send_window_update(&mut self, stream: u32, increment: u32) {
        self.send(WINDOW_UPDATE, 0, stream, &increment.to_be_bytes())
            .await;
    }
}
