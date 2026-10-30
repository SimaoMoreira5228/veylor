use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use veylor_runtime::ConnectionToken;
use veylor_server::{
    EventSlot, HostEventKind, RespondOutcome, RunningServer, Server, ServerConfig,
};
use veylor_websocket::{OpCode, encode_frame};

const DEADLINE: Duration = Duration::from_secs(5);
const MASK: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];

fn managed_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_managed(b"GET", b"/ws", 1024)
        .expect("register managed");
    server.start().expect("start server")
}

fn connect(server: &RunningServer) -> TcpStream {
    let stream = TcpStream::connect(server.addr()).expect("connect loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
}

fn event_slot() -> EventSlot {
    EventSlot {
        kind: HostEventKind::Shutdown,
        token: ConnectionToken::default(),
        route_id: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    }
}

fn poll_request(host: &veylor_server::HostHandle) -> EventSlot {
    let deadline = Instant::now() + DEADLINE;
    let mut events = [event_slot(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    loop {
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::Request {
                return *event;
            }
        }
        assert!(Instant::now() < deadline, "request never arrived");
    }
}

fn poll_tunnel_data(host: &veylor_server::HostHandle, token: ConnectionToken) -> usize {
    let deadline = Instant::now() + DEADLINE;
    let mut events = [event_slot(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    loop {
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::TunnelData && event.token == token {
                return 1;
            }
        }
        assert!(Instant::now() < deadline, "tunnel data never arrived");
    }
}

fn poll_tunnel_closed(host: &veylor_server::HostHandle, token: ConnectionToken) {
    let deadline = Instant::now() + DEADLINE;
    let mut events = [event_slot(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    loop {
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::TunnelClosed && event.token == token {
                return;
            }
        }
        assert!(Instant::now() < deadline, "tunnel close never arrived");
    }
}

fn read_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read head");
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            return head;
        }
    }
}

#[test]
fn managed_websocket_upgrade_and_raw_tunnel() {
    let server = managed_server();
    let host = server.host();
    let mut client = connect(&server);
    let request = b"GET /ws HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: chat, superchat\r\n\r\n";
    client.write_all(request).expect("write handshake");

    let event = poll_request(&host);
    assert_eq!(
        host.upgrade_websocket_with_protocol(event.token, b"nope"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.upgrade_websocket_with_protocol(event.token, b"chat"),
        RespondOutcome::Ok
    );
    let head = read_head(&mut client);
    assert!(
        head.starts_with(b"HTTP/1.1 101 Switching Protocols\r\n"),
        "unexpected handshake: {}",
        String::from_utf8_lossy(&head)
    );
    assert!(
        head.windows(b"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".len())
            .any(|window| window == b"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
    );
    assert!(
        head.windows(b"Sec-WebSocket-Protocol: chat".len())
            .any(|window| window == b"Sec-WebSocket-Protocol: chat")
    );

    let mut frame = [0u8; 64];
    let frame_len = encode_frame(true, OpCode::Text, Some(MASK), b"hello", &mut frame).unwrap();
    client.write_all(&frame[..frame_len]).expect("write frame");

    poll_tunnel_data(&host, event.token);
    let mut inbound = [0u8; 64];
    let (outcome, count) = host.tunnel_read(event.token, &mut inbound);
    assert_eq!(outcome, RespondOutcome::Ok);
    assert_eq!(&inbound[..count], &frame[..frame_len]);

    let mut response = [0u8; 64];
    let response_len = encode_frame(true, OpCode::Text, None, b"world", &mut response).unwrap();
    assert_eq!(
        host.tunnel_write(event.token, &response[..response_len]),
        RespondOutcome::Ok
    );
    let mut echoed = [0u8; 64];
    client
        .read_exact(&mut echoed[..response_len])
        .expect("read tunnel frame");
    assert_eq!(&echoed[..response_len], &response[..response_len]);

    assert_eq!(host.tunnel_close(event.token), RespondOutcome::Ok);
    poll_tunnel_closed(&host, event.token);
    let mut tail = Vec::new();
    client.read_to_end(&mut tail).expect("read close");
    server.stop_accepting();
    let counts = server.join().expect("join");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn managed_upgrade_rejects_plain_request() {
    let server = managed_server();
    let host = server.host();
    let mut client = connect(&server);
    let request = b"GET /ws HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
    client.write_all(request).expect("write request");

    let event = poll_request(&host);
    assert_eq!(host.upgrade_websocket(event.token), RespondOutcome::Invalid);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"plain"),
        RespondOutcome::Ok
    );
    let mut head = [0u8; 256];
    let read = client.read(&mut head).expect("read response");
    assert!(head[..read].starts_with(b"HTTP/1.1 200 OK\r\n"));

    server.stop_accepting();
    server.join().expect("join");
}

#[test]
fn managed_tunnel_peer_close_retires_token() {
    use std::net::Shutdown;
    use std::thread;

    let server = managed_server();
    let host = server.host();
    let mut client = connect(&server);
    let request = b"GET /ws HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    client.write_all(request).expect("write handshake");

    let request_event = poll_request(&host);
    assert_eq!(
        host.upgrade_websocket(request_event.token),
        RespondOutcome::Ok
    );
    read_head(&mut client);

    let mut frame = [0u8; 64];
    let frame_len = encode_frame(true, OpCode::Close, Some(MASK), &[], &mut frame).unwrap();
    client.write_all(&frame[..frame_len]).expect("write close");
    client
        .shutdown(Shutdown::Write)
        .expect("shutdown write side");

    let deadline = Instant::now() + DEADLINE;
    let mut data_ready = false;
    let mut close_signalled = false;
    let mut events = [event_slot(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    while !data_ready || !close_signalled {
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::TunnelData && event.token == request_event.token {
                data_ready = true;
            }
            if event.kind == HostEventKind::TunnelClosed && event.token == request_event.token {
                close_signalled = true;
            }
        }
        assert!(Instant::now() < deadline, "tunnel close never arrived");
    }

    let mut inbound = [0u8; 64];
    let (outcome, count) = host.tunnel_read(request_event.token, &mut inbound);
    assert_eq!(outcome, RespondOutcome::Ok);
    assert_eq!(&inbound[..count], &frame[..frame_len]);

    while host.tunnel_read(request_event.token, &mut inbound).0 != RespondOutcome::Gone {
        assert!(Instant::now() < deadline, "tunnel never retired");
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        host.tunnel_write(request_event.token, &frame[..frame_len]),
        RespondOutcome::Gone
    );

    let mut tail = Vec::new();
    client.read_to_end(&mut tail).expect("read eof");
    server.stop_accepting();
    let counts = server.join().expect("join");
    assert_eq!(counts.peer_closed, 1, "counts: {counts:?}");
}
