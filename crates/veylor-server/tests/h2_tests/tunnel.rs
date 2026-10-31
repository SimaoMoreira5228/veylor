use super::*;
use veylor_websocket::{OpCode, encode_frame};

fn tunnel_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_managed(b"CONNECT", b"/ws", 1024)
        .expect("register connect");
    server.start().expect("start server")
}

fn poll_tunnel_event(host: &HostHandle, kind: HostEventKind, token: ConnectionToken) -> EventSlot {
    let deadline = Instant::now() + DEADLINE;
    let mut events = [host_slot(); 8];
    let mut methods = [0u8; 64];
    let mut paths = [0u8; 4096];
    loop {
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == kind && event.token == token {
                return *event;
            }
        }
        assert!(Instant::now() < deadline, "event never arrived: {kind:?}");
    }
}

fn decode_fields(block: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = [HpackEntrySlot::EMPTY; 16];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 16];
    let (count, _) = decode_header_block(block, &mut decoder, &mut decoded).expect("decode");
    decoded[..count]
        .iter()
        .map(|header| {
            (
                decoder.resolve(header.field.name).expect("name").to_vec(),
                decoder.resolve(header.field.value).expect("value").to_vec(),
            )
        })
        .collect()
}

#[test]
fn h2_extended_connect_tunnels_websocket_bytes() {
    let server = tunnel_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    assert_eq!(
        handshake_advertised_setting(&mut stream, veylor_http2::SETTINGS_ENABLE_CONNECT_PROTOCOL),
        1
    );

    let fields = [
        field(b":method", b"CONNECT"),
        field(b":protocol", b"websocket"),
        field(b":scheme", b"http"),
        field(b":path", b"/ws"),
        field(b":authority", b"localhost"),
        field(b"sec-websocket-protocol", b"chat, superchat"),
    ];
    send_header_fields(&mut stream, 1, &fields, false);

    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"CONNECT");
    assert_eq!(path, b"/ws");
    assert_eq!(
        host.upgrade_websocket_with_protocol(event.token, b"nope"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.upgrade_websocket_with_protocol(event.token, b"chat"),
        RespondOutcome::Ok
    );

    let mut block = Vec::new();
    loop {
        let (header, payload) = read_frame(&mut stream);
        if matches!(
            header.frame_type,
            FrameType::Settings | FrameType::WindowUpdate
        ) {
            continue;
        }
        assert_eq!(header.stream_id, 1);
        assert_eq!(header.frame_type, FrameType::Headers);
        block.extend_from_slice(&payload);
        if header.flags & HEADERS_FLAG_END_HEADERS != 0 {
            break;
        }
    }
    let response = decode_fields(&block);
    assert!(
        response
            .iter()
            .any(|(name, value)| name == b":status" && value == b"200")
    );
    assert!(
        response
            .iter()
            .any(|(name, value)| name == b"sec-websocket-protocol" && value == b"chat")
    );
    assert!(!response.iter().any(|(name, _)| name == b"content-length"));

    let mut frame = [0u8; 64];
    let frame_len = encode_frame(true, OpCode::Text, Some([1, 2, 3, 4]), b"hello", &mut frame)
        .expect("client frame");
    write_frame(&mut stream, FrameType::Data, 0, 1, &frame[..frame_len]);
    poll_tunnel_event(&host, HostEventKind::TunnelData, event.token);
    let mut inbound = [0u8; 64];
    let (outcome, count) = host.tunnel_read(event.token, &mut inbound);
    assert_eq!(outcome, RespondOutcome::Ok);
    assert_eq!(&inbound[..count], &frame[..frame_len]);

    let mut response = [0u8; 64];
    let response_len =
        encode_frame(true, OpCode::Text, None, b"world", &mut response).expect("server frame");
    assert_eq!(
        host.tunnel_write(event.token, &response[..response_len]),
        RespondOutcome::Ok
    );
    let mut echoed = Vec::new();
    loop {
        let (header, payload) = read_frame(&mut stream);
        if header.frame_type != FrameType::Data {
            continue;
        }
        assert_eq!(header.stream_id, 1);
        echoed.extend_from_slice(&payload);
        if echoed.len() >= response_len {
            break;
        }
    }
    assert_eq!(&echoed[..response_len], &response[..response_len]);

    assert_eq!(host.tunnel_close(event.token), RespondOutcome::Ok);
    loop {
        let (header, payload) = read_frame(&mut stream);
        if header.frame_type != FrameType::Data {
            continue;
        }
        assert_eq!(header.stream_id, 1);
        assert!(payload.is_empty());
        assert_eq!(header.flags & DATA_FLAG_END_STREAM, DATA_FLAG_END_STREAM);
        break;
    }
    poll_tunnel_event(&host, HostEventKind::TunnelClosed, event.token);
    assert_eq!(
        host.tunnel_read(event.token, &mut inbound),
        (RespondOutcome::Gone, 0)
    );

    drop(stream);
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 0, "counts: {counts:?}");
}

fn read_tunnel_exact(host: &HostHandle, token: ConnectionToken, length: usize) -> Vec<u8> {
    let deadline = Instant::now() + DEADLINE;
    let mut out = Vec::with_capacity(length);
    let mut chunk = [0u8; 8192];
    while out.len() < length {
        let (outcome, count) = host.tunnel_read(token, &mut chunk);
        match outcome {
            RespondOutcome::Ok => out.extend_from_slice(&chunk[..count]),
            RespondOutcome::WouldBlock => {}
            other => panic!("unexpected tunnel read outcome: {other:?}"),
        }
        assert!(
            Instant::now() < deadline,
            "tunnel bytes never arrived: got {} of {length}",
            out.len()
        );
    }
    assert_eq!(out.len(), length);
    out
}

fn read_window_updates(stream: &mut TcpStream, stream_id: u32, expected: u64) -> (u64, u64) {
    let deadline = Instant::now() + DEADLINE;
    let mut connection = 0u64;
    let mut target = 0u64;
    while connection < expected || target < expected {
        assert!(Instant::now() < deadline, "window updates never arrived");
        let (header, payload) = read_frame(stream);
        if header.frame_type != FrameType::WindowUpdate {
            continue;
        }
        assert_eq!(payload.len(), 4);
        let increment = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        if header.stream_id == 0 {
            connection += u64::from(increment);
        } else if header.stream_id == stream_id {
            target += u64::from(increment);
        }
    }
    (connection, target)
}

#[test]
fn h2_tunnel_grants_receive_window_and_handles_peer_end() {
    let server = tunnel_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    let fields = [
        field(b":method", b"CONNECT"),
        field(b":protocol", b"websocket"),
        field(b":scheme", b"http"),
        field(b":path", b"/ws"),
        field(b":authority", b"localhost"),
    ];
    send_header_fields(&mut stream, 1, &fields, false);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(host.upgrade_websocket(event.token), RespondOutcome::Ok);

    loop {
        let (header, payload) = read_frame(&mut stream);
        if header.frame_type == FrameType::Headers {
            assert_eq!(header.stream_id, 1);
            assert!(!payload.is_empty());
            break;
        }
    }

    let first = vec![0x41u8; 48 * 1024];
    for chunk in first.chunks(16_384) {
        write_frame(&mut stream, FrameType::Data, 0, 1, chunk);
    }
    assert_eq!(read_tunnel_exact(&host, event.token, first.len()), first);
    let (connection_credit, stream_credit) =
        read_window_updates(&mut stream, 1, first.len() as u64);
    assert!(connection_credit >= first.len() as u64);
    assert!(stream_credit >= first.len() as u64);

    let second = vec![0x42u8; 48 * 1024];
    for (index, chunk) in second.chunks(16_384).enumerate() {
        let end = (index + 1) * 16_384 >= second.len();
        let flags = if end { DATA_FLAG_END_STREAM } else { 0 };
        write_frame(&mut stream, FrameType::Data, flags, 1, chunk);
    }
    assert_eq!(read_tunnel_exact(&host, event.token, second.len()), second);
    poll_tunnel_event(&host, HostEventKind::TunnelClosed, event.token);

    assert_eq!(host.tunnel_close(event.token), RespondOutcome::Ok);
    loop {
        let (header, payload) = read_frame(&mut stream);
        if header.frame_type != FrameType::Data {
            continue;
        }
        assert_eq!(header.stream_id, 1);
        assert!(payload.is_empty());
        assert_eq!(header.flags & DATA_FLAG_END_STREAM, DATA_FLAG_END_STREAM);
        break;
    }

    drop(stream);
    stop_join(server);
}
