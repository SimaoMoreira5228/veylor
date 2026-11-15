use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM,
    DEFAULT_MAX_FRAME_SIZE, ErrorCode, FRAME_HEADER_LEN, FrameHeader, FrameType,
    HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackFieldToEncode,
    HpackHeader, HpackRepresentation, HpackTable, SETTINGS_ACK, decode_frame_header,
    decode_goaway_payload, decode_header_block, encode_frame_header, encode_header_block,
};
use veylor_runtime::ConnectionToken;
use veylor_server::{
    EventSlot, HostEventKind, HostHandle, MAX_STATIC_BODY_BYTES, RespondOutcome, RunningServer,
    Server, ServerConfig, TlsConfig,
};
use veylor_tls::{TlsConnector, TlsStream};

const CERT_DER: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_cert.der");
const KEY_DER: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_key.der");
const CERT2_DER: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_cert2.der");
const KEY2_DER: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_key2.der");
const CERT2_PEM: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_cert2.pem");
const KEY2_PEM: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_key2.pem");
const CERT_PEM: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_cert.pem");
const KEY_PEM: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_key.pem");

const DEADLINE: Duration = Duration::from_secs(5);

type Tls = TlsStream<TcpStream>;

fn connector_for(cert: &[u8], alpn: &[u8]) -> TlsConnector {
    TlsConnector::builder()
        .tls13_only(true)
        .pin_certificate(cert)
        .expect("pin fixture")
        .alpn(&[alpn])
        .build()
        .expect("connector")
}

fn client_config() -> TlsConnector {
    connector_for(CERT_DER, b"http/1.1")
}

fn client_config_for(cert: &[u8]) -> TlsConnector {
    connector_for(cert, b"http/1.1")
}

fn h2_client_config() -> TlsConnector {
    connector_for(CERT_DER, b"h2")
}

fn tls_config() -> TlsConfig {
    TlsConfig {
        cert_chain: vec![veylor_server::Certificate::from(CERT_DER.to_vec())],
        key: veylor_server::PrivateKey::Pkcs8(KEY_DER.to_vec()),
        max_handshakes: 16,
    }
}

fn server_config_tls() -> ServerConfig {
    ServerConfig {
        tls: Some(tls_config()),
        ..ServerConfig::default()
    }
}

fn test_server_with_tls() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, server_config_tls()).expect("bind tls");

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

    server.start().expect("start tls")
}

fn connect_raw(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).expect("connect");

    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");

    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");

    stream
}

fn handshake(sock: TcpStream, connector: &TlsConnector) -> Tls {
    try_handshake(sock, connector).expect("handshake succeeds")
}

fn try_handshake(sock: TcpStream, connector: &TlsConnector) -> Result<Tls, String> {
    let conn = connector
        .connect("localhost")
        .map_err(|error| format!("client builds: {error}"))?;
    let mut stream = TlsStream::new(sock, conn);

    stream
        .handshake()
        .map_err(|error| format!("handshake: {error:?}"))?;

    Ok(stream)
}

fn tls_write(client: &mut Tls, plain: &[u8]) {
    client.write_all(plain).expect("write request");
}

fn tls_read_response(client: &mut Tls) -> Vec<u8> {
    let mut plain = Vec::new();
    let mut chunk = [0u8; 4096];

    loop {
        match client.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => plain.extend_from_slice(&chunk[..n]),
        }

        if let Some(end) = find_headers_end(&plain) {
            let content_len = parse_content_length(&plain[..end]).unwrap_or(0);

            if plain.len() - end >= content_len {
                break;
            }
        }
    }

    plain
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

fn parse_content_length(head: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(head).ok()?;

    for line in text.lines().skip(1) {
        if line.is_empty() {
            break;
        }

        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            return value.trim().parse().ok();
        }
    }

    None
}

fn split_response(raw: &[u8]) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let end = find_headers_end(raw).expect("headers end");
    let head = std::str::from_utf8(&raw[..end]).expect("head utf8");
    let mut lines = head.lines();
    let status_line = lines.next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status int");
    let mut headers = Vec::new();

    for line in lines {
        if line.is_empty() {
            continue;
        }

        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_string(), value.trim().to_string()));
        }
    }

    (status, headers, raw[end..].to_vec())
}

#[test]
fn tls_handshake_negotiates_http11_and_serves_static() {
    let running = test_server_with_tls();
    let addr = running.addr();
    let config = client_config();
    let mut client = handshake(connect_raw(addr), &config);

    assert_eq!(
        client.alpn(),
        Some(b"http/1.1".as_slice()),
        "client observes http/1.1"
    );

    tls_write(
        &mut client,
        b"GET /hello HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
    );

    let raw = tls_read_response(&mut client);
    let (status, _, body) = split_response(&raw);

    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    running.stop_accepting();
    running.join().expect("join clean");
}

#[test]
fn tls_file_response_matches_plain_bytes() {
    let dir = std::env::temp_dir().join(format!("veylor-tls-{}", std::process::id()));

    let _ = std::fs::create_dir_all(&dir);

    let mut content = Vec::with_capacity(32768);

    for index in 0..32768 {
        content.push((index % 251) as u8);
    }

    std::fs::write(dir.join("test.bin"), &content).expect("write fixture");

    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, server_config_tls()).expect("bind tls");

    server
        .register_file_mount(b"/files/", &dir)
        .expect("mount files");

    let running = server.start().expect("start tls");
    let sock_addr = running.addr();
    let config = client_config();
    let mut client = handshake(connect_raw(sock_addr), &config);

    tls_write(
        &mut client,
        b"GET /files/test.bin HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
    );

    let raw = tls_read_response(&mut client);
    let (status, _, body) = split_response(&raw);

    assert_eq!(status, 200);
    assert_eq!(body, content);

    running.stop_accepting();
    running.join().expect("join clean");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tls_closed_peer_during_response_closes_cleanly() {
    let dir = std::env::temp_dir().join(format!("veylor-tls-close-{}", std::process::id()));

    let _ = std::fs::create_dir_all(&dir);

    let content = vec![0x41u8; 65536];
    std::fs::write(dir.join("big.bin"), &content).expect("write big");

    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, server_config_tls()).expect("bind tls");

    server
        .register_file_mount(b"/files/", &dir)
        .expect("mount files");

    let running = server.start().expect("start tls");
    let sock_addr = running.addr();
    let config = client_config();
    let mut client = handshake(connect_raw(sock_addr), &config);

    tls_write(
        &mut client,
        b"GET /files/big.bin HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
    );

    drop(client);

    std::thread::sleep(Duration::from_millis(300));

    running.stop_accepting();
    running.join().expect("join clean");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tls_gzip_route_round_trips() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, server_config_tls()).expect("bind tls");

    let mut gzip_body = Vec::new();

    for _ in 0..50 {
        gzip_body.extend_from_slice(b"The quick brown fox jumps over the lazy dog. ");
    }

    server
        .register_gzip_route(
            b"GET",
            b"/gzip",
            200,
            b"OK",
            &gzip_body,
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register gzip");

    let running = server.start().expect("start tls");
    let sock_addr = running.addr();
    let config = client_config();
    let mut client = handshake(connect_raw(sock_addr), &config);

    tls_write(
        &mut client,
        b"GET /gzip HTTP/1.1\r\nHost: test\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n",
    );

    let raw = tls_read_response(&mut client);
    let (status, headers, body) = split_response(&raw);

    assert_eq!(status, 200);

    let encoding = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-encoding"))
        .map(|(_, value)| value.clone())
        .unwrap_or_default();

    assert_eq!(encoding.to_ascii_lowercase(), "gzip");
    assert!(!body.is_empty());

    let mut decoder = flate2::read::GzDecoder::new(&body[..]);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).expect("gunzip");

    assert_eq!(decoded, gzip_body);

    running.stop_accepting();
    running.join().expect("join clean");
}

fn h2_tls_write_frame(client: &mut Tls, frame_type: FrameType, flags: u8, id: u32, payload: &[u8]) {
    let header = FrameHeader {
        payload_length: payload.len() as u32,
        frame_type,
        flags,
        stream_id: id,
    };
    let mut encoded = [0; FRAME_HEADER_LEN];
    encode_frame_header(header, &mut encoded, DEFAULT_MAX_FRAME_SIZE).expect("encode header");
    client.write_all(&encoded).expect("write header");
    client.write_all(payload).expect("write payload");
}

fn h2_tls_drain_plaintext(client: &mut Tls, stash: &mut Vec<u8>) {
    let mut chunk = [0u8; 4096];

    match client.read(&mut chunk) {
        Ok(0) => panic!("eof while reading h2 frame"),
        Ok(n) => stash.extend_from_slice(&chunk[..n]),
        Err(error) => panic!("wire read during h2 frame: {error:?}"),
    }
}

fn h2_tls_read_frame(client: &mut Tls, stash: &mut Vec<u8>) -> (FrameHeader, Vec<u8>) {
    for _ in 0..200 {
        if stash.len() >= FRAME_HEADER_LEN {
            let header = decode_frame_header(&stash[..FRAME_HEADER_LEN], DEFAULT_MAX_FRAME_SIZE)
                .expect("decode header");
            let total = FRAME_HEADER_LEN + header.payload_length as usize;
            if stash.len() >= total {
                let payload = stash[FRAME_HEADER_LEN..total].to_vec();
                stash.drain(..total);
                return (header, payload);
            }
        }
        h2_tls_drain_plaintext(client, stash);
    }
    panic!("h2 frame read timed out");
}

fn h2_field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

fn h2_tls_send_headers(client: &mut Tls, id: u32, method: &[u8], path: &[u8]) {
    let fields = vec![
        h2_field(b":method", method),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", path),
        h2_field(b":authority", b"localhost"),
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 2048];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    let flags = HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM;
    h2_tls_write_frame(client, FrameType::Headers, flags, id, &block[..len]);
}

fn h2_decode_status(block: &[u8]) -> u16 {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (count, _) = decode_header_block(block, &mut decoder, &mut decoded).expect("decode block");
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        if name == b":status" {
            let value = decoder.resolve(header.field.value).expect("value");
            return std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    panic!("response block has no :status");
}

fn h2_tls_read_response(client: &mut Tls, stash: &mut Vec<u8>, id: u32) -> (u16, Vec<u8>) {
    let mut block = Vec::new();
    let mut headers_ended_stream = false;
    loop {
        let (header, payload) = h2_tls_read_frame(client, stash);
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
    let status = h2_decode_status(&block);
    let mut body = Vec::new();
    if !headers_ended_stream {
        loop {
            let (header, payload) = h2_tls_read_frame(client, stash);
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

#[test]
fn tls_alpn_h2_serves_static_get() {
    let running = test_server_with_tls();
    let addr = running.addr();
    let config = h2_client_config();
    let mut client = handshake(connect_raw(addr), &config);

    assert_eq!(client.alpn(), Some(b"h2".as_slice()), "client observes h2");

    let mut stash = Vec::new();
    client
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write preface");
    h2_tls_write_frame(&mut client, FrameType::Settings, 0, 0, &[]);

    let (header, _) = h2_tls_read_frame(&mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.stream_id, 0);

    h2_tls_write_frame(&mut client, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = h2_tls_read_frame(&mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);

    h2_tls_send_headers(&mut client, 1, b"GET", b"/hello");
    let (status, body) = h2_tls_read_response(&mut client, &mut stash, 1);

    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    running.stop_accepting();
    running.join().expect("join clean");
}

#[test]
fn tls_h2_missing_preface_goes_away_and_closes() {
    let running = test_server_with_tls();
    let addr = running.addr();
    let config = h2_client_config();
    let mut client = handshake(connect_raw(addr), &config);

    assert_eq!(client.alpn(), Some(b"h2".as_slice()), "client observes h2");

    let mut stash = Vec::new();
    h2_tls_write_frame(&mut client, FrameType::Settings, 0, 0, &[]);

    let (header, _) = h2_tls_read_frame(&mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);
    let (header, payload) = h2_tls_read_frame(&mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::GoAway);
    let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
    assert_eq!(goaway.error_code, ErrorCode::ProtocolError);

    let mut closed = false;
    let mut chunk = [0u8; 4096];

    for _ in 0..200 {
        match client.read(&mut chunk) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                closed = true;
                break;
            }
            Err(_) => break,
        }
    }
    assert!(closed, "server closed the connection after GOAWAY");

    running.stop_accepting();
    let counts = running.join().expect("join clean");
    assert_eq!(counts.protocol_error, 1);
}

fn poll_tls_request(host: &HostHandle) -> EventSlot {
    let deadline = std::time::Instant::now() + DEADLINE;
    loop {
        let mut events = vec![empty_tls_slot(); 8];
        let mut methods = vec![0; 64];
        let mut paths = vec![0; 4096];
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::Request {
                return *event;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for request"
        );
    }
}

fn empty_tls_slot() -> EventSlot {
    EventSlot {
        kind: HostEventKind::Shutdown,
        token: ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 0,
            generation: 0,
        },
        route_id: 0,
        route_token: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    }
}

fn tls_managed_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, server_config_tls()).expect("bind tls");
    server
        .register_managed(b"GET", b"/work", MAX_STATIC_BODY_BYTES)
        .expect("register managed");
    server.start().expect("start tls")
}

#[test]
fn tls_managed_park_respond_round_trip() {
    let running = tls_managed_server();
    let host = running.host();
    let config = client_config();
    let mut client = handshake(connect_raw(running.addr()), &config);

    tls_write(
        &mut client,
        b"GET /work HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n",
    );

    let event = poll_tls_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"managed-tls"),
        RespondOutcome::Ok
    );

    let raw = tls_read_response(&mut client);
    let (status, _, body) = split_response(&raw);
    assert_eq!(status, 200);
    assert_eq!(body, b"managed-tls");

    running.stop_accepting();
    let counts = running.join().expect("join clean");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
}

#[test]
fn tls_h2_managed_park_respond_round_trip() {
    let running = tls_managed_server();
    let host = running.host();
    let config = h2_client_config();
    let mut client = handshake(connect_raw(running.addr()), &config);

    assert_eq!(client.alpn(), Some(b"h2".as_slice()), "client observes h2");

    let mut stash = Vec::new();
    client
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write preface");
    h2_tls_write_frame(&mut client, FrameType::Settings, 0, 0, &[]);

    let (header, _) = h2_tls_read_frame(&mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.stream_id, 0);

    h2_tls_write_frame(&mut client, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = h2_tls_read_frame(&mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);

    h2_tls_send_headers(&mut client, 1, b"GET", b"/work");

    let event = poll_tls_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"managed-h2-tls"),
        RespondOutcome::Ok
    );

    let (status, body) = h2_tls_read_response(&mut client, &mut stash, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"managed-h2-tls");

    running.stop_accepting();
    let counts = running.join().expect("join clean");
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

fn write_pem_fixtures() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("veylor-tls-pem-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    std::fs::write(dir.join("cert.pem"), CERT_PEM).expect("write cert pem");
    std::fs::write(dir.join("key.pem"), KEY_PEM).expect("write key pem");
    dir
}

#[test]
fn tls_config_from_pem_files_serves_static() {
    use veylor_server::TlsConfigError;

    let dir = write_pem_fixtures();
    let config = TlsConfig::from_files(&dir.join("cert.pem"), &dir.join("key.pem"), 16)
        .expect("load pem files");
    assert_eq!(config.cert_chain.len(), 1);
    assert_eq!(config.cert_chain[0].as_ref(), CERT_DER);

    let missing = TlsConfig::from_files(&dir.join("nope.pem"), &dir.join("key.pem"), 16);
    assert!(
        matches!(missing, Err(TlsConfigError::Io(_))),
        "missing file is Io"
    );

    std::fs::write(dir.join("junk.pem"), b"not pem at all").expect("write junk");
    let junk = TlsConfig::from_files(&dir.join("cert.pem"), &dir.join("junk.pem"), 16);
    assert!(
        matches!(junk, Err(TlsConfigError::InvalidInput)),
        "junk key is InvalidInput"
    );

    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(
        addr,
        ServerConfig {
            tls: Some(config),
            ..ServerConfig::default()
        },
    )
    .expect("bind tls");
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
    let running = server.start().expect("start tls");

    let client_config = client_config();
    let mut client = handshake(connect_raw(running.addr()), &client_config);
    assert_eq!(
        client.alpn(),
        Some(b"http/1.1".as_slice()),
        "client observes http/1.1"
    );
    tls_write(
        &mut client,
        b"GET /hello HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
    );

    let raw = tls_read_response(&mut client);
    let (status, _, body) = split_response(&raw);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    running.stop_accepting();
    running.join().expect("join clean");
    let _ = std::fs::remove_dir_all(&dir);
}

fn tls_hello_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, server_config_tls()).expect("bind tls");
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
    server.start().expect("start tls")
}

fn tls_get_hello(client: &mut Tls, close: bool) -> (u16, Vec<u8>) {
    let connection = if close { "close" } else { "keep-alive" };
    tls_write(
        client,
        format!("GET /hello HTTP/1.1\r\nHost: test\r\nConnection: {connection}\r\n\r\n").as_bytes(),
    );

    let raw = tls_read_response(client);
    let (status, _, body) = split_response(&raw);

    (status, body)
}

#[test]
fn tls_rotate_serves_new_cert_on_new_conns_and_keeps_old_ones() {
    let running = tls_hello_server();

    let mut client1 = handshake(connect_raw(running.addr()), &client_config());
    let (status, body) = tls_get_hello(&mut client1, false);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    running
        .rotate_tls(CERT2_DER, KEY2_DER)
        .expect("rotate to second cert");

    let config2 = client_config_for(CERT2_DER);
    let deadline = std::time::Instant::now() + DEADLINE;
    let mut client2 = loop {
        match try_handshake(connect_raw(running.addr()), &config2) {
            Ok(pair) => break pair,
            Err(_) if std::time::Instant::now() < deadline => continue,
            Err(error) => panic!("rotated handshake never landed: {error}"),
        }
    };
    assert_eq!(
        client2.alpn(),
        Some(b"http/1.1".as_slice()),
        "rotated conn negotiates http/1.1"
    );
    let (status, body) = tls_get_hello(&mut client2, true);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    let (status, body) = tls_get_hello(&mut client1, true);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    running
        .rotate_tls(CERT2_PEM, KEY2_PEM)
        .expect("rotate with pem inputs");
    let deadline = std::time::Instant::now() + DEADLINE;
    let mut client3 = loop {
        match try_handshake(connect_raw(running.addr()), &client_config_for(CERT2_DER)) {
            Ok(pair) => break pair,
            Err(_) if std::time::Instant::now() < deadline => continue,
            Err(error) => panic!("pem-rotated handshake never landed: {error}"),
        }
    };
    let (status, body) = tls_get_hello(&mut client3, true);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    running.stop_accepting();
    running.join().expect("join clean");
}

#[test]
fn tls_rotate_rejects_garbage_and_non_tls() {
    let running = tls_hello_server();

    let mut client = handshake(connect_raw(running.addr()), &client_config());
    let (status, body) = tls_get_hello(&mut client, false);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    assert!(running.rotate_tls(b"not a cert", b"not a key").is_err());
    assert!(running.rotate_tls(&[], &[]).is_err());

    let (status, body) = tls_get_hello(&mut client, true);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    let plain_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let plain = Server::bind(plain_addr, ServerConfig::default())
        .expect("bind plain")
        .start()
        .expect("start plain");
    assert!(plain.rotate_tls(CERT2_DER, KEY2_DER).is_err());
    plain.stop_accepting();
    plain.join().expect("join clean");

    running.stop_accepting();
    running.join().expect("join clean");
}
