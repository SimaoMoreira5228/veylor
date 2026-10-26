use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM,
    DEFAULT_MAX_FRAME_SIZE, ErrorCode, FRAME_HEADER_LEN, FrameHeader, FrameType,
    HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackFieldToEncode,
    HpackHeader, HpackRepresentation, HpackTable, SETTINGS_ACK, decode_frame_header,
    decode_goaway_payload, decode_header_block, encode_frame_header, encode_header_block,
};
use veylor_server::{MAX_STATIC_BODY_BYTES, RunningServer, Server, ServerConfig, TlsConfig};

const CERT_DER: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_cert.der");
const KEY_DER: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_key.der");

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct PinVerifier {
    expected: Vec<u8>,
    inner: Arc<rustls::client::WebPkiServerVerifier>,
}

impl PinVerifier {
    fn new(cert_der: &[u8]) -> Self {
        let mut roots = rustls::RootCertStore::empty();

        roots
            .add(rustls::pki_types::CertificateDer::from(cert_der.to_vec()))
            .expect("fixture anchor");

        let inner = rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .expect("verifier builds");

        Self {
            expected: cert_der.to_vec(),
            inner,
        }
    }
}

impl rustls::client::danger::ServerCertVerifier for PinVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.expected.as_slice() {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("TLS 1.2 disabled".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn client_config() -> Arc<rustls::ClientConfig> {
    let verifier = Arc::new(PinVerifier::new(CERT_DER));

    let mut config =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();

    config.alpn_protocols = vec![b"http/1.1".to_vec()];

    Arc::new(config)
}

fn tls_config() -> TlsConfig {
    TlsConfig {
        cert_der: CERT_DER.to_vec(),
        key_der: KEY_DER.to_vec(),
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

fn handshake(
    mut sock: TcpStream,
    config: &Arc<rustls::ClientConfig>,
) -> (TcpStream, rustls::ClientConnection) {
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("dns name");

    let mut client =
        rustls::ClientConnection::new(Arc::clone(config), name).expect("client builds");

    for _ in 0..100 {
        if client.wants_write() {
            client.write_tls(&mut sock).expect("client flight");
        }

        if !client.is_handshaking() {
            break;
        }

        match client.read_tls(&mut sock) {
            Ok(0) => panic!("eof during handshake"),
            Ok(_) => {
                client.process_new_packets().expect("client processes");
            }
            Err(error) => panic!("read during handshake: {error:?}"),
        }

        if !client.is_handshaking() {
            break;
        }
    }

    assert!(!client.is_handshaking(), "client handshake done");

    (sock, client)
}

fn tls_write(sock: &mut TcpStream, client: &mut rustls::ClientConnection, plain: &[u8]) {
    client.writer().write_all(plain).expect("queue request");
    client.write_tls(sock).expect("emit request");
}

fn tls_read_response(sock: &mut TcpStream, client: &mut rustls::ClientConnection) -> Vec<u8> {
    let mut plain = Vec::new();
    let mut chunk = [0u8; 4096];

    for _ in 0..200 {
        match client.read_tls(sock) {
            Ok(0) => break,
            Ok(_) => {
                client.process_new_packets().expect("process flight");
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(_) => break,
        }

        loop {
            match client.reader().read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => plain.extend_from_slice(&chunk[..n]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(_) => break,
            }
        }

        if let Some(end) = find_headers_end(&plain) {
            let content_len = parse_content_length(&plain[..end]).unwrap_or(0);
            let body_available = plain.len() - end;

            if body_available >= content_len {
                break;
            }
        }
    }

    loop {
        match client.reader().read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => plain.extend_from_slice(&chunk[..n]),
            Err(_) => break,
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

        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                return value.trim().parse().ok();
            }
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
    let (mut sock, mut client) = handshake(connect_raw(addr), &config);

    assert_eq!(
        client.alpn_protocol(),
        Some(b"http/1.1".as_slice()),
        "client observes http/1.1"
    );

    tls_write(
        &mut sock,
        &mut client,
        b"GET /hello HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
    );

    let raw = tls_read_response(&mut sock, &mut client);
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
    let (mut sock, mut client) = handshake(connect_raw(sock_addr), &config);

    tls_write(
        &mut sock,
        &mut client,
        b"GET /files/test.bin HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
    );

    let raw = tls_read_response(&mut sock, &mut client);
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
    let (mut sock, mut client) = handshake(connect_raw(sock_addr), &config);

    tls_write(
        &mut sock,
        &mut client,
        b"GET /files/big.bin HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
    );

    drop(sock);
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
    let (mut sock, mut client) = handshake(connect_raw(sock_addr), &config);

    tls_write(
        &mut sock,
        &mut client,
        b"GET /gzip HTTP/1.1\r\nHost: test\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n",
    );

    let raw = tls_read_response(&mut sock, &mut client);
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

fn h2_client_config() -> Arc<rustls::ClientConfig> {
    let verifier = Arc::new(PinVerifier::new(CERT_DER));

    let mut config =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();

    config.alpn_protocols = vec![b"h2".to_vec()];

    Arc::new(config)
}

fn h2_tls_write_frame(
    sock: &mut TcpStream,
    client: &mut rustls::ClientConnection,
    frame_type: FrameType,
    flags: u8,
    id: u32,
    payload: &[u8],
) {
    let header = FrameHeader {
        payload_length: payload.len() as u32,
        frame_type,
        flags,
        stream_id: id,
    };
    let mut encoded = [0; FRAME_HEADER_LEN];
    encode_frame_header(header, &mut encoded, DEFAULT_MAX_FRAME_SIZE).expect("encode header");
    client.writer().write_all(&encoded).expect("queue header");
    client.writer().write_all(payload).expect("queue payload");
    client.write_tls(sock).expect("emit frame");
}

fn h2_tls_drain_plaintext(
    sock: &mut TcpStream,
    client: &mut rustls::ClientConnection,
    stash: &mut Vec<u8>,
) {
    match client.read_tls(sock) {
        Ok(0) => panic!("eof while reading h2 frame"),
        Ok(_) => {
            client.process_new_packets().expect("process flight");
        }
        Err(error) => panic!("wire read during h2 frame: {error:?}"),
    }
    let mut chunk = [0u8; 4096];
    loop {
        match client.reader().read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => stash.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
}

fn h2_tls_read_frame(
    sock: &mut TcpStream,
    client: &mut rustls::ClientConnection,
    stash: &mut Vec<u8>,
) -> (FrameHeader, Vec<u8>) {
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
        h2_tls_drain_plaintext(sock, client, stash);
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

fn h2_tls_send_headers(
    sock: &mut TcpStream,
    client: &mut rustls::ClientConnection,
    id: u32,
    method: &[u8],
    path: &[u8],
) {
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
    h2_tls_write_frame(sock, client, FrameType::Headers, flags, id, &block[..len]);
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

fn h2_tls_read_response(
    sock: &mut TcpStream,
    client: &mut rustls::ClientConnection,
    stash: &mut Vec<u8>,
    id: u32,
) -> (u16, Vec<u8>) {
    let mut block = Vec::new();
    let mut headers_ended_stream = false;
    loop {
        let (header, payload) = h2_tls_read_frame(sock, client, stash);
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
            let (header, payload) = h2_tls_read_frame(sock, client, stash);
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
    let (mut sock, mut client) = handshake(connect_raw(addr), &config);

    assert_eq!(
        client.alpn_protocol(),
        Some(b"h2".as_slice()),
        "client observes h2"
    );

    let mut stash = Vec::new();
    client
        .writer()
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("queue preface");
    client.write_tls(&mut sock).expect("emit preface");
    h2_tls_write_frame(&mut sock, &mut client, FrameType::Settings, 0, 0, &[]);

    let (header, _) = h2_tls_read_frame(&mut sock, &mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.stream_id, 0);

    h2_tls_write_frame(
        &mut sock,
        &mut client,
        FrameType::Settings,
        SETTINGS_ACK,
        0,
        &[],
    );
    let (header, _) = h2_tls_read_frame(&mut sock, &mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);

    h2_tls_send_headers(&mut sock, &mut client, 1, b"GET", b"/hello");
    let (status, body) = h2_tls_read_response(&mut sock, &mut client, &mut stash, 1);

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
    let (mut sock, mut client) = handshake(connect_raw(addr), &config);

    assert_eq!(
        client.alpn_protocol(),
        Some(b"h2".as_slice()),
        "client observes h2"
    );

    let mut stash = Vec::new();
    h2_tls_write_frame(&mut sock, &mut client, FrameType::Settings, 0, 0, &[]);

    let (header, _) = h2_tls_read_frame(&mut sock, &mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);
    let (header, payload) = h2_tls_read_frame(&mut sock, &mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::GoAway);
    let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
    assert_eq!(goaway.error_code, ErrorCode::ProtocolError);

    let mut closed = false;
    for _ in 0..200 {
        match client.read_tls(&mut sock) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(_) => {
                client.process_new_packets().expect("process close");
                let mut chunk = [0u8; 4096];
                loop {
                    match client.reader().read(&mut chunk) {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            }
            Err(_) => break,
        }
    }
    assert!(closed, "server closed the connection after GOAWAY");

    running.stop_accepting();
    let counts = running.join().expect("join clean");
    assert_eq!(counts.protocol_error, 1);
}
