use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls::pki_types::{CertificateDer, ServerName};
use veylor_runtime::ConnectionToken;
use veylor_server::{
    EventSlot, HostEventKind, HostHandle, RespondOutcome, RunningServer, Server, ServerConfig,
    TlsConfig,
};
use veylor_websocket::{OpCode, encode_frame};

const CERT_DER: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_cert.der");
const KEY_DER: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_key.der");
const DEADLINE: Duration = Duration::from_secs(5);
const MASK: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];

#[derive(Debug)]
struct PinVerifier {
    expected: Vec<u8>,
    inner: Arc<rustls::client::WebPkiServerVerifier>,
}

impl PinVerifier {
    fn new(cert_der: &[u8]) -> Self {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(cert_der.to_vec()))
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
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
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
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("TLS 1.2 disabled".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
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

fn tls_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        tls: Some(TlsConfig {
            cert_chain: vec![veylor_server::CertificateDer::from(CERT_DER.to_vec())],
            key: veylor_server::PrivateKeyDer::Pkcs8(KEY_DER.to_vec().into()),
            max_handshakes: 16,
        }),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind tls");
    server
        .register_managed(b"GET", b"/ws", 1024)
        .expect("register managed");
    server.start().expect("start server")
}

fn connect(server: &RunningServer) -> rustls::StreamOwned<rustls::ClientConnection, TcpStream> {
    let socket = TcpStream::connect(server.addr()).expect("connect loopback");
    socket
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    socket
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    let name = ServerName::try_from("localhost").expect("server name");
    let connection = rustls::ClientConnection::new(client_config(), name).expect("client hello");
    rustls::StreamOwned::new(connection, socket)
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

fn poll_kind(host: &HostHandle, kind: HostEventKind) -> EventSlot {
    let deadline = Instant::now() + DEADLINE;
    let mut events = [event_slot(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    loop {
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == kind {
                return *event;
            }
        }
        assert!(Instant::now() < deadline, "event never arrived: {kind:?}");
    }
}

fn poll_tunnel_data(host: &HostHandle, token: ConnectionToken) -> EventSlot {
    let deadline = Instant::now() + DEADLINE;
    let mut events = [event_slot(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    loop {
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::TunnelData && event.token == token {
                return *event;
            }
        }
        assert!(Instant::now() < deadline, "tunnel data never arrived");
    }
}

fn read_head(stream: &mut impl Read) -> Vec<u8> {
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
fn managed_websocket_upgrade_over_tls() {
    let server = tls_server();
    let host = server.host();
    let mut client = connect(&server);
    let request = b"GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    client.write_all(request).expect("write handshake");

    let event = poll_kind(&host, HostEventKind::Request);
    assert_eq!(host.upgrade_websocket(event.token), RespondOutcome::Ok);
    let head = read_head(&mut client);
    assert!(
        head.starts_with(b"HTTP/1.1 101 Switching Protocols\r\n"),
        "unexpected handshake: {}",
        String::from_utf8_lossy(&head)
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
    let closed = poll_kind(&host, HostEventKind::TunnelClosed);
    assert_eq!(closed.token, event.token);
    let mut tail = Vec::new();
    client.read_to_end(&mut tail).expect("read close");
    server.stop_accepting();
    let counts = server.join().expect("join");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}
