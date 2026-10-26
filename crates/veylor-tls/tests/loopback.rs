use std::io::{Read as _, Write as _};
use std::sync::Arc;

use veylor_tls::{
    MAX_TLS_CONN_BYTES, TlsAcceptor, TlsConn, TlsError, alpn, drain_plaintext, feed_ciphertext,
    negotiated_version,
};

const CERT_DER: &[u8] = include_bytes!("testdata/server_cert.der");
const KEY_DER: &[u8] = include_bytes!("testdata/server_key.der");

const ROUND_TRIP_BYTES: usize = 10 * 1024;
const MAX_WIRE_BYTES: u64 = 1 << 20;
const HANDSHAKE_STEPS: usize = 100;

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
            .expect("fixture cert is a valid trust anchor");

        let inner = rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .expect("verifier builds over fixture anchor");

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
        Err(rustls::Error::General("TLS 1.2 is disabled".into()))
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

fn client_conn(config: &Arc<rustls::ClientConfig>) -> rustls::ClientConnection {
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("valid DNS name");

    rustls::ClientConnection::new(Arc::clone(config), name).expect("client connection builds")
}

fn feed_client(client: &mut rustls::ClientConnection, s2c: &mut Vec<u8>) {
    let mut cursor: &[u8] = s2c;
    let mut consumed = 0;

    while !cursor.is_empty() {
        let read = client.read_tls(&mut cursor).expect("client read succeeds");

        if read == 0 {
            break;
        }

        consumed += read;

        client
            .process_new_packets()
            .expect("client processes flight");
    }

    s2c.drain(..consumed);

    assert!(s2c.is_empty(), "client consumed the whole flight");
}

fn step_handshake(
    server: &mut TlsConn,
    client: &mut rustls::ClientConnection,
    c2s: &mut Vec<u8>,
    s2c: &mut Vec<u8>,
    server_plain: &mut Vec<u8>,
) {
    if client.wants_write() {
        client.write_tls(c2s).expect("client flight emits");
    }

    let consumed = feed_ciphertext(server, c2s, server_plain).expect("server feed succeeds");

    c2s.drain(..consumed);

    drain_plaintext(server, &[], s2c).expect("server flight flushes");

    feed_client(client, s2c);
}

fn handshake(
    server: &mut TlsConn,
    client: &mut rustls::ClientConnection,
    c2s: &mut Vec<u8>,
    s2c: &mut Vec<u8>,
) -> Vec<u8> {
    let mut server_plain = Vec::new();

    for _ in 0..HANDSHAKE_STEPS {
        if !client.is_handshaking() && !server.is_handshaking() {
            break;
        }

        step_handshake(server, client, c2s, s2c, &mut server_plain);
    }

    assert!(!client.is_handshaking(), "client finished the handshake");

    assert!(!server.is_handshaking(), "server finished the handshake");

    assert!(c2s.is_empty(), "client flight fully consumed");

    assert!(s2c.is_empty(), "server flight fully consumed");

    server_plain
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);

    for i in 0..len {
        out.push(seed.wrapping_add((i % 251) as u8));
    }

    out
}

fn read_client_plaintext(client: &mut rustls::ClientConnection) -> Vec<u8> {
    let mut plain = Vec::new();
    let mut chunk = [0u8; 4096];

    loop {
        match client.reader().read(&mut chunk) {
            Ok(0) => break,

            Ok(n) => plain.extend_from_slice(&chunk[..n]),

            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,

            Err(e) => panic!("client reader failed: {e}"),
        }
    }

    plain
}

#[test]
fn handshake_negotiates_tls13_and_http11() {
    let mut acceptor = TlsAcceptor::new(CERT_DER, KEY_DER, 8).expect("acceptor builds");

    let mut server = acceptor.admit().expect("slot available");

    let config = client_config();
    let mut client = client_conn(&config);

    let mut c2s = Vec::new();
    let mut s2c = Vec::new();

    let early_plain = handshake(&mut server, &mut client, &mut c2s, &mut s2c);

    assert!(
        early_plain.is_empty(),
        "no application data before handshake"
    );

    assert_eq!(alpn(&server), Some(b"http/1.1".as_slice()));

    assert_eq!(client.alpn_protocol(), Some(b"http/1.1".as_slice()));

    assert_eq!(
        negotiated_version(&server),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );

    assert_eq!(
        client.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );
}

#[test]
fn round_trip_ten_kibibytes_byte_identical() {
    let mut acceptor = TlsAcceptor::new(CERT_DER, KEY_DER, 8).expect("acceptor builds");

    let mut server = acceptor.admit().expect("slot available");

    let config = client_config();
    let mut client = client_conn(&config);

    let mut c2s = Vec::new();
    let mut s2c = Vec::new();

    handshake(&mut server, &mut client, &mut c2s, &mut s2c);

    let request = pattern(ROUND_TRIP_BYTES, 0xA5);
    let response = pattern(ROUND_TRIP_BYTES, 0x3C);

    client
        .writer()
        .write_all(&request)
        .expect("client queues request");

    client.write_tls(&mut c2s).expect("client emits request");

    let mut got_request = Vec::new();
    let consumed = feed_ciphertext(&mut server, &c2s, &mut got_request).expect("server feeds");

    c2s.drain(..consumed);

    assert_eq!(c2s.len(), 0, "request flight fully consumed");

    assert_eq!(got_request, request, "request arrives byte-identical");

    assert_eq!(
        server.bytes_decrypted(),
        ROUND_TRIP_BYTES as u64,
        "decrypted counter is exact after one request"
    );

    let encrypted_before = server.bytes_encrypted();

    drain_plaintext(&mut server, &response, &mut s2c).expect("server emits response");

    assert_eq!(
        server.bytes_encrypted() - encrypted_before,
        s2c.len() as u64,
        "encrypted counter tracks wire bytes exactly"
    );

    feed_client(&mut client, &mut s2c);

    let got_response = read_client_plaintext(&mut client);

    assert_eq!(got_response, response, "response arrives byte-identical");

    assert!(
        server.bytes_encrypted() > 0 && server.bytes_encrypted() < MAX_WIRE_BYTES,
        "encrypted counter is nonzero and bounded"
    );

    assert!(
        server.bytes_decrypted() > 0 && server.bytes_decrypted() < MAX_WIRE_BYTES,
        "decrypted counter is nonzero and bounded"
    );
}

#[test]
fn slots_reject_second_hello_and_release_recovers() {
    let mut acceptor = TlsAcceptor::new(CERT_DER, KEY_DER, 1).expect("acceptor builds");

    let mut first = acceptor.admit().expect("first slot available");

    assert_eq!(acceptor.active(), 1);

    let err = match acceptor.admit() {
        Ok(_) => panic!("second hello is rejected"),
        Err(e) => e,
    };

    assert!(
        matches!(err, TlsError::SlotsFull),
        "second hello fails with SlotsFull"
    );

    let config = client_config();
    let mut client = client_conn(&config);

    let mut c2s = Vec::new();
    let mut s2c = Vec::new();

    handshake(&mut first, &mut client, &mut c2s, &mut s2c);

    assert_eq!(alpn(&first), Some(b"http/1.1".as_slice()));

    assert_eq!(
        negotiated_version(&first),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );

    acceptor.release();

    assert_eq!(acceptor.active(), 0);

    acceptor.release();

    assert_eq!(acceptor.active(), 0, "release saturates at zero");

    let _second = acceptor.admit().expect("slot reusable after release");

    assert_eq!(acceptor.active(), 1);
}

#[test]
fn early_data_disabled_and_handshake_succeeds() {
    let mut acceptor = TlsAcceptor::new(CERT_DER, KEY_DER, 8).expect("acceptor builds");

    assert_eq!(
        acceptor.max_early_data_size(),
        0,
        "server offers no early data"
    );

    let mut server = acceptor.admit().expect("slot available");

    let config = client_config();
    let mut client = client_conn(&config);

    let mut c2s = Vec::new();
    let mut s2c = Vec::new();

    handshake(&mut server, &mut client, &mut c2s, &mut s2c);

    assert_eq!(
        acceptor.max_early_data_size(),
        0,
        "early data still disabled after handshake"
    );
}

#[test]
fn conn_size_stays_within_budget() {
    assert!(
        std::mem::size_of::<TlsConn>() <= MAX_TLS_CONN_BYTES,
        "TlsConn is {} bytes, budget is {MAX_TLS_CONN_BYTES}",
        std::mem::size_of::<TlsConn>()
    );
}
