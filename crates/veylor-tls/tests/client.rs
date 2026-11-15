#![allow(clippy::err_expect)]

use veylor_tls::{
    TlsAcceptor, TlsConn, TlsConnector, TlsError, alpn, drain_plaintext, feed_ciphertext,
    flush_ciphertext, negotiated_version, queue_close_notify,
};

const CERT: &[u8] = include_bytes!("testdata/leaf_cert.der");
const KEY: &[u8] = include_bytes!("testdata/leaf_key.der");

fn connector(protocols: &[&[u8]]) -> TlsConnector {
    TlsConnector::builder()
        .default_roots(false)
        .add_root(CERT)
        .expect("root")
        .alpn(protocols)
        .build()
        .expect("connector")
}

fn handshake(client: &mut TlsConn, server: &mut TlsConn) -> Result<(), TlsError> {
    let mut to_server = Vec::new();
    let mut to_client = Vec::new();
    let mut sink = Vec::new();

    for _ in 0..8 {
        flush_ciphertext(client, &mut to_server)?;

        feed_ciphertext(server, &to_server, &mut sink)?;
        to_server.clear();

        flush_ciphertext(server, &mut to_client)?;

        feed_ciphertext(client, &to_client, &mut sink)?;
        to_client.clear();

        if !client.is_handshaking() && !server.is_handshaking() {
            flush_ciphertext(client, &mut to_server)?;
            feed_ciphertext(server, &to_server, &mut sink)?;

            return Ok(());
        }
    }

    Err(TlsError::Handshake)
}

fn pair(protocols: &[&[u8]], name: &str) -> Result<(TlsConn, TlsConn), TlsError> {
    let mut acceptor = TlsAcceptor::new(CERT, KEY, 4)?;
    let mut server = acceptor.admit()?;
    let mut client = connector(protocols).connect(name)?;

    handshake(&mut client, &mut server)?;

    Ok((client, server))
}

#[test]
fn offering_both_negotiates_h2() {
    let (client, server) = pair(&[b"h2", b"http/1.1"], "localhost").expect("pair");

    assert_eq!(alpn(&client), Some(b"h2".as_slice()));
    assert_eq!(alpn(&server), Some(b"h2".as_slice()));
}

#[test]
fn offering_http11_only_negotiates_http11() {
    let (client, _) = pair(&[b"http/1.1"], "localhost").expect("pair");

    assert_eq!(alpn(&client), Some(b"http/1.1".as_slice()));
}

#[test]
fn default_connector_offers_h2_and_http11_over_tls13() {
    let connector = TlsConnector::builder()
        .default_roots(false)
        .add_root(CERT)
        .expect("root")
        .build()
        .expect("connector");
    let mut acceptor = TlsAcceptor::new(CERT, KEY, 4).expect("acceptor");
    let mut server = acceptor.admit().expect("admit");
    let mut client = connector.connect("localhost").expect("connect");

    handshake(&mut client, &mut server).expect("handshake");

    assert_eq!(alpn(&client), Some(b"h2".as_slice()));
    assert_eq!(
        negotiated_version(&client),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );
    assert!(!connector.early_data_enabled());
}

#[test]
fn wrong_name_is_a_certificate_error() {
    let error = pair(&[b"http/1.1"], "example.com")
        .err()
        .expect("must fail");

    assert!(matches!(error, TlsError::Certificate), "{error}");
}

#[test]
fn untrusted_server_is_a_certificate_error() {
    let connector = TlsConnector::builder()
        .default_roots(false)
        .add_root(include_bytes!("testdata/server_cert2.der"))
        .expect("root")
        .build()
        .expect("connector");
    let mut acceptor = TlsAcceptor::new(CERT, KEY, 4).expect("acceptor");
    let mut server = acceptor.admit().expect("admit");
    let mut client = connector.connect("localhost").expect("connect");

    let error = handshake(&mut client, &mut server)
        .err()
        .expect("must fail");

    assert!(matches!(error, TlsError::Certificate), "{error}");
}

#[test]
fn default_roots_alone_do_not_trust_the_fixture() {
    let connector = TlsConnector::builder().build().expect("connector");
    let mut acceptor = TlsAcceptor::new(CERT, KEY, 4).expect("acceptor");
    let mut server = acceptor.admit().expect("admit");
    let mut client = connector.connect("localhost").expect("connect");

    assert!(handshake(&mut client, &mut server).is_err());
}

#[test]
fn empty_trust_and_bad_names_are_rejected() {
    assert!(
        TlsConnector::builder()
            .default_roots(false)
            .build()
            .is_err()
    );
    assert!(connector(&[b"http/1.1"]).connect("not a name").is_err());
}

#[test]
fn application_data_and_close_notify_round_trip() {
    let (mut client, mut server) = pair(&[b"http/1.1"], "localhost").expect("pair");
    let mut wire = Vec::new();
    let mut plain = Vec::new();

    drain_plaintext(&mut client, b"ping", &mut wire).expect("send");
    feed_ciphertext(&mut server, &wire, &mut plain).expect("recv");

    assert_eq!(plain, b"ping");

    wire.clear();
    queue_close_notify(&mut server, &mut wire).expect("close");
    plain.clear();
    feed_ciphertext(&mut client, &wire, &mut plain).expect("recv close");

    assert!(plain.is_empty());
    assert!(client.bytes_encrypted() > 0);
    assert_eq!(server.bytes_decrypted(), 4);
}
