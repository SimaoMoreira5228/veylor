use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Buf;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use veylor_h3::{Config, Limits, Server, Tls, http, text_response};

fn identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate certificate");
    let cert = generated.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        generated.signing_key.serialize_der(),
    ));
    (cert, key)
}

fn client_endpoint(cert: CertificateDer<'static>) -> (quinn::Endpoint, quinn::ClientConfig) {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).expect("trust fixture");
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("tls 1.3")
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("quic client config");
    let config = quinn::ClientConfig::new(Arc::new(crypto));
    let endpoint =
        quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0))).expect("client endpoint");
    (endpoint, config)
}

#[test]
fn serves_h3_get_and_post() {
    let (cert, key) = identity();
    let server = Server::bind(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Config {
            tls: Tls::new(vec![cert.clone()], key),
            limits: Limits::default(),
        },
    );
    let running = server
        .start(|request: http::Request<()>, body: bytes::Bytes| {
            let status = if request.method() == http::Method::POST {
                http::StatusCode::CREATED
            } else {
                http::StatusCode::OK
            };
            let path = request.uri().path().as_bytes().to_vec();
            text_response(status, [path, b":".to_vec(), body.to_vec()].concat())
        })
        .expect("start h3 server");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("client runtime");
    runtime.block_on(async {
        let (endpoint, config) = client_endpoint(cert);
        let connection = endpoint
            .connect_with(config, running.address(), "localhost")
            .expect("connect")
            .await
            .expect("handshake");
        let (mut driver, mut client) = h3::client::new(h3_quinn::Connection::new(connection))
            .await
            .expect("h3 client");
        let driver = tokio::spawn(async move { driver.wait_idle().await });

        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri("https://localhost/hello")
            .body(())
            .expect("request");
        let mut stream = client.send_request(request).await.expect("send");
        stream.finish().await.expect("finish");
        let response = stream.recv_response().await.expect("response");
        assert_eq!(response.status(), http::StatusCode::OK);
        let mut body = Vec::new();
        while let Some(chunk) = stream.recv_data().await.expect("data") {
            body.extend_from_slice(chunk.chunk());
        }
        assert_eq!(body, b"/hello:");

        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri("https://localhost/upload")
            .body(())
            .expect("request");
        let mut stream = client.send_request(request).await.expect("send");
        stream
            .send_data(bytes::Bytes::from_static(b"payload"))
            .await
            .expect("body");
        stream.finish().await.expect("finish");
        let response = stream.recv_response().await.expect("response");
        assert_eq!(response.status(), http::StatusCode::CREATED);
        let mut body = Vec::new();
        while let Some(chunk) = stream.recv_data().await.expect("data") {
            body.extend_from_slice(chunk.chunk());
        }
        assert_eq!(body, b"/upload:payload");

        endpoint.close(quinn::VarInt::from_u32(0), b"done");
        let _ = driver.await;
    });

    running.join().expect("join h3 server");
}
