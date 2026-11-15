fn tls_server() -> veylor_server::RunningServer {
    use veylor_server::{MAX_STATIC_BODY_BYTES, Server, ServerConfig, TlsConfig};
    use veylor_tls::{Certificate, PrivateKey};

    let config = ServerConfig {
        tls: Some(TlsConfig {
            cert_chain: vec![Certificate::from(crate::support::CERT.to_vec())],
            key: PrivateKey::Pkcs8(crate::support::KEY.to_vec()),
            max_handshakes: 64,
        }),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)), config)
        .expect("bind");

    server
        .register_route(b"GET", b"/hello", 200, b"OK", b"hello", MAX_STATIC_BODY_BYTES)
        .expect("route");

    server.start().expect("start")
}

#[test]
fn https_negotiates_h2_with_a_custom_root() {
    run(async {
        let server = tls_server();
        let client = Client::builder()
            .runtime(RUNTIME)
            .add_root_certificate(crate::support::CERT)
            .build()
            .expect("client");
        let url = format!("https://localhost:{}/hello", server.addr().port());

        for _ in 0..3 {
            let response = client.get(&url).send().await.expect("send");

            assert_eq!(response.protocol(), Protocol::Http2);
            assert_eq!(response.url().scheme(), "https");
            assert_eq!(response.text().await.expect("text"), "hello");
        }

        assert_eq!(client.pool_stats().h2, 1);

        client.close_idle();
        server.stop_accepting();
        server.join().expect("join");
    });
}

#[test]
fn untrusted_certificates_are_rejected() {
    run(async {
        let server = tls_server();
        let client = client();
        let url = format!("https://localhost:{}/hello", server.addr().port());
        let error = client.get(url).send().await.expect_err("untrusted");

        assert!(!error.is_status() && !error.is_builder(), "{error}");

        server.stop_accepting();
        server.join().expect("join");
    });
}
