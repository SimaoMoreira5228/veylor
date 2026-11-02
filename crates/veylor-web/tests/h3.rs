use std::net::SocketAddr;

use veylor_h3::{Client, ClientConfig, ClientTls, Limits, http};
use veylor_server::{Server, ServerConfig, TlsConfig};
use veylor_web::{Path, Query, Router, ServeExt};

fn fixture(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/../veylor-tls/tests/testdata/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(path).expect("test fixture")
}

fn statement(Path(account): Path<i64>, Query(query): Query<'_>) -> String {
    format!("{account}:{}", query.parse::<i64>("m").unwrap_or(1))
}

#[test]
fn h3_serves_the_same_router() {
    let cert = veylor_tls::load_cert_chain(&fixture("leaf_cert.der")).expect("cert");
    let key = veylor_tls::load_private_key(&fixture("leaf_key.der")).expect("key");
    let config = ServerConfig {
        h3_port: Some(0),
        worker_count: 1,
        tls: Some(TlsConfig {
            cert_chain: cert.clone(),
            key,
            max_handshakes: 8,
        }),
        ..ServerConfig::default()
    };
    let server = Server::bind(SocketAddr::from(([127, 0, 0, 1], 0)), config).expect("bind");
    let running = server
        .serve(Router::new().get("/accounts/:account/statement", statement))
        .expect("serve");

    let address = running.h3_addr().expect("h3 address");
    let client = Client::connect(ClientConfig {
        server: address,
        server_name: "localhost".to_owned(),
        tls: ClientTls::new(cert),
        limits: Limits::default(),
    })
    .expect("connect client");

    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri("https://localhost/accounts/42/statement?m=3")
        .body(())
        .expect("request");
    let response = client
        .request(request, veylor_h3::Bytes::new())
        .expect("response");

    assert_eq!(response.status, http::StatusCode::OK);
    assert_eq!(&response.body[..], b"42:3");

    running.join().expect("join");
}
