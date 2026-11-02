use std::net::SocketAddr;

use veylor_h3::{Bytes, Client, ClientConfig, ClientTls, Limits, http};
use veylor_server::{Server, ServerConfig, TlsConfig};
use veylor_web::{Path, Query, Router, ServeExt, SseEvent, SseSender};

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

fn move_funds(Path(account): Path<i64>) -> String {
    format!("moved:{account}")
}

fn close_account(Path(account): Path<i64>) -> String {
    format!("closed:{account}")
}

fn events(sender: SseSender) {
    sender.send(SseEvent::data("h3"));
    sender.close();
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
        .serve(
            Router::new()
                .get("/accounts/:account/statement", statement)
                .post("/accounts/:account/move", move_funds)
                .post("/accounts/:account/close", close_account)
                .sse("/events", events),
        )
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
    let response = client.request(request, Bytes::new()).expect("response");
    assert_eq!(response.status, http::StatusCode::OK);
    assert_eq!(&response.body[..], b"42:3");

    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("https://localhost/accounts/42/move")
        .body(())
        .expect("request");
    let response = client.request(request, Bytes::new()).expect("response");
    assert_eq!(
        response.status,
        http::StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    assert_eq!(&response.body[..], b"moved:42");

    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("https://localhost/accounts/42/close")
        .body(())
        .expect("request");
    let response = client.request(request, Bytes::new()).expect("response");
    assert_eq!(response.status, http::StatusCode::OK);
    assert_eq!(&response.body[..], b"closed:42");

    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri("https://localhost/events")
        .body(())
        .expect("request");
    let response = client.request(request, Bytes::new()).expect("response");
    assert_eq!(response.status, http::StatusCode::OK);
    assert_eq!(&response.body[..], b"data: h3\n\n");

    running.join().expect("join");
}
