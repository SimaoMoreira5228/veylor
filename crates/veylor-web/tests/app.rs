use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use veylor_server::{Server, ServerConfig};
use veylor_web::{Body, Headers, Path, Query, QueryValues, Response, Router, ServeExt, State};

struct Counter {
    base: i64,
}

fn hello() -> &'static str {
    "hello"
}

fn statement(
    Path(account): Path<i64>,
    Query(query): Query<QueryValues>,
    State(state): State<Counter>,
) -> String {
    let multiplier = query.parse::<i64>("m").unwrap_or(1);
    format!("{}:{}", account + state.base, multiplier)
}

fn echo_header(headers: Headers) -> Response {
    let token = headers.get("x-token").unwrap_or(b"missing");
    Response::new(200, token.to_vec()).header("x-reply", "ok")
}

fn echo_body(Body(body): Body) -> Vec<u8> {
    body
}

async fn async_echo(Body(body): Body) -> Vec<u8> {
    body
}

#[test]
fn serves_routes_over_http1() {
    let config = ServerConfig {
        worker_count: 1,
        ..ServerConfig::default()
    };
    let server = Server::bind(([127, 0, 0, 1], 0).into(), config).expect("bind");
    let app = Router::with_state(Counter { base: 100 })
        .get("/hello", hello)
        .get("/accounts/:account/statement", statement)
        .get("/echo-header", echo_header)
        .post("/echo", echo_body)
        .post_async("/async-echo", async_echo);

    let running = server.serve(app).expect("serve");
    let addr = running.addr();

    let response = request(
        addr,
        "GET /hello HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(response.starts_with("HTTP/1.1 200 "), "{response}");
    assert!(response.ends_with("hello"), "{response}");

    let response = request(
        addr,
        "GET /accounts/42/statement?m=3 HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(response.ends_with("142:3"), "{response}");

    let response = request(
        addr,
        "GET /echo-header HTTP/1.1\r\nHost: t\r\nX-Token: abc\r\nConnection: close\r\n\r\n",
    );
    assert!(response.contains("x-reply: ok"), "{response}");
    assert!(response.ends_with("abc"), "{response}");

    let response = request(
        addr,
        "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 7\r\nConnection: close\r\n\r\npayload",
    );
    assert!(response.ends_with("payload"), "{response:?}");

    let response = request(
        addr,
        "POST /async-echo HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\nConnection: close\r\n\r\nasync",
    );
    assert!(response.ends_with("async"), "{response:?}");

    let response = request(
        addr,
        "GET /missing HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(response.starts_with("HTTP/1.1 404 "), "{response}");

    running.join().expect("join");
}

fn request(addr: SocketAddr, raw: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.write_all(raw.as_bytes()).expect("write");

    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    String::from_utf8_lossy(&response).into_owned()
}
