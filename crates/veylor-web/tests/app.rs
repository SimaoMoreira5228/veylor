#![allow(unsafe_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use veylor_server::{Server, ServerConfig};
use veylor_web::{
    Bytes, Ctx, Headers, Middleware, Next, Path, Query, Request, Response, Router, ServeExt, State,
};

thread_local! {
    static RAW_BODY: std::cell::Cell<[u8; 64]> = const { std::cell::Cell::new([0; 64]) };
}

struct Counter {
    base: i64,
}

fn hello() -> &'static str {
    "hello"
}

fn statement(
    Path(account): Path<i64>,
    State(state): State<Counter>,
    Query(query): Query<'_>,
) -> String {
    let multiplier = query.parse::<i64>("m").unwrap_or(1);
    format!("{}:{}", account + state.base, multiplier)
}

fn raw(request: Request<'_, Counter>) -> Response {
    let mut sum = 0i64;
    for (name, value) in request.query().iter() {
        if name != b"a" && name != b"b" {
            continue;
        }
        if let Ok(text) = std::str::from_utf8(value)
            && let Ok(parsed) = text.parse::<i64>()
        {
            sum += parsed;
        }
    }

    let token = request.header("x-token").unwrap_or(b"none");
    let (pointer, length) = RAW_BODY.with(|cell| {
        let mut buffer = cell.get();
        let mut length = 0;
        for byte in sum.to_string().bytes() {
            buffer[length] = byte;
            length += 1;
        }
        buffer[length] = b':';
        length += 1;
        for byte in token {
            buffer[length] = *byte;
            length += 1;
        }
        cell.set(buffer);
        (cell.as_ptr() as *const u8, length)
    });
    // SAFETY: the thread-local buffer lives for the thread and the engine copies the body
    // before the next handler runs on it.
    Response::new(200, unsafe { std::slice::from_raw_parts(pointer, length) })
}

fn echo_header(Headers(headers): Headers<'_>) -> Response {
    let token = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(b"x-token"))
        .map(|(_, value)| *value)
        .unwrap_or(b"missing");
    Response::new(200, token.to_vec()).header("x-reply", "ok")
}

struct Tag;

impl Middleware<Counter> for Tag {
    fn call<'a, 'b>(&self, ctx: &'a mut Ctx<'b, Counter>, next: Next<'a, Counter>) -> Response {
        next.run(ctx).header("x-layer", "on")
    }
}

fn echo_body(Bytes(body): Bytes) -> Vec<u8> {
    body
}

async fn async_echo(Bytes(body): Bytes) -> Vec<u8> {
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
        .get("/raw", raw)
        .post("/echo", echo_body)
        .post_async("/async-echo", async_echo)
        .layer(Tag);

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
    assert!(response.contains("x-layer: on"), "{response}");
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
        "GET /raw?a=13&b=42 HTTP/1.1\r\nHost: t\r\nX-Token: abc\r\nConnection: close\r\n\r\n",
    );
    assert!(response.ends_with("55:abc"), "{response:?}");

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
