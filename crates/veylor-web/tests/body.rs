use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use veylor_server::{Server, ServerConfig};
use veylor_web::{Bytes, Form, FormValues, Router, ServeExt};

fn form(Form(values): Form<FormValues>) -> String {
    let a = values.parse::<i64>("a").unwrap_or(0);
    let b = values
        .get("b")
        .map(|value| String::from_utf8_lossy(value).into_owned());
    format!("{a}:{}", b.unwrap_or_default())
}

fn bytes(Bytes(body): Bytes) -> Vec<u8> {
    body
}

#[test]
fn form_and_bytes_extractors() {
    let server = Server::bind(
        ([127, 0, 0, 1], 0).into(),
        ServerConfig {
            worker_count: 1,
            ..ServerConfig::default()
        },
    )
    .expect("bind");
    let app = Router::new().post("/form", form).post("/bytes", bytes);

    let running = server.serve(app).expect("serve");
    let addr = running.addr();

    let response = call(
        addr,
        "POST /form HTTP/1.1\r\nHost: t\r\nContent-Length: 17\r\nConnection: close\r\n\r\na=7&b=hello+world",
    );
    assert!(response.ends_with("7:hello world"), "{response:?}");

    let response = call(
        addr,
        "POST /bytes HTTP/1.1\r\nHost: t\r\nContent-Length: 4\r\nConnection: close\r\n\r\nraw!",
    );
    assert!(response.ends_with("raw!"), "{response:?}");

    running.join().expect("join");
}

fn call(addr: SocketAddr, raw: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.write_all(raw.as_bytes()).expect("write");

    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    String::from_utf8_lossy(&response).into_owned()
}
