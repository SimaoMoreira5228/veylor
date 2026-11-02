#![cfg(feature = "json")]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use serde::{Deserialize, Serialize};
use veylor_server::{Server, ServerConfig};
use veylor_web::{Json, Query, QueryValues, Router, ServeExt};

#[derive(Deserialize)]
struct Echo {
    value: i64,
}

#[derive(Serialize)]
struct Reply {
    doubled: i64,
}

fn double(Json(body): Json<Echo>) -> Json<Reply> {
    Json(Reply {
        doubled: body.value * 2,
    })
}

fn answer(Query(query): Query<QueryValues>) -> Json<Reply> {
    Json(Reply {
        doubled: query.parse::<i64>("value").unwrap_or(0),
    })
}

#[test]
fn json_routes_parse_and_render() {
    let server = Server::bind(
        ([127, 0, 0, 1], 0).into(),
        ServerConfig {
            worker_count: 1,
            ..ServerConfig::default()
        },
    )
    .expect("bind");
    let app = Router::new().post("/double", double).get("/answer", answer);

    let running = server.serve(app).expect("serve");
    let addr = running.addr();

    let response = call(
        addr,
        "POST /double HTTP/1.1\r\nHost: t\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"value\":4}",
    );
    assert!(
        response.contains("content-type: application/json"),
        "{response}"
    );

    let response = call(
        addr,
        "GET /answer?value=21 HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(response.ends_with("{\"doubled\":21}"), "{response:?}");

    running.join().expect("join");
}

fn call(addr: SocketAddr, raw: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.write_all(raw.as_bytes()).expect("write");

    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    String::from_utf8_lossy(&response).into_owned()
}
