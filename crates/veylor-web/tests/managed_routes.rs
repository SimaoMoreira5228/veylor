use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use veylor_server::{Server, ServerConfig};
use veylor_web::{Path, Router, ServeExt};

fn run(Path(id): Path<i64>) -> String {
    format!("run:{id}")
}

fn cancel(Path(id): Path<i64>) -> String {
    format!("cancel:{id}")
}

fn job(Path(id): Path<i64>) -> String {
    format!("job:{id}")
}

#[test]
fn managed_routes_pick_the_route_their_token_names() {
    let server = Server::bind(
        ([127, 0, 0, 1], 0).into(),
        ServerConfig {
            worker_count: 1,
            ..ServerConfig::default()
        },
    )
    .expect("bind");
    let app = Router::new()
        .post("/jobs/:id/run", run)
        .post("/jobs/:id/cancel", cancel)
        .post("/jobs/:id", job);

    let running = server.serve(app).expect("serve");
    let addr = running.addr();

    let response = call(
        addr,
        "POST /jobs/7/run HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    assert!(response.ends_with("run:7"), "{response:?}");

    let response = call(
        addr,
        "POST /jobs/7/cancel HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    assert!(response.ends_with("cancel:7"), "{response:?}");

    let response = call(
        addr,
        "POST /jobs/9 HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    assert!(response.ends_with("job:9"), "{response:?}");

    running.join().expect("join");
}

fn call(addr: SocketAddr, raw: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.write_all(raw.as_bytes()).expect("write");

    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    String::from_utf8_lossy(&response).into_owned()
}
