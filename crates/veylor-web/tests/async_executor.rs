use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use veylor_server::{Server, ServerConfig};
use veylor_web::{Path, Router, ServeExt, State, ThreadExecutor};

struct Base(i64);

async fn slow(Path(id): Path<i64>, State(base): State<Base>) -> String {
    std::thread::sleep(Duration::from_millis(150));
    format!("{id}:{}", base.0)
}

#[test]
fn async_handlers_run_on_the_configured_executor() {
    let server = Server::bind(
        ([127, 0, 0, 1], 0).into(),
        ServerConfig {
            worker_count: 1,
            ..ServerConfig::default()
        },
    )
    .expect("bind");
    let app = Router::with_state(Base(5))
        .executor(Arc::new(ThreadExecutor))
        .get_async("/slow/:id", slow);

    let running = server.serve(app).expect("serve");
    let addr = running.addr();

    let start = Instant::now();
    let first = std::thread::spawn(move || call(addr, "/slow/1"));
    let second = std::thread::spawn(move || call(addr, "/slow/2"));
    let first = first.join().expect("first");
    let second = second.join().expect("second");
    let elapsed = start.elapsed();

    assert!(first.ends_with("1:5"), "{first:?}");
    assert!(second.ends_with("2:5"), "{second:?}");
    assert!(
        elapsed >= Duration::from_millis(140),
        "the handlers returned too fast to have run: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(280),
        "two 150ms handlers took {elapsed:?}; the executor did not overlap them"
    );

    running.join().expect("join");
}

async fn boom() -> &'static str {
    panic!("handler panic")
}

#[test]
fn async_panics_answer_500_and_keep_the_server_alive() {
    let server = Server::bind(
        ([127, 0, 0, 1], 0).into(),
        ServerConfig {
            worker_count: 1,
            ..ServerConfig::default()
        },
    )
    .expect("bind");
    let app = Router::new().get_async("/boom", boom).get("/ok", || "ok");

    let running = server.serve(app).expect("serve");
    let addr = running.addr();

    let response = call(addr, "/boom");
    assert!(response.starts_with("HTTP/1.1 500 "), "{response:?}");

    let response = call(addr, "/ok");
    assert!(response.ends_with("ok"), "{response:?}");

    running.join().expect("join");
}

fn call(addr: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("timeout");
    let request = format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write");

    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read");
    String::from_utf8_lossy(&response).into_owned()
}
