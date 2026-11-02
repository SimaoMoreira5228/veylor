use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use veylor_server::{Server, ServerConfig};
use veylor_web::{Router, ServeExt};
use veylor_web_tokio::TokioExecutor;

async fn slow() -> &'static str {
    tokio::time::sleep(Duration::from_millis(150)).await;
    "tokio"
}

#[test]
fn tokio_drives_async_handlers_concurrently() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let server = Server::bind(
        ([127, 0, 0, 1], 0).into(),
        ServerConfig {
            worker_count: 1,
            ..ServerConfig::default()
        },
    )
    .expect("bind");
    let app = Router::new()
        .executor(Arc::new(TokioExecutor::new(runtime.handle().clone())))
        .get_async("/slow", slow);

    let running = server.serve(app).expect("serve");
    let addr = running.addr();

    let start = Instant::now();
    let first = std::thread::spawn(move || call(addr, "/slow"));
    let second = std::thread::spawn(move || call(addr, "/slow"));
    let first = first.join().expect("first");
    let second = second.join().expect("second");
    let elapsed = start.elapsed();

    assert!(first.ends_with("tokio"), "{first:?}");
    assert!(second.ends_with("tokio"), "{second:?}");
    assert!(
        elapsed < Duration::from_millis(280),
        "two 150ms handlers took {elapsed:?}"
    );

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
