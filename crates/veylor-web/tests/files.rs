use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use veylor_server::{Server, ServerConfig};
use veylor_web::{Router, ServeExt, files};

#[test]
fn files_route_mounts_a_directory() {
    let dir = std::env::temp_dir().join(format!("veylor-web-files-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    std::fs::write(dir.join("hello.txt"), b"from disk").expect("write");

    let server = Server::bind(
        ([127, 0, 0, 1], 0).into(),
        ServerConfig {
            worker_count: 1,
            ..ServerConfig::default()
        },
    )
    .expect("bind");
    let app = Router::new().route("/static/*path", files(&dir));

    let running = server.serve(app).expect("serve");
    let addr = running.addr();

    let response = call(
        addr,
        "GET /static/hello.txt HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n",
    );
    assert!(response.starts_with("HTTP/1.1 200 "), "{response}");
    assert!(response.ends_with("from disk"), "{response}");

    running.join().expect("join");
    let _ = std::fs::remove_dir_all(&dir);
}

fn call(addr: SocketAddr, raw: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.write_all(raw.as_bytes()).expect("write");

    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    String::from_utf8_lossy(&response).into_owned()
}
