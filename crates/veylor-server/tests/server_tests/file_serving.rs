use crate::h1_peer::*;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use veylor_server::{Server, ServerConfig};

#[test]
fn file_get_serves_exact_bytes_type_length() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello file");
    assert_eq!(header(&response, "content-length"), "10");
    assert_eq!(header(&response, "content-type"), "text/plain");
    get(&mut stream, "/assets/hello.txt?v=1");
    let queried = read_response(&mut stream);
    assert_eq!(queried.status, 200);
    assert_eq!(queried.body, b"hello file");
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(fixture.blob.len(), FILE_BLOB_LEN);
}

#[test]
fn file_get_serves_precompressed_twin() {
    let (server, fixture) = file_server();
    std::fs::write(fixture.root.join("hello.txt.br"), b"brotli").expect("br twin");
    std::fs::write(fixture.root.join("hello.txt.gz"), b"gzip!").expect("gz twin");

    let mut stream = connect(&server);
    stream
        .write_all(
            b"GET /assets/hello.txt HTTP/1.1\r\n\
              Host: 127.0.0.1\r\n\
              Accept-Encoding: gzip, br\r\n\
              Connection: keep-alive\r\n\r\n",
        )
        .expect("write br request");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(header(&response, "content-encoding"), "br");
    assert_eq!(header(&response, "vary"), "Accept-Encoding");
    assert_eq!(header(&response, "content-length"), "6");
    assert_eq!(response.body, b"brotli");

    stream
        .write_all(
            b"GET /assets/hello.txt HTTP/1.1\r\n\
              Host: 127.0.0.1\r\n\
              Accept-Encoding: gzip\r\n\
              Connection: keep-alive\r\n\r\n",
        )
        .expect("write gzip request");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(header(&response, "content-encoding"), "gzip");
    assert_eq!(header(&response, "content-length"), "5");
    assert_eq!(response.body, b"gzip!");

    get(&mut stream, "/assets/hello.txt");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case("content-encoding"))
    );
    assert_eq!(header(&response, "vary"), "Accept-Encoding");
    assert_eq!(response.body, b"hello file");

    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn file_head_returns_headers_only() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    let request =
        b"HEAD /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    stream.write_all(request).expect("write HEAD");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
    }
    let text = String::from_utf8(head).expect("head utf8");
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(text.contains("Content-Length: 10"), "length: {text}");
    assert!(text.contains("Content-Type: text/plain"), "type: {text}");
    read_eof(&mut stream);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn file_missing_returns_404_then_closes() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/nope.txt");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 404);
    assert_eq!(response.body, b"Not Found");
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn file_traversal_rejected_without_fs_escape() {
    let (server, _fixture) = file_server();
    for path in ["/assets/../outside.txt", "/assets/%2e%2e/outside.txt"] {
        let mut stream = connect(&server);
        get(&mut stream, path);
        let response = read_response(&mut stream);
        assert_eq!(response.status, 404, "path {path}");
        assert_eq!(response.body, b"Not Found", "path {path}");
        read_eof(&mut stream);
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 2, "counts: {counts:?}");
}

#[test]
fn file_blob_streams_exact_1mib() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/blob.bin");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(
        header(&response, "content-length"),
        FILE_BLOB_LEN.to_string()
    );
    assert_eq!(
        header(&response, "content-type"),
        "application/octet-stream"
    );
    assert_eq!(response.body, fixture.blob);
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_slow_reader_tiny_reads_completes() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/blob.bin");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
    }
    let text = String::from_utf8(head).expect("head utf8");
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    let mut body = Vec::with_capacity(FILE_BLOB_LEN);
    while body.len() < FILE_BLOB_LEN {
        let mut byte = [0; 1];
        stream.read_exact(&mut byte).expect("read body byte");
        body.push(byte[0]);
    }
    assert_eq!(body, fixture.blob);
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_disconnect_mid_transfer_server_healthy() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/blob.bin");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
    }
    let mut partial = [0; 4096];
    stream.read_exact(&mut partial).expect("read partial body");
    drop(stream);
    let mut again = connect(&server);
    get(&mut again, "/hello");
    let response = read_response(&mut again);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert!(
        counts.peer_closed + counts.reset + counts.transport_error >= 1,
        "counts: {counts:?}"
    );
}

#[test]
fn file_shutdown_mid_transfer_drains_clean() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/blob.bin");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
    }
    server.stop_accepting();
    let mut body = Vec::with_capacity(FILE_BLOB_LEN);
    stream.read_to_end(&mut body).expect("read drained body");
    assert_eq!(body, fixture.blob);
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn file_closed_receiver_no_sigpipe_stays_healthy() {
    let (server, _fixture) = file_server();
    for _ in 0..8 {
        let mut stream = connect(&server);
        get(&mut stream, "/assets/blob.bin");
        let mut head = Vec::new();
        let mut byte = [0; 1];
        while !head.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte) {
                Ok(0) => break,
                Ok(_) => head.push(byte[0]),
                Err(error) => panic!("read head: {error:?}"),
            }
        }
        let mut partial = [0; 65536];
        stream.read_exact(&mut partial).expect("read partial body");
        drop(stream);
    }
    // NOTE: Rust ignores SIGPIPE process-wide from lang_start, so every closed-receiver
    // sendfile above surfaced as EPIPE/PeerClosed or RST/Reset instead of a signal; this
    // process surviving to serve again pins that discipline with no handler installed.
    let mut again = connect(&server);
    get(&mut again, "/hello");
    let response = read_response(&mut again);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
    assert!(
        counts.peer_closed + counts.reset + counts.transport_error >= 1,
        "counts: {counts:?}"
    );
}

struct TruncGuard {
    dir: PathBuf,
}

impl Drop for TruncGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn file_truncated_mid_transfer_fails_closed() {
    const BIG_LEN: usize = 16 << 20;
    let id = NEXT_FILE_ID.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("veylor-srv-trunc-{}-{id}", std::process::id()));
    let _guard = TruncGuard { dir: dir.clone() };
    let root = dir.join("root");
    std::fs::create_dir_all(&root).expect("fixture root");
    let mut big = Vec::with_capacity(BIG_LEN);
    for i in 0..BIG_LEN {
        big.push((i % 251) as u8);
    }
    std::fs::write(root.join("big.bin"), &big).expect("big file");
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_file_mount(b"/assets/", &root)
        .expect("register assets");
    let server = server.start().expect("start server");
    let mut stream = connect(&server);
    get(&mut stream, "/assets/big.bin");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
    }
    let text = String::from_utf8(head).expect("head utf8");
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(
        text.contains(&format!("Content-Length: {BIG_LEN}")),
        "length: {text}"
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(root.join("big.bin"))
        .expect("open big")
        .set_len(100)
        .expect("truncate big");
    let mut body = Vec::new();
    stream.read_to_end(&mut body).expect("read truncated body");
    // NOTE: a shrink racing a sendfile can zero-fill inside the plan bounds (one call
    // straddled this ftruncate: zeroed partial-page tail plus a not-yet-discarded page),
    // where the read_chunk path would serve a short read instead. Either way the transfer
    // fails closed below: short body, EOF, transport_error, never a short success.
    assert!(body.len() < BIG_LEN, "short body, got {}", body.len());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
    assert_eq!(counts.transport_error, 1, "counts: {counts:?}");
}

#[test]
fn file_duplicate_and_invalid_mounts_reject() {
    let fixture = FileFixture::build();
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_file_mount(b"/assets/", &fixture.root)
        .expect("first mount");
    assert!(
        server
            .register_file_mount(b"/assets/", &fixture.root)
            .is_err()
    );
    assert!(
        server
            .register_file_mount(b"/bad/", &fixture.root.join("hello.txt"))
            .is_err()
    );
    let running = server.start().expect("start server");
    running.stop_accepting();
    let _ = running.join().expect("join server");
}

#[test]
fn file_keepalive_replace_between_transfers_serves_new_bytes() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello file");
    let replacement = b"hello file, replaced and longer";
    std::fs::write(fixture.root.join("hello.txt"), replacement).expect("replace hello");
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, replacement);
    assert_eq!(
        header(&second, "content-length"),
        replacement.len().to_string()
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_eviction_round_robin_across_many_files_stays_exact() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    for _ in 0..2 {
        get(&mut stream, "/assets/hello.txt");
        let hello = read_response(&mut stream);
        assert_eq!(hello.status, 200);
        assert_eq!(hello.body, b"hello file");
        get(&mut stream, "/assets/blob.bin");
        let blob = read_response(&mut stream);
        assert_eq!(blob.status, 200);
        assert_eq!(blob.body, fixture.blob);
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}
