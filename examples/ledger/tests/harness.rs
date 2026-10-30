use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

static TEST_REQUEST_KEY: AtomicUsize = AtomicUsize::new(0);

pub struct Service {
    pub child: Child,
    pub address: SocketAddr,
    _output: BufReader<std::process::ChildStdout>,
}

impl Service {
    pub fn start(database: &Path) -> Self {
        Self::start_with_options(database, &[])
    }

    pub fn start_with_options(database: &Path, options: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_veylor-ledger"))
            .args([
                "--address",
                "127.0.0.1:0",
                "--database",
                database.to_str().unwrap(),
                "--workers",
                "2",
            ])
            .args(options)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();

        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        output.read_line(&mut line).unwrap();

        let address = line.trim().split_once("://").unwrap().1.parse().unwrap();

        Self {
            child,
            address,
            _output: output,
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            let status = Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap();
            assert!(status.success());
        }

        let deadline = std::time::Instant::now() + Duration::from_secs(10);

        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                if !std::thread::panicking() {
                    assert!(status.success());
                }
                return;
            }

            if std::time::Instant::now() >= deadline {
                self.child.kill().unwrap();
                self.child.wait().unwrap();
                panic!("service shutdown exceeded deadline");
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

pub struct Database(PathBuf);

impl Database {
    pub fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("veylor-ledger-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path).unwrap();

        Self(path)
    }

    pub fn file(&self) -> PathBuf {
        self.0.join("accounts.sqlite3")
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

pub fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    fragmented: bool,
) -> (u16, Value) {
    let key = format!("test-{}", TEST_REQUEST_KEY.fetch_add(1, Ordering::Relaxed));
    request_with_idempotency_key(address, method, path, body, fragmented, &key)
}

#[allow(dead_code)]
pub fn request_with_content_type(
    address: SocketAddr,
    method: &str,
    path: &str,
    content_type: &str,
    body: &[u8],
    fragmented: bool,
) -> (u16, Value) {
    let key = format!("test-{}", TEST_REQUEST_KEY.fetch_add(1, Ordering::Relaxed));
    request_with_headers(
        address,
        method,
        path,
        content_type,
        body,
        fragmented,
        &[&key],
    )
}

pub fn request_with_idempotency_key(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    fragmented: bool,
    idempotency_key: &str,
) -> (u16, Value) {
    request_with_headers(
        address,
        method,
        path,
        "application/json",
        body,
        fragmented,
        &[idempotency_key],
    )
}

pub fn request_with_headers(
    address: SocketAddr,
    method: &str,
    path: &str,
    content_type: &str,
    body: &[u8],
    fragmented: bool,
    idempotency_keys: &[&str],
) -> (u16, Value) {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let mut idempotency_headers = String::new();
    for key in idempotency_keys {
        idempotency_headers.push_str(&format!("Idempotency-Key: {key}\r\n"));
    }

    let head = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: {content_type}\r\n\
         {idempotency_headers}\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len(),
    );
    stream.write_all(head.as_bytes()).unwrap();
    if fragmented && !body.is_empty() {
        let middle = body.len() / 2;
        stream.write_all(&body[..middle]).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        stream.write_all(&body[middle..]).unwrap();
    } else {
        stream.write_all(body).unwrap();
    }

    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).unwrap();

    let end = bytes
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap();
    let head = std::str::from_utf8(&bytes[..end]).unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let value = serde_json::from_slice(&bytes[end + 4..]).unwrap_or(Value::Null);

    (status, value)
}
