#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

struct Service {
    child: Child,
    address: SocketAddr,
    _output: BufReader<std::process::ChildStdout>,
}

impl Service {
    fn start(database: &Path) -> Self {
        Self::start_with_options(database, &[])
    }

    fn start_with_options(database: &Path, options: &[&str]) -> Self {
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

struct Database(PathBuf);

impl Database {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("veylor-ledger-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path).unwrap();

        Self(path)
    }

    fn file(&self) -> PathBuf {
        self.0.join("accounts.sqlite3")
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    fragmented: bool,
) -> (u16, Value) {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let head = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
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

#[test]
fn concurrent_debits_across_processes_are_atomic_and_survive_restart() {
    let database = Database::new();

    {
        let first = Service::start(&database.file());
        let second = Service::start(&database.file());
        let addresses = [first.address, second.address];
        let next = AtomicUsize::new(0);
        let accepted = AtomicUsize::new(0);
        let rejected = AtomicUsize::new(0);

        std::thread::scope(|scope| {
            for _ in 0..16 {
                scope.spawn(|| {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= 200 {
                            return;
                        }

                        let body = br#"{"amount":1000,"kind":"debit","description":"purchase"}"#;
                        let (status, _) = request(
                            addresses[index % 2],
                            "POST",
                            "/accounts/1/transactions",
                            body,
                            false,
                        );

                        match status {
                            200 => {
                                accepted.fetch_add(1, Ordering::Relaxed);
                            }
                            422 => {
                                rejected.fetch_add(1, Ordering::Relaxed);
                            }
                            _ => panic!("unexpected transaction status: {status}"),
                        }
                    }
                });
            }
        });

        assert_eq!(accepted.load(Ordering::Relaxed), 100);
        assert_eq!(rejected.load(Ordering::Relaxed), 100);

        let (status, statement) =
            request(first.address, "GET", "/accounts/1/statement", b"", false);
        assert_eq!(status, 200);
        assert_eq!(statement["balance"], -100_000);
        assert_eq!(statement["transactions"].as_array().unwrap().len(), 10);
        assert!(
            statement["transactions"]
                .as_array()
                .unwrap()
                .iter()
                .all(|entry| entry["amount"] == 1000)
        );
    }

    let restarted = Service::start(&database.file());
    let (status, statement) = request(
        restarted.address,
        "GET",
        "/accounts/1/statement",
        b"",
        false,
    );
    assert_eq!(status, 200);
    assert_eq!(statement["balance"], -100_000);
    assert_eq!(statement["transactions"].as_array().unwrap().len(), 10);
}

#[test]
fn validation_fragmentation_and_integer_overflow_preserve_state() {
    let database = Database::new();
    let service = Service::start(&database.file());

    for body in [
        b"{".as_slice(),
        br#"{"amount":1.5,"kind":"credit","description":"invalid"}"#,
        br#"{"amount":0,"kind":"credit","description":"invalid"}"#,
        br#"{"amount":-1,"kind":"credit","description":"invalid"}"#,
        br#"{"amount":1,"kind":"unknown","description":"invalid"}"#,
        br#"{"amount":1,"kind":"credit","description":""}"#,
        br#"{"amount":1,"kind":"credit","description":"too long text"}"#,
        br#"{"amount":1,"kind":"credit","description":"valid","extra":true}"#,
        br#"{"amount":1,"amount":2,"kind":"credit","description":"duplicate"}"#,
    ] {
        let (status, _) = request(
            service.address,
            "POST",
            "/accounts/2/transactions",
            body,
            false,
        );
        assert_eq!(status, 422, "{}", String::from_utf8_lossy(body));
    }

    let (_, statement) = request(service.address, "GET", "/accounts/2/statement", b"", false);
    assert_eq!(statement["balance"], 0);
    assert_eq!(statement["transactions"], json!([]));

    let escaped = br#"{"amount":1,"kind":"credit","description":"\u0061"}"#;
    let (status, _) = request(
        service.address,
        "POST",
        "/accounts/3/transactions",
        escaped,
        false,
    );
    assert_eq!(status, 200);

    let (_, statement) = request(service.address, "GET", "/accounts/3/statement", b"", false);
    assert_eq!(statement["transactions"][0]["description"], "a");

    let body = br#"{"amount":9223372036854775807,"kind":"credit","description":"maximum"}"#;
    let (status, _) = request(
        service.address,
        "POST",
        "/accounts/2/transactions",
        body,
        true,
    );
    assert_eq!(status, 200);

    let body = br#"{"amount":1,"kind":"credit","description":"overflow"}"#;
    let (status, _) = request(
        service.address,
        "POST",
        "/accounts/2/transactions",
        body,
        false,
    );
    assert_eq!(status, 422);

    let (_, statement) = request(service.address, "GET", "/accounts/2/statement", b"", false);
    assert_eq!(statement["balance"], i64::MAX);
    assert_eq!(statement["transactions"].as_array().unwrap().len(), 1);

    let (status, _) = request(service.address, "GET", "/accounts/6/statement", b"", false);
    assert_eq!(status, 404);

    let (status, _) = request(
        service.address,
        "POST",
        "/accounts/2/transactions",
        &[b'x'; 513],
        false,
    );
    assert_eq!(status, 413);
}

#[test]
fn incomplete_upload_saturation_recovers_after_disconnects() {
    let database = Database::new();
    let service = Service::start(&database.file());
    let mut uploads = Vec::with_capacity(128);

    for _ in 0..128 {
        let mut stream = TcpStream::connect(service.address).unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        stream
            .write_all(
                b"POST /accounts/1/transactions HTTP/1.1\r\n\
                  Host: localhost\r\n\
                  Expect: 100-continue\r\n\
                  Content-Length: 1\r\n\r\n",
            )
            .unwrap();

        let mut interim = [0; 25];
        stream.read_exact(&mut interim).unwrap();
        assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");

        uploads.push(stream);

        std::thread::sleep(Duration::from_millis(2));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(2);

    loop {
        let (status, _) = request(service.address, "GET", "/accounts/1/statement", b"", false);

        if status == 503 {
            break;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "pending limit was not reached"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let (status, _) = request(service.address, "GET", "/ready", b"", false);
    assert_eq!(status, 200);

    drop(uploads);

    let deadline = std::time::Instant::now() + Duration::from_secs(2);

    loop {
        let (status, statement) =
            request(service.address, "GET", "/accounts/1/statement", b"", false);

        if status == 200 {
            assert_eq!(statement["balance"], 0);
            assert_eq!(statement["transactions"], json!([]));
            break;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "capacity did not recover"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn sigterm_drains_an_admitted_upload_before_exiting() {
    let database = Database::new();
    let service = Service::start(&database.file());
    let body = br#"{"amount":7,"kind":"credit","description":"drain"}"#;
    let mut stream = TcpStream::connect(service.address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let head = format!(
        "POST /accounts/1/transactions HTTP/1.1\r\n\
         Host: localhost\r\n\
         Expect: 100-continue\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len(),
    );
    stream.write_all(head.as_bytes()).unwrap();

    let mut interim = [0; 25];
    stream.read_exact(&mut interim).unwrap();
    assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");

    let status = Command::new("kill")
        .args(["-TERM", &service.child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());

    stream.write_all(body).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200 "));

    drop(service);

    let restarted = Service::start(&database.file());
    let (status, statement) = request(
        restarted.address,
        "GET",
        "/accounts/1/statement",
        b"",
        false,
    );
    assert_eq!(status, 200);
    assert_eq!(statement["balance"], 7);
    assert_eq!(statement["transactions"].as_array().unwrap().len(), 1);
}

#[test]
fn sigint_exits_successfully_without_stdin() {
    let database = Database::new();
    let service = Service::start(&database.file());

    std::thread::sleep(Duration::from_millis(30));

    let (status, _) = request(service.address, "GET", "/ready", b"", false);
    assert_eq!(status, 200);

    let status = Command::new("kill")
        .args(["-INT", &service.child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());

    drop(service);
}

#[test]
fn configured_tls_serves_verified_https_readiness() {
    let database = Database::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/testdata");
    let cert = root.join("server_cert.der");
    let key = root.join("server_key.der");
    let service = Service::start_with_options(
        &database.file(),
        &[
            "--tls-cert",
            cert.to_str().unwrap(),
            "--tls-key",
            key.to_str().unwrap(),
        ],
    );
    let certificate = include_bytes!("testdata/server_cert.der");
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(
            certificate.to_vec(),
        ))
        .unwrap();

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let connection = rustls::ClientConnection::new(Arc::new(config), name).unwrap();
    let stream = TcpStream::connect(service.address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut tls = rustls::StreamOwned::new(connection, stream);

    tls.write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();

    let mut response = Vec::new();
    let mut buffer = [0; 512];

    while !response.ends_with(b"\r\n\r\nready\n") {
        let count = tls.read(&mut buffer).unwrap();
        assert_ne!(count, 0, "TLS response ended before readiness body");
        response.extend_from_slice(&buffer[..count]);
        assert!(response.len() < 4096);
    }

    assert!(response.starts_with(b"HTTP/1.1 200 "));
}
