#![cfg(unix)]

mod harness;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use harness::{
    Database, Service, request, request_with_content_type, request_with_idempotency_key,
};
use serde_json::json;

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
                        let key = format!("debit-{index}");
                        let deadline = std::time::Instant::now() + Duration::from_secs(5);
                        let status = loop {
                            let (status, _) = request_with_idempotency_key(
                                addresses[index % 2],
                                "POST",
                                "/accounts/1/transactions",
                                body,
                                false,
                                &key,
                            );

                            if status != 503 {
                                break status;
                            }

                            assert!(
                                std::time::Instant::now() < deadline,
                                "database contention did not clear"
                            );
                            std::thread::sleep(Duration::from_millis(1));
                        };

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
    assert_eq!(
        request(service.address, "GET", "/accounts/10/statement", b"", false).0,
        404,
        "the full route capture is the account id, not its first digit"
    );

    let body = br#"{"amount":1,"kind":"credit","description":"no type"}"#;
    let (status, _) = request_with_content_type(
        service.address,
        "POST",
        "/accounts/3/transactions",
        "text/plain",
        body,
        false,
    );
    assert_eq!(status, 415);
    let (_, statement) = request(service.address, "GET", "/accounts/3/statement", b"", false);
    assert_eq!(statement["balance"], 1);

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
fn database_writer_contention_returns_without_blocking_other_requests() {
    let database = Database::new();
    let service = Service::start(&database.file());
    let lock = rusqlite::Connection::open(database.file()).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();

    let payment = br#"{"amount":10,"kind":"credit","description":"retry"}"#;
    let (status, _) = request(
        service.address,
        "POST",
        "/accounts/1/transactions",
        payment,
        false,
    );
    assert_eq!(status, 503);

    let (status, statement) = request(service.address, "GET", "/accounts/1/statement", b"", false);
    assert_eq!(status, 200);
    assert_eq!(statement["balance"], 0);

    lock.execute_batch("ROLLBACK").unwrap();

    let (status, _) = request(
        service.address,
        "POST",
        "/accounts/1/transactions",
        payment,
        false,
    );
    assert_eq!(status, 200);

    let (_, statement) = request(service.address, "GET", "/accounts/1/statement", b"", false);
    assert_eq!(statement["balance"], 10);
    assert_eq!(statement["transactions"].as_array().unwrap().len(), 1);
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
                  Content-Type: application/json\r\n\
                  Idempotency-Key: abandoned-upload\r\n\
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
         Content-Type: application/json\r\n\
         Idempotency-Key: graceful-write\r\n\
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
