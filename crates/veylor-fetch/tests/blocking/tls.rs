use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use veylor_fetch::{Agent, AgentBuilder, Error, Protocol};
use veylor_server::{MAX_STATIC_BODY_BYTES, RunningServer, Server, ServerConfig, TlsConfig};
use veylor_tls::{Certificate, PrivateKey, TlsAcceptor, TlsStream};

const CERT: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/leaf_cert.der");
const KEY: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/leaf_key.der");
const OTHER_CERT: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_cert.der");

const BIG: usize = 600_000;

fn trusting() -> AgentBuilder {
    Agent::builder()
        .builtin_root_certificates(false)
        .add_root_certificate(CERT)
}

fn managed_server() -> RunningServer {
    let config = ServerConfig {
        tls: Some(TlsConfig {
            cert_chain: vec![Certificate::from(CERT.to_vec())],
            key: PrivateKey::Pkcs8(KEY.to_vec()),
            max_handshakes: 64,
        }),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(SocketAddr::from(([127, 0, 0, 1], 0)), config).expect("bind");

    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("route");

    server
        .register_file_mount(b"/files/", &fixture_dir())
        .expect("mount");

    server.start().expect("start")
}

fn fixture_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("veylor-fetch-{}", std::process::id()));

    std::fs::create_dir_all(&dir).expect("dir");
    std::fs::write(dir.join("big.bin"), vec![b'B'; BIG]).expect("fixture");

    dir
}

fn url(server: &RunningServer, path: &str) -> String {
    format!("https://localhost:{}{path}", server.addr().port())
}

fn stop(server: RunningServer) {
    server.stop_accepting();
    server.join().expect("join");
}

#[test]
fn alpn_picks_h2_and_one_connection_serves_every_request() {
    let server = managed_server();
    let agent = trusting().build().expect("agent");

    for _ in 0..5 {
        let response = agent.get(&url(&server, "/hello")).call().expect("response");

        assert_eq!(response.status(), 200);
        assert_eq!(response.protocol(), Protocol::Http2);
        assert_eq!(response.text().expect("body"), "hello");
    }

    assert_eq!(agent.pool_stats().h2, 1);
    assert_eq!(agent.pool_stats().idle_h1, 0);

    stop(server);
}

#[test]
fn h2_streams_a_large_body_through_a_small_reader() {
    let server = managed_server();
    let agent = trusting().build().expect("agent");
    let mut reader = agent
        .get(&url(&server, "/files/big.bin"))
        .call()
        .expect("response")
        .into_reader();
    let mut buf = [0u8; 100];
    let mut seen = 0;

    loop {
        let count = reader.read(&mut buf).expect("read");

        if count == 0 {
            break;
        }

        assert!(buf[..count].iter().all(|byte| *byte == b'B'));
        seen += count;
    }

    assert_eq!(seen, BIG);

    let again = agent.get(&url(&server, "/hello")).call().expect("again");

    assert_eq!(again.text().expect("body"), "hello");
    assert_eq!(agent.pool_stats().h2, 1);

    stop(server);
}

#[test]
fn h2_abandoned_body_falls_back_to_a_new_connection() {
    let server = managed_server();
    let agent = trusting().build().expect("agent");
    let mut reader = agent
        .get(&url(&server, "/files/big.bin"))
        .call()
        .expect("response")
        .into_reader();
    let mut buf = [0u8; 10];

    reader.read_exact(&mut buf).expect("some bytes");
    drop(reader);

    let again = agent.get(&url(&server, "/hello")).call().expect("again");

    assert_eq!(again.text().expect("body"), "hello");

    stop(server);
}

#[test]
fn h2_threads_share_an_agent() {
    let server = managed_server();
    let agent = trusting().build().expect("agent");
    let target = url(&server, "/hello");
    let workers: Vec<_> = (0..6)
        .map(|_| {
            let agent = agent.clone();
            let target = target.clone();

            std::thread::spawn(move || {
                for _ in 0..8 {
                    let text = agent
                        .get(&target)
                        .call()
                        .expect("response")
                        .text()
                        .expect("body");

                    assert_eq!(text, "hello");
                }
            })
        })
        .collect();

    for worker in workers {
        worker.join().expect("worker");
    }

    assert!(agent.pool_stats().h2 >= 1);

    stop(server);
}

#[test]
fn h2_error_statuses_are_errors_with_a_readable_body() {
    let server = managed_server();
    let agent = trusting().build().expect("agent");
    let error = agent
        .get(&url(&server, "/missing"))
        .call()
        .expect_err("404");

    assert_eq!(error.status(), Some(404));
    assert_eq!(
        error.response().map(|response| response.protocol()),
        Some(Protocol::Http2)
    );

    stop(server);
}

#[test]
fn http1_only_agents_use_http11_over_tls() {
    let server = managed_server();
    let agent = trusting().http1_only(true).build().expect("agent");

    for _ in 0..3 {
        let response = agent.get(&url(&server, "/hello")).call().expect("response");

        assert_eq!(response.protocol(), Protocol::Http1);
        assert_eq!(response.text().expect("body"), "hello");
    }

    assert_eq!(agent.pool_stats().h2, 0);
    assert_eq!(agent.pool_stats().idle_h1, 1);

    stop(server);
}

#[test]
fn untrusted_certificates_fail_verification() {
    let server = managed_server();
    let error = Agent::new()
        .get(&url(&server, "/hello"))
        .call()
        .expect_err("untrusted");

    assert!(error.is_connect(), "{error}");

    let wrong_root = Agent::builder()
        .builtin_root_certificates(false)
        .add_root_certificate(OTHER_CERT)
        .build()
        .expect("agent");
    let error = wrong_root
        .get(&url(&server, "/hello"))
        .call()
        .expect_err("wrong root");

    assert!(error.is_connect(), "{error}");

    stop(server);
}

#[test]
fn name_mismatch_fails_but_a_pinned_certificate_connects() {
    let server = managed_server();
    let by_ip = format!("https://127.0.0.1:{}/hello", server.addr().port());
    let error = trusting().build().expect("agent").get(&by_ip).call();

    assert!(matches!(error, Err(Error::Tls(_))), "{error:?}");

    let pinned = Agent::builder()
        .pin_certificate(CERT)
        .build()
        .expect("agent");
    let text = pinned
        .get(&by_ip)
        .call()
        .expect("pinned")
        .text()
        .expect("body");

    assert_eq!(text, "hello");

    let wrong = Agent::builder()
        .pin_certificate(OTHER_CERT)
        .build()
        .expect("agent");

    assert!(
        wrong
            .get(&by_ip)
            .call()
            .expect_err("wrong pin")
            .is_connect()
    );

    stop(server);
}

#[test]
fn tls_http11_keep_alive_against_a_raw_server() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    let mut acceptor = TlsAcceptor::new(CERT, KEY, usize::MAX)
        .expect("acceptor")
        .with_alpn(&[b"http/1.1"]);

    std::thread::spawn(move || {
        for socket in listener.incoming() {
            let Ok(socket) = socket else {
                return;
            };
            let conn = acceptor.admit().expect("conn");

            counter.fetch_add(1, Ordering::SeqCst);

            std::thread::spawn(move || serve_tls(TlsStream::new(socket, conn)));
        }
    });

    let agent = trusting().build().expect("agent");

    for _ in 0..3 {
        let response = agent
            .get(&format!("https://localhost:{port}/"))
            .call()
            .expect("response");

        assert_eq!(response.protocol(), Protocol::Http1);
        assert_eq!(response.text().expect("body"), "secure");
    }

    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

fn serve_tls(mut stream: TlsStream<std::net::TcpStream>) {
    let mut seen = Vec::new();
    let mut chunk = [0u8; 4096];

    loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(count) => seen.extend_from_slice(&chunk[..count]),
        }

        while let Some(end) = seen.windows(4).position(|window| window == b"\r\n\r\n") {
            seen.drain(..end + 4);

            let reply = "HTTP/1.1 200 OK\r\ncontent-length: 6\r\n\r\nsecure";

            if stream.write_all(reply.as_bytes()).is_err() || stream.flush().is_err() {
                return;
            }
        }
    }
}
