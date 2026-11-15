#![allow(clippy::err_expect)]

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use veylor_runtime_tokio::{Client, ClientConfig, ClientRequest, PoolConfig, TlsConnector};
use veylor_tls::{
    TlsAcceptor, TlsConn, TlsError, drain_plaintext, feed_ciphertext, flush_ciphertext,
    queue_close_notify,
};

const CERT: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/leaf_cert.der");
const KEY: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/leaf_key.der");
const OTHER_CERT: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_cert2.pem");

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello";

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    KeepAlive,
    CloseAfterResponse,
    Silent,
}

struct Server {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
}

async fn spawn_plain(mode: Mode) -> Server {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&accepted);

    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.expect("accept");

            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(serve_raw(socket, None, mode));
        }
    });

    Server { addr, accepted }
}

async fn spawn_tls(mode: Mode) -> Server {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&accepted);
    let mut acceptor = TlsAcceptor::new(CERT, KEY, 64).expect("acceptor");

    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.expect("accept");
            let conn = acceptor.admit().expect("admit");

            acceptor.release();
            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(serve_raw(socket, Some(conn), mode));
        }
    });

    Server { addr, accepted }
}

async fn serve_raw(mut socket: TcpStream, mut tls: Option<TlsConn>, mode: Mode) {
    let mut buf = vec![0u8; 16 * 1024];
    let mut plain = Vec::new();
    let mut out = Vec::new();

    loop {
        let read = match socket.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };

        out.clear();

        match tls.as_mut() {
            Some(conn) => {
                feed_ciphertext(conn, &buf[..read], &mut plain).expect("feed");
                flush_ciphertext(conn, &mut out).expect("flush");
            }
            None => plain.extend_from_slice(&buf[..read]),
        }

        let complete = plain.windows(4).any(|w| w == b"\r\n\r\n");

        if complete && mode != Mode::Silent {
            plain.clear();

            match tls.as_mut() {
                Some(conn) => drain_plaintext(conn, RESPONSE, &mut out).expect("drain"),
                None => out.extend_from_slice(RESPONSE),
            }

            if let (Mode::CloseAfterResponse, Some(conn)) = (mode, tls.as_mut()) {
                queue_close_notify(conn, &mut out).expect("close notify");
            }
        }

        if !out.is_empty() && socket.write_all(&out).await.is_err() {
            return;
        }

        if complete && mode == Mode::CloseAfterResponse {
            return;
        }
    }
}

fn connector(root: &[u8], alpn: &[&[u8]]) -> TlsConnector {
    TlsConnector::builder()
        .default_roots(false)
        .add_root(root)
        .expect("root")
        .alpn(alpn)
        .build()
        .expect("connector")
}

fn tls_client(root: &[u8], alpn: &[&[u8]]) -> Client {
    Client::new(ClientConfig {
        tls: Some(connector(root, alpn)),
        ..Default::default()
    })
    .expect("client")
}

fn pooled(pool: PoolConfig) -> Client {
    Client::new(ClientConfig {
        pool,
        head_timeout: Duration::from_millis(300),
        ..Default::default()
    })
    .expect("client")
}

fn tls_error(error: &io::Error) -> Option<&TlsError> {
    error.get_ref()?.downcast_ref::<TlsError>()
}

#[tokio::test]
async fn https_verifies_and_negotiates_http11() {
    let server = spawn_tls(Mode::KeepAlive).await;
    let client = tls_client(CERT, &[b"http/1.1"]);
    let url = format!("https://localhost:{}/", server.addr.port());

    let mut stream = client.send(&ClientRequest::get(&url)).await.expect("send");

    assert_eq!(stream.status(), 200);
    assert_eq!(stream.alpn(), Some(b"http/1.1".as_slice()));
    assert_eq!(stream.collect(1024).await.expect("body"), b"hello");

    let again = client
        .request(&ClientRequest::get(&url))
        .await
        .expect("again");

    assert_eq!(again.body, b"hello");
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn https_wrong_host_fails_verification() {
    let server = spawn_tls(Mode::KeepAlive).await;
    let client = tls_client(CERT, &[b"http/1.1"]);
    let url = format!("https://127.0.0.1:{}/", server.addr.port());

    let error = client
        .request(&ClientRequest::get(&url))
        .await
        .err()
        .expect("must fail");

    assert!(
        matches!(tls_error(&error), Some(TlsError::Certificate)),
        "{error:?}"
    );
    assert_eq!(client.pool_stats().open, 0);
}

#[tokio::test]
async fn https_untrusted_certificate_fails() {
    let server = spawn_tls(Mode::KeepAlive).await;
    let client = tls_client(OTHER_CERT, &[b"http/1.1"]);
    let url = format!("https://localhost:{}/", server.addr.port());

    let error = client
        .request(&ClientRequest::get(&url))
        .await
        .err()
        .expect("must fail");

    assert!(
        matches!(tls_error(&error), Some(TlsError::Certificate)),
        "{error:?}"
    );
}

#[tokio::test]
async fn https_default_roots_reject_the_test_certificate() {
    let server = spawn_tls(Mode::KeepAlive).await;
    let client = Client::new(ClientConfig::default()).expect("client");
    let url = format!("https://localhost:{}/", server.addr.port());

    assert!(client.request(&ClientRequest::get(&url)).await.is_err());
}

#[tokio::test]
async fn offering_h2_is_refused_by_the_http1_client() {
    let server = spawn_tls(Mode::KeepAlive).await;
    let client = tls_client(CERT, &[b"h2", b"http/1.1"]);
    let url = format!("https://localhost:{}/", server.addr.port());

    let error = client
        .request(&ClientRequest::get(&url))
        .await
        .err()
        .expect("must fail");

    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
}

#[tokio::test]
async fn pool_reuses_one_connection() {
    let server = spawn_plain(Mode::KeepAlive).await;
    let client = pooled(PoolConfig::default());
    let url = format!("http://{}/", server.addr);

    for _ in 0..3 {
        let response = client
            .request(&ClientRequest::get(&url))
            .await
            .expect("get");

        assert_eq!(response.body, b"hello");
    }

    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(client.pool_stats().idle, 1);
}

#[tokio::test]
async fn pool_caps_connections_per_origin() {
    let server = spawn_plain(Mode::KeepAlive).await;
    let client = pooled(PoolConfig {
        max_per_origin: 1,
        acquire_timeout: Duration::from_millis(100),
        ..Default::default()
    });
    let url = format!("http://{}/", server.addr);

    let held = client.send(&ClientRequest::get(&url)).await.expect("first");
    let error = client
        .request(&ClientRequest::get(&url))
        .await
        .err()
        .expect("second must wait out");

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);

    held.collect(1024).await.expect("finish first");

    let waiting = client
        .request(&ClientRequest::get(&url))
        .await
        .expect("third");

    assert_eq!(waiting.status, 200);
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn waiter_wakes_when_a_connection_returns() {
    let server = spawn_plain(Mode::KeepAlive).await;
    let client = pooled(PoolConfig {
        max_per_origin: 1,
        ..Default::default()
    });
    let url = format!("http://{}/", server.addr);

    let held = client.send(&ClientRequest::get(&url)).await.expect("first");
    let other = client.clone();
    let other_url = url.clone();
    let waiter = tokio::spawn(async move { other.request(&ClientRequest::get(&other_url)).await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    held.collect(1024).await.expect("finish first");

    let response = waiter.await.expect("join").expect("waiter");

    assert_eq!(response.body, b"hello");
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn idle_cap_closes_extra_connections() {
    let server = spawn_plain(Mode::KeepAlive).await;
    let client = pooled(PoolConfig {
        max_idle_per_origin: 1,
        max_per_origin: 4,
        ..Default::default()
    });
    let url = format!("http://{}/", server.addr);

    let a = client.send(&ClientRequest::get(&url)).await.expect("a");
    let b = client.send(&ClientRequest::get(&url)).await.expect("b");

    a.collect(1024).await.expect("a body");
    b.collect(1024).await.expect("b body");

    let stats = client.pool_stats();

    assert_eq!(stats.idle, 1);
    assert_eq!(stats.open, 1);
}

#[tokio::test]
async fn idle_connections_expire() {
    let server = spawn_plain(Mode::KeepAlive).await;
    let client = pooled(PoolConfig {
        idle_timeout: Duration::from_millis(60),
        ..Default::default()
    });
    let url = format!("http://{}/", server.addr);

    client
        .request(&ClientRequest::get(&url))
        .await
        .expect("first");
    tokio::time::sleep(Duration::from_millis(150)).await;
    client
        .request(&ClientRequest::get(&url))
        .await
        .expect("second");

    assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
    assert_eq!(client.pool_stats().open, 1);
}

#[tokio::test]
async fn server_closing_a_pooled_connection_is_detected() {
    let server = spawn_plain(Mode::CloseAfterResponse).await;
    let client = pooled(PoolConfig::default());
    let url = format!("http://{}/", server.addr);

    client
        .request(&ClientRequest::get(&url))
        .await
        .expect("first");
    tokio::time::sleep(Duration::from_millis(50)).await;
    client
        .request(&ClientRequest::get(&url))
        .await
        .expect("second");

    assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn tls_server_closing_a_pooled_connection_is_detected() {
    let server = spawn_tls(Mode::CloseAfterResponse).await;
    let client = tls_client(CERT, &[b"http/1.1"]);
    let url = format!("https://localhost:{}/", server.addr.port());

    client
        .request(&ClientRequest::get(&url))
        .await
        .expect("first");
    tokio::time::sleep(Duration::from_millis(50)).await;
    client
        .request(&ClientRequest::get(&url))
        .await
        .expect("second");

    assert_eq!(server.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn head_timeout_and_cancellation_release_the_slot() {
    let server = spawn_plain(Mode::Silent).await;
    let client = pooled(PoolConfig {
        max_per_origin: 1,
        ..Default::default()
    });
    let url = format!("http://{}/", server.addr);

    let error = client
        .request(&ClientRequest::get(&url))
        .await
        .err()
        .expect("must time out");

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(client.pool_stats().open, 0);

    let cancelled = tokio::time::timeout(
        Duration::from_millis(50),
        client.request(&ClientRequest::get(&url)),
    )
    .await;

    assert!(cancelled.is_err());
    assert_eq!(client.pool_stats().open, 0);
    assert_eq!(client.pool_stats().idle, 0);
}

#[tokio::test]
async fn connect_failure_reports_error_and_frees_the_slot() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    drop(listener);

    let client = pooled(PoolConfig::default());
    let error = client
        .request(&ClientRequest::get(&format!("http://{addr}/")))
        .await
        .err()
        .expect("refused");

    assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    assert_eq!(client.pool_stats().open, 0);
}

#[tokio::test]
async fn hostname_resolves_and_falls_back_across_families() {
    let server = spawn_plain(Mode::KeepAlive).await;
    let client = pooled(PoolConfig::default());
    let url = format!("http://localhost:{}/", server.addr.port());
    let response = client
        .request(&ClientRequest::get(&url))
        .await
        .expect("get");

    assert_eq!(response.status, 200);
}
