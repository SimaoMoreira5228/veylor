use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

mod support;

use support::{sleep, spawn};
use veylor_runtime_smol::{
    Client, ClientConfig, ClientRequest, H2Config, PoolConfig, Protocol, StreamingBody,
    TlsConnector,
};
use veylor_server::{MAX_STATIC_BODY_BYTES, RunningServer, Server, ServerConfig, TlsConfig};
use veylor_tls::{Certificate, PrivateKey, TlsAcceptor, TlsStream};

const CERT: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/leaf_cert.der");
const KEY: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/leaf_key.der");

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello";

fn connector(alpn: &[&[u8]]) -> TlsConnector {
    TlsConnector::builder()
        .default_roots(false)
        .add_root(CERT)
        .expect("root")
        .alpn(alpn)
        .build()
        .expect("connector")
}

fn client(alpn: &[&[u8]], pool: PoolConfig) -> Client {
    Client::new(ClientConfig {
        tls: Some(connector(alpn)),
        pool,
        ..Default::default()
    })
    .expect("client")
}

fn managed_tls_server() -> RunningServer {
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

    server.start().expect("start")
}

struct RawTls {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
    clean_closes: Arc<AtomicUsize>,
    answers: bool,
}

fn spawn_raw_tls(alpn: &[&[u8]], answers: bool) -> RawTls {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
    let addr = listener.local_addr().expect("addr");
    let accepted = Arc::new(AtomicUsize::new(0));
    let clean_closes = Arc::new(AtomicUsize::new(0));
    let mut acceptor = TlsAcceptor::new(CERT, KEY, usize::MAX)
        .expect("acceptor")
        .with_alpn(alpn);

    let count = Arc::clone(&accepted);
    let clean = Arc::clone(&clean_closes);

    std::thread::spawn(move || {
        for socket in listener.incoming() {
            let Ok(socket) = socket else {
                return;
            };
            let conn = acceptor.admit().expect("conn");
            let clean = Arc::clone(&clean);

            count.fetch_add(1, Ordering::SeqCst);
            std::thread::spawn(move || serve_raw(TlsStream::new(socket, conn), answers, clean));
        }
    });

    RawTls {
        addr,
        accepted,
        clean_closes,
        answers,
    }
}

fn serve_raw(mut stream: TlsStream<std::net::TcpStream>, answers: bool, clean: Arc<AtomicUsize>) {
    let mut buf = [0u8; 4096];
    let mut seen = Vec::new();

    loop {
        match stream.read(&mut buf) {
            Ok(0) => {
                clean.fetch_add(1, Ordering::SeqCst);
                return;
            }
            Ok(n) => seen.extend_from_slice(&buf[..n]),
            Err(_) => return,
        }

        if answers && seen.windows(4).any(|w| w == b"\r\n\r\n") {
            seen.clear();

            if stream.write_all(RESPONSE).is_err() || stream.flush().is_err() {
                return;
            }
        }
    }
}

async fn eventually(mut check: impl FnMut() -> bool) -> bool {
    for _ in 0..100 {
        if check() {
            return true;
        }

        sleep(Duration::from_millis(20)).await;
    }

    false
}

#[test]
fn alpn_selects_h2_against_veylor_server() {
    smol::block_on(async {
        let server = managed_tls_server();
        let client = client(&[b"h2", b"http/1.1"], PoolConfig::default());
        let url = format!("https://localhost:{}/hello", server.addr().port());

        for _ in 0..5 {
            let response = client
                .request(&ClientRequest::get(&url))
                .await
                .expect("response");

            assert_eq!(response.status, 200);
            assert_eq!(response.protocol, Protocol::Http2);
            assert_eq!(response.body, b"hello");
        }

        let stream = client.send(&ClientRequest::get(&url)).await.expect("send");

        assert_eq!(stream.alpn(), Some(b"h2".as_slice()));
        assert_eq!(client.pool_stats().h2, 1);
        assert_eq!(client.pool_stats().open, 0);

        drop(stream);
        server.stop_accepting();
        server.join().expect("join");
    });
}

#[test]
fn alpn_selects_http11_against_veylor_server() {
    smol::block_on(async {
        let server = managed_tls_server();
        let client = client(&[b"http/1.1"], PoolConfig::default());
        let url = format!("https://localhost:{}/hello", server.addr().port());

        for _ in 0..3 {
            let response = client
                .request(&ClientRequest::get(&url))
                .await
                .expect("response");

            assert_eq!(response.protocol, Protocol::Http1);
            assert_eq!(response.body, b"hello");
        }

        assert_eq!(client.pool_stats().h2, 0);
        assert_eq!(client.pool_stats().idle, 1);

        server.stop_accepting();
        server.join().expect("join");
    });
}

#[test]
fn concurrent_first_requests_share_h2_connections() {
    smol::block_on(async {
        let server = managed_tls_server();
        let client = client(&[b"h2", b"http/1.1"], PoolConfig::default());
        let url = format!("https://localhost:{}/hello", server.addr().port());
        let mut tasks = Vec::new();

        for _ in 0..16 {
            let client = client.clone();
            let url = url.clone();

            tasks.push(spawn(async move {
                client.request(&ClientRequest::get(&url)).await
            }));
        }

        for task in tasks {
            let response = task.await.expect("join").expect("response");

            assert_eq!(response.body, b"hello");
        }

        let stats = client.pool_stats();

        assert!(stats.h2 >= 1 && stats.h2 <= 2, "{stats:?}");
        assert_eq!(stats.open, 0);

        server.stop_accepting();
        server.join().expect("join");
    });
}

#[test]
fn falls_back_to_http11_when_the_server_only_offers_it() {
    smol::block_on(async {
        let server = spawn_raw_tls(&[b"http/1.1"], true);
        let client = client(&[b"h2", b"http/1.1"], PoolConfig::default());
        let url = format!("https://localhost:{}/", server.addr.port());

        for _ in 0..3 {
            let response = client
                .request(&ClientRequest::get(&url))
                .await
                .expect("response");

            assert_eq!(response.protocol, Protocol::Http1);
            assert_eq!(response.body, b"hello");
        }

        assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
        assert!(server.answers);
    });
}

#[test]
fn pooled_http11_connections_send_close_notify() {
    smol::block_on(async {
        let server = spawn_raw_tls(&[b"http/1.1"], true);
        let client = client(&[b"http/1.1"], PoolConfig::default());
        let url = format!("https://localhost:{}/", server.addr.port());

        client
            .request(&ClientRequest::get(&url))
            .await
            .expect("response");
        client.close_idle();

        let clean = Arc::clone(&server.clean_closes);

        assert!(eventually(|| clean.load(Ordering::SeqCst) == 1).await);
    });
}

#[test]
fn expired_http11_connections_send_close_notify() {
    smol::block_on(async {
        let server = spawn_raw_tls(&[b"http/1.1"], true);
        let client = client(
            &[b"http/1.1"],
            PoolConfig {
                idle_timeout: Duration::from_millis(100),
                ..PoolConfig::default()
            },
        );
        let url = format!("https://localhost:{}/", server.addr.port());

        client
            .request(&ClientRequest::get(&url))
            .await
            .expect("response");
        sleep(Duration::from_millis(300)).await;
        client
            .request(&ClientRequest::get(&url))
            .await
            .expect("again");

        assert_eq!(server.accepted.load(Ordering::SeqCst), 2);

        let clean = Arc::clone(&server.clean_closes);

        assert!(eventually(|| clean.load(Ordering::SeqCst) >= 1).await);
    });
}

#[test]
fn pooled_h2_connections_send_close_notify() {
    smol::block_on(async {
        let server = spawn_raw_tls(&[b"h2"], false);
        let client = Client::new(ClientConfig {
            tls: Some(connector(&[b"h2"])),
            h2: H2Config {
                handshake_timeout: Duration::from_millis(300),
                ..H2Config::default()
            },
            ..Default::default()
        })
        .expect("client");
        let url = format!("https://localhost:{}/", server.addr.port());
        let pending = {
            let client = client.clone();

            spawn(async move { client.request(&ClientRequest::get(&url)).await })
        };

        assert!(pending.await.expect("join").is_err());
        client.close_idle();

        let clean = Arc::clone(&server.clean_closes);

        assert!(eventually(|| clean.load(Ordering::SeqCst) == 1).await);
    });
}

#[test]
fn cancelling_a_dial_frees_the_origin() {
    smol::block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
        let addr = listener.local_addr().expect("addr");
        let client = client(&[b"h2", b"http/1.1"], PoolConfig::default());
        let url = format!("https://localhost:{}/", addr.port());
        let task = {
            let client = client.clone();
            let url = url.clone();

            spawn(async move { client.request(&ClientRequest::get(&url)).await })
        };

        sleep(Duration::from_millis(100)).await;
        task.abort().await;
        let _ = task.await;

        let stats = client.pool_stats();

        assert_eq!((stats.origins, stats.open, stats.h2), (0, 0, 0));
    });
}

#[test]
fn streaming_bodies_are_chunked_over_http11() {
    smol::block_on(async {
        let server = spawn_raw_tls(&[b"http/1.1"], true);
        let client = client(&[b"http/1.1"], PoolConfig::default());
        let url = format!("https://localhost:{}/", server.addr.port());
        let (body, mut sender) = StreamingBody::new(1024);

        sender.write_all(b"abc").await.expect("write");
        sender.finish();

        let response = client
            .send(&ClientRequest::new(b"POST", &url).streaming(&body))
            .await
            .expect("send");

        assert_eq!(response.status(), 200);
        assert_eq!(response.alpn(), Some(b"http/1.1".as_slice()));
    });
}

#[test]
fn waiters_are_bounded_while_a_dial_is_in_flight() {
    smol::block_on(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
        let addr = listener.local_addr().expect("addr");
        let client = client(
            &[b"h2", b"http/1.1"],
            PoolConfig {
                max_waiters_per_origin: 1,
                ..PoolConfig::default()
            },
        );
        let url = format!("https://localhost:{}/", addr.port());
        let get = |client: &Client| {
            let client = client.clone();
            let url = url.clone();

            spawn(async move { client.request(&ClientRequest::get(&url)).await })
        };
        let dialing = get(&client);

        sleep(Duration::from_millis(100)).await;

        let waiting = get(&client);

        sleep(Duration::from_millis(100)).await;

        let error = get(&client).await.expect("join").err().expect("must fail");

        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

        dialing.abort().await;
        waiting.abort().await;
    });
}
