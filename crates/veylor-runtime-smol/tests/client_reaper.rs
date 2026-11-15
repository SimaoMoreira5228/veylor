use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use smol::io::{AsyncReadExt, AsyncWriteExt};
use smol::net::{TcpListener, TcpStream};
mod support;

use support::{sleep, spawn, timeout};
use veylor_runtime_smol::{Client, ClientConfig, ClientRequest, PoolConfig};

#[allow(dead_code)]
#[path = "h2_client/peer.rs"]
mod peer;

use peer::{HEADERS, Peer, listen};

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";

struct Server {
    url: String,
    closed: Arc<AtomicUsize>,
}

async fn spawn_server() -> Server {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let closed = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&closed);

    spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.expect("accept");

            spawn(serve(socket, Arc::clone(&count)));
        }
    });

    Server {
        url: format!("http://{addr}/"),
        closed,
    }
}

async fn serve(mut socket: TcpStream, closed: Arc<AtomicUsize>) {
    let mut buf = [0u8; 4096];

    loop {
        match socket.read(&mut buf).await {
            Ok(0) | Err(_) => {
                closed.fetch_add(1, Ordering::SeqCst);

                return;
            }
            Ok(_) => {
                if socket.write_all(RESPONSE).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn eventually(mut check: impl FnMut() -> bool) -> bool {
    for _ in 0..150 {
        if check() {
            return true;
        }

        sleep(Duration::from_millis(20)).await;
    }

    false
}

fn client(idle_timeout: Duration, h2c: bool) -> Client {
    Client::new(ClientConfig {
        h2c_prior_knowledge: h2c,
        pool: PoolConfig {
            idle_timeout,
            ..PoolConfig::default()
        },
        ..Default::default()
    })
    .expect("client")
}

#[test]
fn idle_h1_sockets_are_closed_without_another_request() {
    smol::block_on(async {
        let server = spawn_server().await;
        let client = client(Duration::from_millis(200), false);

        client
            .request(&ClientRequest::get(&server.url))
            .await
            .expect("response");

        assert_eq!(client.pool_stats().idle, 1);
        assert!(eventually(|| server.closed.load(Ordering::SeqCst) == 1).await);

        let stats = client.pool_stats();

        assert_eq!((stats.origins, stats.open, stats.idle), (0, 0, 0));
    });
}

#[test]
fn busy_connections_survive_the_reaper() {
    smol::block_on(async {
        let server = spawn_server().await;
        let client = client(Duration::from_millis(100), false);
        let stream = client
            .send(&ClientRequest::get(&server.url))
            .await
            .expect("send");

        sleep(Duration::from_millis(400)).await;

        assert_eq!(server.closed.load(Ordering::SeqCst), 0);
        assert_eq!(stream.collect(16).await.expect("body"), b"ok");
    });
}

#[test]
fn dropping_the_client_closes_pooled_sockets_promptly() {
    smol::block_on(async {
        let server = spawn_server().await;
        let client = client(Duration::from_secs(300), false);

        client
            .request(&ClientRequest::get(&server.url))
            .await
            .expect("response");

        drop(client);

        assert!(eventually(|| server.closed.load(Ordering::SeqCst) == 1).await);
    });
}

#[test]
fn expired_h2_connections_leave_the_pool_on_their_own() {
    smol::block_on(async {
        let (listener, addr) = listen().await;
        let client = client(Duration::from_millis(150), true);
        let url = format!("http://{addr}/");
        let task = {
            let client = client.clone();

            spawn(async move { client.request(&ClientRequest::get(&url)).await })
        };
        let mut peer = Peer::accept(&listener).await;

        peer.handshake(&[]).await;

        let frame = timeout(Duration::from_secs(5), peer.next_non_update())
            .await
            .expect("request deadline");

        assert_eq!(frame.kind, HEADERS);

        peer.respond(frame.stream, "200", b"a").await;
        task.await.expect("join").expect("response");

        assert_eq!(client.pool_stats().h2, 1);
        assert!(eventually(|| client.pool_stats().h2 == 0).await);
    });
}
