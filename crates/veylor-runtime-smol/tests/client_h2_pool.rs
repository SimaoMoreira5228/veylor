use std::time::Duration;

use smol::net::TcpListener;
mod support;

use support::{Join, sleep, spawn, timeout};
use veylor_runtime_smol::{
    Client, ClientConfig, ClientRequest, ClientResponse, PoolConfig, Protocol, StreamingBody,
};

#[allow(dead_code)]
#[path = "h2_client/peer.rs"]
mod peer;

use peer::{HEADERS, Peer, RST_STREAM, SETTING_MAX_CONCURRENT_STREAMS, listen};

type Pending = Join<std::io::Result<ClientResponse>>;

fn h2c_client(pool: PoolConfig) -> Client {
    Client::new(ClientConfig {
        h2c_prior_knowledge: true,
        pool,
        ..Default::default()
    })
    .expect("client")
}

fn spawn_get(client: &Client, url: &str) -> Pending {
    let client = client.clone();
    let url = url.to_owned();

    spawn(async move { client.request(&ClientRequest::get(&url)).await })
}

async fn request_stream(peer: &mut Peer) -> u32 {
    let frame = timeout(Duration::from_secs(5), peer.next_non_update())
        .await
        .expect("request deadline");

    assert_eq!(frame.kind, HEADERS, "{frame:?}");

    frame.stream
}

async fn no_second_connection(listener: &TcpListener) {
    assert!(
        timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err(),
        "unexpected extra connection"
    );
}

#[test]
fn concurrent_first_requests_open_one_connection() {
    smol::block_on(async {
        let (listener, addr) = listen().await;
        let client = h2c_client(PoolConfig::default());
        let url = format!("http://{addr}/");
        let tasks: Vec<Pending> = (0..8).map(|_| spawn_get(&client, &url)).collect();
        let mut peer = Peer::accept(&listener).await;

        peer.handshake(&[]).await;

        let mut streams = Vec::new();

        for _ in 0..8 {
            streams.push(request_stream(&mut peer).await);
        }

        for stream in streams {
            peer.respond(stream, "200", b"ok").await;
        }

        for task in tasks {
            let response = task.await.expect("join").expect("response");

            assert_eq!(response.protocol, Protocol::Http2);
            assert_eq!(response.body, b"ok");
        }

        no_second_connection(&listener).await;
        assert_eq!(client.pool_stats().h2, 1);
    });
}

#[test]
fn saturated_connection_opens_a_second_one() {
    smol::block_on(async {
        let (listener, addr) = listen().await;
        let client = h2c_client(PoolConfig::default());
        let url = format!("http://{addr}/");
        let first = spawn_get(&client, &url);
        let mut one = Peer::accept(&listener).await;

        one.handshake(&[(SETTING_MAX_CONCURRENT_STREAMS, 1)]).await;

        let stream = request_stream(&mut one).await;
        let second = spawn_get(&client, &url);
        let mut two = Peer::accept(&listener).await;

        two.handshake(&[(SETTING_MAX_CONCURRENT_STREAMS, 1)]).await;

        let other = request_stream(&mut two).await;

        one.respond(stream, "200", b"a").await;
        two.respond(other, "200", b"b").await;

        assert_eq!(first.await.expect("join").expect("response").body, b"a");
        assert_eq!(second.await.expect("join").expect("response").body, b"b");
        assert_eq!(client.pool_stats().h2, 2);
    });
}

#[test]
fn connections_per_origin_are_capped() {
    smol::block_on(async {
        let (listener, addr) = listen().await;
        let client = h2c_client(PoolConfig {
            max_h2_per_origin: 1,
            ..PoolConfig::default()
        });
        let url = format!("http://{addr}/");
        let first = spawn_get(&client, &url);
        let mut peer = Peer::accept(&listener).await;

        peer.handshake(&[(SETTING_MAX_CONCURRENT_STREAMS, 1)]).await;

        let one = request_stream(&mut peer).await;
        let second = spawn_get(&client, &url);

        no_second_connection(&listener).await;
        peer.respond(one, "200", b"a").await;

        let two = request_stream(&mut peer).await;

        peer.respond(two, "200", b"b").await;

        assert_eq!(first.await.expect("join").expect("response").body, b"a");
        assert_eq!(second.await.expect("join").expect("response").body, b"b");
    });
}

#[test]
fn goaway_rolls_over_to_a_new_connection() {
    smol::block_on(async {
        let (listener, addr) = listen().await;
        let client = h2c_client(PoolConfig::default());
        let url = format!("http://{addr}/");
        let first = spawn_get(&client, &url);
        let mut one = Peer::accept(&listener).await;

        one.handshake(&[]).await;

        let stream = request_stream(&mut one).await;

        one.respond(stream, "200", b"a").await;
        assert_eq!(first.await.expect("join").expect("response").body, b"a");

        one.send_goaway(stream, 0).await;
        sleep(Duration::from_millis(100)).await;

        let second = spawn_get(&client, &url);
        let mut two = Peer::accept(&listener).await;

        two.handshake(&[]).await;

        let stream = request_stream(&mut two).await;

        two.respond(stream, "200", b"b").await;

        assert_eq!(second.await.expect("join").expect("response").body, b"b");
        assert_eq!(client.pool_stats().h2, 1);
    });
}

#[test]
fn dropped_request_resets_its_stream_and_keeps_the_connection() {
    smol::block_on(async {
        let (listener, addr) = listen().await;
        let client = h2c_client(PoolConfig::default());
        let url = format!("http://{addr}/");
        let doomed = spawn_get(&client, &url);
        let mut peer = Peer::accept(&listener).await;

        peer.handshake(&[]).await;

        let stream = request_stream(&mut peer).await;

        doomed.abort().await;

        let reset = timeout(Duration::from_secs(5), peer.next_non_update())
            .await
            .expect("reset deadline");

        assert_eq!((reset.kind, reset.stream), (RST_STREAM, stream));

        let next = spawn_get(&client, &url);
        let stream = request_stream(&mut peer).await;

        peer.respond(stream, "200", b"again").await;

        assert_eq!(next.await.expect("join").expect("response").body, b"again");
        no_second_connection(&listener).await;
    });
}

#[test]
fn idle_h2_connections_expire_and_are_replaced() {
    smol::block_on(async {
        let (listener, addr) = listen().await;
        let client = h2c_client(PoolConfig {
            idle_timeout: Duration::from_millis(150),
            ..PoolConfig::default()
        });
        let url = format!("http://{addr}/");
        let first = spawn_get(&client, &url);
        let mut one = Peer::accept(&listener).await;

        one.handshake(&[]).await;

        let stream = request_stream(&mut one).await;

        one.respond(stream, "200", b"a").await;
        first.await.expect("join").expect("response");

        sleep(Duration::from_millis(500)).await;

        let second = spawn_get(&client, &url);
        let mut two = Peer::accept(&listener).await;

        two.handshake(&[]).await;

        let stream = request_stream(&mut two).await;

        two.respond(stream, "200", b"b").await;
        second.await.expect("join").expect("response");

        assert_eq!(client.pool_stats().h2, 1);
    });
}

#[test]
fn streaming_upload_and_trailers_over_h2() {
    smol::block_on(async {
        let (listener, addr) = listen().await;
        let client = h2c_client(PoolConfig::default());
        let url = format!("http://{addr}/");
        let (body, mut sender) = StreamingBody::new(1024);
        let task = {
            let client = client.clone();

            spawn(async move {
                let request = ClientRequest::new(b"POST", &url).streaming(&body);
                let response = client.send(&request).await?;

                response.collect(1024).await
            })
        };
        let mut peer = Peer::accept(&listener).await;

        peer.handshake(&[]).await;

        let stream = request_stream(&mut peer).await;

        sender.write_all(b"hello").await.expect("write");
        sender.finish();

        let data = timeout(Duration::from_secs(5), peer.next_non_update())
            .await
            .expect("data deadline");

        assert_eq!(data.payload, b"hello");

        peer.respond(stream, "200", b"done").await;

        assert_eq!(task.await.expect("join").expect("body"), b"done");
    });
}
