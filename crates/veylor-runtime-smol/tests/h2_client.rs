use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

mod support;

use support::{sleep, spawn};
use veylor_runtime_smol::{H2Client, H2Config, H2Error, H2Request, connect_h2};
use veylor_server::{MAX_STATIC_BODY_BYTES, RunningServer, Server, ServerConfig};

#[path = "h2_client/limits.rs"]
mod limits;
#[path = "h2_client/peer.rs"]
mod peer;
#[path = "h2_client/scripted.rs"]
mod scripted;

const BIG_FILE_BYTES: usize = 1_500_000;

fn pattern(length: usize) -> Vec<u8> {
    (0..length).map(|index| (index * 31 % 251) as u8).collect()
}

fn fixture_dir(name: &str) -> PathBuf {
    let dir =
        PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("fixture dir");
    std::fs::write(dir.join("big.bin"), pattern(BIG_FILE_BYTES)).expect("big file");
    dir
}

fn managed_server(name: &str) -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind");
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
        .register_file_mount(b"/files/", &fixture_dir(name))
        .expect("mount");
    server.start().expect("start")
}

async fn connect(server: &RunningServer, config: H2Config) -> H2Client {
    let addr = server.h2c_addr().expect("h2c");
    connect_h2(addr, config).await.expect("connect")
}

async fn shutdown(client: H2Client, server: RunningServer) {
    client.close().await;
    server.stop_accepting();
    server.join().expect("join");
}

#[test]
fn get_over_prior_knowledge_h2c() {
    smol::block_on(async {
        let server = managed_server("get");
        let client = connect(&server, H2Config::default()).await;

        let mut response = client
            .send(H2Request::get(b"localhost", b"/hello"))
            .await
            .expect("response");
        assert_eq!(response.status, 200);
        let body = response.body.read_to_end(1024).await.expect("body");
        assert_eq!(body, b"hello");
        assert!(client.is_reusable());

        shutdown(client, server).await;
    });
}

#[test]
fn long_lived_connection_outlives_engine_slot_tables() {
    smol::block_on(async {
        let server = managed_server("sequential");
        let client = connect(&server, H2Config::default()).await;

        for round in 0..400 {
            let mut response = client
                .send(
                    H2Request::get(b"localhost", b"/hello")
                        .header(b"x-round", round.to_string().as_bytes()),
                )
                .await
                .unwrap_or_else(|error| panic!("round {round}: {error}"));
            assert_eq!(response.body.read_to_end(64).await.expect("body"), b"hello");
        }

        shutdown(client, server).await;
    });
}

#[test]
fn concurrent_requests_multiplex_over_one_connection() {
    smol::block_on(async {
        let server = managed_server("concurrent");
        let client = connect(&server, H2Config::default()).await;

        let mut tasks = Vec::new();
        for task in 0..24 {
            let client = client.clone();
            tasks.push(spawn(async move {
                for round in 0..20 {
                    let request = H2Request::get(b"localhost", b"/hello")
                        .header(b"x-task", format!("{task}-{round}").as_bytes());
                    let mut response = client.send(request).await.expect("response");
                    assert_eq!(response.status, 200);
                    assert_eq!(response.body.read_to_end(64).await.expect("body"), b"hello");
                }
            }));
        }
        for task in tasks {
            task.await.expect("task");
        }

        shutdown(client, server).await;
    });
}

#[test]
fn large_bodies_flow_through_small_windows() {
    smol::block_on(async {
        let server = managed_server("windows");
        let config = H2Config {
            stream_window: 16 * 1024,
            connection_window: 40 * 1024,
            ..H2Config::default()
        };
        let client = connect(&server, config).await;
        let expected = pattern(BIG_FILE_BYTES);

        let mut tasks = Vec::new();
        for task in 0..4 {
            let client = client.clone();
            let expected = expected.clone();
            tasks.push(spawn(async move {
                let mut response = client
                    .send(H2Request::get(b"localhost", b"/files/big.bin"))
                    .await
                    .expect("response");
                assert_eq!(response.status, 200);
                let mut received = Vec::new();
                let mut chunk = vec![0u8; 3000 + task * 700];
                loop {
                    let count = response.body.read(&mut chunk).await.expect("read");
                    if count == 0 {
                        break;
                    }
                    received.extend_from_slice(&chunk[..count]);
                    if received.len() % 7 == 0 {
                        sleep(Duration::from_micros(200)).await;
                    }
                }
                assert_eq!(received.len(), expected.len());
                assert!(received == expected, "task {task} saw corrupted body");
            }));
        }
        for task in tasks {
            task.await.expect("task");
        }

        shutdown(client, server).await;
    });
}

#[test]
fn dropping_a_response_resets_only_that_stream() {
    smol::block_on(async {
        let server = managed_server("cancel");
        let config = H2Config {
            stream_window: 16 * 1024,
            connection_window: 32 * 1024,
            ..H2Config::default()
        };
        let client = connect(&server, config).await;

        for _ in 0..50 {
            let mut response = client
                .send(H2Request::get(b"localhost", b"/files/big.bin"))
                .await
                .expect("response");
            let mut chunk = [0u8; 1024];
            let count = response.body.read(&mut chunk).await.expect("read");
            assert!(count > 0);
            drop(response);
        }

        let mut response = client
            .send(H2Request::get(b"localhost", b"/hello"))
            .await
            .expect("connection survives cancellations");
        assert_eq!(response.body.read_to_end(64).await.expect("body"), b"hello");
        assert!(client.is_reusable());

        shutdown(client, server).await;
    });
}

#[test]
fn graceful_close_refuses_new_requests() {
    smol::block_on(async {
        let server = managed_server("close");
        let client = connect(&server, H2Config::default()).await;
        let mut response = client
            .send(H2Request::get(b"localhost", b"/hello"))
            .await
            .expect("response");
        assert_eq!(response.body.read_to_end(64).await.expect("body"), b"hello");

        client.close().await;
        assert!(!client.is_reusable());
        let error = client
            .send(H2Request::get(b"localhost", b"/hello"))
            .await
            .err()
            .expect("closed");
        assert_eq!(error, H2Error::Closed);

        server.stop_accepting();
        server.join().expect("join");
    });
}
