use std::time::Duration;

use crate::support::{Join, sleep, spawn, timeout};
use veylor_client::h2::ErrorCode;
use veylor_runtime_smol::{
    H2Client, H2Config, H2Error, H2Request, H2Response, TimeoutKind, connect_h2,
};

use super::peer::{
    ACK, DATA, GOAWAY, HEADERS, PING, Peer, RST_STREAM, SETTING_INITIAL_WINDOW_SIZE,
    SETTING_MAX_CONCURRENT_STREAMS, WINDOW_UPDATE, listen,
};

async fn setup(config: H2Config, settings: &[(u16, u32)]) -> (H2Client, Peer) {
    let (listener, addr) = listen().await;
    let (client, accepted) =
        smol::future::zip(connect_h2(addr, config), Peer::accept(&listener)).await;
    let mut peer = accepted;
    peer.handshake(settings).await;
    (client.expect("connect"), peer)
}

fn spawn_get(client: &H2Client, path: &'static str) -> Join<Result<H2Response, H2Error>> {
    let client = client.clone();
    spawn(async move { client.send(H2Request::get(b"peer", path.as_bytes())).await })
}

async fn request_frame(peer: &mut Peer) -> super::peer::Frame {
    let frame = peer.next_non_update().await;
    assert_eq!(
        frame.kind, HEADERS,
        "expected request HEADERS, got {frame:?}"
    );
    frame
}

#[test]
fn ping_is_acknowledged_and_trailers_arrive_after_the_body() {
    smol::block_on(async {
        let (client, mut peer) = setup(H2Config::default(), &[]).await;
        let task = spawn_get(&client, "/t");

        let request = request_frame(&mut peer).await;
        assert_eq!(request.stream, 1);
        peer.send(PING, 0, 0, b"12345678").await;
        peer.send_headers(
            1,
            false,
            &[(":status", "200"), ("content-type", "text/plain")],
        )
        .await;
        peer.send(DATA, 0, 1, b"hello ").await;
        peer.send(DATA, 0, 1, b"world").await;
        peer.send_headers(1, true, &[("x-sum", "abc"), ("x-count", "2")])
            .await;

        let mut response = task.await.expect("join").expect("response");
        assert_eq!(response.status, 200);
        assert_eq!(
            response.headers.get(b"content-type"),
            Some(b"text/plain".as_slice())
        );
        let body = response.body.read_to_end(64).await.expect("body");
        assert_eq!(body, b"hello world");
        let trailers = response.body.trailers().expect("trailers");
        assert_eq!(trailers.get(b"x-sum"), Some(b"abc".as_slice()));
        assert_eq!(trailers.get(b"x-count"), Some(b"2".as_slice()));

        let ack = loop {
            let frame = peer.next().await;
            if frame.kind == PING {
                break frame;
            }
        };
        assert_eq!(ack.flags & ACK, ACK);
        assert_eq!(ack.payload, b"12345678");
    });
}

#[test]
fn max_concurrent_streams_queues_then_rejects_beyond_the_bound() {
    smol::block_on(async {
        let config = H2Config {
            max_pending_requests: 3,
            ..H2Config::default()
        };
        let (client, mut peer) = setup(config, &[(SETTING_MAX_CONCURRENT_STREAMS, 1)]).await;

        let first = spawn_get(&client, "/1");
        let second = spawn_get(&client, "/2");
        let third = spawn_get(&client, "/3");
        let fourth = spawn_get(&client, "/4");

        let request = request_frame(&mut peer).await;
        assert_eq!(request.stream, 1);

        let mut results = Vec::new();
        sleep(Duration::from_millis(100)).await;
        assert!(!first.is_finished());
        for task in [fourth] {
            results.push(task.await.expect("join"));
        }
        assert_eq!(results[0].as_ref().err(), Some(&H2Error::QueueFull));

        let mut seen = vec![1];
        peer.respond(1, "200", b"").await;
        for expected in [3, 5] {
            let request = request_frame(&mut peer).await;
            assert_eq!(request.stream, expected);
            seen.push(request.stream);
            peer.respond(request.stream, "200", b"").await;
        }
        assert_eq!(seen, [1, 3, 5]);

        for task in [first, second, third] {
            assert_eq!(task.await.expect("join").expect("response").status, 200);
        }
    });
}

#[test]
fn goaway_finishes_in_flight_streams_and_refuses_the_rest() {
    smol::block_on(async {
        let (client, mut peer) = setup(H2Config::default(), &[]).await;
        let kept = spawn_get(&client, "/kept");
        let refused = spawn_get(&client, "/refused");
        assert_eq!(request_frame(&mut peer).await.stream, 1);
        assert_eq!(request_frame(&mut peer).await.stream, 3);

        peer.send_goaway(1, 0).await;
        peer.respond(1, "200", b"done").await;

        let mut response = kept.await.expect("join").expect("kept stream completes");
        assert_eq!(response.body.read_to_end(16).await.expect("body"), b"done");
        let error = refused.await.expect("join").err().expect("refused");
        assert!(matches!(error, H2Error::GoAway { .. }));
        assert!(error.is_retryable());

        assert!(!client.is_reusable());
        let late = client
            .send(H2Request::get(b"peer", b"/late"))
            .await
            .err()
            .expect("refused");
        assert!(late.is_retryable());
    });
}

#[test]
fn server_reset_fails_one_stream_and_keeps_the_connection() {
    smol::block_on(async {
        let (client, mut peer) = setup(H2Config::default(), &[]).await;
        let doomed = spawn_get(&client, "/doomed");
        let sibling = spawn_get(&client, "/sibling");
        assert_eq!(request_frame(&mut peer).await.stream, 1);
        assert_eq!(request_frame(&mut peer).await.stream, 3);

        peer.send_headers(1, false, &[(":status", "200")]).await;
        peer.send(DATA, 0, 1, b"partial").await;
        peer.send_rst(1, 8).await;
        peer.respond(3, "200", b"fine").await;

        let mut response = doomed.await.expect("join").expect("head arrived first");
        let error = response.body.read_to_end(64).await.expect_err("reset");
        assert_eq!(error, H2Error::Reset(ErrorCode::Cancel));
        assert!(!error.is_retryable());

        let mut sibling = sibling.await.expect("join").expect("sibling");
        assert_eq!(sibling.body.read_to_end(16).await.expect("body"), b"fine");

        let refused = spawn_get(&client, "/refused");
        let request = request_frame(&mut peer).await;
        peer.send_rst(request.stream, 7).await;
        let error = refused.await.expect("join").err().expect("refused");
        assert_eq!(error, H2Error::Reset(ErrorCode::RefusedStream));
        assert!(error.is_retryable());
        assert!(client.is_reusable());
    });
}

#[test]
fn dropping_the_request_future_resets_the_stream_only() {
    smol::block_on(async {
        let (client, mut peer) = setup(H2Config::default(), &[]).await;
        let slow = {
            let client = client.clone();
            spawn(async move {
                let request = H2Request::get(b"peer", b"/slow");
                let _ = timeout(Duration::from_millis(100), client.send(request)).await;
            })
        };
        let request = request_frame(&mut peer).await;
        assert_eq!(request.stream, 1);
        slow.await.expect("join");

        let reset = loop {
            let frame = peer.next_non_update().await;
            if frame.kind == RST_STREAM {
                break frame;
            }
        };
        assert_eq!((reset.stream, reset.code()), (1, 8));

        let next = spawn_get(&client, "/next");
        let request = request_frame(&mut peer).await;
        assert_eq!(request.stream, 3);
        peer.respond(3, "204", b"").await;
        assert_eq!(next.await.expect("join").expect("response").status, 204);
        assert!(client.is_reusable());
    });
}

#[test]
fn uploads_wait_for_the_peer_window() {
    smol::block_on(async {
        let (client, mut peer) =
            setup(H2Config::default(), &[(SETTING_INITIAL_WINDOW_SIZE, 1000)]).await;
        let payload: Vec<u8> = (0..5000u32).map(|value| value as u8).collect();
        let task = {
            let client = client.clone();
            let payload = payload.clone();
            spawn(async move {
                let request = H2Request::new(b"POST", b"peer", b"/upload").body(payload);
                client.send(request).await
            })
        };

        let request = request_frame(&mut peer).await;
        assert_eq!(request.stream, 1);
        let mut received = Vec::new();
        while received.len() < 1000 {
            let frame = peer.next_non_update().await;
            assert_eq!(frame.kind, DATA);
            received.extend_from_slice(&frame.payload);
        }
        assert_eq!(received.len(), 1000);
        assert!(
            timeout(Duration::from_millis(200), peer.next_non_update())
                .await
                .is_err(),
            "client sent past the advertised window"
        );

        let mut ended = false;
        while !ended {
            peer.send_window_update(1, 1000).await;
            peer.send_window_update(0, 1000).await;
            let frame = timeout(Duration::from_secs(5), peer.next_non_update())
                .await
                .expect("more data");
            assert_eq!(frame.kind, DATA);
            received.extend_from_slice(&frame.payload);
            ended = frame.end_stream();
        }
        assert_eq!(received, payload);

        peer.respond(1, "200", b"ok").await;
        assert_eq!(task.await.expect("join").expect("response").status, 200);
    });
}

#[test]
fn streaming_upload_applies_backpressure_to_the_producer() {
    smol::block_on(async {
        let (client, mut peer) =
            setup(H2Config::default(), &[(SETTING_INITIAL_WINDOW_SIZE, 100)]).await;
        let (request, mut sender) =
            H2Request::new(b"POST", b"peer", b"/stream").streaming_body(256);
        let sending = {
            let client = client.clone();
            spawn(async move { client.send(request).await })
        };

        let producer = spawn(async move {
            let chunk = [7u8; 1000];
            for _ in 0..10 {
                sender.write_all(&chunk).await.expect("write");
            }
            sender.finish();
        });
        request_frame(&mut peer).await;

        sleep(Duration::from_millis(200)).await;
        assert!(!producer.is_finished(), "producer outran the window");

        let mut total = 0;
        let mut ended = false;
        while !ended {
            let frame = peer.next_non_update().await;
            assert_eq!(frame.kind, DATA);
            total += frame.payload.len();
            ended = frame.end_stream();
            if !ended {
                peer.send_window_update(1, 700).await;
                peer.send_window_update(0, 700).await;
            }
        }
        assert_eq!(total, 10_000);
        producer.await.expect("producer");

        peer.respond(1, "200", b"").await;
        assert_eq!(sending.await.expect("join").expect("response").status, 200);
    });
}

#[test]
fn unread_responses_hold_back_window_credit() {
    smol::block_on(async {
        let config = H2Config {
            stream_window: 16 * 1024,
            connection_window: 64 * 1024,
            ..H2Config::default()
        };
        let (client, mut peer) = setup(config, &[]).await;
        let task = spawn_get(&client, "/big");
        request_frame(&mut peer).await;

        peer.send_headers(1, false, &[(":status", "200")]).await;
        peer.send(DATA, 0, 1, &[1u8; 16 * 1024]).await;
        let mut response = task.await.expect("join").expect("response");

        let mut stream_credit = 0;
        while let Ok(frame) = timeout(Duration::from_millis(300), peer.next()).await {
            if frame.kind == WINDOW_UPDATE && frame.stream == 1 {
                stream_credit += frame.increment();
            }
        }
        assert_eq!(stream_credit, 0, "credit released for bytes nobody read");

        let mut chunk = vec![0u8; 16 * 1024];
        let mut read = 0;
        while read < 16 * 1024 {
            read += response.body.read(&mut chunk).await.expect("read");
        }
        let frame = loop {
            let frame = peer.next().await;
            if frame.kind == WINDOW_UPDATE && frame.stream == 1 {
                break frame;
            }
        };
        assert!(frame.increment() >= 8 * 1024);
    });
}

#[test]
fn padded_data_does_not_leak_window() {
    smol::block_on(async {
        let config = H2Config {
            stream_window: 16 * 1024,
            connection_window: 32 * 1024,
            ..H2Config::default()
        };
        let (client, mut peer) = setup(config, &[]).await;
        let task = spawn_get(&client, "/padded");
        request_frame(&mut peer).await;
        peer.send_headers(1, false, &[(":status", "200")]).await;

        let mut sent_flow = 0u64;
        let mut response = task.await.expect("join").expect("response");
        let mut credited = 0u64;
        for _ in 0..200 {
            let mut payload = vec![200u8];
            payload.extend_from_slice(&[9u8; 50]);
            payload.extend_from_slice(&[0u8; 200]);
            peer.send(DATA, 8, 1, &payload).await;
            sent_flow += payload.len() as u64;
            let mut chunk = [0u8; 64];
            assert_eq!(response.body.read(&mut chunk).await.expect("read"), 50);
            while let Ok(frame) = timeout(Duration::from_millis(20), peer.next()).await {
                if frame.kind == WINDOW_UPDATE && frame.stream == 1 {
                    credited += u64::from(frame.increment());
                }
            }
        }
        assert!(sent_flow > 16 * 1024 * 2);
        assert!(
            credited + 16 * 1024 >= sent_flow,
            "padding was never credited back"
        );
    });
}

#[test]
fn stream_deadline_resets_that_stream_alone() {
    smol::block_on(async {
        let config = H2Config {
            stream_timeout: Some(Duration::from_millis(150)),
            ..H2Config::default()
        };
        let (client, mut peer) = setup(config, &[]).await;
        let task = spawn_get(&client, "/silent");
        assert_eq!(request_frame(&mut peer).await.stream, 1);

        let error = task.await.expect("join").err().expect("timeout");
        assert_eq!(error, H2Error::Timeout(TimeoutKind::Stream));
        let reset = loop {
            let frame = peer.next_non_update().await;
            if frame.kind == RST_STREAM {
                break frame;
            }
        };
        assert_eq!((reset.stream, reset.code()), (1, 8));

        let again = {
            let client = client.clone();
            spawn(async move {
                let request = H2Request::get(b"peer", b"/again").timeout(None);
                client.send(request).await
            })
        };
        assert_eq!(request_frame(&mut peer).await.stream, 3);
        peer.respond(3, "200", b"").await;
        assert_eq!(again.await.expect("join").expect("response").status, 200);
    });
}

#[test]
fn silent_peer_fails_the_handshake_deadline() {
    smol::block_on(async {
        let (listener, addr) = listen().await;
        let config = H2Config {
            handshake_timeout: Duration::from_millis(150),
            ..H2Config::default()
        };
        let (client, _peer) =
            smol::future::zip(connect_h2(addr, config), Peer::accept(&listener)).await;
        let client = client.expect("connect");

        let error = client
            .send(H2Request::get(b"peer", b"/"))
            .await
            .err()
            .expect("deadline");
        assert_eq!(error, H2Error::Timeout(TimeoutKind::Handshake));
        assert!(!client.is_reusable());
    });
}

#[test]
fn peer_disconnect_fails_in_flight_streams() {
    smol::block_on(async {
        let (client, mut peer) = setup(H2Config::default(), &[]).await;
        let task = spawn_get(&client, "/gone");
        request_frame(&mut peer).await;
        drop(peer);

        let error = task.await.expect("join").err().expect("closed");
        assert!(matches!(error, H2Error::Io(_)), "{error:?}");
        assert!(!client.is_reusable());
    });
}

#[test]
fn graceful_close_sends_goaway_after_in_flight_work() {
    smol::block_on(async {
        let (client, mut peer) = setup(H2Config::default(), &[]).await;
        let task = spawn_get(&client, "/last");
        request_frame(&mut peer).await;

        let closing = {
            let client = client.clone();
            spawn(async move { client.close().await })
        };
        sleep(Duration::from_millis(100)).await;
        assert!(
            !closing.is_finished(),
            "close must wait for the in-flight stream"
        );
        peer.respond(1, "200", b"bye").await;

        let mut response = task.await.expect("join").expect("response");
        assert_eq!(response.body.read_to_end(8).await.expect("body"), b"bye");
        let goaway = loop {
            let frame = peer.next_non_update().await;
            if frame.kind == GOAWAY {
                break frame;
            }
        };
        assert_eq!(&goaway.payload[4..8], &[0, 0, 0, 0]);
        closing.await.expect("close");
    });
}
