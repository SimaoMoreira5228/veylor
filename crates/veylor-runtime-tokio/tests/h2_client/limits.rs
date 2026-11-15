use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::timeout;
use veylor_client::h2::ErrorCode;
use veylor_runtime_tokio::{H2Client, H2Config, H2Error, H2Request, H2Response};

use super::peer::{DATA, END_HEADERS, END_STREAM, HEADERS, Peer, RST_STREAM, listen};

const DEADLINE: Duration = Duration::from_secs(10);

async fn setup(config: H2Config, settings: &[(u16, u32)]) -> (H2Client, Peer) {
    let (listener, addr) = listen().await;
    let (client, accepted) = tokio::join!(H2Client::connect(addr, config), Peer::accept(&listener));
    let mut peer = accepted;
    peer.handshake(settings).await;
    (client.expect("connect"), peer)
}

fn spawn_get(client: &H2Client, path: String) -> JoinHandle<Result<H2Response, H2Error>> {
    let client = client.clone();
    tokio::spawn(async move { client.send(H2Request::get(b"peer", path.as_bytes())).await })
}

fn cookie_fields(count: usize, value_length: usize) -> Vec<(String, String)> {
    let mut fields = vec![(":status".to_string(), "200".to_string())];
    for index in 0..count {
        let mut value = format!("session{index}=");
        while value.len() < value_length {
            value.push('x');
        }
        fields.push(("set-cookie".to_string(), value));
    }
    fields
}

async fn send_fields(peer: &mut Peer, stream: u32, fields: &[(String, String)]) {
    let borrowed: Vec<(&str, &str)> = fields
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    peer.send_headers(stream, true, &borrowed).await;
}

#[tokio::test]
async fn response_headers_past_the_old_four_kib_cap_arrive_intact() {
    let (client, mut peer) = setup(H2Config::default(), &[]).await;
    let task = spawn_get(&client, "/cookies".to_string());
    assert_eq!(peer.next_non_update().await.kind, HEADERS);

    let fields = cookie_fields(90, 110);
    send_fields(&mut peer, 1, &fields).await;

    let response = timeout(DEADLINE, task)
        .await
        .expect("deadline")
        .expect("join")
        .expect("response");
    assert_eq!(response.status, 200);
    let cookies = response
        .headers
        .iter()
        .filter(|(name, _)| *name == b"set-cookie")
        .count();
    assert_eq!(cookies, 90);
}

#[tokio::test]
async fn header_lists_over_the_advertised_limit_refuse_only_that_stream() {
    let (client, mut peer) = setup(H2Config::default(), &[]).await;
    let task = spawn_get(&client, "/too-big".to_string());
    assert_eq!(peer.next_non_update().await.kind, HEADERS);

    let fields = cookie_fields(105, 125);
    send_fields(&mut peer, 1, &fields).await;
    let outcome = timeout(DEADLINE, task)
        .await
        .expect("deadline")
        .expect("join");
    assert_eq!(
        outcome.err(),
        Some(H2Error::Reset(ErrorCode::RefusedStream))
    );

    let second = spawn_get(&client, "/after".to_string());
    let reset = peer.next_non_update().await;
    assert_eq!(reset.kind, RST_STREAM);
    assert_eq!(reset.code(), ErrorCode::RefusedStream.value());
    let request = peer.next_non_update().await;
    assert_eq!(request.kind, HEADERS);
    peer.respond(request.stream, "200", b"").await;
    assert_eq!(second.await.expect("join").expect("response").status, 200);
}

#[tokio::test]
async fn a_hundred_concurrent_streams_share_one_connection() {
    let (client, mut peer) = setup(H2Config::default(), &[]).await;
    let tasks: Vec<_> = (0..100)
        .map(|index| spawn_get(&client, format!("/{index}")))
        .collect();

    let mut streams = Vec::new();
    while streams.len() < 100 {
        let frame = peer.next_non_update().await;
        assert_eq!(frame.kind, HEADERS);
        streams.push(frame.stream);
    }
    for stream in streams {
        peer.respond(stream, "200", b"ok").await;
    }
    for task in tasks {
        let mut response = timeout(DEADLINE, task)
            .await
            .expect("deadline")
            .expect("join")
            .expect("response");
        assert_eq!(response.status, 200);
        assert_eq!(response.body.read_to_end(8).await.expect("body"), b"ok");
    }
}

#[tokio::test]
async fn configured_concurrency_is_clamped_to_the_slot_ceiling() {
    let config = H2Config {
        max_concurrent_streams: usize::MAX,
        ..H2Config::default()
    };
    let (client, mut peer) = setup(config, &[]).await;
    let task = spawn_get(&client, "/one".to_string());
    let request = peer.next_non_update().await;
    peer.respond(request.stream, "204", b"").await;
    assert_eq!(task.await.expect("join").expect("response").status, 204);
}

#[tokio::test]
async fn full_body_trailers_follow_the_last_data_frame() {
    let (client, mut peer) = setup(H2Config::default(), &[]).await;
    let request = H2Request::new(b"POST", b"peer", b"/upload")
        .body(b"payload".to_vec())
        .trailer(b"x-checksum", b"abc");
    let task = tokio::spawn({
        let client = client.clone();
        async move { client.send(request).await }
    });

    let head = peer.next_non_update().await;
    assert_eq!(head.kind, HEADERS);
    assert_eq!(head.flags & END_STREAM, 0);
    let data = peer.next_non_update().await;
    assert_eq!(data.kind, DATA);
    assert_eq!(data.payload, b"payload");
    assert_eq!(data.flags & END_STREAM, 0);
    let trailers = peer.next_non_update().await;
    assert_eq!(trailers.kind, HEADERS);
    assert_eq!(trailers.stream, head.stream);
    assert_eq!(
        trailers.flags & (END_STREAM | END_HEADERS),
        END_STREAM | END_HEADERS
    );
    assert!(!trailers.payload.is_empty());

    peer.respond(head.stream, "200", b"").await;
    assert_eq!(task.await.expect("join").expect("response").status, 200);
}

#[tokio::test]
async fn streaming_upload_trailers_close_the_request() {
    let (client, mut peer) = setup(H2Config::default(), &[]).await;
    let (request, mut sender) = H2Request::new(b"POST", b"peer", b"/stream").streaming_body(1024);
    let task = tokio::spawn({
        let client = client.clone();
        async move { client.send(request).await }
    });

    let head = peer.next_non_update().await;
    assert_eq!(head.kind, HEADERS);
    sender.write_all(b"chunk").await.expect("write");
    let data = peer.next_non_update().await;
    assert_eq!(data.kind, DATA);
    assert_eq!(data.flags & END_STREAM, 0);
    sender.finish_with_trailers(&[(b"x-done".as_slice(), b"yes".as_slice())]);
    let trailers = peer.next_non_update().await;
    assert_eq!(trailers.kind, HEADERS);
    assert_eq!(trailers.flags & END_STREAM, END_STREAM);

    peer.respond(head.stream, "200", b"").await;
    assert_eq!(task.await.expect("join").expect("response").status, 200);
}

#[tokio::test]
async fn invalid_trailers_reset_the_stream_not_the_connection() {
    let (client, mut peer) = setup(H2Config::default(), &[]).await;
    let request = H2Request::new(b"POST", b"peer", b"/bad")
        .body(b"x".to_vec())
        .trailer(b":path", b"/");
    let task = tokio::spawn({
        let client = client.clone();
        async move { client.send(request).await }
    });
    let head = peer.next_non_update().await;
    assert_eq!(head.kind, HEADERS);
    let outcome = timeout(DEADLINE, task)
        .await
        .expect("deadline")
        .expect("join");
    assert!(outcome.is_err());

    let second = spawn_get(&client, "/fine".to_string());
    loop {
        let frame = peer.next_non_update().await;
        if frame.kind == HEADERS {
            peer.respond(frame.stream, "200", b"").await;
            break;
        }
    }
    assert_eq!(second.await.expect("join").expect("response").status, 200);
}

#[tokio::test]
async fn incrementally_indexed_literals_at_the_limit_fit_the_decoder_scratch() {
    let (client, mut peer) = setup(H2Config::default(), &[]).await;
    let task = spawn_get(&client, "/indexed".to_string());
    assert_eq!(peer.next_non_update().await.kind, HEADERS);

    let mut block = vec![0x88];
    let mut list = 42;
    let mut index = 0;
    while list + 32 + 10 + 120 <= 16 * 1024 - 200 {
        let name = format!("x-cookie{:02}", index % 90);
        let value = format!("{index:03}{}", "v".repeat(117));
        block.push(0x40);
        block.push(name.len() as u8);
        block.extend_from_slice(name.as_bytes());
        block.push(value.len() as u8);
        block.extend_from_slice(value.as_bytes());
        list += name.len() + value.len() + 32;
        index += 1;
    }
    assert!(block.len() > 12 * 1024);
    peer.send(HEADERS, END_HEADERS | END_STREAM, 1, &block)
        .await;

    let response = timeout(DEADLINE, task)
        .await
        .expect("deadline")
        .expect("join")
        .expect("response");
    assert_eq!(response.status, 200);
    assert_eq!(response.headers.len(), index);
}
