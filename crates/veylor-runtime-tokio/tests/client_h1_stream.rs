use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use veylor_runtime_tokio::{
    Client, ClientConfig, ClientRequest, ClientResponse, StreamingBody, UploadSender,
};

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 4\r\n\r\ndone";

struct Server {
    url: String,
    accepted: Arc<AtomicUsize>,
    seen: UnboundedReceiver<Vec<u8>>,
}

async fn spawn_server() -> Server {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&accepted);
    let (tx, seen) = unbounded_channel();

    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.expect("accept");

            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(serve(socket, tx.clone()));
        }
    });

    Server {
        url: format!("http://{addr}/"),
        accepted,
        seen,
    }
}

async fn serve(mut socket: TcpStream, seen: UnboundedSender<Vec<u8>>) {
    let mut buf = vec![0u8; 16 * 1024];
    let mut request = Vec::new();

    loop {
        let read = match socket.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };

        request.extend_from_slice(&buf[..read]);
        let _ = seen.send(request.clone());

        if !complete(&request) {
            continue;
        }

        request.clear();

        if socket.write_all(RESPONSE).await.is_err() {
            return;
        }
    }
}

fn complete(request: &[u8]) -> bool {
    let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
        return false;
    };
    let head = request[..end].to_ascii_lowercase();
    let chunked = head.windows(26).any(|w| w == b"transfer-encoding: chunked");

    !chunked || request[end + 4..].ends_with(b"\r\n\r\n")
}

fn decode(request: &[u8]) -> Vec<u8> {
    let end = request
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("head");
    let mut rest = &request[end + 4..];
    let mut body = Vec::new();

    loop {
        let line = rest.windows(2).position(|w| w == b"\r\n").expect("size");
        let size = usize::from_str_radix(std::str::from_utf8(&rest[..line]).expect("utf8"), 16)
            .expect("hex");

        rest = &rest[line + 2..];

        if size == 0 {
            return body;
        }

        body.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

async fn wait_for(seen: &mut UnboundedReceiver<Vec<u8>>, ready: impl Fn(&[u8]) -> bool) -> Vec<u8> {
    timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = seen.recv().await.expect("server alive");

            if ready(&snapshot) {
                return snapshot;
            }
        }
    })
    .await
    .expect("server deadline")
}

fn has(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn upload(
    client: &Client,
    url: &str,
    capacity: usize,
) -> (UploadSender, JoinHandle<std::io::Result<ClientResponse>>) {
    let (body, sender) = StreamingBody::new(capacity);
    let client = client.clone();
    let url = url.to_owned();

    let task = tokio::spawn(async move {
        let request = ClientRequest::new(b"POST", &url).streaming(&body);

        client.send(&request).await?.into_buffered(1024).await
    });

    (sender, task)
}

#[tokio::test]
async fn chunks_reach_the_server_before_the_upload_finishes() {
    let mut server = spawn_server().await;
    let client = Client::new(ClientConfig::default()).expect("client");
    let (mut sender, task) = upload(&client, &server.url, 1024);

    sender.write_all(b"hello").await.expect("write");

    let partial = wait_for(&mut server.seen, |r| has(r, b"5\r\nhello\r\n")).await;

    assert!(has(
        &partial.to_ascii_lowercase(),
        b"transfer-encoding: chunked"
    ));
    assert!(!task.is_finished());

    sender.write_all(b" world").await.expect("write");
    sender.finish();

    let full = wait_for(&mut server.seen, |r| r.ends_with(b"0\r\n\r\n")).await;

    assert_eq!(decode(&full), b"hello world");

    let response = task.await.expect("join").expect("response");

    assert_eq!(
        (response.status, response.body.as_slice()),
        (200, b"done".as_slice())
    );
}

#[tokio::test]
async fn trailers_close_the_chunked_body() {
    let mut server = spawn_server().await;
    let client = Client::new(ClientConfig::default()).expect("client");
    let (mut sender, task) = upload(&client, &server.url, 1024);

    sender.write_all(b"abc").await.expect("write");
    sender.finish_with_trailers(&[(b"x-sum", b"42")]);

    let full = wait_for(&mut server.seen, |r| has(r, b"x-sum: 42\r\n\r\n")).await;

    assert!(has(&full, b"3\r\nabc\r\n0\r\nx-sum: 42\r\n\r\n"));
    task.await.expect("join").expect("response");
}

#[tokio::test]
async fn large_uploads_flow_through_a_small_buffer() {
    let mut server = spawn_server().await;
    let client = Client::new(ClientConfig::default()).expect("client");
    let (mut sender, task) = upload(&client, &server.url, 1024);
    let payload: Vec<u8> = (0..200_000u32).map(|n| (n % 251) as u8).collect();

    sender.write_all(&payload).await.expect("write");
    sender.finish();

    let full = wait_for(&mut server.seen, |r| r.ends_with(b"0\r\n\r\n")).await;

    assert_eq!(decode(&full), payload);
    task.await.expect("join").expect("response");
}

#[tokio::test]
async fn the_connection_is_reused_after_a_streamed_upload() {
    let server = spawn_server().await;
    let client = Client::new(ClientConfig::default()).expect("client");
    let (mut sender, task) = upload(&client, &server.url, 64);

    sender.write_all(b"x").await.expect("write");
    sender.finish();
    task.await.expect("join").expect("response");

    let again = client
        .request(&ClientRequest::get(&server.url))
        .await
        .expect("again");

    assert_eq!(again.status, 200);
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_abandoned_upload_fails_and_drops_the_connection() {
    let mut server = spawn_server().await;
    let client = Client::new(ClientConfig::default()).expect("client");
    let (mut sender, task) = upload(&client, &server.url, 64);

    sender.write_all(b"half").await.expect("write");
    wait_for(&mut server.seen, |r| has(r, b"4\r\nhalf\r\n")).await;
    drop(sender);

    let error = task.await.expect("join").err().expect("must fail");

    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);

    let stats = client.pool_stats();

    assert_eq!((stats.open, stats.idle), (0, 0));
}
