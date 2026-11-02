use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use veylor_http1::Field;
use veylor_runtime_tokio::{Client, Config, Request, Response, serve_connection, serve_until};

fn echo(request: Request<'_>) -> Response<'_> {
    match request.target() {
        b"/echo" => Response::ok(request.body()),
        b"/header" => Response::ok(request.header("x-test").unwrap_or(b"none")),
        _ => Response::text(b"ok"),
    }
}

const HOST: Field = Field {
    name: b"host",
    value: b"localhost",
};

fn boom(_request: Request<'_>) -> Response<'_> {
    panic!("handler panic")
}

async fn listener() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    (listener, addr)
}

async fn spawn_one(listener: TcpListener) {
    let (socket, _) = listener.accept().await.expect("accept");
    serve_connection(socket, echo, Config::default())
        .await
        .expect("serve");
}

#[tokio::test]
async fn client_and_server_exchange_keep_alive() {
    let (listener, addr) = listener().await;
    tokio::spawn(spawn_one(listener));

    let mut client = Client::connect(addr).await.expect("connect");

    let get = client
        .request(b"GET", b"/", &[HOST], b"")
        .await
        .expect("get");
    assert_eq!(get.status, 200);
    assert_eq!(get.body, b"ok");

    let post = client
        .request(b"POST", b"/echo", &[HOST], b"hello")
        .await
        .expect("post");
    assert_eq!(post.status, 200);
    assert_eq!(post.body, b"hello");

    let header = Field {
        name: b"x-test",
        value: b"value",
    };
    let header = client
        .request(b"GET", b"/header", &[HOST, header], b"")
        .await
        .expect("header");
    assert_eq!(header.body, b"value");

    assert!(client.is_reusable());
}

#[tokio::test]
async fn chunked_upload_is_buffered() {
    let (listener, addr) = listener().await;
    tokio::spawn(spawn_one(listener));

    let mut socket = TcpStream::connect(addr).await.expect("connect");
    socket
        .write_all(
            b"POST /echo HTTP/1.1\r\n\
              Host: localhost\r\n\
              Transfer-Encoding: chunked\r\n\
              Connection: close\r\n\r\n\
              5\r\nhello\r\n\
              0\r\n\r\n",
        )
        .await
        .expect("write");

    let mut response = Vec::new();
    socket.read_to_end(&mut response).await.expect("read");
    assert!(
        String::from_utf8_lossy(&response).ends_with("hello"),
        "{}",
        String::from_utf8_lossy(&response)
    );
}

#[tokio::test]
async fn expect_continue_gets_an_interim_response() {
    let (listener, addr) = listener().await;
    tokio::spawn(spawn_one(listener));

    let mut socket = TcpStream::connect(addr).await.expect("connect");
    socket
        .write_all(
            b"POST /echo HTTP/1.1\r\n\
              Host: localhost\r\n\
              Content-Length: 5\r\n\
              Expect: 100-continue\r\n\
              Connection: close\r\n\r\n",
        )
        .await
        .expect("write");

    let mut interim = [0u8; 25];
    socket.read_exact(&mut interim).await.expect("interim");
    assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");

    socket.write_all(b"hello").await.expect("body");
    let mut response = Vec::new();
    socket.read_to_end(&mut response).await.expect("read");
    assert!(
        String::from_utf8_lossy(&response).ends_with("hello"),
        "{}",
        String::from_utf8_lossy(&response)
    );
}

#[tokio::test]
async fn oversized_content_length_is_rejected() {
    let (listener, addr) = listener().await;
    let config = Config {
        max_body: 4,
        ..Config::default()
    };
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        serve_connection(socket, echo, config).await.expect("serve");
    });

    let mut socket = TcpStream::connect(addr).await.expect("connect");
    socket
        .write_all(
            b"POST /echo HTTP/1.1\r\n\
              Host: localhost\r\n\
              Content-Length: 10\r\n\
              Connection: close\r\n\r\n",
        )
        .await
        .expect("write");

    let mut response = Vec::new();
    socket.read_to_end(&mut response).await.expect("read");
    assert!(
        String::from_utf8_lossy(&response).starts_with("HTTP/1.1 413 "),
        "{}",
        String::from_utf8_lossy(&response)
    );
}

#[tokio::test]
async fn handler_panic_returns_500() {
    let (listener, addr) = listener().await;
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let _ = serve_connection(socket, boom, Config::default()).await;
    });

    let mut socket = TcpStream::connect(addr).await.expect("connect");
    socket
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write");

    let mut response = Vec::new();
    socket.read_to_end(&mut response).await.expect("read");
    assert!(
        String::from_utf8_lossy(&response).starts_with("HTTP/1.1 500 "),
        "{}",
        String::from_utf8_lossy(&response)
    );
}

#[tokio::test]
async fn stalled_request_head_times_out() {
    let (listener, addr) = listener().await;
    let config = Config {
        head_timeout: Duration::from_millis(50),
        ..Config::default()
    };
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let _ = serve_connection(socket, echo, config).await;
    });

    let mut socket = TcpStream::connect(addr).await.expect("connect");
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
        .await
        .expect("server timed out")
        .expect("read");
    assert_eq!(read, 0);
}

#[tokio::test]
async fn shutdown_stops_accepting() {
    let (listener, addr) = listener().await;
    let server = tokio::spawn(serve_until(listener, echo, Config::default(), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }));

    let mut client = Client::connect(addr).await.expect("connect");
    let response = client
        .request(b"GET", b"/", &[HOST], b"")
        .await
        .expect("request");
    assert_eq!(response.body, b"ok");
    drop(client);

    let _ = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server stopped")
        .expect("join");

    let refused = TcpStream::connect(addr).await;
    assert!(refused.is_err(), "listener still accepting");
}

#[tokio::test]
async fn aborting_the_task_cancels_the_connection() {
    let (listener, addr) = listener().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let _ = serve_connection(socket, echo, Config::default()).await;
    });

    let mut socket = TcpStream::connect(addr).await.expect("connect");
    socket
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .expect("partial head");

    server.abort();
    let mut byte = [0u8; 1];
    match tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("server sent bytes after cancellation"),
        Err(_) => panic!("server did not release the socket"),
    }
}

#[tokio::test]
async fn idle_connection_closes_cleanly() {
    let (listener, addr) = listener().await;
    let task = tokio::spawn(spawn_one(listener));

    let socket = TcpStream::connect(addr).await.expect("connect");
    drop(socket);

    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("server closed")
        .expect("join");
}
