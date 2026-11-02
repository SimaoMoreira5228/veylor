use std::future::Future;
use std::time::Duration;

use smol::Timer;
use smol::future;
use smol::io::{AsyncReadExt, AsyncWriteExt};
use smol::net::{TcpListener, TcpStream};
use veylor_http1::Field;
use veylor_runtime_smol::{Client, Config, Request, Response, serve_connection, serve_until};

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

async fn within<F: Future>(future: F, limit: Duration) -> Option<F::Output> {
    future::or(async { Some(future.await) }, async {
        Timer::after(limit).await;
        None
    })
    .await
}

fn run<F: Future>(future: F) -> F::Output {
    smol::block_on(future)
}

fn boom(_request: Request<'_>) -> Response<'_> {
    panic!("handler panic")
}

#[test]
fn handler_panic_returns_500() {
    run(async {
        let (listener, addr) = listener().await;
        smol::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let _ = serve_connection(socket, boom, Config::default()).await;
        })
        .detach();

        let mut socket = TcpStream::connect(addr).await.expect("connect");
        socket
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write");

        let mut response = Vec::new();
        let _ = within(socket.read_to_end(&mut response), Duration::from_secs(2))
            .await
            .expect("response");
        assert!(
            String::from_utf8_lossy(&response).starts_with("HTTP/1.1 500 "),
            "{}",
            String::from_utf8_lossy(&response)
        );
    });
}

#[test]
fn client_and_server_exchange_keep_alive() {
    run(async {
        let (listener, addr) = listener().await;
        smol::spawn(spawn_one(listener)).detach();

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
    });
}

#[test]
fn chunked_upload_is_buffered() {
    run(async {
        let (listener, addr) = listener().await;
        smol::spawn(spawn_one(listener)).detach();

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
    });
}

#[test]
fn expect_continue_gets_an_interim_response() {
    run(async {
        let (listener, addr) = listener().await;
        smol::spawn(spawn_one(listener)).detach();

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
    });
}

#[test]
fn oversized_content_length_is_rejected() {
    run(async {
        let (listener, addr) = listener().await;
        let config = Config {
            max_body: 4,
            ..Config::default()
        };
        smol::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            serve_connection(socket, echo, config).await.expect("serve");
        })
        .detach();

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
    });
}

#[test]
fn stalled_request_head_times_out() {
    run(async {
        let (listener, addr) = listener().await;
        let config = Config {
            head_timeout: Duration::from_millis(50),
            ..Config::default()
        };
        smol::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let _ = serve_connection(socket, echo, config).await;
        })
        .detach();

        let mut socket = TcpStream::connect(addr).await.expect("connect");
        let mut byte = [0u8; 1];
        let read = within(socket.read(&mut byte), Duration::from_secs(2))
            .await
            .expect("server timed out")
            .expect("read");
        assert_eq!(read, 0);
    });
}

#[test]
fn shutdown_stops_accepting() {
    run(async {
        let (listener, addr) = listener().await;
        let server = smol::spawn(serve_until(listener, echo, Config::default(), async {
            Timer::after(Duration::from_millis(200)).await;
        }));

        let mut client = Client::connect(addr).await.expect("connect");
        let response = client
            .request(b"GET", b"/", &[HOST], b"")
            .await
            .expect("request");
        assert_eq!(response.body, b"ok");
        drop(client);

        within(server, Duration::from_secs(2))
            .await
            .expect("server stopped")
            .expect("join");

        let refused = TcpStream::connect(addr).await;
        assert!(refused.is_err(), "listener still accepting");
    });
}

#[test]
fn aborting_the_task_cancels_the_connection() {
    run(async {
        let (listener, addr) = listener().await;
        let server = smol::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let _ = serve_connection(socket, echo, Config::default()).await;
        });

        let mut socket = TcpStream::connect(addr).await.expect("connect");
        socket
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .expect("partial head");

        drop(server);
        let mut byte = [0u8; 1];
        match within(socket.read(&mut byte), Duration::from_secs(2)).await {
            Some(Ok(0)) | Some(Err(_)) => {}
            Some(Ok(_)) => panic!("server sent bytes after cancellation"),
            None => panic!("server did not release the socket"),
        }
    });
}

#[test]
fn connection_limit_pauses_accept() {
    run(async {
        let (listener, addr) = listener().await;
        let config = Config {
            max_connections: 1,
            ..Config::default()
        };
        smol::spawn(async move {
            let _ = veylor_runtime_smol::serve(listener, echo, config).await;
        })
        .detach();

        let mut first = TcpStream::connect(addr).await.expect("first");
        first
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .expect("partial");
        Timer::after(Duration::from_millis(100)).await;

        let mut second = TcpStream::connect(addr).await.expect("second");
        second
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("request");
        let mut byte = [0u8; 1];
        assert!(
            within(second.read(&mut byte), Duration::from_millis(150))
                .await
                .is_none(),
            "second connection served while the limit was full"
        );

        drop(first);
        let mut response = Vec::new();
        within(second.read_to_end(&mut response), Duration::from_secs(2))
            .await
            .expect("served after slot")
            .expect("read");
        assert!(!response.is_empty());
    });
}

#[test]
fn idle_connection_closes_cleanly() {
    run(async {
        let (listener, addr) = listener().await;
        let task = smol::spawn(spawn_one(listener));

        let socket = TcpStream::connect(addr).await.expect("connect");
        drop(socket);

        within(task, Duration::from_secs(2))
            .await
            .expect("server closed");
    });
}
