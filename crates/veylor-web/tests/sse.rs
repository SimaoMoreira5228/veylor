use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use veylor_server::{Server, ServerConfig};
use veylor_web::{Router, ServeExt, SseEvent, SseSender, State};

struct Base(i64);

fn events(State(base): State<Base>, sender: SseSender) {
    sender.comment("open");
    sender.send(SseEvent::data("one").event("tick").id("1"));
    sender.send(SseEvent::data("two\nlines"));
    sender.send(SseEvent::data(format!("base:{}", base.0)));
    sender.close();
}

#[test]
fn sse_streams_events_over_http1() {
    let server = Server::bind(
        ([127, 0, 0, 1], 0).into(),
        ServerConfig {
            worker_count: 1,
            ..ServerConfig::default()
        },
    )
    .expect("bind");
    let app = Router::with_state(Base(7)).sse("/events", events);

    let running = server.serve(app).expect("serve");
    let mut stream = TcpStream::connect(running.addr()).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("timeout");
    stream
        .write_all(b"GET /events HTTP/1.1\r\nHost: t\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n")
        .expect("write");

    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read");
    let response = String::from_utf8_lossy(&response);

    assert!(response.starts_with("HTTP/1.1 200 "), "{response}");
    assert!(
        response.contains("content-type: text/event-stream"),
        "{response}"
    );
    assert!(response.contains("cache-control: no-cache"), "{response}");
    assert!(response.contains(": open\n"), "{response}");
    assert!(
        response.contains("event: tick\nid: 1\ndata: one\n"),
        "{response}"
    );
    assert!(response.contains("data: two\ndata: lines\n"), "{response}");
    assert!(response.contains("data: base:7\n"), "{response}");

    running.join().expect("join");
}
