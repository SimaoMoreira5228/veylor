use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

use super::h1_peer::{connect, get, read_response, test_server};

const SLAB_HOLD_SETTLE: Duration = Duration::from_millis(120);

#[test]
fn keep_alive_connection_resumes_after_the_slab_hold_window() {
    let server = test_server();
    let mut stream = connect(&server);

    for _ in 0..3 {
        get(&mut stream, "/hello");
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
        std::thread::sleep(SLAB_HOLD_SETTLE);
    }
}

#[test]
fn pipelined_requests_are_answered_in_order_after_a_released_slab() {
    let server = test_server();
    let mut stream = connect(&server);

    get(&mut stream, "/hello");
    assert_eq!(read_response(&mut stream).body, b"hello");
    std::thread::sleep(SLAB_HOLD_SETTLE);

    let batch = b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n\
                  GET /empty HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n";
    stream.write_all(batch).expect("write pipelined batch");

    assert_eq!(read_response(&mut stream).body, b"hello");
    assert_eq!(read_response(&mut stream).body, b"");
}

#[test]
fn alternating_connections_release_and_retake_slabs() {
    let server = test_server();
    let mut streams: Vec<TcpStream> = (0..4).map(|_| connect(&server)).collect();

    for round in 0..6 {
        let index = round % streams.len();
        let stream = &mut streams[index];
        get(stream, "/hello");
        let response = read_response(stream);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
        std::thread::sleep(SLAB_HOLD_SETTLE);
    }
}

#[test]
fn churn_across_the_hold_window_keeps_serving() {
    let server = test_server();

    for _ in 0..32 {
        let mut stream = connect(&server);
        get(&mut stream, "/hello");
        assert_eq!(read_response(&mut stream).body, b"hello");
        drop(stream);
    }
}
