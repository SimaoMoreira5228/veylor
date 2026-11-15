use crate::h1_peer::*;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use veylor_server::{MAX_STATIC_BODY_BYTES, Server, ServerConfig};

#[test]
fn eight_concurrent_clients() {
    let server = test_server();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let addr = server.addr();
        handles.push(std::thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).expect("connect loopback");
            stream
                .set_read_timeout(Some(DEADLINE))
                .expect("read timeout");
            stream
                .set_write_timeout(Some(DEADLINE))
                .expect("write timeout");
            let request = b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
            stream.write_all(request).expect("write request");
            let mut response = Vec::new();
            stream.read_to_end(&mut response).expect("read response");
            let text = String::from_utf8(response).expect("response utf8");
            assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
            assert!(text.ends_with("hello"), "body: {text}");
        }));
    }
    for handle in handles {
        handle.join().expect("client thread");
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 8, "counts: {counts:?}");
}

#[test]
fn graceful_drain_finishes_in_flight_request() {
    let server = test_server();
    let addr = server.addr();
    let mut stream = connect(&server);
    get(&mut stream, "/hello");
    server.stop_accepting();
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    read_eof(&mut stream);
    let counts = server.join().expect("join server");
    assert!(
        counts.completed + counts.shutdown >= 1,
        "counts: {counts:?}"
    );
    assert!(
        TcpStream::connect(addr).is_err(),
        "listener closed after join"
    );
}

#[test]
fn second_server_instance_coexists() {
    let first = test_server();
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut second = Server::bind(addr, ServerConfig::default()).expect("bind second");
    second
        .register_route(
            b"GET",
            b"/other",
            200,
            b"OK",
            b"other",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register other");
    let second = second.start().expect("start second");
    assert_ne!(first.addr(), second.addr());

    let mut stream = connect(&first);
    get(&mut stream, "/hello");
    let first_response = read_response(&mut stream);
    assert_eq!(first_response.body, b"hello");

    let mut other = connect(&second);
    get(&mut other, "/other");
    let second_response = read_response(&mut other);
    assert_eq!(second_response.body, b"other");

    first.stop_accepting();
    second.stop_accepting();
    let _ = first.join().expect("join first");
    let _ = second.join().expect("join second");
}

#[test]
fn trickle_head_times_out_and_slot_reusable() {
    let config = tight_config(100_000_000, 5_000_000_000, 5_000_000_000, 5_000_000_000);
    let server = test_server_with_config(config);
    let mut stream = connect(&server);
    stream
        .write_all(b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n")
        .expect("write partial head");
    read_eof(&mut stream);
    let mut again = connect(&server);
    get(&mut again, "/hello");
    let response = read_response(&mut again);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.timeout, 1, "counts: {counts:?}");
}

#[test]
fn stalled_drain_times_out_mid_body() {
    let config = tight_config(5_000_000_000, 100_000_000, 5_000_000_000, 5_000_000_000);
    let server = test_server_with_config(config);
    let mut stream = connect(&server);
    let head = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 1024\r\n\r\n";
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(&[b'a'; 100]).expect("write partial body");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.timeout, 1, "counts: {counts:?}");
}

#[test]
fn idle_keep_alive_closes_after_idle_timeout() {
    let config = tight_config(5_000_000_000, 5_000_000_000, 5_000_000_000, 100_000_000);
    let server = test_server_with_config(config);
    let mut stream = connect(&server);
    get(&mut stream, "/hello");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.timeout, 1, "counts: {counts:?}");
}

#[test]
fn generous_timeouts_full_exchange_without_false_timeout() {
    let config = tight_config(5_000_000_000, 5_000_000_000, 5_000_000_000, 5_000_000_000);
    let server = test_server_with_config(config);
    let mut stream = connect(&server);
    get(&mut stream, "/hello");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    get(&mut stream, "/empty");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert!(second.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.timeout, 0, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn warm_keep_alive_round_trip_stays_responsive() {
    let server = test_server();
    let mut stream = connect(&server);
    get(&mut stream, "/hello");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    let start = Instant::now();
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    assert!(
        start.elapsed() < Duration::from_millis(50),
        "round trip stalled"
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn stop_accepting_with_idle_keep_alive_returns_promptly() {
    let server = test_server();
    let mut stream = connect(&server);
    get(&mut stream, "/hello");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    let start = Instant::now();
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "stop join stalled"
    );
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn hot_keep_alive_connection_does_not_starve_probe() {
    const HOT_EXCHANGES: u64 = 3000;
    const PROBES: usize = 25;
    const PROBE_INTERVAL: Duration = Duration::from_millis(2);
    const PROBE_LATENCY_BOUND: Duration = Duration::from_millis(500);

    let server = test_server();
    let addr = server.addr();

    let hot_done = Arc::new(AtomicU64::new(0));
    let hot = {
        let hot_done = Arc::clone(&hot_done);
        thread::spawn(move || {
            let mut stream = connect_addr(addr);

            let request =
                "GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n";
            stream.write_all(request.as_bytes()).expect("hot prime");
            stream.write_all(request.as_bytes()).expect("hot prime");
            for _ in 0..HOT_EXCHANGES {
                let response = read_response(&mut stream);
                assert_eq!(response.status, 200);
                assert_eq!(response.body, b"hello");
                stream.write_all(request.as_bytes()).expect("hot request");
                hot_done.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    let mut probe = connect(&server);
    let mut worst = Duration::ZERO;
    for _ in 0..PROBES {
        thread::sleep(PROBE_INTERVAL);
        let start = Instant::now();
        get(&mut probe, "/hello");
        let response = read_response(&mut probe);
        worst = worst.max(start.elapsed());
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
    }

    hot.join().expect("hot thread");
    assert_eq!(hot_done.load(Ordering::Relaxed), HOT_EXCHANGES);
    assert!(
        worst < PROBE_LATENCY_BOUND,
        "probe tail {worst:?} exceeds bound"
    );
    server.stop_accepting();
    server.join().expect("join server");
}
