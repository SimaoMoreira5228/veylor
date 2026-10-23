use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use veylor_runtime::{ConnectionTimeouts, ConnectionToken};
use veylor_server::{
    EventSlot, HostEventKind, MAX_STATIC_BODY_BYTES, RespondOutcome, RunningServer, Server,
    ServerConfig,
};

const DEADLINE: Duration = Duration::from_secs(5);

fn test_server() -> RunningServer {
    test_server_with_config(ServerConfig::default())
}

fn test_server_with_config(config: ServerConfig) -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register hello");
    server
        .register_route(b"GET", b"/empty", 200, b"OK", b"", MAX_STATIC_BODY_BYTES)
        .expect("register empty");
    server
        .register_route(
            b"POST",
            b"/submit",
            200,
            b"OK",
            b"submitted",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register submit");
    server.start().expect("start server")
}

fn connect(server: &RunningServer) -> TcpStream {
    let stream = TcpStream::connect(server.addr()).expect("connect loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
}

struct ClientResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn read_response(stream: &mut TcpStream) -> ClientResponse {
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
        assert!(head.len() < 65536, "head overflow");
    }
    let text = String::from_utf8(head).expect("head utf8");
    let mut lines = text.split("\r\n");
    let status_line = lines.next().expect("status line");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status int");
    let mut headers = Vec::new();
    let mut content_length = 0;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').expect("header colon");
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().expect("content length int");
        }
        headers.push((name.to_string(), value.trim().to_string()));
    }
    let mut body = vec![0; content_length];
    stream.read_exact(&mut body).expect("read body");
    ClientResponse {
        status,
        headers,
        body,
    }
}

fn read_eof(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        other => panic!("expected EOF, got {other:?}"),
    }
}

fn read_closed(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Err(_) => {}
        Ok(_) => panic!("expected close, got data"),
    }
}

fn get(stream: &mut TcpStream, path: &str) {
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write request");
}

fn tight_config(head_ns: u64, body_ns: u64, write_ns: u64, idle_ns: u64) -> ServerConfig {
    ServerConfig {
        timeouts: ConnectionTimeouts {
            request_head_ns: head_ns,
            body_progress_ns: body_ns,
            response_write_ns: write_ns,
            keep_alive_idle_ns: idle_ns,
        },
        ..ServerConfig::default()
    }
}

#[test]
fn persistent_double_get_keep_alive() {
    let server = test_server();
    let mut stream = connect(&server);
    get(&mut stream, "/hello");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    get(&mut stream, "/empty");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert!(second.body.is_empty());
    let content_length = second
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .expect("content-length present");
    assert_eq!(content_length.1, "0");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn pipelined_double_get_served_from_tail() {
    let server = test_server();
    let mut stream = connect(&server);
    let pipelined = concat!(
        "GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n",
        "GET /empty HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n",
    );
    stream
        .write_all(pipelined.as_bytes())
        .expect("write pipelined");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert!(second.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn unknown_path_returns_404_then_closes() {
    let server = test_server();
    let mut stream = connect(&server);
    get(&mut stream, "/missing");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 404);
    assert_eq!(response.body, b"Not Found");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn malformed_request_line_closes_with_400() {
    let server = test_server();
    let mut stream = connect(&server);
    stream
        .write_all(b"BAD REQUEST\r\n\r\n")
        .expect("write malformed");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 400);
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn request_body_on_static_route_closes_after_response() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\n\r\nhello";
    stream
        .write_all(request.as_bytes())
        .expect("write body request");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 404, "static POST has no route");
    assert_eq!(response.body, b"Not Found");
    read_eof(&mut stream);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn small_get_body_drained_then_keep_alive_reusable() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\n\r\nhello";
    stream
        .write_all(request.as_bytes())
        .expect("write GET with body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    get(&mut stream, "/empty");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert!(second.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn post_small_body_drained_then_keep_alive_reusable() {
    let server = test_server();
    let mut stream = connect(&server);
    let request =
        "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 11\r\n\r\nhello world";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn post_large_body_drains_across_pumps_then_reusable() {
    let server = test_server();
    let mut stream = connect(&server);
    let body = vec![b'a'; 20_000];
    let head = format!(
        "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(&body).expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn oversized_body_rejected_without_reading() {
    let server = test_server();
    let mut stream = connect(&server);
    let head = format!(
        "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\n\r\n",
        MAX_STATIC_BODY_BYTES + 1024
    );
    stream.write_all(head.as_bytes()).expect("write head");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 413);
    assert_eq!(response.body, b"Payload Too Large");
    let chunk = vec![b'x'; 1024];
    let _ = stream.write_all(&chunk);
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn chunked_request_rejected_without_reading() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write head");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 413);
    assert_eq!(response.body, b"Payload Too Large");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn expect_continue_oversized_rejected_without_interim() {
    let server = test_server();
    let mut stream = connect(&server);
    let head = format!(
        "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nExpect: 100-continue\r\n\r\n",
        MAX_STATIC_BODY_BYTES + 1024
    );
    stream.write_all(head.as_bytes()).expect("write head");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 413);
    assert_eq!(response.body, b"Payload Too Large");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn expect_continue_withheld_body_gets_interim_then_final() {
    let server = test_server();
    let mut stream = connect(&server);
    let head = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n";
    stream.write_all(head.as_bytes()).expect("write head");
    let interim = read_response(&mut stream);
    assert_eq!(interim.status, 100);
    assert!(interim.body.is_empty());
    stream.write_all(b"hello").expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn expect_continue_pipelined_body_gets_interim_before_final() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\nhello";
    stream.write_all(request.as_bytes()).expect("write POST");
    let interim = read_response(&mut stream);
    assert_eq!(interim.status, 100);
    assert!(interim.body.is_empty());
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn expect_continue_chunked_rejected_without_interim() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write head");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 413);
    assert_eq!(response.body, b"Payload Too Large");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn unsupported_expectation_rejected_with_417_then_close() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nExpect: chocolate\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write head");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 417);
    assert_eq!(response.body, b"Expectation Failed");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn http10_expect_ignored_without_interim() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /submit HTTP/1.0\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\nhello";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    read_eof(&mut stream);
    let mut other = connect(&server);
    let bogus = "POST /submit HTTP/1.0\r\nContent-Length: 5\r\nExpect: chocolate\r\n\r\nhello";
    other.write_all(bogus.as_bytes()).expect("write bogus");
    let second = read_response(&mut other);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"submitted");
    read_eof(&mut other);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 2, "counts: {counts:?}");
}

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
fn duplicate_route_rejected() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_route(b"GET", b"/dup", 200, b"OK", b"a", MAX_STATIC_BODY_BYTES)
        .expect("first register");
    assert!(
        server
            .register_route(b"GET", b"/dup", 200, b"OK", b"b", u64::MAX)
            .is_err()
    );
    let running = server.start().expect("start server");
    running.stop_accepting();
    let _ = running.join().expect("join server");
}

#[test]
fn head_falls_back_to_get_without_body() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = b"HEAD /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    stream.write_all(request).expect("write HEAD");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
    }
    let text = String::from_utf8(head).expect("head utf8");
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(
        text.contains("Content-Length: 5"),
        "length preserved: {text}"
    );
    read_eof(&mut stream);
    server.stop_accepting();
    let _ = server.join().expect("join server");
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

fn capped_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_route(b"POST", b"/tight", 200, b"OK", b"tight", 16)
        .expect("tight");
    server
        .register_route(b"POST", b"/zero", 200, b"OK", b"zero", 0)
        .expect("zero");
    server
        .register_route(b"POST", b"/huge", 200, b"OK", b"huge", u64::MAX)
        .expect("huge");
    server
        .register_managed(b"POST", b"/mupload", 16)
        .expect("managed");
    server.start().expect("start server")
}

fn post_head(stream: &mut TcpStream, path: &str, length: usize) {
    let head =
        format!("POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {length}\r\n\r\n");
    stream.write_all(head.as_bytes()).expect("write head");
}

#[test]
fn tight_cap_drains_at_cap_and_rejects_one_over() {
    let server = capped_server();
    let mut stream = connect(&server);
    post_head(&mut stream, "/tight", 16);
    stream.write_all(&[b'a'; 16]).expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"tight");
    post_head(&mut stream, "/tight", 0);
    let reuse = read_response(&mut stream);
    assert_eq!(reuse.status, 200);
    let mut over = connect(&server);
    post_head(&mut over, "/tight", 17);
    let rejected = read_response(&mut over);
    assert_eq!(rejected.status, 413);
    read_closed(&mut over);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn zero_cap_admits_empty_body_and_rejects_one_byte() {
    let server = capped_server();
    let mut stream = connect(&server);
    post_head(&mut stream, "/zero", 0);
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"zero");
    post_head(&mut stream, "/zero", 1);
    let rejected = read_response(&mut stream);
    assert_eq!(rejected.status, 413);
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn uncapped_route_drains_20kib() {
    let server = capped_server();
    let mut stream = connect(&server);
    let body = vec![b'b'; 20_000];
    post_head(&mut stream, "/huge", body.len());
    stream.write_all(&body).expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"huge");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn managed_cap_rejects_over_cap_and_parks_under_cap() {
    let server = capped_server();
    let host = server.host();
    let mut over = connect(&server);
    post_head(&mut over, "/mupload", 17);
    let rejected = read_response(&mut over);
    assert_eq!(rejected.status, 413);
    read_closed(&mut over);
    // NOTE: the 413 above proves the worker consumed the head, so an empty nonblocking
    // poll proves the rejection parked nothing instead of racing a late event.
    let report = host.poll(0, &mut [], &mut [], &mut []);
    assert_eq!(report.copied, 0);
    assert_eq!(report.events_required, 0);
    let mut under = connect(&server);
    post_head(&mut under, "/mupload", 5);
    under.write_all(b"hello").expect("write body");
    let token = ConnectionToken {
        worker_id: 0,
        slot: 0,
        request_seq: 0,
        stream_id: 0,
        worker_generation: 0,
        generation: 0,
    };
    let mut events = [EventSlot {
        kind: HostEventKind::Shutdown,
        token,
        route_id: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    }];
    let mut methods = [0; 64];
    let mut paths = [0; 4096];
    let report = host.poll(5_000_000_000, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, HostEventKind::Request);
    let mut staged = [0; 16];
    // NOTE: the head and body travel in separate segments, so the park event can win the race
    // against the body pump; retry the pull until the staged bytes land or the test times out.
    let pulled = {
        let start = Instant::now();
        loop {
            let pulled = host.pull_body(events[0].token, &mut staged);
            if pulled == (RespondOutcome::Ok, 5, true) || start.elapsed() >= DEADLINE {
                break pulled;
            }
            std::thread::yield_now();
        }
    };
    assert_eq!(pulled, (RespondOutcome::Ok, 5, true));
    assert_eq!(&staged[..5], b"hello");
    assert_eq!(
        host.respond(events[0].token, 200, b"OK", b"m-ok"),
        RespondOutcome::Ok
    );
    let done = read_response(&mut under);
    assert_eq!(done.status, 200);
    assert_eq!(done.body, b"m-ok");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}
