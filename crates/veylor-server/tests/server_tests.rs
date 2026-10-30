use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use veylor_runtime::{ConnectionTimeouts, ConnectionToken};
use veylor_server::{
    EventSlot, HostEventKind, MAX_RESPONSE_BODY, MAX_STATIC_BODY_BYTES, NativeRequestView,
    NativeResponseOut, RespondOutcome, RunningServer, Server, ServerConfig, ServerError,
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
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn method_mismatch_answers_405_with_allow_then_closes() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 405);
    assert_eq!(response.body, b"Method Not Allowed");
    assert_eq!(header(&response, "allow"), "GET");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn head_on_post_only_answers_405_without_body() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "HEAD /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write HEAD");
    let text = read_head(&mut stream);
    assert!(
        text.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"),
        "status: {text}"
    );
    assert!(text.contains("Allow: POST"), "allow: {text}");
    assert!(text.contains("Content-Length: 18"), "length: {text}");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn native_method_mismatch_answers_405_with_allow() {
    let server = native_server();
    let mut stream = connect(&server);
    let request = "POST /fixed HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 405);
    assert_eq!(response.body, b"Method Not Allowed");
    assert_eq!(header(&response, "allow"), "GET");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn managed_method_mismatch_answers_405_without_parking() {
    let server = capped_server();
    let mut stream = connect(&server);
    let request = "GET /mupload HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write GET");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 405);
    assert_eq!(response.body, b"Method Not Allowed");
    assert_eq!(header(&response, "allow"), "POST");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    // NOTE: a parked managed request would surface a host event, so an empty nonblocking
    // poll proves the 405 rejected at dispatch instead of racing a late park.
    let report = server.host().poll(0, &mut [], &mut [], &mut []);
    assert_eq!(report.copied, 0);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn file_method_mismatch_answers_405_with_allow() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    let request = "POST /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 405);
    assert_eq!(response.body, b"Method Not Allowed");
    assert_eq!(header(&response, "allow"), "GET, HEAD");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    let mut other = connect(&server);
    let missing = "POST /assets/nope.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n";
    other
        .write_all(missing.as_bytes())
        .expect("write missing POST");
    let not_found = read_response(&mut other);
    assert_eq!(not_found.status, 404);
    assert_eq!(not_found.body, b"Not Found");
    read_eof(&mut other);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 2, "counts: {counts:?}");
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
fn oversized_header_section_closes_with_431() {
    let server = test_server();
    let mut stream = connect(&server);
    let mut request = String::from("GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n");
    for index in 0..120 {
        request.push_str(&format!("X-Pad-{index:03}: abcdefgh\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).expect("write headers");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 431);
    assert_eq!(response.body, b"Request Header Fields Too Large");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn oversized_request_target_answers_414() {
    let server = test_server();
    let mut stream = connect(&server);
    let target = format!("/{}", "a".repeat(9000));
    let request = format!("GET {target} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .expect("write long target");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 414);
    assert_eq!(response.body, b"URI Too Long");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn oversized_request_method_stays_400() {
    let server = test_server();
    let mut stream = connect(&server);
    let method = "A".repeat(9000);
    let request = format!("{method} /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .expect("write long method");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 400);
    assert_eq!(response.body, b"Bad Request");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn oversized_headers_with_bad_expect_answers_431() {
    // NOTE: drift alarm for H1-6 (error precedence), managed half: with both the section cap
    // breached and Expect unsupported, the shared head parser fails on the section overflow
    // first (request_head/parser.rs scans line order; the 101st header trips before the Expect
    // line), so the 431 arm wins over the 417 arm. The conn crate inherits the same decoder
    // verdict and collapses it to ProtocolError (request_read/tests
    // head_error_corpus_matches_managed_status_table pins the mirror row); the 417-vs-431 order
    // itself is decoder truth, not a mapper choice — the parse_exchange arms are disjoint.
    let server = test_server();
    let mut stream = connect(&server);
    let mut request = String::from("GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n");
    for index in 0..120 {
        request.push_str(&format!("X-Pad-{index:03}: abcdefgh\r\n"));
    }
    request.push_str("Expect: magic\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write headers");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 431);
    assert_eq!(response.body, b"Request Header Fields Too Large");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.protocol_error, 1, "counts: {counts:?}");
}

#[test]
fn full_buffer_without_head_answers_400() {
    // NOTE: drift alarm for H1-6 (error precedence), transport-split row: a full INPUT_CAP with
    // no complete head and no newline attributes 400 (no target side to blame 414 on). The
    // host-driven conn crate deliberately diverges here — the host owns its buffers, so the same
    // bytes read NeedMore (request_read/tests head_error_corpus_matches_managed_status_table
    // pins that half). Sharing the attribution helper would drag pump buffer semantics into the
    // host API, so the split is documented, not converged.
    let server = test_server();
    let mut stream = connect(&server);
    stream
        .write_all(&vec![b'A'; 8192])
        .expect("write full buffer");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 400);
    assert_eq!(response.body, b"Bad Request");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_closed(&mut stream);
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
    assert_eq!(response.status, 405, "static POST on GET-only is 405");
    assert_eq!(response.body, b"Method Not Allowed");
    assert_eq!(header(&response, "allow"), "GET");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
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

#[test]
fn managed_fixed_body_echo_round_trip_reuses_keep_alive() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut builder = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    builder
        .register_managed(b"POST", b"/echo", MAX_STATIC_BODY_BYTES)
        .expect("register echo");
    let server = builder.start().expect("start server");
    let host = server.host();
    let mut stream = connect(&server);

    for body in [b"hello echo".as_slice(), b"second pass".as_slice()] {
        let head = format!(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).expect("write head");
        stream.write_all(body).expect("write body");
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
        // NOTE: the head and body travel in separate segments, so the park event can win
        // the race against the body pump; retry the pull until the staged bytes complete
        // or the test times out.
        let mut collected = Vec::new();
        let start = Instant::now();
        loop {
            let mut staged = [0; 16];
            let (outcome, n, complete) = host.pull_body(events[0].token, &mut staged);
            assert_eq!(outcome, RespondOutcome::Ok);
            collected.extend_from_slice(&staged[..n]);
            if complete {
                break;
            }
            assert!(start.elapsed() < DEADLINE, "echo body stalls");
            if n == 0 {
                std::thread::yield_now();
            }
        }
        assert_eq!(collected, body);
        assert_eq!(
            host.respond(events[0].token, 200, b"OK", &collected),
            RespondOutcome::Ok
        );
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, body);
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);
const FILE_BLOB_LEN: usize = 1 << 20;

struct FileFixture {
    dir: PathBuf,
    root: PathBuf,
    blob: Vec<u8>,
}

impl FileFixture {
    fn build() -> Self {
        let id = NEXT_FILE_ID.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("veylor-srv-file-{}-{id}", std::process::id()));
        let root = dir.join("root");
        std::fs::create_dir_all(&root).expect("fixture root");
        std::fs::write(root.join("hello.txt"), b"hello file").expect("hello file");
        std::fs::write(root.join("empty.txt"), b"").expect("empty file");
        std::fs::write(dir.join("outside.txt"), b"outside secret").expect("outside file");
        let mut blob = Vec::with_capacity(FILE_BLOB_LEN);
        for i in 0..FILE_BLOB_LEN {
            blob.push((i % 251) as u8);
        }
        std::fs::write(root.join("blob.bin"), &blob).expect("blob file");
        Self { dir, root, blob }
    }
}

impl Drop for FileFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn file_server() -> (RunningServer, FileFixture) {
    let fixture = FileFixture::build();
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
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
        .register_file_mount(b"/assets/", &fixture.root)
        .expect("register assets");
    (server.start().expect("start server"), fixture)
}

fn header<'a>(response: &'a ClientResponse, name: &str) -> &'a str {
    response
        .headers
        .iter()
        .find(|(field, _)| field.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
        .expect("header present")
}

#[test]
fn file_get_serves_exact_bytes_type_length() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello file");
    assert_eq!(header(&response, "content-length"), "10");
    assert_eq!(header(&response, "content-type"), "text/plain");
    get(&mut stream, "/assets/hello.txt?v=1");
    let queried = read_response(&mut stream);
    assert_eq!(queried.status, 200);
    assert_eq!(queried.body, b"hello file");
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(fixture.blob.len(), FILE_BLOB_LEN);
}

#[test]
fn file_head_returns_headers_only() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    let request =
        b"HEAD /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
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
    assert!(text.contains("Content-Length: 10"), "length: {text}");
    assert!(text.contains("Content-Type: text/plain"), "type: {text}");
    read_eof(&mut stream);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn file_missing_returns_404_then_closes() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/nope.txt");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 404);
    assert_eq!(response.body, b"Not Found");
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn file_traversal_rejected_without_fs_escape() {
    let (server, _fixture) = file_server();
    for path in ["/assets/../outside.txt", "/assets/%2e%2e/outside.txt"] {
        let mut stream = connect(&server);
        get(&mut stream, path);
        let response = read_response(&mut stream);
        assert_eq!(response.status, 404, "path {path}");
        assert_eq!(response.body, b"Not Found", "path {path}");
        read_eof(&mut stream);
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 2, "counts: {counts:?}");
}

#[test]
fn file_blob_streams_exact_1mib() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/blob.bin");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(
        header(&response, "content-length"),
        FILE_BLOB_LEN.to_string()
    );
    assert_eq!(
        header(&response, "content-type"),
        "application/octet-stream"
    );
    assert_eq!(response.body, fixture.blob);
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_slow_reader_tiny_reads_completes() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/blob.bin");
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
    let mut body = Vec::with_capacity(FILE_BLOB_LEN);
    while body.len() < FILE_BLOB_LEN {
        let mut byte = [0; 1];
        stream.read_exact(&mut byte).expect("read body byte");
        body.push(byte[0]);
    }
    assert_eq!(body, fixture.blob);
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_disconnect_mid_transfer_server_healthy() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/blob.bin");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
    }
    let mut partial = [0; 4096];
    stream.read_exact(&mut partial).expect("read partial body");
    drop(stream);
    let mut again = connect(&server);
    get(&mut again, "/hello");
    let response = read_response(&mut again);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert!(
        counts.peer_closed + counts.reset + counts.transport_error >= 1,
        "counts: {counts:?}"
    );
}

#[test]
fn file_shutdown_mid_transfer_drains_clean() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/blob.bin");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
    }
    server.stop_accepting();
    let mut body = Vec::with_capacity(FILE_BLOB_LEN);
    stream.read_to_end(&mut body).expect("read drained body");
    assert_eq!(body, fixture.blob);
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn file_closed_receiver_no_sigpipe_stays_healthy() {
    let (server, _fixture) = file_server();
    for _ in 0..8 {
        let mut stream = connect(&server);
        get(&mut stream, "/assets/blob.bin");
        let mut head = Vec::new();
        let mut byte = [0; 1];
        while !head.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte) {
                Ok(0) => break,
                Ok(_) => head.push(byte[0]),
                Err(error) => panic!("read head: {error:?}"),
            }
        }
        let mut partial = [0; 65536];
        stream.read_exact(&mut partial).expect("read partial body");
        drop(stream);
    }
    // NOTE: Rust ignores SIGPIPE process-wide from lang_start, so every closed-receiver
    // sendfile above surfaced as EPIPE/PeerClosed or RST/Reset instead of a signal; this
    // process surviving to serve again pins that discipline with no handler installed.
    let mut again = connect(&server);
    get(&mut again, "/hello");
    let response = read_response(&mut again);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
    assert!(
        counts.peer_closed + counts.reset + counts.transport_error >= 1,
        "counts: {counts:?}"
    );
}

struct TruncGuard {
    dir: PathBuf,
}

impl Drop for TruncGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn file_truncated_mid_transfer_fails_closed() {
    const BIG_LEN: usize = 16 << 20;
    let id = NEXT_FILE_ID.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("veylor-srv-trunc-{}-{id}", std::process::id()));
    let _guard = TruncGuard { dir: dir.clone() };
    let root = dir.join("root");
    std::fs::create_dir_all(&root).expect("fixture root");
    let mut big = Vec::with_capacity(BIG_LEN);
    for i in 0..BIG_LEN {
        big.push((i % 251) as u8);
    }
    std::fs::write(root.join("big.bin"), &big).expect("big file");
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_file_mount(b"/assets/", &root)
        .expect("register assets");
    let server = server.start().expect("start server");
    let mut stream = connect(&server);
    get(&mut stream, "/assets/big.bin");
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
        text.contains(&format!("Content-Length: {BIG_LEN}")),
        "length: {text}"
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(root.join("big.bin"))
        .expect("open big")
        .set_len(100)
        .expect("truncate big");
    let mut body = Vec::new();
    stream.read_to_end(&mut body).expect("read truncated body");
    // NOTE: a shrink racing a sendfile can zero-fill inside the plan bounds (one call
    // straddled this ftruncate: zeroed partial-page tail plus a not-yet-discarded page),
    // where the read_chunk path would serve a short read instead. Either way the transfer
    // fails closed below: short body, EOF, transport_error, never a short success.
    assert!(body.len() < BIG_LEN, "short body, got {}", body.len());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
    assert_eq!(counts.transport_error, 1, "counts: {counts:?}");
}

#[test]
fn file_duplicate_and_invalid_mounts_reject() {
    let fixture = FileFixture::build();
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_file_mount(b"/assets/", &fixture.root)
        .expect("first mount");
    assert!(
        server
            .register_file_mount(b"/assets/", &fixture.root)
            .is_err()
    );
    assert!(
        server
            .register_file_mount(b"/bad/", &fixture.root.join("hello.txt"))
            .is_err()
    );
    let running = server.start().expect("start server");
    running.stop_accepting();
    let _ = running.join().expect("join server");
}

#[test]
fn file_keepalive_replace_between_transfers_serves_new_bytes() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello file");
    let replacement = b"hello file, replaced and longer";
    std::fs::write(fixture.root.join("hello.txt"), replacement).expect("replace hello");
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, replacement);
    assert_eq!(
        header(&second, "content-length"),
        replacement.len().to_string()
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_eviction_round_robin_across_many_files_stays_exact() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    for _ in 0..2 {
        get(&mut stream, "/assets/hello.txt");
        let hello = read_response(&mut stream);
        assert_eq!(hello.status, 200);
        assert_eq!(hello.body, b"hello file");
        get(&mut stream, "/assets/blob.bin");
        let blob = read_response(&mut stream);
        assert_eq!(blob.status, 200);
        assert_eq!(blob.body, fixture.blob);
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

fn get_range(stream: &mut TcpStream, path: &str, range: &str) {
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nRange: {range}\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .expect("write ranged request");
}

fn read_head(stream: &mut TcpStream) -> String {
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
    String::from_utf8(head).expect("head utf8")
}

#[test]
fn file_range_exact_slice_then_keepalive_full() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=0-3");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 206);
    assert_eq!(first.body, b"hell");
    assert_eq!(header(&first, "content-length"), "4");
    assert_eq!(header(&first, "content-range"), "bytes 0-3/10");
    assert_eq!(header(&first, "accept-ranges"), "bytes");
    assert_eq!(header(&first, "content-type"), "text/plain");
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    assert_eq!(header(&second, "content-length"), "10");
    assert_eq!(header(&second, "accept-ranges"), "bytes");
    assert!(
        second
            .headers
            .iter()
            .all(|(field, _)| !field.eq_ignore_ascii_case("content-range")),
        "200 carries no content-range"
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_open_suffix_and_clamp_serve_exact_windows() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=6-");
    let open = read_response(&mut stream);
    assert_eq!(open.status, 206);
    assert_eq!(open.body, b"file");
    assert_eq!(header(&open, "content-range"), "bytes 6-9/10");
    get_range(&mut stream, "/assets/hello.txt", "bytes=-4");
    let suffix = read_response(&mut stream);
    assert_eq!(suffix.status, 206);
    assert_eq!(suffix.body, b"file");
    assert_eq!(header(&suffix, "content-range"), "bytes 6-9/10");
    get_range(&mut stream, "/assets/hello.txt", "bytes=4-100");
    let clamped = read_response(&mut stream);
    assert_eq!(clamped.status, 206);
    assert_eq!(clamped.body, b"o file");
    assert_eq!(header(&clamped, "content-range"), "bytes 4-9/10");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_unsatisfiable_answers_416_then_closes() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=10-");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 416);
    assert!(response.body.is_empty());
    assert_eq!(header(&response, "content-range"), "bytes */10");
    assert_eq!(header(&response, "content-length"), "0");
    read_eof(&mut stream);
    let mut empty = connect(&server);
    get_range(&mut empty, "/assets/empty.txt", "bytes=0-");
    let starved = read_response(&mut empty);
    assert_eq!(starved.status, 416);
    assert_eq!(header(&starved, "content-range"), "bytes */0");
    read_eof(&mut empty);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 2, "counts: {counts:?}");
}

#[test]
fn file_range_ignored_headers_serve_200_full() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    for range in ["items=0-1", "bytes=nope", "bytes=5-3"] {
        get_range(&mut stream, "/assets/hello.txt", range);
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200, "range {range}");
        assert_eq!(response.body, b"hello file", "range {range}");
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_head_ignores_range_with_full_headers() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    let request = "HEAD /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nRange: bytes=0-3\r\n\r\n";
    stream
        .write_all(request.as_bytes())
        .expect("write HEAD range");
    let text = read_head(&mut stream);
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(text.contains("Content-Length: 10"), "length: {text}");
    assert!(!text.contains("Content-Range:"), "no range: {text}");
    assert!(text.contains("Accept-Ranges: bytes"), "accept: {text}");
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_overflow_start_is_unsatisfiable() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(
        &mut stream,
        "/assets/hello.txt",
        "bytes=18446744073709551615-",
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 416);
    assert_eq!(header(&response, "content-range"), "bytes */10");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn file_range_blob_slice_streams_exact_window() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/blob.bin", "bytes=1000-1999");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 206);
    assert_eq!(
        header(&response, "content-range"),
        format!("bytes 1000-1999/{}", FILE_BLOB_LEN)
    );
    assert_eq!(response.body, &fixture.blob[1000..2000]);
    get_range(&mut stream, "/assets/blob.bin", "bytes=-16");
    let tail = read_response(&mut stream);
    assert_eq!(tail.status, 206);
    assert_eq!(tail.body, &fixture.blob[FILE_BLOB_LEN - 16..]);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

fn multipart_boundary(response: &ClientResponse) -> &str {
    header(response, "content-type")
        .strip_prefix("multipart/byteranges; boundary=")
        .expect("multipart content type")
}

fn expected_multipart(
    boundary: &str,
    content_type: &str,
    total: usize,
    windows: &[(usize, usize)],
    file: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    for (start, end) in windows.iter().copied() {
        body.extend_from_slice(b"--");
        body.extend_from_slice(boundary.as_bytes());
        body.extend_from_slice(b"\r\nContent-Type: ");
        body.extend_from_slice(content_type.as_bytes());
        body.extend_from_slice(
            format!(
                "\r\nContent-Range: bytes {start}-{}/{total}\r\n\r\n",
                end - 1
            )
            .as_bytes(),
        );
        body.extend_from_slice(&file[start..end]);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"--\r\n");
    body
}

#[test]
fn file_range_two_part_serves_byte_exact_multipart() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=0-3,6-9");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 206);
    let boundary = multipart_boundary(&response).to_string();
    let expected = expected_multipart(
        &boundary,
        "text/plain",
        10,
        &[(0, 4), (6, 10)],
        b"hello file",
    );
    assert_eq!(response.body, expected);
    assert_eq!(
        header(&response, "content-length"),
        expected.len().to_string()
    );
    assert_eq!(header(&response, "accept-ranges"), "bytes");
    assert!(
        response
            .headers
            .iter()
            .all(|(field, _)| !field.eq_ignore_ascii_case("content-range")),
        "multipart carries no top-level content-range"
    );
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_mixed_set_serves_valid_subset() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=0-3,99-");
    let single = read_response(&mut stream);
    assert_eq!(single.status, 206);
    assert_eq!(single.body, b"hell");
    assert_eq!(header(&single, "content-range"), "bytes 0-3/10");
    assert_eq!(header(&single, "content-type"), "text/plain");
    get_range(&mut stream, "/assets/hello.txt", "bytes=0-1,4-5,99-");
    let multi = read_response(&mut stream);
    assert_eq!(multi.status, 206);
    let boundary = multipart_boundary(&multi).to_string();
    let expected = expected_multipart(
        &boundary,
        "text/plain",
        10,
        &[(0, 2), (4, 6)],
        b"hello file",
    );
    assert_eq!(multi.body, expected);
    assert_eq!(header(&multi, "content-length"), expected.len().to_string());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_all_unsatisfiable_set_answers_416() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=10-,20-30");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 416);
    assert!(response.body.is_empty());
    assert_eq!(header(&response, "content-range"), "bytes */10");
    assert_eq!(header(&response, "content-length"), "0");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn file_range_malformed_and_overcap_sets_serve_200_full() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    for range in [
        "bytes=a-b,c-d",
        "bytes=,",
        "bytes=0-0,1-1,2-2,3-3,4-4,5-5,6-6,7-7,8-8",
    ] {
        get_range(&mut stream, "/assets/hello.txt", range);
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200, "range {range}");
        assert_eq!(response.body, b"hello file", "range {range}");
        assert_eq!(
            header(&response, "content-type"),
            "text/plain",
            "range {range}"
        );
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_head_multi_ignores_range_with_full_headers() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    let request = "HEAD /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nRange: bytes=0-3,6-9\r\n\r\n";
    stream
        .write_all(request.as_bytes())
        .expect("write HEAD range");
    let text = read_head(&mut stream);
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(text.contains("Content-Length: 10"), "length: {text}");
    assert!(!text.contains("Content-Range:"), "no range: {text}");
    assert!(!text.contains("multipart"), "no multipart: {text}");
    assert!(text.contains("Accept-Ranges: bytes"), "accept: {text}");
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_large_multipart_streams_across_turns() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get_range(
        &mut stream,
        "/assets/blob.bin",
        "bytes=0-99999,200000-299999,600000-699999",
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 206);
    let boundary = multipart_boundary(&response).to_string();
    let expected = expected_multipart(
        &boundary,
        "application/octet-stream",
        FILE_BLOB_LEN,
        &[(0, 100000), (200000, 300000), (600000, 700000)],
        &fixture.blob,
    );
    assert_eq!(response.body, expected);
    assert_eq!(
        header(&response, "content-length"),
        expected.len().to_string()
    );
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

fn get_conditional(stream: &mut TcpStream, path: &str, field: &str, value: &str) {
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n{field}: {value}\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .expect("write conditional request");
}

fn last_modified(response: &ClientResponse) -> String {
    header(response, "last-modified").to_string()
}

#[test]
fn file_last_modified_present_and_imf_shaped() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    let seen = last_modified(&response);
    assert_eq!(seen.len(), 29, "IMF-fixdate width: {seen}");
    assert!(seen.ends_with(" GMT"), "GMT only: {seen}");
    assert_eq!(seen.as_bytes()[3], b',', "weekday comma: {seen}");
    get_range(&mut stream, "/assets/hello.txt", "bytes=0-3");
    let partial = read_response(&mut stream);
    assert_eq!(partial.status, 206);
    assert_eq!(
        last_modified(&partial),
        seen,
        "206 carries the same validator"
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_conditional_round_trip_304_keeps_alive() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    let seen = last_modified(&first);
    get_conditional(&mut stream, "/assets/hello.txt", "If-Modified-Since", &seen);
    let not_modified = read_response(&mut stream);
    assert_eq!(not_modified.status, 304);
    assert!(not_modified.body.is_empty());
    assert!(
        not_modified
            .headers
            .iter()
            .all(|(field, _)| !field.eq_ignore_ascii_case("content-length")),
        "304 omits content-length: only the would-be-200 length is legal"
    );
    assert_eq!(
        last_modified(&not_modified),
        seen,
        "304 repeats the validator"
    );
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_conditional_older_and_garbage_dates_serve_200_full() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_conditional(
        &mut stream,
        "/assets/hello.txt",
        "If-Modified-Since",
        "Thu, 01 Jan 1970 00:00:00 GMT",
    );
    let older = read_response(&mut stream);
    assert_eq!(older.status, 200);
    assert_eq!(older.body, b"hello file");
    for garbage in ["not-a-date", "Sun, 06 Nov 1994 08:49:37 UTC", ""] {
        let request = format!(
            "GET /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nIf-Modified-Since: {garbage}\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .expect("write garbage date");
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200, "garbage {garbage:?}");
        assert_eq!(response.body, b"hello file", "garbage {garbage:?}");
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_conditional_future_date_answers_304() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_conditional(
        &mut stream,
        "/assets/hello.txt",
        "If-Modified-Since",
        "Tue, 06 Nov 2035 08:49:37 GMT",
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 304);
    assert!(response.body.is_empty());
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_conditional_head_matches_get_logic() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let first = read_response(&mut stream);
    let seen = last_modified(&first);
    let fresh = format!(
        "HEAD /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nIf-Modified-Since: {seen}\r\n\r\n"
    );
    stream
        .write_all(fresh.as_bytes())
        .expect("write HEAD fresh");
    let not_modified = read_head(&mut stream);
    assert!(
        not_modified.starts_with("HTTP/1.1 304 Not Modified\r\n"),
        "status: {not_modified}"
    );
    assert!(
        not_modified.contains("Last-Modified: "),
        "validator: {not_modified}"
    );
    let stale = "HEAD /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nIf-Modified-Since: Thu, 01 Jan 1970 00:00:00 GMT\r\n\r\n";
    stream
        .write_all(stale.as_bytes())
        .expect("write HEAD stale");
    let full = read_head(&mut stream);
    assert!(full.starts_with("HTTP/1.1 200 OK\r\n"), "status: {full}");
    assert!(full.contains("Content-Length: 10"), "length: {full}");
    assert!(full.contains("Last-Modified: "), "validator: {full}");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_if_none_match_ignored_serves_200() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    for etag in ["*", "\"abc\"", "W/\"abc\""] {
        get_conditional(&mut stream, "/assets/hello.txt", "If-None-Match", etag);
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200, "etag {etag}");
        assert_eq!(response.body, b"hello file", "etag {etag}");
    }
    get(&mut stream, "/assets/hello.txt");
    let first = read_response(&mut stream);
    let seen = last_modified(&first);
    let both = format!(
        "GET /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nIf-None-Match: *\r\nIf-Modified-Since: {seen}\r\n\r\n"
    );
    stream
        .write_all(both.as_bytes())
        .expect("write both validators");
    let not_modified = read_response(&mut stream);
    assert_eq!(
        not_modified.status, 304,
        "If-Modified-Since still drives 304"
    );
    assert!(not_modified.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_conditional_with_range_still_304() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let first = read_response(&mut stream);
    let seen = last_modified(&first);
    let request = format!(
        "GET /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nRange: bytes=0-3\r\nIf-Modified-Since: {seen}\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .expect("write ranged conditional");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 304, "precondition precedes Range");
    assert!(response.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

// NOTE: native-handler suite: handlers below are extern "C" to prove the C-ABI boundary.
// A panicking handler aborts the process (nounwind frame, uncatchable engine-side), so
// native_boom contains its own panic and reports failure — the contractual handler pattern
// — which the engine fails closed without a partial write.
#[allow(unsafe_code)]
unsafe extern "C" fn native_fixed(
    _user_data: *mut std::ffi::c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }

    let view = unsafe { &*request };
    let out = unsafe { &mut *response };
    if view.method_len == 0 || view.path_len == 0 {
        return 1;
    }
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = b"native-hello".as_ptr();
    out.body_len = 12;
    0
}

#[allow(unsafe_code)]
unsafe extern "C" fn native_stray_304(
    _user_data: *mut std::ffi::c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if response.is_null() {
        return 1;
    }

    let out = unsafe { &mut *response };
    out.status = 304;
    out.reason = b"Not Modified".as_ptr();
    out.reason_len = 12;
    out.body = b"stray-body".as_ptr();
    out.body_len = 10;
    0
}

#[allow(unsafe_code)]
unsafe extern "C" fn native_boom(
    _user_data: *mut std::ffi::c_void,
    _request: *const NativeRequestView,
    _response: *mut NativeResponseOut,
) -> i32 {
    let contained = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        panic!("native handler must not unwind across the boundary");
    }));
    i32::from(contained.is_ok())
}

static NATIVE_REENTER_REJECTED: AtomicBool = AtomicBool::new(false);

#[allow(unsafe_code)]
unsafe extern "C" fn native_reenter(
    _user_data: *mut std::ffi::c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut nested = Server::bind(addr, ServerConfig::default()).expect("nested bind");
    let rejected = nested
        .register_route(b"GET", b"/nested", 200, b"OK", b"x", MAX_STATIC_BODY_BYTES)
        .is_err();
    NATIVE_REENTER_REJECTED.store(rejected, Ordering::SeqCst);
    if response.is_null() {
        return 1;
    }

    let out = unsafe { &mut *response };
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = b"reentered".as_ptr();
    out.body_len = 9;
    0
}

static NATIVE_LARGE_BODY: [u8; MAX_RESPONSE_BODY] = [b'N'; MAX_RESPONSE_BODY];

// NOTE: echo/record statics below are per-handler (never shared across handlers), so parallel
// tests recording through different routes never observe each other. The echo buffer is static
// storage the engine copies before return; the default single-worker pump invokes handlers
// sequentially, so no concurrent invoke can realloc it mid-copy.
static NATIVE_ECHO_BODY: std::sync::Mutex<Vec<u8>> = std::sync::Mutex::new(Vec::new());
static NATIVE_RECORD_BODY_NULL: AtomicBool = AtomicBool::new(false);
static NATIVE_RECORD_HAS_BODY: AtomicBool = AtomicBool::new(false);
static NATIVE_RECORD_BODY_LEN: AtomicU64 = AtomicU64::new(0);

#[allow(unsafe_code)]
unsafe extern "C" fn native_echo(
    _user_data: *mut std::ffi::c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }

    let view = unsafe { &*request };
    let out = unsafe { &mut *response };
    let mut staged = NATIVE_ECHO_BODY.lock().expect("echo lock");
    staged.clear();
    if !view.body.is_null() && view.body_len > 0 {
        let bytes = unsafe { std::slice::from_raw_parts(view.body, view.body_len as usize) };
        staged.extend_from_slice(bytes);
    }
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = staged.as_ptr();
    out.body_len = staged.len();
    0
}

#[allow(unsafe_code)]
unsafe extern "C" fn native_record(
    _user_data: *mut std::ffi::c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }

    let view = unsafe { &*request };
    let out = unsafe { &mut *response };
    NATIVE_RECORD_BODY_NULL.store(view.body.is_null(), Ordering::SeqCst);
    NATIVE_RECORD_HAS_BODY.store(view.has_body != 0, Ordering::SeqCst);
    NATIVE_RECORD_BODY_LEN.store(view.body_len, Ordering::SeqCst);
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = b"recorded".as_ptr();
    out.body_len = 8;
    0
}
#[allow(unsafe_code)]
unsafe extern "C" fn native_large(
    _user_data: *mut std::ffi::c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if response.is_null() {
        return 1;
    }

    let out = unsafe { &mut *response };
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = NATIVE_LARGE_BODY.as_ptr();
    out.body_len = NATIVE_LARGE_BODY.len();
    0
}

fn native_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_native(b"GET", b"/fixed", Some(native_fixed), 0)
        .expect("register fixed");
    server
        .register_native(b"GET", b"/boom", Some(native_boom), 0)
        .expect("register boom");
    server
        .register_native(b"GET", b"/reenter", Some(native_reenter), 0)
        .expect("register reenter");
    server
        .register_native(b"POST", b"/large", Some(native_large), 0)
        .expect("register large");
    server
        .register_native(b"POST", b"/echo", Some(native_echo), 0)
        .expect("register echo");
    server
        .register_native(b"POST", b"/record", Some(native_record), 0)
        .expect("register record");
    server
        .register_native(b"GET", b"/stray", Some(native_stray_304), 0)
        .expect("register stray");
    server.start().expect("start server")
}

#[test]
fn native_304_drops_stray_body_and_keeps_alive() {
    // NOTE: H1 mirror of h2_native_304_drops_handler_body: a native 304 with a stray
    // body must shed it (no content-length, no DATA) and keep the connection alive.
    let server = native_server();
    let mut stream = connect(&server);
    get(&mut stream, "/stray");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 304);
    assert!(response.body.is_empty(), "body: {:?}", response.body);
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| name != "content-length"),
        "headers: {:?}",
        response.headers
    );
    get(&mut stream, "/fixed");
    let sibling = read_response(&mut stream);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"native-hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_fixed_bytes_served_with_keep_alive() {
    let server = native_server();
    let mut stream = connect(&server);
    get(&mut stream, "/fixed");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"native-hello");
    stream
        .write_all(b"HEAD /fixed HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write HEAD");
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
    assert!(text.contains("Content-Length: 12"), "length kept: {text}");
    get(&mut stream, "/fixed");
    let third = read_response(&mut stream);
    assert_eq!(third.status, 200);
    assert_eq!(third.body, b"native-hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_panic_fails_the_request_closed() {
    let server = native_server();
    let mut stream = connect(&server);
    get(&mut stream, "/boom");
    read_closed(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.response_overflow, 1, "counts: {counts:?}");
}

#[test]
fn native_reentrant_registration_rejected_while_outer_succeeds() {
    NATIVE_REENTER_REJECTED.store(false, Ordering::SeqCst);
    let server = native_server();
    let mut stream = connect(&server);
    get(&mut stream, "/reenter");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"reentered");
    assert!(
        NATIVE_REENTER_REJECTED.load(Ordering::SeqCst),
        "nested server call must fail while a handler runs"
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_large_body_within_caps_drains_then_reusable() {
    let server = native_server();
    let mut stream = connect(&server);
    let payload = vec![b'p'; 1024];
    let head = format!(
        "POST /large HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        payload.len()
    );
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(&payload).expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body.len(), MAX_RESPONSE_BODY);
    assert!(response.body.iter().all(|byte| *byte == b'N'));
    get(&mut stream, "/fixed");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"native-hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_echo_returns_staged_body_then_reusable() {
    let server = native_server();
    let mut stream = connect(&server);
    let request = "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 11\r\nConnection: keep-alive\r\n\r\nhello world";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello world");
    get(&mut stream, "/fixed");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"native-hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_zero_length_body_echoes_empty() {
    let server = native_server();
    let mut stream = connect(&server);
    let request = "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_partial_tail_invokes_framing_only_then_drains() {
    NATIVE_RECORD_BODY_NULL.store(false, Ordering::SeqCst);
    NATIVE_RECORD_HAS_BODY.store(false, Ordering::SeqCst);
    NATIVE_RECORD_BODY_LEN.store(0, Ordering::SeqCst);
    let server = native_server();
    let mut stream = connect(&server);
    let head = "POST /record HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\n";
    stream.write_all(head.as_bytes()).expect("write head");
    // NOTE: the pump invokes at head-parse time, so a withheld tail guarantees the handler runs
    // framing-only; the sleep only lets that turn happen before the body arrives.
    std::thread::sleep(Duration::from_millis(300));
    stream.write_all(b"hello").expect("write body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"recorded");
    assert!(
        NATIVE_RECORD_BODY_NULL.load(Ordering::SeqCst),
        "withheld tail must invoke with null body"
    );
    assert!(NATIVE_RECORD_HAS_BODY.load(Ordering::SeqCst));
    assert_eq!(NATIVE_RECORD_BODY_LEN.load(Ordering::SeqCst), 5);
    get(&mut stream, "/fixed");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"native-hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn native_registration_rejects_duplicates_and_null() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/dup",
            200,
            b"OK",
            b"static",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register static");
    let duplicate = server.register_native(b"GET", b"/dup", Some(native_fixed), 0);
    assert!(
        matches!(duplicate, Err(ServerError::DuplicateRoute)),
        "native over static must be Duplicate, got {duplicate:?}"
    );
    let null = server.register_native(b"GET", b"/null", None, 0);
    assert!(
        matches!(null, Err(ServerError::InvalidRoute)),
        "null handler must be Invalid, got {null:?}"
    );
    server
        .register_native(b"GET", b"/ok", Some(native_fixed), 0)
        .expect("register native");
    let back = server.register_route(b"GET", b"/ok", 200, b"OK", b"static", MAX_STATIC_BODY_BYTES);
    assert!(
        matches!(back, Err(ServerError::DuplicateRoute)),
        "static over native must be Duplicate, got {back:?}"
    );
}

static NATIVE_CTX_SENTINEL: usize = 0x5EED_C0DE;

#[allow(unsafe_code)]
unsafe extern "C" fn native_ctx(
    user_data: *mut std::ffi::c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if user_data as usize != NATIVE_CTX_SENTINEL || request.is_null() || response.is_null() {
        return 1;
    }

    let out = unsafe { &mut *response };
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = b"ctx-ok".as_ptr();
    out.body_len = 6;
    0
}

#[test]
fn native_user_data_passes_through_verbatim() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_native(b"GET", b"/ctx", Some(native_ctx), NATIVE_CTX_SENTINEL)
        .expect("register ctx");
    let server = server.start().expect("start server");
    let mut stream = connect(&server);
    get(&mut stream, "/ctx");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"ctx-ok");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

fn date_shape(response: &ClientResponse) -> String {
    let seen = header(response, "date").to_string();
    assert_eq!(seen.len(), 29, "IMF-fixdate width: {seen}");
    assert!(seen.ends_with(" GMT"), "GMT only: {seen}");
    assert_eq!(seen.as_bytes()[3], b',', "weekday comma: {seen}");
    let count = response
        .headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("date"))
        .count();
    assert_eq!(count, 1, "exactly one Date");
    seen
}

#[test]
fn h1_date_present_and_shaped_across_static_paths() {
    let server = test_server();
    let mut stream = connect(&server);
    get(&mut stream, "/hello");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    date_shape(&first);
    get(&mut stream, "/hello");
    let reuse = read_response(&mut stream);
    assert_eq!(reuse.status, 200);
    assert_eq!(reuse.body, b"hello");
    date_shape(&reuse);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");

    let missing = test_server();
    let mut stream = connect(&missing);
    get(&mut stream, "/missing");
    let not_found = read_response(&mut stream);
    assert_eq!(not_found.status, 404);
    assert_eq!(not_found.body, b"Not Found");
    date_shape(&not_found);
    read_eof(&mut stream);
    missing.stop_accepting();
    let _ = missing.join().expect("join server");

    let rejected = test_server();
    let mut stream = connect(&rejected);
    let head = format!(
        "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\n\r\n",
        MAX_STATIC_BODY_BYTES + 1024
    );
    stream.write_all(head.as_bytes()).expect("write head");
    let too_large = read_response(&mut stream);
    assert_eq!(too_large.status, 413);
    assert_eq!(too_large.body, b"Payload Too Large");
    date_shape(&too_large);
    read_closed(&mut stream);
    rejected.stop_accepting();
    let _ = rejected.join().expect("join server");

    let (files, _fixture) = file_server();
    let mut stream = connect(&files);
    get(&mut stream, "/assets/hello.txt");
    let file = read_response(&mut stream);
    assert_eq!(file.status, 200);
    assert_eq!(file.body, b"hello file");
    date_shape(&file);
    files.stop_accepting();
    let _ = files.join().expect("join server");

    let native = native_server();
    let mut stream = connect(&native);
    get(&mut stream, "/fixed");
    let served = read_response(&mut stream);
    assert_eq!(served.status, 200);
    assert_eq!(served.body, b"native-hello");
    date_shape(&served);
    native.stop_accepting();
    let _ = native.join().expect("join server");
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

fn connect_addr(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).expect("connect loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
}

fn gzip_body() -> Vec<u8> {
    b"The quick brown fox jumps over the lazy dog. ".repeat(50)
}

fn gzip_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_gzip_route(
            b"GET",
            b"/gzip",
            200,
            b"OK",
            &gzip_body(),
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register gzip");
    server.start().expect("start server")
}

fn get_with_encoding(stream: &mut TcpStream, path: &str, extra: &str) {
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n{extra}\r\n");
    stream.write_all(request.as_bytes()).expect("write request");
}

fn gunzip(body: &[u8]) -> Vec<u8> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(body);
    let mut plain = Vec::new();
    decoder.read_to_end(&mut plain).expect("gunzip body");
    plain
}

#[test]
fn gzip_static_body_round_trips_with_encoding_headers() {
    let server = gzip_server();
    let mut stream = connect(&server);
    get_with_encoding(&mut stream, "/gzip", "Accept-Encoding: gzip\r\n");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(header(&response, "content-encoding"), "gzip");
    assert_eq!(header(&response, "vary"), "Accept-Encoding");
    date_shape(&response);
    assert!(response.body.len() < gzip_body().len());
    assert_eq!(gunzip(&response.body), gzip_body());
    let length: usize = header(&response, "content-length")
        .parse()
        .expect("length int");
    assert_eq!(length, response.body.len());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn gzip_identity_fallback_without_accept_encoding() {
    let server = gzip_server();
    let mut stream = connect(&server);
    get_with_encoding(&mut stream, "/gzip", "");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, gzip_body());
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case("content-encoding")),
        "no content-encoding"
    );
    assert_eq!(header(&response, "vary"), "Accept-Encoding");
    date_shape(&response);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn gzip_q0_disables_twin() {
    let server = gzip_server();
    let mut stream = connect(&server);
    get_with_encoding(&mut stream, "/gzip", "Accept-Encoding: gzip;q=0\r\n");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, gzip_body());
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case("content-encoding")),
        "no content-encoding"
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn gzip_head_reports_compressed_length() {
    let server = gzip_server();
    let mut stream = connect(&server);
    let request = "HEAD /gzip HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write HEAD");
    let text = read_head(&mut stream);
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(text.contains("Content-Encoding: gzip"), "encoding: {text}");
    assert!(text.contains("Vary: Accept-Encoding"), "vary: {text}");
    let gzipped = {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&gzip_body()).expect("gzip expected");
        encoder.finish().expect("finish expected")
    };
    assert!(
        text.contains(&format!("Content-Length: {}", gzipped.len())),
        "length: {text}"
    );
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

static GZIP_NATIVE_BODY: OnceLock<Vec<u8>> = OnceLock::new();

fn gzip_native_body() -> &'static [u8] {
    GZIP_NATIVE_BODY.get_or_init(gzip_body)
}

#[allow(unsafe_code)]
unsafe extern "C" fn native_gzip(
    _user_data: *mut std::ffi::c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }
    let view = unsafe { &*request };
    let out = unsafe { &mut *response };
    if view.method_len == 0 || view.path_len == 0 {
        return 1;
    }
    let body = gzip_native_body();
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = body.as_ptr();
    out.body_len = body.len();
    0
}

fn gzip_native_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_native_gzip(b"GET", b"/gzip-native", Some(native_gzip), 0)
        .expect("register gzip native");
    server.start().expect("start server")
}

#[test]
fn gzip_native_route_round_trips_with_encoding_headers() {
    let server = gzip_native_server();
    let mut stream = connect(&server);
    get_with_encoding(&mut stream, "/gzip-native", "Accept-Encoding: gzip\r\n");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(header(&response, "content-encoding"), "gzip");
    assert_eq!(header(&response, "vary"), "Accept-Encoding");
    date_shape(&response);
    assert!(response.body.len() < gzip_body().len());
    assert_eq!(gunzip(&response.body), gzip_body());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn gzip_native_identity_fallback_without_accept_encoding() {
    let server = gzip_native_server();
    let mut stream = connect(&server);
    get_with_encoding(&mut stream, "/gzip-native", "");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, gzip_body());
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case("content-encoding")),
        "no content-encoding"
    );
    assert_eq!(header(&response, "vary"), "Accept-Encoding");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn options_asterisk_answers_server_capabilities() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "OPTIONS * HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write OPTIONS");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    let allow = header(&response, "allow");
    assert!(allow.contains("GET"), "allow: {allow}");
    assert!(allow.contains("POST"), "allow: {allow}");
    date_shape(&response);
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn options_asterisk_ignores_accept_encoding() {
    let server = gzip_server();
    let mut stream = connect(&server);
    let request = "OPTIONS * HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write OPTIONS");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case("content-encoding")),
        "no content-encoding"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}
