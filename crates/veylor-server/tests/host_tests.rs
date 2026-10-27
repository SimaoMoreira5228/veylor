use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use veylor_runtime::{ConnectionTimeouts, ConnectionToken};
use veylor_server::{
    CancelOutcome, EventSlot, HostEventKind, HostHandle, HostShared, MAX_STATIC_BODY_BYTES,
    PollReport, RespondOutcome, RunningServer, Server, ServerConfig, ServerError, TrailerOutcome,
    TrailerReport,
};

const DEADLINE: Duration = Duration::from_secs(5);
const PARKED_QUIET: Duration = Duration::from_millis(150);
const SPIN: Duration = Duration::from_secs(5);

fn managed_server(config: ServerConfig) -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_managed(b"GET", b"/work", MAX_STATIC_BODY_BYTES)
        .expect("register managed");
    server
        .register_managed_with_trailers(
            b"POST",
            b"/upload",
            MAX_STATIC_BODY_BYTES,
            &[b"x-checksum"],
        )
        .expect("register managed upload");
    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register static");
    server.start().expect("start server")
}

fn default_managed_server() -> RunningServer {
    managed_server(ServerConfig::default())
}

fn tight_managed_server(body_ns: u64, grace_ns: u64) -> RunningServer {
    managed_server(ServerConfig {
        timeouts: ConnectionTimeouts {
            request_head_ns: 5_000_000_000,
            body_progress_ns: body_ns,
            response_write_ns: 5_000_000_000,
            keep_alive_idle_ns: 30_000_000_000,
        },
        shutdown_grace_ns: grace_ns,
        ..ServerConfig::default()
    })
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

fn get(stream: &mut TcpStream, path: &str) {
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write request");
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
    let mut content_length = 0;
    let mut headers = Vec::new();
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

fn read_head(stream: &mut TcpStream) -> (u16, usize) {
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
    let mut content_length = 0;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').expect("header colon");
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().expect("content length int");
        }
    }
    (status, content_length)
}

fn read_exact_body(stream: &mut TcpStream, length: usize) -> Vec<u8> {
    let mut body = vec![0; length];
    stream.read_exact(&mut body).expect("read body");
    body
}

fn expect_eof(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        other => panic!("expected EOF, got {other:?}"),
    }
}

fn header<'a>(response: &'a ClientResponse, name: &str) -> &'a str {
    response
        .headers
        .iter()
        .find(|(field, _)| field.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
        .expect("header present")
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
    assert!(
        response
            .headers
            .first()
            .is_some_and(|(name, _)| name.eq_ignore_ascii_case("date")),
        "Date rides first"
    );
    seen
}

fn stream_head_exact(head: &[u8], status_line: &str) -> String {
    let prefix = format!("{status_line}\r\nDate: ");
    let rest = head
        .strip_prefix(prefix.as_bytes())
        .expect("status plus Date first");
    let value = rest.get(..29).expect("29-byte IMF-fixdate");
    let seen = std::str::from_utf8(value).expect("date utf8").to_string();
    assert_eq!(seen.len(), 29, "IMF-fixdate width: {seen}");
    assert!(seen.ends_with(" GMT"), "GMT only: {seen}");
    assert_eq!(seen.as_bytes()[3], b',', "weekday comma: {seen}");
    assert_eq!(
        &rest[29..],
        b"\r\nTransfer-Encoding: chunked\r\n\r\n",
        "framing exact past Date"
    );
    let text = std::str::from_utf8(head).expect("head utf8");
    let count = text
        .lines()
        .filter(|line| {
            line.get(..5)
                .is_some_and(|name| name.eq_ignore_ascii_case("date:"))
        })
        .count();
    assert_eq!(count, 1, "exactly one Date: {text}");
    seen
}

fn spin_until(mut ready: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + SPIN;
    while Instant::now() < deadline {
        if ready() {
            return true;
        }
        std::thread::yield_now();
    }
    ready()
}

fn is_closed(stream: &mut TcpStream) -> bool {
    match stream.read(&mut [0; 1]) {
        Ok(0) => true,
        Ok(_) => false,
        Err(error) => error.kind() != std::io::ErrorKind::WouldBlock,
    }
}

fn empty_slot() -> EventSlot {
    EventSlot {
        kind: HostEventKind::Shutdown,
        token: ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 0,
            generation: 0,
        },
        route_id: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    }
}

fn poll_batch(
    host: &HostHandle,
    timeout_ns: u64,
    slots: usize,
) -> (Vec<EventSlot>, Vec<u8>, Vec<u8>, PollReport) {
    let mut events = vec![empty_slot(); slots];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = host.poll(timeout_ns, &mut events, &mut methods, &mut paths);
    events.truncate(report.copied);
    (events, methods, paths, report)
}

fn poll_request(host: &HostHandle) -> (EventSlot, Vec<u8>, Vec<u8>) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let (events, methods, paths, _) = poll_batch(host, 100_000_000, 8);
        for event in events {
            if event.kind == HostEventKind::Request {
                let method =
                    methods[event.method_offset..event.method_offset + event.method_len].to_vec();
                let path = paths[event.path_offset..event.path_offset + event.path_len].to_vec();
                return (event, method, path);
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for request event"
        );
    }
}

#[test]
fn managed_park_poll_respond_keep_alive_reusable() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    stream
        .set_read_timeout(Some(PARKED_QUIET))
        .expect("quiet timeout");
    let mut byte = [0; 1];
    assert!(
        stream.read(&mut byte).is_err(),
        "parked request sends no bytes"
    );
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    let (event, method, path) = poll_request(&host);
    assert_eq!(event.kind, HostEventKind::Request);
    assert_eq!(method, b"GET");
    assert_eq!(path, b"/work");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"done");
    get(&mut stream, "/work");
    let (second_event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second_event.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"again");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
}

#[test]
fn managed_parked_http10_keep_alive_echoes_and_reuses() {
    // NOTE: drift alarm for the converged keep-alive verdict: an HTTP/1.0 request with
    // Connection: keep-alive must echo Connection: keep-alive on every H1 path (static,
    // parked fixed, streaming Begin). The static path emits the shared
    // connection_header_for_response verdict directly; the parked paths must carry that same
    // verdict, not a bool-erased Omit.
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"GET /hello HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write static 1.0");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    assert!(
        first
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("connection")
                && value.eq_ignore_ascii_case("keep-alive")),
        "static echoes keep-alive: {:?}",
        first.headers
    );
    stream
        .write_all(b"GET /work HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write parked 1.0");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"done");
    assert!(
        second
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("connection")
                && value.eq_ignore_ascii_case("keep-alive")),
        "parked fixed echoes keep-alive: {:?}",
        second.headers
    );
    stream
        .write_all(b"GET /work HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write reuse 1.0");
    let (third, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(third.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let again = read_response(&mut stream);
    assert_eq!(again.status, 200);
    assert_eq!(again.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_begin_http10_keep_alive_echoes() {
    // NOTE: same drift alarm as above for the streaming Begin arm: the head must carry the
    // shared keep-alive verdict so an HTTP/1.0 client reuses instead of closing.
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"GET /work HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write stream 1.0");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let head = read_raw_head(&mut stream);
    let text = String::from_utf8(head).expect("head utf8");
    assert!(
        text.to_ascii_lowercase().contains("connection: keep-alive"),
        "stream Begin echoes keep-alive: {text}"
    );
    let mut terminal = [0; 5];
    stream.read_exact(&mut terminal).expect("terminal");
    assert_eq!(&terminal, b"0\r\n\r\n");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_date_present_and_shaped_one_shot_stream_and_reuse() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"done");
    date_shape(&first);
    get(&mut stream, "/work");
    let (missing, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(missing.token, 404, b"Not Found", b"nope"),
        RespondOutcome::Ok
    );
    let not_found = read_response(&mut stream);
    assert_eq!(not_found.status, 404);
    assert_eq!(not_found.body, b"nope");
    date_shape(&not_found);
    get(&mut stream, "/work");
    let (begun, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(begun.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, begun.token);
    assert_eq!(host.end_stream(begun.token), RespondOutcome::Ok);
    let head = read_raw_head(&mut stream);
    stream_head_exact(&head, "HTTP/1.1 200 OK");
    let mut terminal = [0; 5];
    stream.read_exact(&mut terminal).expect("terminal");
    assert_eq!(&terminal, b"0\r\n\r\n", "bare end carries no trailer block");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_cancel_closes_without_response() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(host.cancel(event.token), CancelOutcome::Ok);
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Err(_) => {}
        Ok(_) => panic!("cancel must send no response bytes"),
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.cancelled, 1, "counts: {counts:?}");
}

#[test]
fn managed_duplicate_respond_and_stale_cancel_fail_closed() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (first, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(first.token, 200, b"OK", b"one"),
        RespondOutcome::Ok
    );
    assert_eq!(
        host.respond(first.token, 200, b"OK", b"one"),
        RespondOutcome::Duplicate
    );
    assert_eq!(host.cancel(first.token), CancelOutcome::Stale);
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"one");
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(first.token, 200, b"OK", b"hijack"),
        RespondOutcome::Duplicate
    );
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"two"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"two");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_poll_reports_required_capacities() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let mut events = vec![empty_slot(); 1];
    let mut paths = vec![0; 4096];
    let report = host.poll(2_000_000_000, &mut events, &mut [], &mut paths);
    assert_eq!(report.copied, 0);
    assert_eq!(report.method_required, 3);
    let (event, method, path) = poll_request(&host);
    assert_eq!(method, b"GET");
    assert_eq!(path, b"/work");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.body, b"done");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_parked_timeout_releases_slot() {
    let server = tight_managed_server(100_000_000, 5_000_000_000);
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let mut byte = [0xFF; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        other => panic!("expected EOF after parked timeout, got {other:?}"),
    }
    assert!(spin_until(|| host.parked_count() == 0), "park expires");
    let mut again = connect(&server);
    get(&mut again, "/work");
    let (stale, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(stale.token, 200, b"OK", b"late"),
        RespondOutcome::Gone
    );
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"reused"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut again);
    assert_eq!(response.body, b"reused");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.parked_timeout, 1, "counts: {counts:?}");
}

#[test]
fn managed_queue_full_fails_closed_server_healthy() {
    let config = ServerConfig {
        shutdown_grace_ns: 300_000_000,
        ..ServerConfig::default()
    };
    let server = managed_server(config);
    let host = server.host();
    let mut clients = Vec::new();
    for _ in 0..65 {
        let mut stream = connect(&server);
        get(&mut stream, "/work");
        stream.set_nonblocking(true).expect("nonblocking");
        clients.push(stream);
    }
    let mut parked = 0;
    // NOTE: no polling until the queue saturates: draining events early would free room
    // and let the 65th head park, so the test waits for the fail-closed attempt first.
    assert!(
        spin_until(|| host.counters().queue_full == 1),
        "one park attempt fails closed while full"
    );
    let deadline = Instant::now() + DEADLINE;
    while parked < 64 && Instant::now() < deadline {
        let (events, _, _, _) = poll_batch(&host, 100_000_000, 8);
        parked += events
            .iter()
            .filter(|event| event.kind == HostEventKind::Request)
            .count();
        std::thread::yield_now();
    }
    assert_eq!(parked, 64, "queue holds exactly one depth");
    let (_, _, _, empty) = poll_batch(&host, 100_000_000, 8);
    assert_eq!(empty.copied, 0, "no 65th event escapes the full queue");
    let closed = spin_until(|| {
        let mut closed = 0;
        for stream in clients.iter_mut() {
            if is_closed(stream) {
                closed += 1;
            }
        }
        closed == 1 && host.counters().queue_full == 1
    });
    assert!(closed, "one connection fails closed with queue-full");
    let mut closed_count = 0;
    let mut pending_count = 0;
    for stream in clients.iter_mut() {
        match stream.read(&mut [0; 1]) {
            Ok(0) => closed_count += 1,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => pending_count += 1,
            Err(_) => closed_count += 1,
            Ok(_) => panic!("parked connection must send no bytes"),
        }
    }
    assert_eq!(closed_count, 1, "exactly one connection closed");
    assert_eq!(pending_count, 64, "the rest stay parked with no bytes");
    let mut healthy = connect(&server);
    get(&mut healthy, "/hello");
    let response = read_response(&mut healthy);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.queue_full, 1, "counts: {counts:?}");
}

#[test]
fn managed_shutdown_emits_one_event_per_worker() {
    let config = ServerConfig {
        worker_count: 2,
        shutdown_grace_ns: 300_000_000,
        ..ServerConfig::default()
    };
    let server = managed_server(config);
    let host = server.host();
    server.stop_accepting();
    let mut shutdowns = 0;
    let deadline = Instant::now() + DEADLINE;
    while shutdowns < 2 && Instant::now() < deadline {
        let (events, _, _, _) = poll_batch(&host, 100_000_000, 8);
        shutdowns += events
            .iter()
            .filter(|event| event.kind == HostEventKind::Shutdown)
            .count();
    }
    assert_eq!(shutdowns, 2, "one shutdown event per worker");
    let _ = server.join().expect("join server");
}

#[test]
fn managed_respond_after_disconnect_is_gone() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    drop(stream);
    assert!(
        spin_until(|| host.parked_count() == 0),
        "peer close unparks"
    );
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone
    );
    assert_eq!(host.counters().responds_gone, 1);
    let mut again = connect(&server);
    get(&mut again, "/work");
    let (fresh, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(fresh.token, 200, b"OK", b"live"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut again);
    assert_eq!(response.body, b"live");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_expect_pulls_offsets_and_reuses() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 11\r\nExpect: 100-continue\r\nConnection: keep-alive\r\n\r\n")
        .expect("write head");
    let (interim, _) = read_head(&mut stream);
    assert_eq!(
        interim, 100,
        "admitted body emits interim before body bytes flow"
    );
    let (event, method, path) = poll_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    stream.write_all(b"hello").expect("write first chunk");
    let mut collected = Vec::new();
    let deadline = Instant::now() + DEADLINE;
    while collected.len() < 5 {
        assert!(Instant::now() < deadline, "first body chunk stalls");
        let mut buf = [0; 16];
        let (outcome, n, complete) = host.pull_body(event.token, &mut buf);
        assert_eq!(outcome, RespondOutcome::Ok);
        assert!(!complete, "end-of-body before declared length");
        collected.extend_from_slice(&buf[..n]);
        if n == 0 {
            std::thread::yield_now();
        }
    }
    assert_eq!(collected, b"hello");
    stream.write_all(b" world").expect("write second chunk");
    let deadline = Instant::now() + DEADLINE;
    while collected.len() < 11 {
        assert!(Instant::now() < deadline, "second body chunk stalls");
        let mut buf = [0; 16];
        let (outcome, n, complete) = host.pull_body(event.token, &mut buf);
        assert_eq!(outcome, RespondOutcome::Ok);
        collected.extend_from_slice(&buf[..n]);
        assert_eq!(
            complete,
            collected.len() == 11,
            "completion tracks pulled offsets"
        );
        if n == 0 {
            std::thread::yield_now();
        }
    }
    assert_eq!(collected, b"hello world");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_over_cap_rejects_without_interim_or_read() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    let request = format!(
        "POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nExpect: 100-continue\r\nConnection: keep-alive\r\n\r\n",
        MAX_STATIC_BODY_BYTES + 1
    );
    stream.write_all(request.as_bytes()).expect("write head");
    let (status, length) = read_head(&mut stream);
    assert_eq!(status, 413, "over-cap sends no interim and parks nothing");
    assert_eq!(read_exact_body(&mut stream, length), b"Payload Too Large");
    let (_, _, _, report) = poll_batch(&host, 0, 8);
    assert_eq!(report.copied, 0, "no host event for rejected bodies");
    expect_eof(&mut stream);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_chunked_split_size_line_assembles_and_reuses() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\nConnection: keep-alive\r\n\r\n")
        .expect("write head");
    let (interim, _) = read_head(&mut stream);
    assert_eq!(
        interim, 100,
        "admitted chunked body emits interim before framing bytes flow"
    );
    let (event, method, path) = poll_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    stream.write_all(b"5\r").expect("split chunk-size line");
    stream
        .write_all(b"\nhello\r\n")
        .expect("size line rest and data");
    stream
        .write_all(b"6;ext=v\r\n world\r\n")
        .expect("chunk with extension");
    stream.write_all(b"0\r\n\r\n").expect("terminal chunk");
    let mut collected = Vec::new();
    let mut complete = false;
    let deadline = Instant::now() + DEADLINE;
    while !complete {
        assert!(Instant::now() < deadline, "chunked body stalls");
        let mut buf = [0; 16];
        let (outcome, n, done) = host.pull_body(event.token, &mut buf);
        assert_eq!(outcome, RespondOutcome::Ok);
        collected.extend_from_slice(&buf[..n]);
        complete = done;
        if n == 0 && !done {
            std::thread::yield_now();
        }
    }
    assert_eq!(collected, b"hello world");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_chunked_bad_size_closes() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\nZZ\r\nnope\r\n0\r\n\r\n")
        .expect("write malformed chunks");
    let (event, _, _) = poll_request(&host);
    expect_eof(&mut stream);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone,
        "malformed framing fails the connection closed"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_chunked_disallowed_trailer_closes() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n5\r\nhello\r\n0\r\nX-Note: hi\r\n\r\n")
        .expect("write trailer chunks");
    let (event, _, _) = poll_request(&host);
    expect_eof(&mut stream);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone,
        "empty route allowlist rejects every trailer"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_chunked_over_cap_closes_mid_stream() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\nConnection: keep-alive\r\n\r\n")
        .expect("write head");
    let (interim, _) = read_head(&mut stream);
    assert_eq!(
        interim, 100,
        "chunked admission sends interim even without a declared length"
    );
    let (event, _, _) = poll_request(&host);
    let chunk = [b'a'; 1024];
    for _ in 0..65 {
        stream.write_all(b"400\r\n").expect("chunk size");
        stream.write_all(&chunk).expect("chunk data");
        stream.write_all(b"\r\n").expect("chunk end");
    }
    expect_eof(&mut stream);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone,
        "over-cap aggregate closes mid-stream instead of 413"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_cancel_mid_body_closes_sibling_unaffected() {
    let server = default_managed_server();
    let host = server.host();
    let mut first = connect(&server);
    first
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nabc")
        .expect("write partial body");
    let (upload, method, path) = poll_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    let deadline = Instant::now() + DEADLINE;
    let mut staged = Vec::new();
    while staged.len() < 3 {
        assert!(Instant::now() < deadline, "staged body stalls");
        let mut scratch = [0; 16];
        let (outcome, n, complete) = host.pull_body(upload.token, &mut scratch);
        assert_eq!(outcome, RespondOutcome::Ok);
        assert!(!complete);
        staged.extend_from_slice(&scratch[..n]);
        if n == 0 {
            std::thread::yield_now();
        }
    }
    assert_eq!(staged, b"abc");
    let mut second = connect(&server);
    get(&mut second, "/work");
    let (work, _, _) = poll_request(&host);
    assert_eq!(host.cancel(upload.token), CancelOutcome::Ok);
    expect_eof(&mut first);
    assert!(
        spin_until(|| host.parked_count() == 1),
        "cancel unparks exactly one"
    );
    let mut scratch = [0; 16];
    assert_eq!(
        host.pull_body(upload.token, &mut scratch).0,
        RespondOutcome::Stale,
        "cancelled body tomb reports stale"
    );
    assert_eq!(
        host.respond(work.token, 200, b"OK", b"live"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut second);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"live");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.cancelled, 1, "counts: {counts:?}");
}

#[test]
fn managed_body_unread_respond_drains_within_cap() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nabc")
        .expect("write partial body");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"ok"),
        RespondOutcome::Ok
    );
    stream.write_all(b"defghij").expect("write rest of body");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"ok");
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn zero_cap_route_rejects_chunked_without_park_or_interim() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut builder = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    builder
        .register_managed(b"POST", b"/void", 0)
        .expect("register zero-cap");
    let server = builder.start().expect("start server");
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /void HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\nConnection: keep-alive\r\n\r\n")
        .expect("write head");
    let (first_status, first_len) = read_head(&mut stream);
    assert_eq!(
        first_status, 413,
        "zero-cap chunked rejects at admission with no interim"
    );
    let rejected = read_exact_body(&mut stream, first_len);
    assert_eq!(rejected, b"Payload Too Large");
    expect_eof(&mut stream);
    assert_eq!(host.parked_count(), 0, "rejected chunked never parks");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

fn pull_trailer_set(
    host: &HostHandle,
    token: ConnectionToken,
) -> (TrailerReport, Vec<u8>, Vec<u8>) {
    let mut names = vec![0; 256];
    let mut values = vec![0; 256];
    let report = host.pull_trailers(token, &mut names, &mut values);
    names.truncate(report.names_copied);
    values.truncate(report.values_copied);
    (report, names, values)
}

fn pull_body_complete(host: &HostHandle, token: ConnectionToken) -> Vec<u8> {
    let mut collected = Vec::new();
    let mut complete = false;
    let deadline = Instant::now() + DEADLINE;
    while !complete {
        assert!(Instant::now() < deadline, "chunked body stalls");
        let mut buf = [0; 16];
        let (outcome, n, done) = host.pull_body(token, &mut buf);
        assert_eq!(outcome, RespondOutcome::Ok);
        collected.extend_from_slice(&buf[..n]);
        complete = done;
        if n == 0 && !done {
            std::thread::yield_now();
        }
    }
    collected
}

#[test]
fn managed_trailers_listed_stage_and_copy_exact() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n5\r\nhello\r\n0\r\nX-Checksum: abc123\r\n\r\n")
        .expect("write chunks with listed trailer");
    let (event, _, _) = poll_request(&host);
    assert_eq!(pull_body_complete(&host, event.token), b"hello");
    let mut tiny_names = [0; 4];
    let mut tiny_values = [0; 64];
    let short = host.pull_trailers(event.token, &mut tiny_names, &mut tiny_values);
    assert_eq!(short.outcome, TrailerOutcome::Ok);
    assert_eq!(short.fields, 1);
    assert_eq!(short.names_copied, 0);
    assert_eq!(short.values_copied, 0);
    assert_eq!(short.names_required, 10);
    assert_eq!(short.values_required, 6);
    let (report, names, values) = pull_trailer_set(&host, event.token);
    assert_eq!(report.outcome, TrailerOutcome::Ok);
    assert_eq!(report.fields, 1);
    assert_eq!(report.names_copied, 10);
    assert_eq!(report.values_copied, 6);
    assert_eq!(names, b"X-Checksum");
    assert_eq!(values, b"abc123");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_trailers_pull_before_complete_not_ready() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n5\r\nhello\r\n")
        .expect("write data chunk without terminal");
    let (event, _, _) = poll_request(&host);
    let mut names = [0; 64];
    let mut values = [0; 64];
    assert_eq!(
        host.pull_trailers(event.token, &mut names, &mut values)
            .outcome,
        TrailerOutcome::NotReady
    );
    stream
        .write_all(b"0\r\n\r\n")
        .expect("terminal chunk without trailers");
    assert_eq!(pull_body_complete(&host, event.token), b"hello");
    let (report, names, values) = pull_trailer_set(&host, event.token);
    assert_eq!(report.outcome, TrailerOutcome::Ok);
    assert_eq!(report.fields, 0);
    assert!(names.is_empty() && values.is_empty());
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.body, b"done");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_trailers_over_count_closes() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    let mut request = b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n3\r\nabc\r\n0\r\n".to_vec();
    for index in 0..9 {
        request.extend_from_slice(format!("X-Checksum: v{index}\r\n").as_bytes());
    }
    request.extend_from_slice(b"\r\n");
    stream
        .write_all(&request)
        .expect("write nine listed trailers");
    let (event, _, _) = poll_request(&host);
    expect_eof(&mut stream);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone,
        "ninth staged field breaches slot capacity and fails closed"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_trailers_over_bytes_close() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    let mut request = b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n3\r\nabc\r\n0\r\nX-Checksum: ".to_vec();
    request.extend_from_slice(&[b'a'; 2048]);
    request.extend_from_slice(b"\r\n\r\n");
    stream
        .write_all(&request)
        .expect("write oversized trailer value");
    let (event, _, _) = poll_request(&host);
    expect_eof(&mut stream);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone,
        "staged trailer bytes past 1 KiB fail closed"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_trailers_registration_rejects_bad_allowlist() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    let overlong: Vec<Vec<u8>> = (0..33)
        .map(|index| format!("x-t{index:02}").into_bytes())
        .collect();
    let refs: Vec<&[u8]> = overlong.iter().map(Vec::as_slice).collect();
    assert!(
        matches!(
            server.register_managed_with_trailers(b"POST", b"/t", MAX_STATIC_BODY_BYTES, &refs),
            Err(ServerError::InvalidRoute)
        ),
        "allowlist past 32 names rejects at registration"
    );
    assert!(
        matches!(
            server.register_managed_with_trailers(
                b"POST",
                b"/t",
                MAX_STATIC_BODY_BYTES,
                &[b"bad name"]
            ),
            Err(ServerError::InvalidRoute)
        ),
        "non-token trailer name rejects at registration"
    );
    assert!(
        matches!(
            server.register_managed_with_trailers(
                b"POST",
                b"/t",
                MAX_STATIC_BODY_BYTES,
                &[b"x-a", b"X-A"]
            ),
            Err(ServerError::InvalidRoute)
        ),
        "duplicate trailer name rejects at registration"
    );
    server
        .register_managed_with_trailers(b"POST", b"/t", MAX_STATIC_BODY_BYTES, &[b"x-a"])
        .expect("valid allowlist registers");
}

fn wait_writable(host: &HostHandle, token: ConnectionToken) {
    assert!(
        spin_until(|| host.is_writable(token)),
        "stream never armed writable"
    );
}

fn read_raw_head(stream: &mut TcpStream) -> Vec<u8> {
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
    head
}

fn read_chunk_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut line = Vec::new();
    let mut byte = [0; 1];
    while !line.ends_with(b"\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => panic!("eof mid-chunk-size"),
            Ok(_) => line.push(byte[0]),
            Err(error) => panic!("read chunk size: {error:?}"),
        }
    }
    let size = usize::from_str_radix(
        std::str::from_utf8(&line[..line.len() - 2])
            .expect("size utf8")
            .trim(),
        16,
    )
    .expect("size hex");
    if size == 0 {
        let mut crlf = [0; 2];
        stream.read_exact(&mut crlf).expect("final crlf");
        assert_eq!(&crlf, b"\r\n");
        return None;
    }
    let mut chunk = vec![0; size];
    stream.read_exact(&mut chunk).expect("chunk bytes");
    let mut crlf = [0; 2];
    stream.read_exact(&mut crlf).expect("chunk crlf");
    assert_eq!(&crlf, b"\r\n");
    Some(chunk)
}

fn read_chunked(stream: &mut TcpStream) -> (u16, Vec<u8>, Vec<u8>) {
    let head = read_raw_head(stream);
    let text = String::from_utf8(head.clone()).expect("head utf8");
    let status = text
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status int");
    assert!(
        text.contains("Transfer-Encoding: chunked"),
        "stream head must be chunked"
    );
    let mut raw = head;
    let mut body = Vec::new();
    loop {
        let mut line = Vec::new();
        let mut byte = [0; 1];
        while !line.ends_with(b"\r\n") {
            match stream.read(&mut byte) {
                Ok(0) => panic!("eof mid-chunk-size"),
                Ok(_) => line.push(byte[0]),
                Err(error) => panic!("read chunk size: {error:?}"),
            }
        }
        raw.extend_from_slice(&line);
        let size = usize::from_str_radix(
            std::str::from_utf8(&line[..line.len() - 2]).expect("size utf8"),
            16,
        )
        .expect("size hex");
        if size == 0 {
            let mut crlf = [0; 2];
            stream.read_exact(&mut crlf).expect("final crlf");
            raw.extend_from_slice(&crlf);
            break;
        }
        let mut chunk = vec![0; size];
        stream.read_exact(&mut chunk).expect("chunk bytes");
        raw.extend_from_slice(&chunk);
        let mut crlf = [0; 2];
        stream.read_exact(&mut crlf).expect("chunk crlf");
        raw.extend_from_slice(&crlf);
        body.extend_from_slice(&chunk);
    }
    (status, raw, body)
}

#[test]
fn managed_stream_begin_writes_end_exact_and_reuses() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(
        host.write_stream(event.token, b"hello "),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"world"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (status, raw, body) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello world");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("head end")
        + 4;
    stream_head_exact(&raw[..split], "HTTP/1.1 200 OK");
    assert_eq!(&raw[split..], b"6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n");
    assert_eq!(
        host.end_stream(event.token),
        RespondOutcome::Gone,
        "post-end ops observe Gone after unpark"
    );
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_write_without_writable_rejected_then_admitted() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.write_stream(event.token, &[]),
        RespondOutcome::Invalid,
        "empty chunk rejects"
    );
    assert_eq!(
        host.write_stream(event.token, &[b'x'; 16_384 + 1]),
        RespondOutcome::BodyTooLarge,
        "chunk past MAX_STREAM_WRITE rejects"
    );
    assert_eq!(
        host.write_stream(event.token, &[b'x'; 9000]),
        RespondOutcome::BodyTooLarge,
        "chunk whose frame cannot fit the output rejects at stage"
    );
    assert_eq!(
        host.write_stream(event.token, b"early"),
        RespondOutcome::NotWritable,
        "write with no stream rejects"
    );
    let unknown = ConnectionToken {
        request_seq: 9999,
        ..event.token
    };
    assert!(!host.is_writable(unknown), "unknown token never writable");
    assert_eq!(
        host.write_stream(unknown, b"early"),
        RespondOutcome::NotWritable,
        "write with no token rejects"
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"late"), RespondOutcome::Ok);
    // NOTE: the queued window is transient by design (the worker applies within microseconds),
    // so the test loops until the queue is observed: every iteration is either an admission
    // (a full worker cycle completed first) or the expected rejection, and the admitted bytes
    // are tracked so the wire body stays exact. A wedged worker breaks the loop immediately
    // instead of hanging the suite.
    let mut agains = 0u32;
    loop {
        match host.write_stream(event.token, b"again") {
            RespondOutcome::Ok => agains += 1,
            RespondOutcome::WouldBlock | RespondOutcome::NotWritable => break,
            other => panic!("unexpected write outcome: {other:?}"),
        }
        assert!(agains < 1000, "worker outran the stage loop");
    }
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"!"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (_, _, body) = read_chunked(&mut stream);
    let mut expected = b"late".to_vec();
    for _ in 0..agains {
        expected.extend_from_slice(b"again");
    }
    expected.extend_from_slice(b"!");
    assert_eq!(body, expected);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_end_twice_fails_closed() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    // NOTE: the queued end applies within microseconds, so a single second end races between
    // Duplicate (queued) and Gone (applied): loop until the apply is observed. Every iteration
    // is one of the two legal outcomes, so the test cannot flake either way.
    let mut spins = 0u32;
    loop {
        match host.end_stream(event.token) {
            RespondOutcome::Duplicate => spins += 1,
            RespondOutcome::Gone => break,
            other => panic!("unexpected end outcome: {other:?}"),
        }
        assert!(spins < 1_000_000, "staged end never applied");
    }
    let (status, raw, body) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    assert!(body.is_empty());
    assert!(
        raw.ends_with(b"0\r\n\r\n"),
        "stream ends with terminal chunk"
    );
    assert_eq!(host.end_stream(event.token), RespondOutcome::Gone);
    assert_eq!(
        host.write_stream(event.token, b"late"),
        RespondOutcome::Gone
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Gone
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_cancel_mid_stream_closes() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(
        host.write_stream(event.token, b"partial"),
        RespondOutcome::Ok
    );
    let head = read_raw_head(&mut stream);
    let text = String::from_utf8(head).expect("head utf8");
    assert!(
        text.starts_with("HTTP/1.1 200 OK"),
        "stream head sent: {text}"
    );
    assert_eq!(
        read_chunk_frame(&mut stream),
        Some(b"partial".to_vec()),
        "first chunk flushes before cancel"
    );
    assert_eq!(host.cancel(event.token), CancelOutcome::Ok);
    expect_eof(&mut stream);
    assert_eq!(
        host.write_stream(event.token, b"late"),
        RespondOutcome::Gone,
        "cancelled stream tombs Gone"
    );
    assert_eq!(host.end_stream(event.token), RespondOutcome::Gone);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.cancelled, 1, "counts: {counts:?}");
}

#[test]
fn managed_stream_begin_no_body_status_rejected() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 204, b"No Content"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.begin_stream(event.token, 304, b"Not Modified"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.begin_stream(event.token, 100, b"Continue"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"bad\r\nreason"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"fallback"),
        RespondOutcome::Ok,
        "rejected begins stage nothing"
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"fallback");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

fn read_trailer_block(stream: &mut TcpStream) -> Vec<u8> {
    let mut block = Vec::new();
    let mut byte = [0; 1];
    while !block.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => panic!("eof mid-trailers"),
            Ok(_) => block.push(byte[0]),
            Err(error) => panic!("read trailers: {error:?}"),
        }
        assert!(block.len() < 65536, "trailer overflow");
    }
    block
}

#[test]
fn managed_stream_end_with_trailers_exact_and_reuses() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"foo"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    let fields: &[(&[u8], &[u8])] = &[(b"x-sum", b"ok"), (b"x-n", b"7")];
    assert_eq!(
        host.end_stream_with_trailers(event.token, fields),
        RespondOutcome::Ok
    );
    let head = read_raw_head(&mut stream);
    stream_head_exact(&head, "HTTP/1.1 200 OK");
    assert_eq!(read_chunk_frame(&mut stream), Some(b"foo".to_vec()));
    let mut size = [0; 3];
    stream.read_exact(&mut size).expect("terminal size");
    assert_eq!(&size, b"0\r\n");
    assert_eq!(
        read_trailer_block(&mut stream),
        b"x-sum: ok\r\nx-n: 7\r\n\r\n",
        "terminal chunk carries the trailer block"
    );
    assert_eq!(
        host.end_stream(event.token),
        RespondOutcome::Gone,
        "applied terminal unparks"
    );
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_end_with_trailers_invalid_stages_nothing() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    let bad_names: &[&[u8]] = &[
        b"authorization",
        b"content-encoding",
        b"content-length",
        b"content-range",
        b"content-type",
        b"transfer-encoding",
        b"proxy-authorization",
        b"proxy-connection",
        b"te",
        b"trailer",
        b"expect",
        b"host",
        b"connection",
        b"upgrade",
        b"keep-alive",
        b"set-cookie",
        b"",
        b":pseudo",
        b"bad name",
    ];
    for name in bad_names {
        let fields: &[(&[u8], &[u8])] = &[(*name, b"v")];
        assert_eq!(
            host.end_stream_with_trailers(event.token, fields),
            RespondOutcome::Invalid,
            "prohibited trailer name rejects"
        );
        assert!(
            host.is_writable(event.token),
            "rejected trailers stage nothing"
        );
    }
    let bad_value: &[(&[u8], &[u8])] = &[(b"x-ok", b"a\r\nb")];
    assert_eq!(
        host.end_stream_with_trailers(event.token, bad_value),
        RespondOutcome::Invalid,
        "CRLF value rejects"
    );
    assert!(host.is_writable(event.token));
    let many: &[(&[u8], &[u8])] = &[
        (b"x-0", b"v"),
        (b"x-1", b"v"),
        (b"x-2", b"v"),
        (b"x-3", b"v"),
        (b"x-4", b"v"),
        (b"x-5", b"v"),
        (b"x-6", b"v"),
        (b"x-7", b"v"),
        (b"x-8", b"v"),
    ];
    assert_eq!(
        host.end_stream_with_trailers(event.token, many),
        RespondOutcome::Invalid,
        "ninth field rejects"
    );
    assert!(host.is_writable(event.token));
    let big = vec![b'v'; 1025];
    let big_field: &[(&[u8], &[u8])] = &[(b"x-big", &big)];
    assert_eq!(
        host.end_stream_with_trailers(event.token, big_field),
        RespondOutcome::Invalid,
        "past-1KiB staged bytes reject"
    );
    assert!(host.is_writable(event.token));
    assert_eq!(
        host.write_stream(event.token, b"recovered"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (status, raw, body) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"recovered");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("head end")
        + 4;
    stream_head_exact(&raw[..split], "HTTP/1.1 200 OK");
    assert_eq!(
        &raw[split..],
        b"9\r\nrecovered\r\n0\r\n\r\n",
        "bare end unchanged after rejected trailers"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_write_after_trailers_fails_closed() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"a"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    let fields: &[(&[u8], &[u8])] = &[(b"x-done", b"yes")];
    assert_eq!(
        host.end_stream_with_trailers(event.token, fields),
        RespondOutcome::Ok
    );
    // NOTE: the staged terminal races the apply like a bare end: WouldBlock while queued, Gone
    // after; a repeat terminal is Duplicate while queued. Every iteration is legal, so neither
    // loop can flake, and no write is ever admitted past the terminal.
    let mut spins = 0u32;
    loop {
        match host.write_stream(event.token, b"late") {
            RespondOutcome::WouldBlock | RespondOutcome::NotWritable => spins += 1,
            RespondOutcome::Gone => break,
            other => panic!("unexpected write outcome: {other:?}"),
        }
        assert!(spins < 1_000_000, "staged terminal never applied");
    }
    loop {
        match host.end_stream_with_trailers(event.token, fields) {
            RespondOutcome::Duplicate => spins += 1,
            RespondOutcome::Gone => break,
            other => panic!("unexpected terminal outcome: {other:?}"),
        }
        assert!(spins < 1_000_000, "staged terminal never applied");
    }
    let head = read_raw_head(&mut stream);
    stream_head_exact(&head, "HTTP/1.1 200 OK");
    assert_eq!(read_chunk_frame(&mut stream), Some(b"a".to_vec()));
    let mut size = [0; 3];
    stream.read_exact(&mut size).expect("terminal size");
    assert_eq!(&size, b"0\r\n");
    assert_eq!(
        read_trailer_block(&mut stream),
        b"x-done: yes\r\n\r\n",
        "refused writes leave the trailer block exact"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_end_then_trailers_fails_closed() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let fields: &[(&[u8], &[u8])] = &[(b"x-late", b"1")];
    let mut spins = 0u32;
    loop {
        match host.end_stream_with_trailers(event.token, fields) {
            RespondOutcome::Duplicate => spins += 1,
            RespondOutcome::Gone => break,
            other => panic!("unexpected terminal outcome: {other:?}"),
        }
        assert!(spins < 1_000_000, "staged end never applied");
    }
    let head = read_raw_head(&mut stream);
    stream_head_exact(&head, "HTTP/1.1 200 OK");
    let mut terminal = [0; 5];
    stream.read_exact(&mut terminal).expect("terminal");
    assert_eq!(&terminal, b"0\r\n\r\n", "bare end carries no trailer block");
    assert_eq!(host.end_stream(event.token), RespondOutcome::Gone);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_body_unread_respond_repeated_sequential() {
    // NOTE: regression test for the park-time race where a host respond staged between
    // try_park and the buffered-body push killed the connection with TransportError instead
    // of draining (~3% per exchange, ~60% per 50-exchange run pre-fix). Repeating the
    // respond-then-write exchange exercises both orders across runs; every order must reuse.
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    for _ in 0..25 {
        stream
            .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 10\r\nConnection: keep-alive\r\n\r\nabc")
            .expect("write head and partial body");
        let (event, _, _) = poll_request(&host);
        assert_eq!(
            host.respond(event.token, 200, b"OK", b"ok"),
            RespondOutcome::Ok
        );
        stream.write_all(b"defghij").expect("write rest of body");
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"ok");
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

fn unit_token(slot: u32) -> ConnectionToken {
    ConnectionToken {
        worker_id: 0,
        slot,
        request_seq: 0,
        stream_id: 0,
        worker_generation: 1,
        generation: 1,
    }
}

fn unit_park(shared: &HostShared, slot: u32, total: u64) -> ConnectionToken {
    shared
        .try_park(0, unit_token(slot), 7, b"POST", b"/upload", total, &[])
        .expect("unit park stages request")
}

fn unit_poll(shared: &HostShared) -> Vec<EventSlot> {
    let mut events = vec![empty_slot(); 8];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    events.truncate(report.copied);
    events
}

#[test]
fn host_body_available_staged_at_admission_coalesced_latest_wins() {
    let shared = HostShared::new(1);
    let token = unit_park(&shared, 3, 10);
    assert!(
        shared.push_body(token, b"abc"),
        "first admission stages bytes and signal"
    );
    assert!(
        shared.push_body(token, b"defg"),
        "second admission coalesces onto the pending signal"
    );
    let events = unit_poll(&shared);
    assert_eq!(
        events.len(),
        2,
        "one request plus one coalesced signal: {events:?}"
    );
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert_eq!(events[1].kind, HostEventKind::BodyAvailable);
    assert_eq!(events[1].token, token);
    assert_eq!(events[1].route_id, 0);
    assert_eq!(
        events[1].method_len, 7,
        "latest-wins counts every staged byte"
    );
    assert_eq!(
        events[1].path_len, 0,
        "partial window never claims completion"
    );
    assert_eq!((events[1].method_offset, events[1].path_offset), (0, 0));
    let mut pulled = [0; 16];
    assert_eq!(
        shared.pull_body(token, &mut pulled),
        (RespondOutcome::Ok, 7, false)
    );
    assert!(shared.push_body(token, b"hij"), "tail admission stages");
    let events = unit_poll(&shared);
    assert_eq!(events.len(), 1, "tail stages exactly one terminal signal");
    assert_eq!(events[0].kind, HostEventKind::BodyAvailable);
    assert_eq!(events[0].method_len, 3);
    assert_eq!(events[0].path_len, 1, "pulled plus staged covers the total");
}

#[test]
fn host_body_available_full_queue_refuses_admission_fail_closed() {
    let shared = HostShared::new(1);
    let mut tokens = Vec::new();
    for slot in 0..64 {
        tokens.push(unit_park(&shared, slot, 64));
    }
    assert_eq!(shared.counters().queue_full, 0);
    assert!(
        !shared.push_body(tokens[0], b"x"),
        "signal with no coalesce target and no room refuses the bytes too"
    );
    assert_eq!(shared.counters().queue_full, 1);
    let mut scratch = [0; 8];
    assert_eq!(
        shared.pull_body(tokens[0], &mut scratch),
        (RespondOutcome::Ok, 0, false),
        "refused bytes never stage"
    );
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1, "one drain frees exactly one room");
    assert!(
        shared.push_body(tokens[0], b"x"),
        "retry after drain stages"
    );
    let mut events = vec![empty_slot(); 128];
    let mut methods = vec![0; 2048];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    let signals = events[..report.copied]
        .iter()
        .filter(|event| event.kind == HostEventKind::BodyAvailable)
        .count();
    assert_eq!(signals, 1);
}

#[test]
fn host_body_available_retired_on_respond_and_close() {
    let shared = HostShared::new(1);
    let token = unit_park(&shared, 3, 5);
    assert!(shared.push_body(token, b"hello"));
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert_eq!(
        shared.stage_respond(token, 200, b"OK", &[], b"x"),
        RespondOutcome::Ok
    );
    assert!(
        unit_poll(&shared).is_empty(),
        "respond retires the pending signal"
    );
    let token = unit_park(&shared, 5, 5);
    assert!(shared.push_body(token, b"hello"));
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert!(shared.unpark_closed(0, 5), "close unparks");
    assert!(
        unit_poll(&shared).is_empty(),
        "close retires the pending signal"
    );
}

#[test]
fn host_stage_writable_helper_coalesces_and_maps() {
    let shared = HostShared::new(1);
    let token = unit_park(&shared, 3, 0);
    assert_eq!(
        shared.begin_stream(token, 200, b"OK"),
        RespondOutcome::Ok,
        "unit park stages a stream command"
    );
    assert!(shared.stage_writable(token), "first release stages");
    assert!(
        shared.stage_writable(token),
        "repeat coalesces without duplicating"
    );
    let bogus = ConnectionToken {
        request_seq: 9999,
        ..token
    };
    assert!(
        !shared.stage_writable(bogus),
        "unknown sequence stages nothing"
    );
    let events = unit_poll(&shared);
    assert_eq!(
        events.len(),
        2,
        "one request plus one coalesced writable: {events:?}"
    );
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert_eq!(events[1].kind, HostEventKind::Writable);
    assert_eq!(events[1].token, token);
    assert_eq!(events[1].route_id, 0);
    assert_eq!((events[1].method_len, events[1].path_len), (0, 0));
    assert_eq!((events[1].method_offset, events[1].path_offset), (0, 0));
}

#[test]
fn managed_body_available_event_admits_pull_then_count() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(
            b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 11\r\n\r\nhello world",
        )
        .expect("write post");
    // NOTE: the worker stages body bytes right behind the park, so the admission signal often
    // shares the first batches with its own REQUEST: a request-only poll would eat it. The loop
    // below keeps every signal seen before the REQUEST instead.
    let deadline = Instant::now() + DEADLINE;
    let mut kept = Vec::new();
    let (request, method, path) = loop {
        let (events, methods, paths, _) = poll_batch(&host, 100_000_000, 8);
        let mut found = None;
        for event in events {
            if event.kind == HostEventKind::Request {
                let method =
                    methods[event.method_offset..event.method_offset + event.method_len].to_vec();
                let path = paths[event.path_offset..event.path_offset + event.path_len].to_vec();
                found = Some((event, method, path));
            } else if event.kind == HostEventKind::BodyAvailable {
                kept.push(event);
            }
        }
        if let Some(found) = found {
            break found;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for request event"
        );
    };
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    // NOTE: segment splits may stage partial updates first; every signal carries the token with
    // reused counts, and the terminal full window always arrives because admission is atomic.
    // Signals kept from the request batches join the wait below.
    let deadline = Instant::now() + DEADLINE;
    let signal = loop {
        if let Some(done) = kept.iter().find(|event| {
            event.kind == HostEventKind::BodyAvailable
                && event.token == request.token
                && event.path_len == 1
        }) {
            break *done;
        }
        let (events, _, _, _) = poll_batch(&host, 100_000_000, 8);
        kept.extend(events.into_iter().filter(|event| {
            event.kind == HostEventKind::BodyAvailable && event.token == request.token
        }));
        assert!(Instant::now() < deadline, "terminal body signal stalls");
    };
    assert_eq!(signal.token, request.token);
    assert_eq!(signal.route_id, 0);
    assert_eq!(signal.method_len, 11);
    assert_eq!((signal.method_offset, signal.path_offset), (0, 0));
    assert_eq!(pull_body_complete(&host, request.token), b"hello world");
    assert_eq!(
        host.respond(request.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

fn wait_writable_event(host: &HostHandle, token: ConnectionToken) -> EventSlot {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let (events, _, _, _) = poll_batch(host, 100_000_000, 8);
        for event in events {
            if event.kind == HostEventKind::Writable && event.token == token {
                return event;
            }
        }
        assert!(Instant::now() < deadline, "writable signal stalls");
    }
}

// NOTE: WRITABLE emission pin: the flushed head and each consumed write queue exactly one
// coalesced signal, and each signal admits exactly one write. The waiter above keeps advisory
// signals instead of assuming batch shape, so REQUEST-only polls stay single-event.
#[test]
fn managed_stream_writable_event_signals_each_release() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    let first = wait_writable_event(&host, event.token);
    assert_eq!(first.kind, HostEventKind::Writable);
    assert_eq!(first.token, event.token);
    assert_eq!(first.route_id, 0);
    assert_eq!((first.method_len, first.path_len), (0, 0));
    assert_eq!((first.method_offset, first.path_offset), (0, 0));
    assert_eq!(
        host.write_stream(event.token, b"hello "),
        RespondOutcome::Ok
    );
    let second = wait_writable_event(&host, event.token);
    assert_eq!(second.token, event.token);
    assert_eq!(second.route_id, 0);
    assert_eq!((second.method_len, second.path_len), (0, 0));
    assert_eq!(host.write_stream(event.token, b"world"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (status, raw, body) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello world");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("head end")
        + 4;
    stream_head_exact(&raw[..split], "HTTP/1.1 200 OK");
    assert_eq!(&raw[split..], b"6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn host_writable_full_queue_refuses_and_resignals_after_drain() {
    let shared = HostShared::new(1);
    let token = unit_park(&shared, 3, 0);
    assert_eq!(
        shared.begin_stream(token, 200, b"OK"),
        RespondOutcome::Ok,
        "unit park stages a stream command"
    );
    for slot in 0..64 {
        if slot == 3 {
            continue;
        }
        unit_park(&shared, slot, 0);
    }
    assert!(
        !shared.stage_writable(token),
        "full queue with no coalesce target refuses"
    );
    assert_eq!(shared.counters().queue_full, 1);
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1, "one drain frees exactly one room");
    assert!(shared.stage_writable(token), "retry after drain stages");
    assert!(
        shared.stage_writable(token),
        "repeat coalesces without duplicating"
    );
    let mut events = vec![empty_slot(); 128];
    let mut methods = vec![0; 2048];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    let signals = events[..report.copied]
        .iter()
        .filter(|event| event.kind == HostEventKind::Writable && event.token == token)
        .count();
    assert_eq!(signals, 1);
}

#[test]
fn host_writable_retired_on_cancel_and_close() {
    let shared = HostShared::new(1);
    let token = unit_park(&shared, 3, 0);
    assert_eq!(shared.begin_stream(token, 200, b"OK"), RespondOutcome::Ok);
    assert!(shared.stage_writable(token));
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert_eq!(shared.stage_cancel(token), CancelOutcome::Ok);
    assert!(
        unit_poll(&shared).is_empty(),
        "cancel retires the pending signal"
    );
    let token = unit_park(&shared, 5, 0);
    assert_eq!(shared.begin_stream(token, 200, b"OK"), RespondOutcome::Ok);
    assert!(shared.stage_writable(token));
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert!(shared.unpark_closed(0, 5), "close unparks");
    assert!(
        unit_poll(&shared).is_empty(),
        "close retires the pending signal"
    );
}

#[test]
fn managed_respond_with_headers_sends_them_after_date() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[
                (b"content-type".as_slice(), b"text/plain".as_slice()),
                (b"x-custom".as_slice(), b"yes".as_slice()),
            ],
            b"done",
        ),
        RespondOutcome::Ok
    );
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"done");
    assert_eq!(header(&first, "content-type"), "text/plain");
    assert_eq!(header(&first, "x-custom"), "yes");
    date_shape(&first);
    let names: Vec<&str> = first
        .headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(
        names,
        ["Date", "content-type", "x-custom", "Content-Length"]
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn managed_gzip_body_passes_through_with_declared_encoding() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    let plain = b"The quick brown fox jumps over the lazy dog. ".repeat(50);
    let gzipped = {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plain).expect("gzip body");
        encoder.finish().expect("finish gzip")
    };
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"content-encoding".as_slice(), b"gzip".as_slice())],
            &gzipped,
        ),
        RespondOutcome::Ok
    );
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, gzipped);
    assert_eq!(header(&first, "content-encoding"), "gzip");
    assert_eq!(
        first
            .headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("vary"))
            .count(),
        0
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_respond_with_headers_rejects_bad_fields() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"x bad".as_slice(), b"yes".as_slice())],
            b"done",
        ),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"x-ok".as_slice(), b"a\r\nb".as_slice())],
            b"done",
        ),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"content-length".as_slice(), b"4".as_slice())],
            b"done",
        ),
        RespondOutcome::Invalid
    );
    let many: Vec<(&[u8], &[u8])> = (0..9)
        .map(|_| (b"x-a".as_slice(), b"b".as_slice()))
        .collect();
    assert_eq!(
        host.respond_with_headers(event.token, 200, b"OK", &many, b"done"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"done");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_begin_with_headers_sends_them_after_date() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"content-type".as_slice(), b"text/plain".as_slice())],
        ),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"chunk"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (status, raw, body) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"chunk");
    let head_end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("head end")
        + 4;
    let head = std::str::from_utf8(&raw[..head_end]).expect("head utf8");
    assert!(
        head.starts_with("HTTP/1.1 200 OK\r\nDate: "),
        "head: {head:?}"
    );
    assert!(
        head.to_ascii_lowercase()
            .contains("\r\ncontent-type: text/plain\r\n")
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_begin_rejects_bad_headers() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"content-length".as_slice(), b"5".as_slice())],
        ),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (status, _, _) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}
