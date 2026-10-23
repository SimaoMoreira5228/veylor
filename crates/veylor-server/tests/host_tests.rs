use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use veylor_runtime::{ConnectionTimeouts, ConnectionToken};
use veylor_server::{
    CancelOutcome, EventSlot, HostEventKind, HostHandle, MAX_STATIC_BODY_BYTES, PollReport,
    RespondOutcome, RunningServer, Server, ServerConfig, ServerError, TrailerOutcome,
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
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').expect("header colon");
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().expect("content length int");
        }
    }
    let mut body = vec![0; content_length];
    stream.read_exact(&mut body).expect("read body");
    ClientResponse { status, body }
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
