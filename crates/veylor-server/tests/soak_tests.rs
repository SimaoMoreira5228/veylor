use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use veylor_runtime::ConnectionToken;
use veylor_server::{
    CancelOutcome, CloseCounts, EventSlot, HostEventKind, HostHandle, MAX_STATIC_BODY_BYTES,
    RespondOutcome, RunningServer, Server, ServerConfig,
};

const DEADLINE: Duration = Duration::from_secs(5);
const SPIN: Duration = Duration::from_secs(5);

fn static_server() -> RunningServer {
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
    server.start().expect("start server")
}

fn managed_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
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

fn get_close(stream: &mut TcpStream, path: &str) {
    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
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
    let status = lines
        .next()
        .expect("status line")
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

fn read_close_response(addr: SocketAddr, path: &str) {
    let mut stream = TcpStream::connect(addr).expect("connect loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    get_close(&mut stream, path);
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read close response");
    let text = String::from_utf8_lossy(&raw);
    assert!(
        text.starts_with("HTTP/1.1 200 OK\r\n"),
        "close status: {text}"
    );
    assert!(text.ends_with("hello"), "close body: {text}");
}

fn expect_closed(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Err(_) => {}
        Ok(_) => panic!("cancel must send no response bytes"),
    }
}

fn total_closes(counts: &CloseCounts) -> u64 {
    counts.completed
        + counts.peer_closed
        + counts.reset
        + counts.timeout
        + counts.parked_timeout
        + counts.queue_full
        + counts.cancelled
        + counts.protocol_error
        + counts.response_overflow
        + counts.transport_error
        + counts.shutdown
        + counts.h2_served
        + counts.h2_refused
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

fn poll_request(host: &HostHandle) -> EventSlot {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let mut events = vec![empty_slot(); 8];
        let mut methods = vec![0; 64];
        let mut paths = vec![0; 4096];
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.into_iter().take(report.copied as usize) {
            if event.kind == HostEventKind::Request {
                return event;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for request event"
        );
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

fn read_chunked_body(stream: &mut TcpStream) -> (u16, Vec<u8>) {
    let head = read_raw_head(stream);
    let text = String::from_utf8(head).expect("head utf8");
    let status = text
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status int");
    let mut body = Vec::new();
    while let Some(chunk) = read_chunk_frame(stream) {
        body.extend_from_slice(&chunk);
    }
    (status, body)
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
fn soak_sequential_keep_alive_5k_stable() {
    const CONNS: usize = 5;
    const PER_CONN: usize = 1000;
    let server = static_server();
    for _ in 0..CONNS {
        let mut stream = connect(&server);
        for _ in 0..PER_CONN {
            get(&mut stream, "/hello");
            let response = read_response(&mut stream);
            assert_eq!(response.status, 200);
            assert_eq!(response.body, b"hello");
        }
        get_close(&mut stream, "/hello");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("read close tail");
        let text = String::from_utf8_lossy(&raw);
        assert!(
            text.starts_with("HTTP/1.1 200 OK\r\n"),
            "tail status: {text}"
        );
        assert!(text.ends_with("hello"), "tail body: {text}");
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    // NOTE: every initiated connection must surface exactly one close reason; the
    // summed close buckets equal the dial count, so neither leaks nor strays hide
    // across 5k+ sequential exchanges.
    assert_eq!(total_closes(&counts), CONNS as u64, "counts: {counts:?}");
    assert_eq!(counts.completed, CONNS as u64, "counts: {counts:?}");
}

#[test]
fn soak_connection_churn_mixed_close_conserves() {
    const GRACEFUL: usize = 150;
    const IDLE_ABORT: usize = 75;
    const MID_ABORT: usize = 75;
    const TOTAL: usize = GRACEFUL + IDLE_ABORT + MID_ABORT;
    let server = static_server();
    let addr = server.addr();
    for index in 0..GRACEFUL {
        read_close_response(addr, "/hello");
        if index < IDLE_ABORT {
            let stream = TcpStream::connect(addr).expect("connect loopback");
            drop(stream);
        } else {
            let mut stream = TcpStream::connect(addr).expect("connect loopback");
            stream
                .set_read_timeout(Some(DEADLINE))
                .expect("read timeout");
            stream
                .set_write_timeout(Some(DEADLINE))
                .expect("write timeout");
            get(&mut stream, "/hello");
            drop(stream);
        }
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    // NOTE: graceful closes land in completed while abortive FIN/RST lands in the
    // peer/reset/transport buckets; the sum must still equal dials exactly.
    assert_eq!(total_closes(&counts), TOTAL as u64, "counts: {counts:?}");
    assert_eq!(counts.completed, GRACEFUL as u64, "counts: {counts:?}");
    assert_eq!(
        total_closes(&counts) - counts.completed,
        (IDLE_ABORT + MID_ABORT) as u64,
        "counts: {counts:?}"
    );
}

#[test]
fn soak_managed_park_respond_cancel_cycles() {
    const RESPONDS: usize = 100;
    const RACES: usize = 20;
    const CANCELS: usize = 60;
    let server = managed_server();
    let host = server.host();
    let mut reuse = connect(&server);
    for _ in 0..RESPONDS {
        get(&mut reuse, "/work");
        let event = poll_request(&host);
        assert_eq!(
            host.respond(event.token, 200, b"OK", b"done"),
            RespondOutcome::Ok
        );
        let response = read_response(&mut reuse);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"done");
    }
    for _ in 0..RACES {
        get(&mut reuse, "/work");
        let event = poll_request(&host);
        assert_eq!(
            host.respond(event.token, 200, b"OK", b"raced"),
            RespondOutcome::Ok
        );
        assert_eq!(
            host.cancel(event.token),
            CancelOutcome::Stale,
            "cancel-after-respond races Stale"
        );
        let response = read_response(&mut reuse);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"raced");
    }
    for _ in 0..CANCELS {
        let mut stream = connect(&server);
        get(&mut stream, "/work");
        let event = poll_request(&host);
        assert_eq!(host.cancel(event.token), CancelOutcome::Ok);
        expect_closed(&mut stream);
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    // NOTE: one reused keep-alive plus one conn per cancel; conservation pins that
    // every cancel unparks exactly once and no respond leaks a park.
    assert_eq!(
        total_closes(&counts),
        (1 + CANCELS) as u64,
        "counts: {counts:?}"
    );
    assert_eq!(counts.cancelled, CANCELS as u64, "counts: {counts:?}");
}

#[test]
fn soak_worker_shutdown_restart_cycles() {
    const ROUNDS: usize = 3;
    const PER_ROUND: usize = 50;
    for _ in 0..ROUNDS {
        let server = static_server();
        let addr = server.addr();
        for _ in 0..PER_ROUND {
            read_close_response(addr, "/hello");
        }
        server.stop_accepting();
        let counts = server.join().expect("join server");
        assert_eq!(
            total_closes(&counts),
            PER_ROUND as u64,
            "counts: {counts:?}"
        );
        assert_eq!(counts.completed, PER_ROUND as u64, "counts: {counts:?}");
    }
}

#[test]
fn soak_trailer_streaming_interleaved() {
    const ROUNDS: usize = 30;
    let server = managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    for _ in 0..ROUNDS {
        get(&mut stream, "/work");
        let event = poll_request(&host);
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
        let (status, body) = read_chunked_body(&mut stream);
        assert_eq!(status, 200);
        assert_eq!(body, b"hello world");

        get(&mut stream, "/hello");
        let static_response = read_response(&mut stream);
        assert_eq!(static_response.status, 200);
        assert_eq!(static_response.body, b"hello");

        get(&mut stream, "/work");
        let trailer_event = poll_request(&host);
        assert_eq!(
            host.begin_stream(trailer_event.token, 200, b"OK"),
            RespondOutcome::Ok
        );
        wait_writable(&host, trailer_event.token);
        assert_eq!(
            host.write_stream(trailer_event.token, b"foo"),
            RespondOutcome::Ok
        );
        wait_writable(&host, trailer_event.token);
        let fields: &[(&[u8], &[u8])] = &[(b"x-done", b"yes")];
        assert_eq!(
            host.end_stream_with_trailers(trailer_event.token, fields),
            RespondOutcome::Ok
        );
        let head = read_raw_head(&mut stream);
        let text = String::from_utf8(head).expect("head utf8");
        assert!(text.starts_with("HTTP/1.1 200 OK"), "stream head: {text}");
        assert_eq!(read_chunk_frame(&mut stream), Some(b"foo".to_vec()));
        let mut size = [0; 3];
        stream.read_exact(&mut size).expect("terminal size");
        assert_eq!(&size, b"0\r\n");
        assert_eq!(read_trailer_block(&mut stream), b"x-done: yes\r\n\r\n");
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    // NOTE: one keep-alive carries every streaming, static, and trailer exchange;
    // a single shutdown close proves the interleaved stages never strand a park.
    assert_eq!(total_closes(&counts), 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}
