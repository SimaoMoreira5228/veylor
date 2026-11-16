use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};
use veylor_runtime::{ConnectionTimeouts, ConnectionToken};
use veylor_server::{
    EventSlot, HostEventKind, HostHandle, MAX_STATIC_BODY_BYTES, PollReport, RespondOutcome,
    RunningServer, Server, ServerConfig,
};

pub(crate) const DEADLINE: Duration = Duration::from_secs(5);

pub(crate) const PARKED_QUIET: Duration = Duration::from_millis(150);

pub(crate) const SPIN: Duration = Duration::from_secs(5);

pub(crate) fn managed_server(config: ServerConfig) -> RunningServer {
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

pub(crate) fn default_managed_server() -> RunningServer {
    managed_server(ServerConfig::default())
}

pub(crate) fn tight_managed_server(body_ns: u64, grace_ns: u64) -> RunningServer {
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

pub(crate) fn connect(server: &RunningServer) -> TcpStream {
    let stream = TcpStream::connect(server.addr()).expect("connect loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
}

pub(crate) fn get(stream: &mut TcpStream, path: &str) {
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write request");
}

pub(crate) struct ClientResponse {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

pub(crate) fn read_response(stream: &mut TcpStream) -> ClientResponse {
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

pub(crate) fn read_head(stream: &mut TcpStream) -> (u16, usize) {
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

pub(crate) fn read_exact_body(stream: &mut TcpStream, length: usize) -> Vec<u8> {
    let mut body = vec![0; length];
    stream.read_exact(&mut body).expect("read body");
    body
}

pub(crate) fn expect_eof(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        other => panic!("expected EOF, got {other:?}"),
    }
}

pub(crate) fn header<'a>(response: &'a ClientResponse, name: &str) -> &'a str {
    response
        .headers
        .iter()
        .find(|(field, _)| field.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
        .expect("header present")
}

pub(crate) fn date_shape(response: &ClientResponse) -> String {
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

pub(crate) fn stream_head_exact(head: &[u8], status_line: &str) -> String {
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

pub(crate) fn spin_until(mut ready: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + SPIN;
    while Instant::now() < deadline {
        if ready() {
            return true;
        }
        std::thread::yield_now();
    }
    ready()
}

pub(crate) fn empty_slot() -> EventSlot {
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
        route_token: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    }
}

pub(crate) fn poll_batch(
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

pub(crate) fn poll_request(host: &HostHandle) -> (EventSlot, Vec<u8>, Vec<u8>) {
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

pub(crate) fn pull_body_complete(host: &HostHandle, token: ConnectionToken) -> Vec<u8> {
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

pub(crate) fn wait_writable(host: &HostHandle, token: ConnectionToken) {
    assert!(
        spin_until(|| host.is_writable(token)),
        "stream never armed writable"
    );
}

pub(crate) fn read_raw_head(stream: &mut TcpStream) -> Vec<u8> {
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

pub(crate) fn read_chunk_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
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

pub(crate) fn read_chunked(stream: &mut TcpStream) -> (u16, Vec<u8>, Vec<u8>) {
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
