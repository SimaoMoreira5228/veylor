use crate::h1_peer::*;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};
use veylor_runtime::TurnConfig;
use veylor_server::{
    Certificate, MAX_STATIC_BODY_BYTES, PrivateKey, RunningServer, Server, ServerConfig, TlsConfig,
};
use veylor_tls::{TlsConnector, TlsStream};

const PROGRESS_DEADLINE: Duration = Duration::from_secs(2);
const BODY_LEN: usize = 20_000;

const CERT_DER: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_cert.der");
const KEY_DER: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_key.der");

fn budgeted_server(bytes: u64) -> veylor_server::RunningServer {
    let config = ServerConfig {
        turn: TurnConfig {
            byte_budget_per_turn: bytes,
            ..TurnConfig::DEFAULT
        },
        ..ServerConfig::default()
    };
    test_server_with_config(config)
}

fn post_with_body() -> Vec<u8> {
    let head =
        format!("POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {BODY_LEN}\r\n\r\n");
    let mut wire = Vec::with_capacity(head.len() + BODY_LEN);
    wire.extend_from_slice(head.as_bytes());
    wire.extend_from_slice(&vec![b'a'; BODY_LEN]);
    wire
}

#[test]
fn h1_first_request_body_drains_without_batching() {
    // NOTE: the accept turn leaves read_ready set, so the first body read is the direct
    // path; a full slab there keeps its claim on progress and must keep doing so.
    let server = test_server();
    let mut stream = connect(&server);
    stream.write_all(&post_with_body()).expect("write request");
    let started = Instant::now();
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    assert!(
        started.elapsed() < PROGRESS_DEADLINE,
        "first-request body waited {:?}",
        started.elapsed()
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn h1_keep_alive_body_written_ahead_resumes_without_an_edge() {
    let server = test_server();
    let mut stream = connect(&server);
    get(&mut stream, "/empty");
    assert_eq!(read_response(&mut stream).status, 200);
    // NOTE: the whole request lands in one segment with the idle claim cleared, so the
    // batch read fills the slab while the peer then stays silent.
    stream.write_all(&post_with_body()).expect("write request");
    let started = Instant::now();
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    // NOTE: the drain must consume the rest of the body before the next exchange parses,
    // so a reuse request proves the stalled read actually resumed.
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    assert!(
        started.elapsed() < PROGRESS_DEADLINE,
        "keep-alive body waited {:?} after the read filled the slab",
        started.elapsed()
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.transport_error, 0, "counts: {counts:?}");
}

#[test]
fn h1_budget_split_head_resumes_without_an_edge() {
    let server = budgeted_server(8);
    let mut stream = connect(&server);
    stream
        .write_all(b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .expect("write head");
    let started = Instant::now();
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    assert!(
        started.elapsed() < PROGRESS_DEADLINE,
        "budget-split head waited {:?} after an exact fill",
        started.elapsed()
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn h1_exact_fill_then_drained_waits_for_the_next_arrival() {
    // NOTE: eight bytes fill the whole turn budget and drain the socket; the reader must
    // stop there, not spin on a readiness edge that cannot arrive until more bytes do.
    let server = budgeted_server(8);
    let mut stream = connect(&server);
    stream.write_all(b"GET /hel").expect("write exact fill");
    std::thread::sleep(Duration::from_millis(50));
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .expect("short read timeout");
    let mut byte = [0u8; 1];
    match std::io::Read::read(&mut stream, &mut byte) {
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) => {}
        other => panic!("server answered a partial head: {other:?}"),
    }
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .write_all(b"lo HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .expect("write rest");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn h1_slow_peer_one_byte_at_a_time_completes() {
    let server = budgeted_server(1);
    let mut stream = connect(&server);
    let request = b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
    let started = Instant::now();
    for byte in request {
        stream.write_all(&[*byte]).expect("write byte");
        std::thread::sleep(Duration::from_millis(1));
    }
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    assert!(
        started.elapsed() < PROGRESS_DEADLINE,
        "byte-at-a-time peer waited {:?}",
        started.elapsed()
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

fn tls_server() -> RunningServer {
    let config = ServerConfig {
        tls: Some(TlsConfig {
            cert_chain: vec![Certificate::from(CERT_DER.to_vec())],
            key: PrivateKey::Pkcs8(KEY_DER.to_vec()),
            max_handshakes: 4,
        }),
        ..ServerConfig::default()
    };
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, config).expect("bind tls");
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
        .register_route(
            b"POST",
            b"/submit",
            200,
            b"OK",
            b"submitted",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register submit");
    server.start().expect("start tls")
}

fn tls_handshake(server: &RunningServer) -> TlsStream<TcpStream> {
    let connector = TlsConnector::builder()
        .tls13_only(true)
        .pin_certificate(CERT_DER)
        .expect("pin fixture")
        .alpn(&[b"http/1.1"])
        .build()
        .expect("connector");
    let sock = TcpStream::connect(server.addr()).expect("connect tls");
    sock.set_read_timeout(Some(DEADLINE)).expect("read timeout");
    sock.set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    let conn = connector.connect("localhost").expect("tls client");
    let mut stream = TlsStream::new(sock, conn);
    stream.handshake().expect("handshake");
    stream
}

fn read_tls_response(stream: &mut TlsStream<TcpStream>) -> ClientResponse {
    let mut plain = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let end = plain
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4);
        if let Some(end) = end {
            let head = String::from_utf8_lossy(&plain[..end]).to_string();
            let length = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if plain.len() - end >= length {
                let status = head
                    .split_whitespace()
                    .nth(1)
                    .and_then(|code| code.parse().ok())
                    .expect("status code");
                return ClientResponse {
                    status,
                    headers: Vec::new(),
                    body: plain[end..end + length].to_vec(),
                };
            }
        }
        let read = stream.read(&mut chunk).expect("read tls");
        assert!(read > 0, "tls peer closed mid-response");
        plain.extend_from_slice(&chunk[..read]);
    }
}

#[test]
fn h1_tls_body_written_ahead_drains_without_batching() {
    // NOTE: TLS connections never enter the batch ring, so the large body rides the TLS
    // plaintext claim; this pins that the read re-arm rule did not reach them.
    let server = tls_server();
    let mut stream = tls_handshake(&server);
    stream
        .write_all(&post_with_body())
        .expect("write tls request");
    let started = Instant::now();
    let response = read_tls_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"submitted");
    stream
        .write_all(b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .expect("write tls reuse");
    let second = read_tls_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    assert!(
        started.elapsed() < PROGRESS_DEADLINE,
        "tls body waited {:?}",
        started.elapsed()
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}
