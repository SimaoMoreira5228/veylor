use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use veylor_server::{
    CloseCounts, MAX_CONNECTIONS, MAX_STATIC_BODY_BYTES, RunningServer, Server, ServerConfig,
    ServerError,
};

const DEADLINE: Duration = Duration::from_secs(5);
const EXTRA_DEADLINE: Duration = Duration::from_millis(1000);
const IDLE_HOLD: Duration = Duration::from_millis(500);
const FANOUT_CLIENTS: usize = 32;
const MAX_ABSURD: usize = 17;
const SCALE_IDLE: usize = 1000;
const SCALE_THREADS: usize = 8;
const BURST_CAP: usize = 64;
const BURST_TOTAL: usize = 128;
const SMALL_CAP: usize = 32;
const SMALL_EXTRA: usize = 4;
const BURST_THREADS: usize = 4;

fn multi_config(worker_count: usize) -> ServerConfig {
    ServerConfig {
        worker_count,
        ..ServerConfig::default()
    }
}

fn test_multi_server(worker_count: usize) -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, multi_config(worker_count)).expect("bind loopback");
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

fn scale_config(max_connections: usize) -> ServerConfig {
    let mut config = multi_config(1);
    config.max_connections = max_connections;
    config.turn.max_accepts_per_turn = BURST_CAP;
    config
}

fn test_scale_server(max_connections: usize) -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, scale_config(max_connections)).expect("bind loopback");
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

fn read_response(stream: &mut TcpStream) -> (u16, Vec<u8>) {
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
    (status, body)
}

fn total_closes(counts: &CloseCounts) -> u64 {
    counts.completed
        + counts.peer_closed
        + counts.reset
        + counts.timeout
        + counts.protocol_error
        + counts.response_overflow
        + counts.transport_error
        + counts.shutdown
}

fn read_eof(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        other => panic!("expected EOF, got {other:?}"),
    }
}

fn close_client(addr: SocketAddr) {
    let mut stream = TcpStream::connect(addr).expect("connect loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
        .write_all(b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .expect("write request");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    let text = String::from_utf8(response).expect("response utf8");
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(text.ends_with("hello"), "body: {text}");
}

fn fanout(worker_count: usize) {
    let server = test_multi_server(worker_count);
    let addr = server.addr();
    let mut handles = Vec::with_capacity(FANOUT_CLIENTS);
    for _ in 0..FANOUT_CLIENTS {
        handles.push(std::thread::spawn(move || close_client(addr)));
    }
    for handle in handles {
        handle.join().expect("client thread");
    }
    server.stop_accepting();
    let per_worker = server.join_per_worker().expect("join server");
    assert_eq!(per_worker.len(), worker_count, "workers: {per_worker:?}");
    let totals: Vec<u64> = per_worker.iter().map(total_closes).collect();
    let served: u64 = totals.iter().sum();
    eprintln!("worker_count={worker_count} distribution={totals:?}");
    assert_eq!(served, FANOUT_CLIENTS as u64, "served: {totals:?}");
    if worker_count > 1 {
        for (index, total) in totals.iter().enumerate() {
            assert!(*total >= 1, "worker {index} accepted none: {totals:?}");
        }
    }
}

#[test]
fn default_config_is_single_worker() {
    assert_eq!(ServerConfig::default().worker_count, 1);
}

#[test]
fn invalid_worker_counts_rejected() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    for worker_count in [0, MAX_ABSURD, usize::MAX] {
        let err = Server::bind(addr, multi_config(worker_count))
            .err()
            .expect("bind must fail");
        assert!(
            matches!(err, ServerError::InvalidConfig),
            "count {worker_count}: {err:?}"
        );
    }
}

#[test]
fn upper_bound_worker_count_binds() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let _ = Server::bind(addr, multi_config(16)).expect("bind sixteen workers");
}

#[test]
fn fanout_single_worker() {
    fanout(1);
}

#[test]
fn fanout_two_workers() {
    fanout(2);
}

#[test]
fn fanout_four_workers() {
    fanout(4);
}

#[test]
fn graceful_drain_two_workers() {
    let server = test_multi_server(2);
    let mut first = connect(&server);
    let mut second = connect(&server);
    get(&mut first, "/hello");
    get(&mut second, "/hello");
    server.stop_accepting();
    let (status, body) = read_response(&mut first);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    let (status, body) = read_response(&mut second);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    read_eof(&mut first);
    read_eof(&mut second);
    let per_worker = server.join_per_worker().expect("join server");
    assert_eq!(per_worker.len(), 2, "workers: {per_worker:?}");
    let total: u64 = per_worker
        .iter()
        .map(|counts| counts.completed + counts.shutdown)
        .sum();
    assert_eq!(total, 2, "counts: {per_worker:?}");
}

#[test]
fn forced_stop_two_workers() {
    let server = test_multi_server(2);
    let mut idle = connect(&server);
    get(&mut idle, "/hello");
    let (status, body) = read_response(&mut idle);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    server.stop_force();
    read_eof(&mut idle);
    let counts = server.join().expect("join server");
    assert!(counts.shutdown >= 1, "counts: {counts:?}");
}

#[test]
fn force_stop_mid_load_two_workers() {
    const LOAD: usize = 4;
    let server = test_multi_server(2);
    let mut clients = Vec::with_capacity(LOAD);
    for _ in 0..LOAD {
        let mut stream = connect(&server);
        get(&mut stream, "/hello");
        clients.push(stream);
    }
    for stream in clients.iter_mut() {
        let (status, body) = read_response(stream);
        assert_eq!(status, 200);
        assert_eq!(body, b"hello");
    }
    server.stop_force();
    for stream in clients.iter_mut() {
        read_eof(stream);
    }
    let per_worker = server.join_per_worker().expect("join server");
    assert_eq!(per_worker.len(), 2, "workers: {per_worker:?}");
    let total: u64 = per_worker.iter().map(total_closes).sum();
    assert_eq!(total, LOAD as u64, "counts: {per_worker:?}");
    let shutdown: u64 = per_worker.iter().map(|counts| counts.shutdown).sum();
    assert_eq!(shutdown, LOAD as u64, "counts: {per_worker:?}");
}

#[test]
fn reuse_port_conflict_semantics() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut first = Server::bind(addr, multi_config(2)).expect("bind first");
    first
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register hello");
    let first = first.start().expect("start first");
    let bound = first.addr();
    assert_ne!(bound.port(), 0);
    let err = Server::bind(bound, ServerConfig::default())
        .err()
        .expect("plain bind must fail");
    assert!(
        matches!(err, ServerError::Bind(_)),
        "plain bind must not join a reuse-port group: {err:?}"
    );
    let mut second = Server::bind(bound, multi_config(2)).expect("second reuse-port bind shares");
    second
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register hello");
    let second = second.start().expect("start second");
    let mut shared = connect(&second);
    get(&mut shared, "/hello");
    let (status, body) = read_response(&mut shared);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    // NOTE: the kernel spreads accepts across every reuse-port listener on the
    // port, so `shared` may belong to either server; the summed close count is
    // exact either way while per-server ownership is not pinned.
    second.stop_accepting();
    let second_counts = second.join().expect("join second");
    let mut remaining = connect(&first);
    get(&mut remaining, "/hello");
    let (status, body) = read_response(&mut remaining);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    first.stop_accepting();
    let first_counts = first.join().expect("join first");
    let total = total_closes(&second_counts) + total_closes(&first_counts);
    assert_eq!(
        total, 2,
        "second: {second_counts:?} first: {first_counts:?}"
    );
}

#[test]
fn default_preserves_historical_ceiling() {
    let default = ServerConfig::default().max_connections;
    assert!(default >= 1, "default must admit: {default}");
    assert!(
        default < MAX_CONNECTIONS,
        "default stays opt-in below ceiling: {default}"
    );
}

#[test]
fn invalid_connection_counts_rejected() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    for count in [0, MAX_CONNECTIONS + 1, usize::MAX] {
        let mut config = multi_config(1);
        config.max_connections = count;
        let err = Server::bind(addr, config).err().expect("bind must fail");
        assert!(
            matches!(err, ServerError::InvalidConfig),
            "count {count}: {err:?}"
        );
    }
}

fn serve_hold(addr: SocketAddr, count: usize) -> Vec<TcpStream> {
    let mut held = Vec::with_capacity(count);
    for _ in 0..count {
        let mut stream = TcpStream::connect(addr).expect("connect loopback");
        stream
            .set_read_timeout(Some(DEADLINE))
            .expect("read timeout");
        stream
            .set_write_timeout(Some(DEADLINE))
            .expect("write timeout");
        get(&mut stream, "/hello");
        let (status, body) = read_response(&mut stream);
        assert_eq!(status, 200);
        assert_eq!(body, b"hello");
        held.push(stream);
    }
    held
}

#[test]
fn thousand_idle_hold_single_worker() {
    const PER_THREAD: usize = SCALE_IDLE / SCALE_THREADS;
    let server = test_scale_server(MAX_CONNECTIONS);
    let addr = server.addr();
    let mut handles = Vec::with_capacity(SCALE_THREADS);
    for _ in 0..SCALE_THREADS {
        handles.push(std::thread::spawn(move || serve_hold(addr, PER_THREAD)));
    }
    let mut held = Vec::with_capacity(SCALE_IDLE);
    for handle in handles {
        held.extend(handle.join().expect("client thread"));
    }
    assert_eq!(held.len(), SCALE_IDLE);
    std::thread::sleep(IDLE_HOLD);
    for stream in held.iter_mut() {
        get(stream, "/hello");
    }
    for stream in held.iter_mut() {
        let (status, body) = read_response(stream);
        assert_eq!(status, 200);
        assert_eq!(body, b"hello");
    }
    server.stop_accepting();
    for stream in held.iter_mut() {
        read_eof(stream);
    }
    let counts = server.join().expect("join server");
    assert_eq!(
        total_closes(&counts),
        SCALE_IDLE as u64,
        "counts: {counts:?}"
    );
    assert_eq!(counts.shutdown, SCALE_IDLE as u64, "counts: {counts:?}");
}

#[test]
fn burst_drain_past_staging() {
    const PER_THREAD: usize = BURST_TOTAL / BURST_THREADS;
    let server = test_scale_server(BURST_CAP);
    let addr = server.addr();
    let mut handles = Vec::with_capacity(BURST_THREADS);
    for _ in 0..BURST_THREADS {
        handles.push(std::thread::spawn(move || {
            for _ in 0..PER_THREAD {
                close_client(addr);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("client thread");
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(
        total_closes(&counts),
        BURST_TOTAL as u64,
        "counts: {counts:?}"
    );
    assert_eq!(counts.completed, BURST_TOTAL as u64, "counts: {counts:?}");
}

#[test]
fn over_cap_gets_zero_service() {
    let server = test_scale_server(SMALL_CAP);
    let mut held = Vec::with_capacity(SMALL_CAP);
    for _ in 0..SMALL_CAP {
        let mut stream = connect(&server);
        get(&mut stream, "/hello");
        let (status, body) = read_response(&mut stream);
        assert_eq!(status, 200);
        assert_eq!(body, b"hello");
        held.push(stream);
    }
    for _ in 0..SMALL_EXTRA {
        let mut stream = connect(&server);
        stream
            .set_read_timeout(Some(EXTRA_DEADLINE))
            .expect("extra timeout");
        get(&mut stream, "/hello");
        let mut byte = [0; 1];
        let outcome = stream.read(&mut byte);
        assert!(outcome.is_err(), "over-cap conn got service: {outcome:?}");
    }
    server.stop_accepting();
    for stream in held.iter_mut() {
        read_eof(stream);
    }
    let counts = server.join().expect("join server");
    assert_eq!(
        total_closes(&counts),
        SMALL_CAP as u64,
        "counts: {counts:?}"
    );
}
