use crate::h1_peer::*;
use std::io::Write;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use veylor_server::{MAX_STATIC_BODY_BYTES, Server, ServerConfig};

// NOTE: MAX_RESPONSE_BODY caps a static route body, so the widest response one turn can
// stage is the whole 15 KiB; a full socket send buffer then has no room for any of it.
const BULK_LEN: usize = 15 * 1024;
const PIPELINED: usize = 400;
const HEALTHY_DEADLINE: Duration = Duration::from_secs(1);

#[test]
fn h1_unread_peer_does_not_stall_batched_sends() {
    let server = bulk_server();
    let mut blocked = connect(&server);
    let mut wire = Vec::with_capacity(PIPELINED * 32);
    for _ in 0..PIPELINED {
        wire.extend_from_slice(b"GET /bulk HTTP/1.1\r\nHost: x\r\n\r\n");
    }
    blocked.write_all(&wire).expect("pipeline bulk requests");
    // NOTE: the worker responds to the pipeline one exchange per turn until the peer's
    // receive window and then the server's send buffer are full; nothing drains them.
    std::thread::sleep(Duration::from_millis(150));

    let mut worst = Duration::ZERO;
    for _ in 0..3 {
        let mut healthy = connect(&server);
        let started = Instant::now();
        get(&mut healthy, "/hello");
        let response = read_response(&mut healthy);
        let elapsed = started.elapsed();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
        assert!(
            elapsed < HEALTHY_DEADLINE,
            "healthy connection waited {elapsed:?} behind the unread peer"
        );
        worst = worst.max(elapsed);
    }
    eprintln!("worst healthy round trip behind an unread peer: {worst:?}");

    for _ in 0..PIPELINED {
        let response = read_response(&mut blocked);
        assert_eq!(response.status, 200);
        assert_eq!(response.body.len(), BULK_LEN);
    }

    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.transport_error, 0, "counts: {counts:?}");
    assert_eq!(counts.protocol_error, 0, "counts: {counts:?}");
}

fn bulk_server() -> veylor_server::RunningServer {
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
    let bulk = vec![b'x'; BULK_LEN];
    server
        .register_route(
            b"GET",
            b"/bulk",
            200,
            b"application/octet-stream",
            &bulk,
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register bulk");
    server.start().expect("start server")
}
