use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::io::Write;
use std::net::SocketAddr;
use std::time::Duration;
use veylor_http2::{
    ErrorCode, H2Role, HpackEntrySlot, HpackHeader, HpackTable, StreamSlot, StreamTable,
};
use veylor_runtime::ConnectionTimeouts;
use veylor_server::{
    ACK_SLOTS, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES, ENCODE_ENTRIES, FrozenRoute,
    FrozenRoutesView, H2Conn, INPUT_STAGING_CAP, MAX_ROUTES, MAX_STATIC_BODY_BYTES,
    RouteBuildError, Server, ServerConfig,
    h2::{HEADER_BLOCK_CAP, HEADER_SLOTS, STREAM_SLOTS},
};

#[test]
fn graceful_drain_sends_goaway() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    server.stop_accepting();
    let (last, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    assert_eq!(last, 1);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 1);
}

#[test]
fn concurrent_h1_and_h2_traffic_on_both_ports() {
    let server = h2_server();
    let mut h1 = h1_connect(&server);
    let mut h2 = h2_connect(&server);
    h1.write_all(b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write h1 request");
    handshake(&mut h2);
    send_headers(&mut h2, 1, b"GET", b"/hello", true);
    let (status, body) = read_h1_body(&mut h1);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    let response = read_response(&mut h2, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let (_, code) = read_goaway(&mut h2);
    assert_eq!(code, ErrorCode::NoError);
    read_eof(&mut h2);
    read_eof(&mut h1);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 2);
}

#[test]
fn forced_stop_closes_promptly() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    server.stop_force();
    let counts = server.join().expect("join server");
    read_closed(&mut client);
    assert_eq!(counts.shutdown, 1);
    assert_eq!(counts.h2_served, 0);
}

#[test]
fn multi_worker_h2c_fans_out_and_joins() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        worker_count: 2,
        ..ServerConfig::default()
    };
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
    let server = server.start().expect("start server");
    let mut clients = Vec::new();
    for id in [1, 3, 5, 7] {
        let mut client = h2_connect(&server);
        handshake(&mut client);
        send_headers(&mut client, id, b"GET", b"/hello", true);
        let response = read_response(&mut client, id);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
        clients.push(client);
    }
    server.stop_accepting();
    for client in clients.iter_mut() {
        let (_, code) = read_goaway(client);
        assert_eq!(code, ErrorCode::NoError);
        read_eof(client);
    }
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 4);
    assert_eq!(counts.shutdown, 4);
}

#[test]
fn slot_reuse_across_ten_sequential_conns() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    config.timeouts.keep_alive_idle_ns = 100_000_000;
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
    let server = server.start().expect("start server");
    // NOTE: 10 sequential conns over 8 slots forces reuse; each generation is
    // synchronized by the server-sent GOAWAY+EOF (idle timeout), so the slot is
    // provably free server-side before the next connect. No sleeps: blocking
    // reads wait out the 100 ms idle bound.
    for _ in 0..10 {
        let mut client = h2_connect(&server);
        handshake(&mut client);
        send_headers(&mut client, 1, b"GET", b"/hello", true);
        let response = read_response(&mut client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
        let (_, code) = read_goaway(&mut client);
        assert_eq!(code, ErrorCode::NoError);
        read_eof(&mut client);
    }
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 10);
    assert_eq!(counts.timeout, 10);
}

#[test]
fn step_driver_rejects_bad_magic_direct() {
    let mut slots = [StreamSlot::EMPTY; STREAM_SLOTS];
    let mut header_block = [0u8; HEADER_BLOCK_CAP];
    let mut streams = StreamTable::new(
        &mut slots,
        H2Role::Server,
        &mut header_block,
        HEADER_BLOCK_CAP as u32,
    );
    let mut decoder_entries = [HpackEntrySlot::EMPTY; DECODE_ENTRIES];
    let mut decoder_scratch = [0u8; DECODE_BYTES];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; ENCODE_ENTRIES];
    let mut encoder_scratch = [0u8; ENCODE_BYTES];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut ack_limits = [0u32; ACK_SLOTS];
    let mut headers = [HpackHeader::EMPTY; HEADER_SLOTS];
    let mut conn = H2Conn::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut ack_limits,
        &mut headers,
    );
    conn.install_staging(Box::new([0; INPUT_STAGING_CAP]));
    conn.push_input(b"NOT-H2C-MAGIC-NOT-H2C-MAGIC!!!")
        .expect("stage garbage");
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    let mut output = [0u8; 1024];
    let outcome = conn.step(&mut output, &view);
    assert!(outcome.goaway);
    assert!(outcome.closed);
    assert!(outcome.written > 0);
    assert!(conn.is_closed());
}

#[test]
fn route_view_rejects_bad_input() {
    let bad_status = FrozenRoute {
        method: b"GET",
        path: b"/a",
        token: 0,
        status: 204,
        body: b"",
        gzip_body: None,
        managed: false,
        max_body_bytes: 0,
        file_root: None,
        trailers: &[],
    };
    assert_eq!(
        FrozenRoutesView::new(&[bad_status]).unwrap_err(),
        RouteBuildError::Invalid
    );
    let bad_method = FrozenRoute {
        method: b"GE T",
        path: b"/a",
        token: 0,
        status: 200,
        body: b"x",
        gzip_body: None,
        managed: false,
        max_body_bytes: 0,
        file_root: None,
        trailers: &[],
    };
    assert_eq!(
        FrozenRoutesView::new(&[bad_method]).unwrap_err(),
        RouteBuildError::Invalid
    );
    let big_body = FrozenRoute {
        method: b"GET",
        path: b"/a",
        token: 0,
        status: 200,
        body: &[0; 15 * 1024 + 1],
        gzip_body: None,
        managed: false,
        max_body_bytes: 0,
        file_root: None,
        trailers: &[],
    };
    assert_eq!(
        FrozenRoutesView::new(&[big_body]).unwrap_err(),
        RouteBuildError::BodyTooLarge
    );
    let first = FrozenRoute {
        method: b"GET",
        path: b"/a",
        token: 0,
        status: 200,
        body: b"a",
        gzip_body: None,
        managed: false,
        max_body_bytes: 0,
        file_root: None,
        trailers: &[],
    };
    let dupe = FrozenRoute {
        method: b"GET",
        path: b"/a",
        token: 0,
        status: 200,
        body: b"b",
        gzip_body: None,
        managed: false,
        max_body_bytes: 0,
        file_root: None,
        trailers: &[],
    };
    assert_eq!(
        FrozenRoutesView::new(&[first, dupe]).unwrap_err(),
        RouteBuildError::Duplicate
    );
    let mut paths = Vec::new();
    for index in 0..MAX_ROUTES + 1 {
        paths.push(format!("/r{index}").into_bytes());
    }
    let full: Vec<FrozenRoute<'_>> = paths
        .iter()
        .map(|path| FrozenRoute {
            method: b"GET",
            path,
            token: 0,
            status: 200,
            body: b"x",
            gzip_body: None,
            managed: false,
            max_body_bytes: 0,
            file_root: None,
            trailers: &[],
        })
        .collect();
    assert_eq!(
        FrozenRoutesView::new(&full).unwrap_err(),
        RouteBuildError::Full
    );
}

#[test]
fn h2_progressing_conn_after_h1_drain_stays_bound_by_grace() {
    // NOTE: once the last H1 connection closes mid-shutdown, the cancelled grace timer must
    // not leave a progressing H2 connection unforced. Body progress is stretched past the
    // join guard and drain linger is disabled, so only the grace bound can release the
    // worker. The keep-alive round trips synchronize the stop observation without sleeps: a
    // response is only produced in a turn after the previous response.
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        drain_idle_linger_ns: u64::MAX,
        timeouts: ConnectionTimeouts {
            body_progress_ns: 3_600_000_000_000,
            ..ConnectionTimeouts::production()
        },
        ..ServerConfig::default()
    };
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
    let server = server.start().expect("start server");
    let mut h1 = h1_connect(&server);
    h1.write_all(b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
        .expect("write h1 request");
    let (status, _) = read_h1_body(&mut h1);
    assert_eq!(status, 200);
    let mut h2 = h2_connect(&server);
    handshake(&mut h2);
    send_headers(&mut h2, 1, b"GET", b"/hello", true);
    let hello = read_response(&mut h2, 1);
    assert_eq!(hello.status, 200);
    server.stop_accepting();

    // NOTE: the worker runs two stop observations (a backlog turn, then begin_shutdown), and
    // each sequential keep-alive response crosses a turn boundary, so these round trips enter
    // Draining before H1 drops and then prove the drained server still serves.
    for _ in 0..3 {
        h1.write_all(b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n")
            .expect("write drain h1 request");
        let (status, body) = read_h1_body(&mut h1);
        assert_eq!(status, 200);
        assert_eq!(body, b"hello");
    }

    send_headers(&mut h2, 3, b"GET", b"/hello", true);
    let drained = read_response(&mut h2, 3);
    assert_eq!(drained.status, 200);

    drop(h1);

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done_tx.send(stop_join(server));
    });
    let counts = done_rx
        .recv_timeout(Duration::from_secs(25))
        .expect("join hung past grace");
    assert!(counts.h2_served >= 2, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn h2_ninth_concurrent_connection_closes_with_transport_error() {
    // NOTE: connection-ceiling pin beside the stream-ceiling pin: 8 slots hold 8
    // handshaked connections, so the 9th accept finds no free slot and the server
    // drops it (TransportError) instead of handshaking — fail-closed, counted.
    let server = managed_h2_server();
    let mut held = Vec::new();
    for _ in 0..veylor_server::MAX_H2_CONNS {
        let mut stream = h2_connect(&server);
        handshake(&mut stream);
        held.push(stream);
    }
    let mut ninth = h2_connect(&server);
    ninth
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("short timeout");
    assert!(
        try_read_frame(&mut ninth).is_none(),
        "9th connection must close without a frame"
    );
    let counts = stop_join(server);
    assert_eq!(counts.transport_error, 1, "counts: {counts:?}");
}
