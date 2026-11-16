use crate::frame_io::*;
use std::ffi::c_void;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};
use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, FrameType, H2Role, HpackEntrySlot, HpackHeader, HpackTable,
    SETTINGS_ACK, StreamSlot, StreamTable,
};
use veylor_runtime::ConnectionToken;
use veylor_server::{
    ACK_SLOTS, CloseCounts, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES, ENCODE_ENTRIES, EventSlot,
    FrozenRoute, FrozenRoutesView, H2_BODY_CAP, H2Conn, H2HostCtx, HostEventKind, HostHandle,
    HostShared, INPUT_STAGING_CAP, MAX_STATIC_BODY_BYTES, NativeRequestView, NativeResponseOut,
    RunningServer, Server, ServerConfig,
    h2::{FrozenNativeRoute, HEADER_BLOCK_CAP, HEADER_SLOTS, STREAM_SLOTS},
};

pub(crate) type TrailerFields = Vec<(Vec<u8>, Vec<u8>)>;

pub(crate) const DEADLINE: Duration = Duration::from_secs(5);

pub(crate) static ROUTES: [FrozenRoute<'static>; 2] = [
    FrozenRoute {
        method: b"GET",
        path: b"/hello",
        token: 0,
        status: 200,
        body: b"",
        gzip_body: None,
        managed: true,
        max_body_bytes: 32_768,
        file_root: None,
        trailers: &[],
    },
    FrozenRoute {
        method: b"GET",
        path: b"/empty",
        token: 0,
        status: 200,
        body: b"",
        gzip_body: None,
        managed: false,
        max_body_bytes: 0,
        file_root: None,
        trailers: &[],
    },
];

pub(crate) fn h2_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
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
    server
        .register_route(b"GET", b"/empty", 200, b"OK", b"", MAX_STATIC_BODY_BYTES)
        .expect("register empty");
    server.start().expect("start server")
}

pub(crate) fn h2_connect(server: &RunningServer) -> TcpStream {
    let addr = server.h2c_addr().expect("h2c enabled");
    let stream = TcpStream::connect(addr).expect("connect h2c loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
}

pub(crate) fn h1_connect(server: &RunningServer) -> TcpStream {
    let stream = TcpStream::connect(server.addr()).expect("connect h1 loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
}

pub(crate) fn stop_join(server: RunningServer) -> CloseCounts {
    server.stop_accepting();
    server.join().expect("join server")
}

// NOTE: host-dispatch rig for the H2 stream mapping: a loopback socketpair carries real H2C
// bytes, the test thread pumps the transport-neutral driver on the server side and drives the
// host handle directly (park/poll/respond/cancel), then applies staged work through the H2
// writers. No worker thread, no sleeps: every blocking read consumes already-sent bytes and
// every client read follows a server flush. Streams park into stream-keyed subslots, so
// siblings multiplex while one stream awaits the host.

pub(crate) fn host_base() -> ConnectionToken {
    ConnectionToken {
        worker_id: 0,
        slot: 0,
        request_seq: 0,
        stream_id: 0,
        worker_generation: 1,
        generation: 1,
    }
}

pub(crate) fn socketpair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind rig");
    let addr = listener.local_addr().expect("rig addr");
    let client = TcpStream::connect(addr).expect("rig connect");
    let (server, _) = listener.accept().expect("rig accept");
    client
        .set_read_timeout(Some(DEADLINE))
        .expect("client timeout");
    client
        .set_write_timeout(Some(DEADLINE))
        .expect("client timeout");
    server
        .set_read_timeout(Some(DEADLINE))
        .expect("server timeout");
    server
        .set_write_timeout(Some(DEADLINE))
        .expect("server timeout");
    (client, server)
}

pub(crate) fn with_rig(
    body: impl FnOnce(&mut H2Conn<'_>, &HostShared, &mut TcpStream, &mut TcpStream),
) {
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
    let shared = HostShared::new(1);
    let (mut client, mut server) = socketpair();
    body(&mut conn, &shared, &mut client, &mut server)
}

pub(crate) fn server_pump(
    conn: &mut H2Conn<'_>,
    server: &mut TcpStream,
    routes: &FrozenRoutesView<'_>,
    ctx: Option<H2HostCtx<'_>>,
) -> veylor_server::StepOutcome {
    let mut chunk = [0u8; 4096];
    let read = server.read(&mut chunk).expect("rig read");
    assert!(read > 0, "rig eof");
    conn.push_input(&chunk[..read]).expect("rig stage");
    let mut output = [0u8; 16384];
    let outcome = match ctx {
        Some(ctx) => conn.step_host(&mut output, routes, ctx),
        None => conn.step(&mut output, routes),
    };
    server
        .write_all(&output[..outcome.written])
        .expect("rig flush");
    if outcome.closed {
        server
            .shutdown(std::net::Shutdown::Both)
            .expect("rig shutdown");
    }
    outcome
}

pub(crate) fn pump_until(
    conn: &mut H2Conn<'_>,
    server: &mut TcpStream,
    routes: &FrozenRoutesView<'_>,
    ctx: Option<H2HostCtx<'_>>,
    mut done: impl FnMut(&veylor_server::StepOutcome) -> bool,
) -> veylor_server::StepOutcome {
    for _ in 0..64 {
        let outcome = server_pump(conn, server, routes, ctx);
        if done(&outcome) {
            return outcome;
        }
    }
    panic!("rig pump stalled");
}

pub(crate) fn rig_handshake(
    conn: &mut H2Conn<'_>,
    client: &mut TcpStream,
    server: &mut TcpStream,
    routes: &FrozenRoutesView<'_>,
) {
    client
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write magic");
    write_frame(client, FrameType::Settings, 0, 0, &[]);
    pump_until(conn, server, routes, None, |outcome| outcome.written > 0);
    let (header, _) = read_frame(client);
    assert_eq!(header.frame_type, FrameType::Settings);
    write_frame(client, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = read_frame(client);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);
    for _ in 0..64 {
        server_pump(conn, server, routes, None);
        if conn.handshake_complete() {
            break;
        }
    }
    assert!(conn.handshake_complete());
}

pub(crate) fn host_slot() -> EventSlot {
    EventSlot {
        kind: HostEventKind::Shutdown,
        token: host_base(),
        route_id: 0,
        route_token: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    }
}

pub(crate) fn poll_request_event(shared: &HostShared) -> (EventSlot, Vec<u8>, Vec<u8>) {
    let mut events = [host_slot(); 8];
    let mut methods = [0; 64];
    let mut paths = [0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    let event = events[0];
    assert_eq!(event.kind, HostEventKind::Request);
    let method = methods[event.method_offset..event.method_offset + event.method_len].to_vec();
    let path = paths[event.path_offset..event.path_offset + event.path_len].to_vec();
    (event, method, path)
}

// NOTE: managed-turn host tests: loopback H2C through the worker turn (the turn drives the
// pumps; no test-owned pumps on these paths). A managed route parks into the host mapping,
// the test thread polls/responds/cancels via HostHandle, and the turn applies staged work
// back onto the streams. Bounded event waits only (100ms polls under a 5s deadline), no sleeps.

pub(crate) fn managed_h2_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
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
    server
        .register_managed(b"GET", b"/work", MAX_STATIC_BODY_BYTES)
        .expect("register managed");
    server.start().expect("start server")
}

pub(crate) fn poll_h2_request(host: &HostHandle) -> (EventSlot, Vec<u8>, Vec<u8>) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let mut events = [host_slot(); 8];
        let mut methods = [0; 64];
        let mut paths = [0; 4096];
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::Request {
                let method =
                    methods[event.method_offset..event.method_offset + event.method_len].to_vec();
                let path = paths[event.path_offset..event.path_offset + event.path_len].to_vec();
                return (*event, method, path);
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting for request");
    }
}

pub(crate) fn poll_h2_requests(
    host: &HostHandle,
    count: usize,
) -> Vec<(EventSlot, Vec<u8>, Vec<u8>)> {
    let deadline = Instant::now() + DEADLINE;
    let mut out = Vec::new();
    while out.len() < count {
        let mut events = [host_slot(); 8];
        let mut methods = [0; 64];
        let mut paths = [0; 4096];
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::Request {
                let method =
                    methods[event.method_offset..event.method_offset + event.method_len].to_vec();
                let path = paths[event.path_offset..event.path_offset + event.path_len].to_vec();
                out.push((*event, method, path));
                if out.len() == count {
                    break;
                }
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting for requests");
    }
    out
}

// NOTE: managed H2 request-body tests: fixed-length POST bodies stage per parked stream,
// the host pulls them with pull-observed completion, and drained bytes reopen both flow
// windows (grant-on-consume). H2 has no chunked coding: END_STREAM on DATA frames an
// unknown-length body, which the managed route cap bounds while data arrives.

pub(crate) fn managed_h2_body_server() -> RunningServer {
    managed_h2_body_server_with_cap(H2_BODY_CAP)
}

pub(crate) fn managed_h2_body_server_with_cap(max_body_bytes: u64) -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
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
    server
        .register_managed(b"POST", b"/upload", max_body_bytes)
        .expect("register upload");
    server
        .register_managed(b"GET", b"/accounts/:id/statement", 0)
        .expect("register managed statement");
    server.start().expect("start server")
}

// NOTE: H2 native-handler tests: FrozenNativeRoute entries ride the frozen view beside
// static/managed kinds, and HEADERS-time dispatch invokes them through the shared
// transport-neutral invoke (same guard/panic/validation contract as H1). The rig pumps
// the transport-neutral driver directly, so no worker thread or host is involved.

pub(crate) static H2N_OK: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/native",
    handler: h2n_ok,
    user_data: 0,
    gzip: false,
}];

pub(crate) fn h2n_view(
    natives: &'static [FrozenNativeRoute<'static>],
) -> FrozenRoutesView<'static> {
    FrozenRoutesView::new(&ROUTES)
        .expect("frozen routes")
        .with_natives(natives)
        .expect("natives")
}

#[allow(unsafe_code)]
pub(crate) fn h2n_respond(
    response: *mut NativeResponseOut,
    status: i32,
    reason: &'static [u8],
    body: &'static [u8],
) -> i32 {
    if response.is_null() {
        return 1;
    }

    let out = unsafe { &mut *response };
    out.status = status;
    out.reason = reason.as_ptr();
    out.reason_len = reason.len();
    out.body = body.as_ptr();
    out.body_len = body.len();
    0
}

#[allow(unsafe_code)]
pub(crate) unsafe extern "C" fn h2n_ok(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 200, b"OK", b"native-hello")
}

#[allow(unsafe_code)]
pub(crate) unsafe extern "C" fn live_native_hello(
    _user_data: *mut c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }

    let out = unsafe { &mut *response };
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = b"h2-native".as_ptr();
    out.body_len = 9;
    0
}

pub(crate) fn file_mount_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("veylor-h2f-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("mount root");
    root
}

pub(crate) fn file_mount_server(root: &std::path::Path) -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_route(
            b"GET",
            b"/assets/exact",
            200,
            b"OK",
            b"static-wins",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register exact");
    server
        .register_file_mount(b"/assets/", root)
        .expect("register mount");
    server.start().expect("start server")
}
