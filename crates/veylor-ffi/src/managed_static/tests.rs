use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::time::Duration;

use super::*;
use crate::abi::{VylBytes, VylField, VylResult};

fn test_config(port: u16) -> VylManagedStaticConfig {
    VylManagedStaticConfig {
        abi_version: VYL_MANAGED_STATIC_ABI_VERSION,
        struct_size: std::mem::size_of::<VylManagedStaticConfig>(),
        request_head_timeout_ns: 5_000_000_000,
        body_progress_timeout_ns: 5_000_000_000,
        response_write_timeout_ns: 5_000_000_000,
        keep_alive_idle_timeout_ns: 30_000_000_000,
        shutdown_grace_ns: 5_000_000_000,
        drain_idle_linger_ns: 200_000_000,
        max_accepts_per_turn: 8,
        max_events_per_turn: 64,
        byte_budget_per_turn: 65_536,
        poll_wait_ms: 10,
        port,
        reserved: 0,
    }
}

fn bytes(data: &[u8]) -> VylBytes {
    VylBytes {
        data: data.as_ptr(),
        length: data.len(),
    }
}

unsafe fn create(config: &VylManagedStaticConfig) -> *mut VylManagedStaticServer {
    let mut server = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_managed_static_new(config, &mut server) },
        VylResult::Ok
    );
    assert!(!server.is_null());
    server
}

unsafe fn register(server: *mut VylManagedStaticServer, method: &[u8], path: &[u8], body: &[u8]) {
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route(
                server,
                bytes(method),
                bytes(path),
                200,
                bytes(b"OK"),
                bytes(body),
            )
        },
        VylResult::Ok
    );
}

unsafe fn register_capped(
    server: *mut VylManagedStaticServer,
    method: &[u8],
    path: &[u8],
    body: &[u8],
    cap: u64,
) {
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route_with_body_cap(
                server,
                bytes(method),
                bytes(path),
                200,
                bytes(b"OK"),
                bytes(body),
                cap,
            )
        },
        VylResult::Ok
    );
}

unsafe fn register_managed(
    server: *mut VylManagedStaticServer,
    method: &[u8],
    path: &[u8],
    cap: u64,
) {
    assert_eq!(
        unsafe {
            vyl_managed_static_register_managed_route(server, bytes(method), bytes(path), cap)
        },
        VylResult::Ok
    );
}

unsafe fn local_port(server: *mut VylManagedStaticServer) -> u16 {
    let mut port = 0;
    assert_eq!(
        unsafe { vyl_managed_static_local_addr(server, &mut port) },
        VylResult::Ok
    );
    assert_ne!(port, 0);
    port
}

fn get(port: u16, path: &str) -> String {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write request");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    String::from_utf8(response).expect("utf8 response")
}

unsafe fn stop_join(server: *mut VylManagedStaticServer) -> VylManagedStaticCloseCounts {
    assert_eq!(
        unsafe { vyl_managed_static_stop_accepting(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    counts
}

fn counts_total(counts: VylManagedStaticCloseCounts) -> u64 {
    counts.completed
        + counts.peer_closed
        + counts.reset
        + counts.timeout
        + counts.protocol_error
        + counts.response_overflow
        + counts.transport_error
        + counts.shutdown
}

#[test]
fn serves_registered_static_routes_and_reports_close_counts() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    unsafe { register(server, b"GET", b"/data", b"abc") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };

    let hello = get(port, "/hello");
    assert!(
        hello.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {hello:?}"
    );
    assert!(hello.ends_with("hello"), "unexpected body: {hello:?}");
    let data = get(port, "/data");
    assert!(
        data.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {data:?}"
    );
    assert!(data.ends_with("abc"), "unexpected body: {data:?}");
    let missing = get(port, "/missing");
    assert!(
        missing.starts_with("HTTP/1.1 404 Not Found"),
        "unexpected head: {missing:?}"
    );

    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 3);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn rejects_duplicate_routes_before_start() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route(
                server,
                bytes(b"GET"),
                bytes(b"/hello"),
                200,
                bytes(b"OK"),
                bytes(b"again"),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn rejects_registration_after_start_and_joins_cleanly() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route(
                server,
                bytes(b"GET"),
                bytes(b"/late"),
                200,
                bytes(b"OK"),
                bytes(b"late"),
            )
        },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(counts_total(counts), 0);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn rejects_invalid_configs_before_touching_anything() {
    let mut null_server = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_managed_static_new(std::ptr::null(), &mut null_server) },
        VylResult::InvalidArgument
    );
    assert!(null_server.is_null());
    let config = test_config(0);
    assert_eq!(
        unsafe { vyl_managed_static_new(&config, std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );

    let mut bad = test_config(0);
    bad.abi_version = VYL_MANAGED_STATIC_ABI_VERSION + 1;
    assert_eq!(
        unsafe { vyl_managed_static_new(&bad, &mut null_server) },
        VylResult::InvalidArgument
    );
    assert!(null_server.is_null());

    bad = test_config(0);
    bad.struct_size = 0;
    assert_eq!(
        unsafe { vyl_managed_static_new(&bad, &mut null_server) },
        VylResult::InvalidArgument
    );
    assert!(null_server.is_null());

    for mutator in [
        (|config: &mut VylManagedStaticConfig| config.request_head_timeout_ns = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.body_progress_timeout_ns = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.response_write_timeout_ns = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.keep_alive_idle_timeout_ns = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.max_accepts_per_turn = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.max_events_per_turn = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.byte_budget_per_turn = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.poll_wait_ms = 0)
            as fn(&mut VylManagedStaticConfig),
    ] {
        bad = test_config(0);
        mutator(&mut bad);
        assert_eq!(
            unsafe { vyl_managed_static_new(&bad, &mut null_server) },
            VylResult::InvalidArgument
        );
        assert!(null_server.is_null());
    }
}

#[test]
fn rejects_null_handles_and_bad_state_transitions() {
    assert_eq!(
        unsafe { vyl_managed_static_destroy(std::ptr::null_mut()) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_start(std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_stop_accepting(std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(std::ptr::null_mut(), &mut counts) },
        VylResult::InvalidArgument
    );

    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { vyl_managed_static_stop_accepting(server) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::BadState
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { vyl_managed_static_start(server) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_destroy(server) },
        VylResult::BadState
    );
    let joined = unsafe { stop_join(server) };
    assert_eq!(counts_total(joined), 0);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

fn post_exchange(stream: &mut TcpStream, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: test\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).expect("write head");
    stream.write_all(body).expect("write body");
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read head byte");
        raw.push(byte[0]);
        if raw.ends_with(b"\r\n\r\n".as_slice()) {
            break;
        }
    }
    let head_text = String::from_utf8(raw).expect("utf8 head");
    let status = head_text
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status int");
    let length = head_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.trim().eq_ignore_ascii_case("content-length") {
                value.trim().parse().ok()
            } else {
                None
            }
        })
        .expect("content length");
    let mut response_body = vec![0u8; length];
    stream.read_exact(&mut response_body).expect("read body");
    (status, response_body)
}

fn connect_loopback(port: u16) -> TcpStream {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    stream
}

const DISPATCH_TIMEOUT_NS: u64 = 5_000_000_000;

unsafe fn poll_into(
    server: *mut VylManagedStaticServer,
    timeout_ns: u64,
    events: &mut [VylManagedStaticEvent],
    methods: &mut [u8],
    paths: &mut [u8],
) -> VylManagedStaticPollReport {
    let mut report = VylManagedStaticPollReport::default();
    assert_eq!(
        unsafe {
            vyl_managed_static_poll_events(
                server,
                timeout_ns,
                events.as_mut_ptr(),
                events.len(),
                methods.as_mut_ptr(),
                methods.len(),
                paths.as_mut_ptr(),
                paths.len(),
                &mut report,
            )
        },
        VylResult::Ok
    );
    report
}

unsafe fn register_managed_with_trailers(
    server: *mut VylManagedStaticServer,
    method: &[u8],
    path: &[u8],
    cap: u64,
    trailers: &[VylBytes],
) -> VylResult {
    unsafe {
        super::vyl_managed_static_register_managed_route_with_trailers(
            server,
            bytes(method),
            bytes(path),
            cap,
            trailers.as_ptr(),
            trailers.len(),
        )
    }
}

unsafe fn pull_trailers_into(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    names: &mut [u8],
    values: &mut [u8],
) -> (VylResult, VylManagedStaticTrailerReport) {
    let mut report = VylManagedStaticTrailerReport::default();
    let result = unsafe {
        super::vyl_managed_static_host_pull_trailers(
            server,
            token,
            names.as_mut_ptr(),
            names.len(),
            values.as_mut_ptr(),
            values.len(),
            &mut report,
        )
    };
    (result, report)
}

fn trailer_bytes(names: &[&[u8]]) -> Vec<VylBytes> {
    names
        .iter()
        .map(|name| VylBytes {
            data: name.as_ptr(),
            length: name.len(),
        })
        .collect()
}

fn write_chunked(stream: &mut TcpStream, path: &str, chunks: &[&[u8]], trailers: &[(&str, &str)]) {
    let head = format!("POST {path} HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n");
    stream.write_all(head.as_bytes()).expect("write head");
    for chunk in chunks {
        stream
            .write_all(format!("{:X}\r\n", chunk.len()).as_bytes())
            .expect("write size");
        stream.write_all(chunk).expect("write chunk");
        stream.write_all(b"\r\n").expect("write end");
    }
    stream.write_all(b"0\r\n").expect("write terminal");
    for (name, value) in trailers {
        stream
            .write_all(format!("{name}: {value}\r\n").as_bytes())
            .expect("write trailer");
    }
    stream.write_all(b"\r\n").expect("write close");
}

unsafe fn pull_into(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    out: &mut [u8],
) -> (VylResult, usize, u32) {
    let mut pulled = 0usize;
    let mut end = 0u32;
    let result = unsafe {
        vyl_managed_static_host_pull_body(
            server,
            token,
            out.as_mut_ptr(),
            out.len(),
            &mut pulled,
            &mut end,
        )
    };
    (result, pulled, end)
}

unsafe fn drain_body(server: *mut VylManagedStaticServer, token: VylManagedStaticToken) -> Vec<u8> {
    let mut staged = Vec::new();
    let mut chunk = [0u8; 16];
    loop {
        let (result, pulled, end) = unsafe { pull_into(server, token, &mut chunk) };
        assert_eq!(result, VylResult::Ok);
        staged.extend_from_slice(&chunk[..pulled]);
        if end == 1 {
            return staged;
        }
    }
}

unsafe fn poll_one(server: *mut VylManagedStaticServer) -> VylManagedStaticToken {
    let mut events = [VylManagedStaticEvent::default(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    let report = unsafe {
        poll_into(
            server,
            DISPATCH_TIMEOUT_NS,
            &mut events,
            &mut methods,
            &mut paths,
        )
    };
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, VYL_MANAGED_STATIC_EVENT_REQUEST);
    events[0].token
}

unsafe fn is_writable(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) -> (VylResult, u32) {
    let mut writable = 0u32;
    let result = unsafe { vyl_managed_static_host_is_writable(server, token, &mut writable) };
    (result, writable)
}

unsafe fn wait_stream_writable(server: *mut VylManagedStaticServer, token: VylManagedStaticToken) {
    for _ in 0..500 {
        let (result, writable) = unsafe { is_writable(server, token) };
        assert_eq!(result, VylResult::Ok);
        if writable == 1 {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("stream never armed writable");
}

fn read_streamed(stream: &mut TcpStream) -> (u16, Vec<u8>, Vec<u8>) {
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read head byte");
        raw.push(byte[0]);
        if raw.ends_with(b"\r\n\r\n".as_slice()) {
            break;
        }
        assert!(raw.len() < 65536, "head overflow");
    }
    let head_text = String::from_utf8(raw.clone()).expect("utf8 head");
    let status = head_text
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status int");
    assert!(
        head_text.contains("Transfer-Encoding: chunked"),
        "stream head must be chunked: {head_text:?}"
    );
    let mut body = Vec::new();
    loop {
        let mut line = Vec::new();
        loop {
            stream.read_exact(&mut byte).expect("read chunk size");
            line.push(byte[0]);
            if line.ends_with(b"\r\n".as_slice()) {
                break;
            }
        }
        raw.extend_from_slice(&line);
        let size = usize::from_str_radix(
            std::str::from_utf8(&line[..line.len() - 2]).expect("size utf8"),
            16,
        )
        .expect("size hex");
        if size == 0 {
            let mut crlf = [0u8; 2];
            stream.read_exact(&mut crlf).expect("final crlf");
            assert_eq!(&crlf, b"\r\n");
            raw.extend_from_slice(&crlf);
            break;
        }
        let mut chunk = vec![0u8; size];
        stream.read_exact(&mut chunk).expect("chunk bytes");
        raw.extend_from_slice(&chunk);
        let mut crlf = [0u8; 2];
        stream.read_exact(&mut crlf).expect("chunk crlf");
        assert_eq!(&crlf, b"\r\n");
        raw.extend_from_slice(&crlf);
        body.extend_from_slice(&chunk);
    }
    (status, raw, body)
}

fn read_stream_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
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

fn read_stream_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    while !line.ends_with(b"\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => panic!("eof mid-chunk-size"),
            Ok(_) => line.push(byte[0]),
            Err(error) => panic!("read chunk size: {error:?}"),
        }
    }
    let size = usize::from_str_radix(
        std::str::from_utf8(&line[..line.len() - 2]).expect("size utf8"),
        16,
    )
    .expect("size hex");
    if size == 0 {
        let mut crlf = [0u8; 2];
        stream.read_exact(&mut crlf).expect("final crlf");
        assert_eq!(&crlf, b"\r\n");
        return None;
    }
    let mut chunk = vec![0u8; size];
    stream.read_exact(&mut chunk).expect("chunk bytes");
    let mut crlf = [0u8; 2];
    stream.read_exact(&mut crlf).expect("chunk crlf");
    assert_eq!(&crlf, b"\r\n");
    Some(chunk)
}

#[test]
fn stream_begin_writes_end_exact_framing() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"early")) },
        VylResult::BadState,
        "write with no open stream rejects"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Duplicate,
        "second begin loses to the open stream"
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"hello ")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"world")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Ok
    );
    let (status, raw, body) = read_streamed(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello world");
    assert_eq!(
        raw,
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Gone,
        "post-end ops observe Gone after unpark"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"late")) },
        VylResult::Gone
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Gone
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_pacing_rejects_unarmed_then_admits() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"")) },
        VylResult::InvalidArgument,
        "empty chunk rejects"
    );
    let oversized = vec![b'x'; 16_384 + 1];
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(&oversized)) },
        VylResult::BodyTooLong,
        "chunk past 16 KiB rejects"
    );
    let unframeable = vec![b'x'; 9000];
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(&unframeable)) },
        VylResult::BodyTooLong,
        "chunk whose frame cannot fit the output rejects at stage"
    );
    let unknown = VylManagedStaticToken {
        request_seq: 9999,
        ..token
    };
    assert_eq!(
        unsafe { is_writable(server, unknown) },
        (VylResult::Ok, 0),
        "unknown-but-wellformed token answers not-writable"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, unknown, bytes(b"early")) },
        VylResult::BadState,
        "write with no token rejects"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"late")) },
        VylResult::Ok
    );
    // NOTE: the queued window is transient by design, so the test loops until the queue is
    // observed: every iteration is either an admission or the expected rejection, and admitted
    // bytes are tracked so the wire body stays exact.
    let mut agains = 0u32;
    loop {
        match unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"again")) } {
            VylResult::Ok => agains += 1,
            VylResult::WouldBlock | VylResult::BadState => break,
            other => panic!("unexpected write outcome: {other:?}"),
        }
        assert!(agains < 1000, "worker outran the stage loop");
    }
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"!")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Ok
    );
    let (status, _, body) = read_streamed(&mut stream);
    assert_eq!(status, 200);
    let mut expected = b"late".to_vec();
    for _ in 0..agains {
        expected.extend_from_slice(b"again");
    }
    expected.extend_from_slice(b"!");
    assert_eq!(body, expected);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_end_twice_reports_duplicate() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Ok
    );
    // NOTE: the queued end applies within microseconds, so the immediate second end wins the race
    // in practice: a queued end reports duplicate, an applied one gone. The terminal-chunk read
    // below proves the apply, pinning the later gone assertions either way.
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Duplicate,
        "queued end reports duplicate"
    );
    let (status, raw, body) = read_streamed(&mut stream);
    assert_eq!(status, 200);
    assert!(body.is_empty());
    assert!(
        raw.ends_with(b"0\r\n\r\n"),
        "stream ends with terminal chunk"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Gone
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"late")) },
        VylResult::Gone
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_cancel_mid_stream_closes() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"partial")) },
        VylResult::Ok
    );
    let head = read_stream_head(&mut stream);
    let text = String::from_utf8(head).expect("head utf8");
    assert!(
        text.starts_with("HTTP/1.1 200 OK"),
        "stream head sent: {text}"
    );
    assert_eq!(
        read_stream_frame(&mut stream),
        Some(b"partial".to_vec()),
        "first chunk flushes before cancel"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_cancel(server, token) },
        VylResult::Ok
    );
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).expect("read close");
    assert!(rest.is_empty(), "cancel must close without tail: {rest:?}");
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"late")) },
        VylResult::Gone,
        "cancelled stream tombs Gone"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Gone
    );
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_begin_rejects_no_body_status_without_staging() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 204, bytes(b"No Content")) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 304, bytes(b"Not Modified")) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"bad\r\nreason"))
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"")) },
        VylResult::InvalidArgument,
        "empty reason rejects"
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"fallback"))
        },
        VylResult::Ok,
        "rejected begins stage nothing"
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"fallback");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_rejects_bad_state_and_null_inputs() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    let zero_token = VylManagedStaticToken::default();
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, zero_token, 200, bytes(b"OK")) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, zero_token, bytes(b"x")) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, zero_token) },
        VylResult::BadState
    );
    let mut writable = 0u32;
    assert_eq!(
        unsafe { vyl_managed_static_host_is_writable(server, zero_token, &mut writable) },
        VylResult::BadState
    );
    assert_eq!(writable, 0);
    assert_eq!(
        unsafe {
            vyl_managed_static_host_begin_stream(
                std::ptr::null_mut(),
                zero_token,
                200,
                bytes(b"OK"),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_write_stream(std::ptr::null_mut(), zero_token, bytes(b"x"))
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(std::ptr::null_mut(), zero_token) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_is_writable(std::ptr::null_mut(), zero_token, &mut writable)
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_begin_stream(
                server,
                zero_token,
                200,
                VylBytes {
                    data: std::ptr::null(),
                    length: 1,
                },
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_write_stream(
                server,
                zero_token,
                VylBytes {
                    data: std::ptr::null(),
                    length: 1,
                },
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_is_writable(server, zero_token, std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { vyl_managed_static_host_is_writable(server, zero_token, &mut writable) },
        VylResult::BadState,
        "zero-generation token is a shape problem, never Ok(false)"
    );
    assert_eq!(writable, 0);
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, zero_token, 200, bytes(b"OK")) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, zero_token, bytes(b"x")) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, zero_token) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_is_writable(server, zero_token, &mut writable) },
        VylResult::BadState
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn trailers_rejects_bad_allowlists_before_start() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let many = vec![bytes(b"x"); 33];
    assert_eq!(
        unsafe {
            super::vyl_managed_static_register_managed_route_with_trailers(
                server,
                bytes(b"POST"),
                bytes(b"/many"),
                u64::MAX,
                many.as_ptr(),
                many.len(),
            )
        },
        VylResult::InvalidArgument
    );
    let long = vec![b'x'; 129];
    let overlong = trailer_bytes(&[&long]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/long", u64::MAX, &overlong) },
        VylResult::InvalidArgument
    );
    let empty = trailer_bytes(&[b""]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/empty", u64::MAX, &empty) },
        VylResult::InvalidArgument
    );
    let dupe = trailer_bytes(&[b"X-Tag", b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/dupe", u64::MAX, &dupe) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::vyl_managed_static_register_managed_route_with_trailers(
                server,
                bytes(b"POST"),
                bytes(b"/null"),
                u64::MAX,
                std::ptr::null(),
                1,
            )
        },
        VylResult::InvalidArgument
    );
    let one = trailer_bytes(&[b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/t", u64::MAX, &one) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/t", u64::MAX, &one) },
        VylResult::InvalidArgument
    );
    unsafe { register_managed(server, b"POST", b"/plain", u64::MAX) };
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/plain", u64::MAX, &one) },
        VylResult::InvalidArgument
    );
    unsafe { register(server, b"POST", b"/static", b"x") };
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/static", u64::MAX, &one) },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/late", u64::MAX, &one) },
        VylResult::BadState
    );
    let port = unsafe { local_port(server) };
    let _ = connect_loopback(port);
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn trailers_copy_whole_set_with_capacity_retry() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let allow = trailer_bytes(&[b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/t", u64::MAX, &allow) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    write_chunked(&mut stream, "/t", &[b"hello"], &[("X-Tag", "v1")]);
    let token = unsafe { poll_one(server) };
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    let mut names = [0u8; 2];
    let mut values = [0u8; 2];
    let (short, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(short, VylResult::Ok);
    assert_eq!(report.fields, 1);
    assert_eq!((report.names_copied, report.values_copied), (0, 0));
    assert_eq!((report.names_required, report.values_required), (5, 2));
    assert_eq!((names, values), ([0u8; 2], [0u8; 2]));
    let mut names = [0u8; 5];
    let mut values = [0u8; 2];
    let (full, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(full, VylResult::Ok);
    assert_eq!(report.fields, 1);
    assert_eq!((report.names_copied, report.values_copied), (5, 2));
    assert_eq!((report.names_required, report.values_required), (0, 0));
    assert_eq!(&names, b"X-Tag");
    assert_eq!(&values, b"v1");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"stored"))
        },
        VylResult::Ok
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"stored");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn trailers_report_would_block_before_end_of_body() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let allow = trailer_bytes(&[b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/n", u64::MAX, &allow) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(
            b"POST /n HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n",
        )
        .expect("write partial");
    let token = unsafe { poll_one(server) };
    let mut names = [0xAAu8; 8];
    let mut values = [0xAAu8; 8];
    let (early, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(early, VylResult::WouldBlock);
    assert_eq!(report, VylManagedStaticTrailerReport::default());
    assert_eq!((names, values), ([0xAAu8; 8], [0xAAu8; 8]));
    stream
        .write_all(b"0\r\nX-Tag: v1\r\n\r\n")
        .expect("write terminal");
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    let (ready, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(ready, VylResult::Ok);
    assert_eq!(report.fields, 1);
    assert_eq!(&names[..report.names_copied], b"X-Tag");
    assert_eq!(&values[..report.values_copied], b"v1");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"done");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn trailers_empty_allowlist_reports_empty_set() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            super::vyl_managed_static_register_managed_route_with_trailers(
                server,
                bytes(b"POST"),
                bytes(b"/e"),
                u64::MAX,
                std::ptr::null(),
                0,
            )
        },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    write_chunked(&mut stream, "/e", &[b"hello"], &[]);
    let token = unsafe { poll_one(server) };
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    let mut names = [0u8; 8];
    let mut values = [0u8; 8];
    let (result, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(result, VylResult::Ok);
    assert_eq!(report.fields, 0);
    assert_eq!((report.names_copied, report.values_copied), (0, 0));
    assert_eq!((report.names_required, report.values_required), (0, 0));
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"empty"))
        },
        VylResult::Ok
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"empty");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn trailers_unlisted_field_closes_connection() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let allow = trailer_bytes(&[b"x-ok"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/u", u64::MAX, &allow) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"POST /u HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n")
        .expect("write head");
    let token = unsafe { poll_one(server) };
    stream
        .write_all(b"5\r\nhello\r\n0\r\nX-Evil: 1\r\n\r\n")
        .expect("write unlisted");
    let mut observed = VylResult::Ok;
    for _ in 0..250 {
        let mut names = [0u8; 8];
        let mut values = [0u8; 8];
        let (result, _) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
        if result == VylResult::Gone {
            observed = result;
            break;
        }
        assert_eq!(result, VylResult::WouldBlock);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(observed, VylResult::Gone);
    let mut scratch = [0u8; 8];
    let (pull_result, _, _) = unsafe { pull_into(server, token, &mut scratch) };
    assert_eq!(pull_result, VylResult::Gone);
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).expect("read close");
    assert!(
        rest.is_empty(),
        "refused trailer must close without bytes: {rest:?}"
    );
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn trailers_rejects_bad_pull_inputs_and_state() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let allow = trailer_bytes(&[b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/t", u64::MAX, &allow) },
        VylResult::Ok
    );
    let zero_token = VylManagedStaticToken::default();
    let mut names = [0u8; 8];
    let mut values = [0u8; 8];
    let mut report = VylManagedStaticTrailerReport::default();
    assert_eq!(
        unsafe {
            super::vyl_managed_static_host_pull_trailers(
                server,
                zero_token,
                names.as_mut_ptr(),
                names.len(),
                values.as_mut_ptr(),
                values.len(),
                &mut report,
            )
        },
        VylResult::BadState
    );
    assert_eq!(
        unsafe {
            super::vyl_managed_static_host_pull_trailers(
                std::ptr::null_mut(),
                zero_token,
                names.as_mut_ptr(),
                names.len(),
                values.as_mut_ptr(),
                values.len(),
                &mut report,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::vyl_managed_static_host_pull_trailers(
                server,
                zero_token,
                names.as_mut_ptr(),
                names.len(),
                values.as_mut_ptr(),
                values.len(),
                std::ptr::null_mut(),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::vyl_managed_static_host_pull_trailers(
                server,
                zero_token,
                std::ptr::null_mut(),
                1,
                values.as_mut_ptr(),
                values.len(),
                &mut report,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::vyl_managed_static_host_pull_trailers(
                server,
                zero_token,
                names.as_mut_ptr(),
                names.len(),
                std::ptr::null_mut(),
                1,
                &mut report,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

fn read_response(stream: &mut TcpStream) -> (u16, Vec<u8>) {
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read head byte");
        raw.push(byte[0]);
        if raw.ends_with(b"\r\n\r\n".as_slice()) {
            break;
        }
    }
    let head_text = String::from_utf8(raw).expect("utf8 head");
    let status = head_text
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status int");
    let length = head_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.trim().eq_ignore_ascii_case("content-length") {
                value.trim().parse().ok()
            } else {
                None
            }
        })
        .expect("content length");
    let mut response_body = vec![0u8; length];
    stream.read_exact(&mut response_body).expect("read body");
    (status, response_body)
}

#[test]
fn dispatch_round_trip_reuse_and_duplicate_replay() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/m", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    let mut events = [VylManagedStaticEvent::default(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    for expected in [b"first".as_slice(), b"second".as_slice()] {
        let request = b"GET /m HTTP/1.1\r\nHost: test\r\n\r\n";
        stream.write_all(request).expect("write request");
        let report = unsafe {
            poll_into(
                server,
                DISPATCH_TIMEOUT_NS,
                &mut events,
                &mut methods,
                &mut paths,
            )
        };
        assert_eq!(report.copied, 1);
        assert_eq!(events[0].kind, VYL_MANAGED_STATIC_EVENT_REQUEST);
        let method =
            &methods[events[0].method_offset..events[0].method_offset + events[0].method_len];
        assert_eq!(method, b"GET");
        let path = &paths[events[0].path_offset..events[0].path_offset + events[0].path_len];
        assert_eq!(path, b"/m");
        let token = events[0].token;
        assert_eq!(
            unsafe {
                vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(expected))
            },
            VylResult::Ok
        );
        let (status, body) = read_response(&mut stream);
        assert_eq!(status, 200);
        assert_eq!(body, expected);
        assert_eq!(
            unsafe {
                vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"again"))
            },
            VylResult::Duplicate
        );
        let mut scratch = [0u8; 8];
        let (pull_result, _, _) = unsafe { pull_into(server, token, &mut scratch) };
        assert_eq!(pull_result, VylResult::Duplicate);
    }
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn dispatch_rejects_invalid_answers_without_consuming_one_shot() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/m", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /m HTTP/1.1\r\nHost: test\r\n\r\n")
        .expect("write request");
    let mut events = [VylManagedStaticEvent::default(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    let report = unsafe {
        poll_into(
            server,
            DISPATCH_TIMEOUT_NS,
            &mut events,
            &mut methods,
            &mut paths,
        )
    };
    assert_eq!(report.copied, 1);
    let token = events[0].token;
    assert_eq!(
        unsafe { vyl_managed_static_host_respond(server, token, 99, bytes(b"OK"), bytes(b"x")) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_respond(server, token, 200, bytes(b""), bytes(b"x")) },
        VylResult::InvalidArgument
    );
    let oversized = vec![b'x'; 4096 + 1];
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(&oversized))
        },
        VylResult::BodyTooLong
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"kept"))
        },
        VylResult::Ok
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"kept");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn dispatch_cancel_closes_parked_and_reports_stale_after() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/c", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /c HTTP/1.1\r\nHost: test\r\n\r\n")
        .expect("write request");
    let mut events = [VylManagedStaticEvent::default(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    let report = unsafe {
        poll_into(
            server,
            DISPATCH_TIMEOUT_NS,
            &mut events,
            &mut methods,
            &mut paths,
        )
    };
    assert_eq!(report.copied, 1);
    let token = events[0].token;
    assert_eq!(
        unsafe { vyl_managed_static_host_cancel(server, token) },
        VylResult::Ok
    );
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).expect("read close");
    assert!(rest.is_empty(), "cancel must close without bytes: {rest:?}");
    assert_eq!(
        unsafe { vyl_managed_static_host_cancel(server, token) },
        VylResult::StaleToken
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"x")) },
        VylResult::StaleToken
    );
    let mut scratch = [0u8; 8];
    let (pull_result, _, _) = unsafe { pull_into(server, token, &mut scratch) };
    assert_eq!(pull_result, VylResult::StaleToken);
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn dispatch_peer_close_reports_gone() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/g", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /g HTTP/1.1\r\nHost: test\r\n\r\n")
        .expect("write request");
    let mut events = [VylManagedStaticEvent::default(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    let report = unsafe {
        poll_into(
            server,
            DISPATCH_TIMEOUT_NS,
            &mut events,
            &mut methods,
            &mut paths,
        )
    };
    assert_eq!(report.copied, 1);
    let token = events[0].token;
    drop(stream);
    let mut observed = VylResult::Ok;
    for _ in 0..250 {
        let mut pulled = 0usize;
        let mut end = 0u32;
        observed = unsafe {
            vyl_managed_static_host_pull_body(
                server,
                token,
                std::ptr::null_mut(),
                0,
                &mut pulled,
                &mut end,
            )
        };
        if observed == VylResult::Gone {
            break;
        }
        assert_eq!(observed, VylResult::Ok);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(observed, VylResult::Gone);
    assert_eq!(
        unsafe { vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"x")) },
        VylResult::Gone
    );
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn dispatch_pull_body_partial_copies_and_completion_query() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/upload", 64) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 11\r\n\r\nhello world")
        .expect("write request");
    let mut events = [VylManagedStaticEvent::default(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    let report = unsafe {
        poll_into(
            server,
            DISPATCH_TIMEOUT_NS,
            &mut events,
            &mut methods,
            &mut paths,
        )
    };
    assert_eq!(report.copied, 1);
    let token = events[0].token;
    let mut staged = Vec::new();
    let mut chunk = [0u8; 4];
    let (first, pulled, end) = unsafe { pull_into(server, token, &mut chunk) };
    assert_eq!((first, pulled, end), (VylResult::Ok, 4, 0));
    staged.extend_from_slice(&chunk[..pulled]);
    let (second, pulled, end) = unsafe { pull_into(server, token, &mut chunk) };
    assert_eq!((second, pulled, end), (VylResult::Ok, 4, 0));
    staged.extend_from_slice(&chunk[..pulled]);
    let (third, pulled, end) = unsafe { pull_into(server, token, &mut chunk) };
    assert_eq!((third, pulled, end), (VylResult::Ok, 3, 1));
    staged.extend_from_slice(&chunk[..pulled]);
    assert_eq!(staged, b"hello world");
    let mut pulled = 0usize;
    let mut end = 0u32;
    assert_eq!(
        unsafe {
            vyl_managed_static_host_pull_body(
                server,
                token,
                std::ptr::null_mut(),
                0,
                &mut pulled,
                &mut end,
            )
        },
        VylResult::Ok
    );
    assert_eq!((pulled, end), (0, 1));
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"stored"))
        },
        VylResult::Ok
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"stored");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn dispatch_shutdown_event_arrives_after_stop() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { vyl_managed_static_stop_accepting(server) },
        VylResult::Ok
    );
    let mut events = [VylManagedStaticEvent::default(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    let report = unsafe {
        poll_into(
            server,
            DISPATCH_TIMEOUT_NS,
            &mut events,
            &mut methods,
            &mut paths,
        )
    };
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, VYL_MANAGED_STATIC_EVENT_SHUTDOWN);
    assert_eq!(events[0].method_len, 0);
    assert_eq!(events[0].path_len, 0);
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_poll_events(
                server,
                0,
                events.as_mut_ptr(),
                8,
                methods.as_mut_ptr(),
                256,
                paths.as_mut_ptr(),
                4096,
                &mut VylManagedStaticPollReport::default(),
            )
        },
        VylResult::BadState
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn dispatch_rejects_bad_state_and_null_inputs() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/m", u64::MAX) };
    let mut events = [VylManagedStaticEvent::default(); 1];
    let mut methods = [0u8; 16];
    let mut paths = [0u8; 64];
    let mut report = VylManagedStaticPollReport::default();
    let zero_token = VylManagedStaticToken::default();
    assert_eq!(
        unsafe {
            vyl_managed_static_poll_events(
                server,
                0,
                events.as_mut_ptr(),
                events.len(),
                methods.as_mut_ptr(),
                methods.len(),
                paths.as_mut_ptr(),
                paths.len(),
                &mut report,
            )
        },
        VylResult::BadState
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, zero_token, 200, bytes(b"OK"), bytes(b"x"))
        },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_cancel(server, zero_token) },
        VylResult::BadState
    );
    let mut pulled = 0usize;
    let mut end = 0u32;
    assert_eq!(
        unsafe {
            vyl_managed_static_host_pull_body(
                server,
                zero_token,
                methods.as_mut_ptr(),
                methods.len(),
                &mut pulled,
                &mut end,
            )
        },
        VylResult::BadState
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_poll_events(
                std::ptr::null_mut(),
                0,
                events.as_mut_ptr(),
                events.len(),
                methods.as_mut_ptr(),
                methods.len(),
                paths.as_mut_ptr(),
                paths.len(),
                &mut report,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_poll_events(
                server,
                0,
                events.as_mut_ptr(),
                events.len(),
                methods.as_mut_ptr(),
                methods.len(),
                paths.as_mut_ptr(),
                paths.len(),
                std::ptr::null_mut(),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_poll_events(
                server,
                0,
                std::ptr::null_mut(),
                1,
                methods.as_mut_ptr(),
                methods.len(),
                paths.as_mut_ptr(),
                paths.len(),
                &mut report,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(
                std::ptr::null_mut(),
                zero_token,
                200,
                bytes(b"OK"),
                bytes(b"x"),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_cancel(std::ptr::null_mut(), zero_token) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_pull_body(
                std::ptr::null_mut(),
                zero_token,
                methods.as_mut_ptr(),
                methods.len(),
                &mut pulled,
                &mut end,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_pull_body(
                server,
                zero_token,
                methods.as_mut_ptr(),
                methods.len(),
                std::ptr::null_mut(),
                &mut end,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn tight_cap_drains_at_cap_and_rejects_one_over() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_capped(server, b"POST", b"/tight", b"tight", 16) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    let (status, body) = post_exchange(&mut stream, "/tight", &[b'a'; 16]);
    assert_eq!(status, 200);
    assert_eq!(body, b"tight");
    let (reuse_status, reuse_body) = post_exchange(&mut stream, "/tight", &[]);
    assert_eq!(reuse_status, 200);
    assert_eq!(reuse_body, b"tight");
    let mut over = connect_loopback(port);
    let head =
        "POST /tight HTTP/1.1\r\nHost: test\r\nContent-Length: 17\r\nConnection: close\r\n\r\n";
    over.write_all(head.as_bytes()).expect("write head");
    let mut rejected = Vec::new();
    over.read_to_end(&mut rejected).expect("read rejection");
    let rejected = String::from_utf8(rejected).expect("utf8 rejection");
    assert!(
        rejected.starts_with("HTTP/1.1 413"),
        "unexpected head: {rejected:?}"
    );
    assert!(rejected.contains("Payload Too Large"));
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn managed_cap_rejects_over_cap_and_parks_under_cap() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/mupload", 16) };
    assert_eq!(
        unsafe {
            vyl_managed_static_register_managed_route(
                server,
                bytes(b"POST"),
                bytes(b"/mupload"),
                16,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route(
                server,
                bytes(b"POST"),
                bytes(b"/mupload"),
                200,
                bytes(b"OK"),
                bytes(b"x"),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut over = connect_loopback(port);
    let head =
        "POST /mupload HTTP/1.1\r\nHost: test\r\nContent-Length: 17\r\nConnection: close\r\n\r\n";
    over.write_all(head.as_bytes()).expect("write head");
    let mut rejected = Vec::new();
    over.read_to_end(&mut rejected).expect("read rejection");
    let rejected = String::from_utf8(rejected).expect("utf8 rejection");
    assert!(
        rejected.starts_with("HTTP/1.1 413"),
        "unexpected head: {rejected:?}"
    );
    // NOTE: this ABI slice exposes no host-dispatch calls, so an under-cap managed request
    // parks with no response bytes until the 5 s handler deadline; silence here proves the
    // park, where a rejection would answer immediately.
    let mut under = connect_loopback(port);
    under
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("read timeout");
    let head = "POST /mupload HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\n";
    under.write_all(head.as_bytes()).expect("write head");
    under.write_all(b"hello").expect("write body");
    let mut probe = [0u8; 1];
    let parked = under.read(&mut probe);
    assert!(
        matches!(&parked, Err(error) if matches!(error.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock)),
        "under-cap request must park, got {parked:?}"
    );
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

fn field(name: &[u8], value: &[u8]) -> VylField {
    VylField {
        name: bytes(name),
        value: bytes(value),
    }
}

unsafe fn end_with_trailers(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    trailers: &[VylField],
) -> VylResult {
    unsafe {
        vyl_managed_static_host_end_stream_with_trailers(
            server,
            token,
            trailers.as_ptr(),
            trailers.len(),
        )
    }
}

fn read_trailer_terminated(stream: &mut TcpStream) -> (u16, Vec<u8>, Vec<u8>, Vec<u8>) {
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read head byte");
        raw.push(byte[0]);
        if raw.ends_with(b"\r\n\r\n".as_slice()) {
            break;
        }
        assert!(raw.len() < 65536, "head overflow");
    }
    let head_text = String::from_utf8(raw.clone()).expect("utf8 head");
    let status = head_text
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status int");
    assert!(
        head_text.contains("Transfer-Encoding: chunked"),
        "stream head must be chunked: {head_text:?}"
    );
    let mut body = Vec::new();
    let mut terminal = Vec::new();
    loop {
        let mut line = Vec::new();
        loop {
            stream.read_exact(&mut byte).expect("read chunk size");
            line.push(byte[0]);
            if line.ends_with(b"\r\n".as_slice()) {
                break;
            }
        }
        raw.extend_from_slice(&line);
        let size = usize::from_str_radix(
            std::str::from_utf8(&line[..line.len() - 2]).expect("size utf8"),
            16,
        )
        .expect("size hex");
        if size == 0 {
            loop {
                let mut field_line = Vec::new();
                loop {
                    stream.read_exact(&mut byte).expect("read trailer line");
                    field_line.push(byte[0]);
                    if field_line.ends_with(b"\r\n".as_slice()) {
                        break;
                    }
                }
                raw.extend_from_slice(&field_line);
                if field_line == b"\r\n" {
                    break;
                }
                terminal.extend_from_slice(&field_line);
            }
            break;
        }
        let mut chunk = vec![0u8; size];
        stream.read_exact(&mut chunk).expect("chunk bytes");
        raw.extend_from_slice(&chunk);
        let mut crlf = [0u8; 2];
        stream.read_exact(&mut crlf).expect("chunk crlf");
        assert_eq!(&crlf, b"\r\n");
        raw.extend_from_slice(&crlf);
        body.extend_from_slice(&chunk);
    }
    (status, raw, body, terminal)
}

#[test]
fn stream_end_with_trailers_exact_framing_and_reuse() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"hello ")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"world")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    let trailers = [field(b"x-checksum", b"abc"), field(b"x-tag", b"v1")];
    assert_eq!(
        unsafe { end_with_trailers(server, token, &trailers) },
        VylResult::Ok
    );
    assert!(
        matches!(
            unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"late")) },
            VylResult::WouldBlock | VylResult::BadState | VylResult::Gone
        ),
        "write after trailer end fails closed while queued"
    );
    let (status, raw, body, terminal) = read_trailer_terminated(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello world");
    assert_eq!(terminal, b"x-checksum: abc\r\nx-tag: v1\r\n");
    assert_eq!(
        raw,
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nhello \r\n5\r\nworld\r\n0\r\nx-checksum: abc\r\nx-tag: v1\r\n\r\n"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"late")) },
        VylResult::Gone
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Gone
    );
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("reuse");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Ok
    );
    let (status, raw, body) = read_streamed(&mut stream);
    assert_eq!(status, 200);
    assert!(body.is_empty());
    assert!(
        raw.ends_with(b"0\r\n\r\n"),
        "bare end still terminates: {raw:?}"
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_end_with_trailers_rejects_bad_set_without_staging() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    let prohibited = [field(b"content-length", b"5")];
    assert_eq!(
        unsafe { end_with_trailers(server, token, &prohibited) },
        VylResult::InvalidArgument
    );
    let bad_token = [field(b"bad\r\nname", b"v")];
    assert_eq!(
        unsafe { end_with_trailers(server, token, &bad_token) },
        VylResult::InvalidArgument
    );
    let bad_value = [field(b"x-tag", b"bad\r\nvalue")];
    assert_eq!(
        unsafe { end_with_trailers(server, token, &bad_value) },
        VylResult::InvalidArgument
    );
    let empty_name = [field(b"", b"v")];
    assert_eq!(
        unsafe { end_with_trailers(server, token, &empty_name) },
        VylResult::InvalidArgument
    );
    let oversized_value = vec![b'v'; 1024];
    let over_bytes = [field(b"x", &oversized_value)];
    assert_eq!(
        unsafe { end_with_trailers(server, token, &over_bytes) },
        VylResult::InvalidArgument
    );
    let many: Vec<VylField> = (0..9).map(|_| field(b"x-tag", b"v")).collect();
    assert_eq!(
        unsafe { end_with_trailers(server, token, &many) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_end_stream_with_trailers(server, token, std::ptr::null(), 1)
        },
        VylResult::InvalidArgument
    );
    let (result, writable) = unsafe { is_writable(server, token) };
    assert_eq!(result, VylResult::Ok);
    assert_eq!(
        writable, 1,
        "rejected sets stage nothing; stream stays open"
    );
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Ok
    );
    let (status, raw, body) = read_streamed(&mut stream);
    assert_eq!(status, 200);
    assert!(body.is_empty());
    assert!(
        raw.ends_with(b"0\r\n\r\n"),
        "bare end works after rejected trailer sets"
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_end_then_trailers_reports_duplicate() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /s HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n")
        .expect("write request");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Ok
    );
    let trailers = [field(b"x-tag", b"v1")];
    assert!(
        matches!(
            unsafe { end_with_trailers(server, token, &trailers) },
            VylResult::Duplicate | VylResult::Gone
        ),
        "queued end wins (duplicate) or applied end tombs (gone); either fails closed"
    );
    let (status, raw, body) = read_streamed(&mut stream);
    assert_eq!(status, 200);
    assert!(body.is_empty());
    assert!(raw.ends_with(b"0\r\n\r\n"));
    assert_eq!(
        unsafe { end_with_trailers(server, token, &trailers) },
        VylResult::Gone
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn stream_end_with_trailers_rejects_bad_state_and_null_inputs() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/s", u64::MAX) };
    let zero_token = VylManagedStaticToken::default();
    let trailers = [field(b"x-tag", b"v1")];
    assert_eq!(
        unsafe { end_with_trailers(server, zero_token, &trailers) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_host_end_stream_with_trailers(
                std::ptr::null_mut(),
                zero_token,
                trailers.as_ptr(),
                trailers.len(),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { end_with_trailers(server, zero_token, &trailers) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { end_with_trailers(server, zero_token, &trailers) },
        VylResult::BadState
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}
