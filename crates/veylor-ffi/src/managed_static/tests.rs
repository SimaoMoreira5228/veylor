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
        io_backend: 0,
        address: VylBytes {
            data: std::ptr::null(),
            length: 0,
        },
        max_connections: 0,
        worker_count: 0,
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

fn get_raw(port: u16, path: &str, extra: &str) -> (u16, String, Vec<u8>) {
    let mut stream = connect_loopback(port);
    let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\n{extra}Connection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write request");
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read head byte");
        raw.push(byte[0]);
        if raw.ends_with(b"\r\n\r\n".as_slice()) {
            break;
        }
    }
    let head = String::from_utf8(raw).expect("utf8 head");
    let status = head
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status int");
    let length = head
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
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).expect("read body");
    (status, head, body)
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
fn gzip_route_serves_precompressed_and_identity_fallback() {
    let plain = b"The quick brown fox jumps over the lazy dog. ".repeat(50);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route_with_gzip(
                server,
                bytes(b"GET"),
                bytes(b"/gzip"),
                200,
                bytes(b"OK"),
                bytes(&plain),
            )
        },
        VylResult::Ok
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route_with_gzip(
                server,
                bytes(b"GET"),
                bytes(b"/gzip"),
                200,
                bytes(b"OK"),
                bytes(&plain),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };

    let (status, head, gzipped) = get_raw(port, "/gzip", "Accept-Encoding: gzip\r\n");
    assert_eq!(status, 200);
    assert!(
        head.contains("Content-Encoding: gzip"),
        "encoding: {head:?}"
    );
    assert!(head.contains("Vary: Accept-Encoding"), "vary: {head:?}");
    assert_eq!(&gzipped[..2], &[0x1f, 0x8b], "gzip magic");
    assert!(gzipped.len() < plain.len());
    let framed: usize = head
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
    assert_eq!(framed, gzipped.len());

    let (plain_status, plain_head, identity) = get_raw(port, "/gzip", "");
    assert_eq!(plain_status, 200);
    assert_eq!(identity, plain);
    assert!(
        !plain_head.to_ascii_lowercase().contains("content-encoding"),
        "no content-encoding: {plain_head:?}"
    );
    assert!(
        plain_head.contains("Vary: Accept-Encoding"),
        "vary: {plain_head:?}"
    );

    let (_, _, again) = get_raw(port, "/gzip", "Accept-Encoding: gzip\r\n");
    assert_eq!(again, gzipped);

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

    #[repr(C)]
    struct ShortConfig {
        abi_version: u32,
        struct_size: usize,
    }

    let short = ShortConfig {
        abi_version: VYL_MANAGED_STATIC_ABI_VERSION,
        struct_size: std::mem::size_of::<ShortConfig>(),
    };
    assert_eq!(
        unsafe {
            vyl_managed_static_new(
                &short as *const ShortConfig as *const VylManagedStaticConfig,
                &mut null_server,
            )
        },
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
    // NOTE: one write_all for head+body: native handlers invoke at head-parse time, so a
    // head/body syscall split lets the worker parse between the two and invoke framing-only.
    // A single small loopback write arrives coalesced; split writes flaked ~1/5.
    let mut request = head.into_bytes();
    request.extend_from_slice(body);
    stream.write_all(&request).expect("write request");
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
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut events = [VylManagedStaticEvent::default(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    loop {
        let report =
            unsafe { poll_into(server, 100_000_000, &mut events, &mut methods, &mut paths) };
        for event in events.iter().take(report.copied) {
            if event.kind == VYL_MANAGED_STATIC_EVENT_REQUEST {
                return event.token;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for request event"
        );
    }
}

// NOTE: polls until a REQUEST arrives, keeping any BODY_AVAILABLE signals seen along the way.
// poll_one-style discarding would eat the admission signal: the worker stages body bytes right
// behind the park, so the signal often shares the first batches with its own REQUEST.
unsafe fn poll_request_with_signals(
    server: *mut VylManagedStaticServer,
) -> (VylManagedStaticToken, Vec<VylManagedStaticEvent>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut signals = Vec::new();
    let mut events = [VylManagedStaticEvent::default(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    loop {
        let report =
            unsafe { poll_into(server, 100_000_000, &mut events, &mut methods, &mut paths) };
        let mut token = None;
        for event in events.iter().take(report.copied) {
            if event.kind == VYL_MANAGED_STATIC_EVENT_REQUEST {
                token = Some(event.token);
            }
            if event.kind == VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE {
                signals.push(*event);
            }
        }
        if let Some(token) = token {
            return (token, signals);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for request event"
        );
    }
}

// NOTE: appends fresh BODY_AVAILABLE signals to the set; callers loop until their terminal
// shape appears. Signals are advisory snapshots, so only the end-flagged full window is exact.
unsafe fn gather_signals(
    server: *mut VylManagedStaticServer,
    signals: &mut Vec<VylManagedStaticEvent>,
) {
    let mut events = [VylManagedStaticEvent::default(); 8];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    let report = unsafe { poll_into(server, 100_000_000, &mut events, &mut methods, &mut paths) };
    for event in events.iter().take(report.copied) {
        if event.kind == VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE {
            signals.push(*event);
        }
    }
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
    for _ in 0..3000 {
        let (result, writable) = unsafe { is_writable(server, token) };
        assert_eq!(result, VylResult::Ok);
        if writable == 1 {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("stream never armed writable");
}

fn split_head(raw: &[u8]) -> (&[u8], &[u8]) {
    let end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("head terminator")
        + 4;
    raw.split_at(end)
}

fn head_with_date(head: &[u8], status_line: &[u8], rest: &[u8]) {
    let mut prefix = Vec::from(status_line);
    prefix.extend_from_slice(b"\r\nDate: ");
    let after = head
        .strip_prefix(prefix.as_slice())
        .expect("status plus Date first");
    let value = after.get(..29).expect("29-byte IMF-fixdate");
    let seen = std::str::from_utf8(value).expect("date utf8");
    assert_eq!(seen.len(), 29, "IMF-fixdate width: {seen}");
    assert!(seen.ends_with(" GMT"), "GMT only: {seen}");
    assert_eq!(seen.as_bytes()[3], b',', "weekday comma: {seen}");
    assert_eq!(&after[29..], rest, "framing exact past Date");
    let text = std::str::from_utf8(head).expect("head utf8");
    let count = text
        .lines()
        .filter(|line| {
            line.get(..5)
                .is_some_and(|name| name.eq_ignore_ascii_case("date:"))
        })
        .count();
    assert_eq!(count, 1, "exactly one Date: {text}");
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
    let (head, tail) = split_head(&raw);
    head_with_date(
        head,
        b"HTTP/1.1 200 OK",
        b"\r\nTransfer-Encoding: chunked\r\n\r\n",
    );
    assert_eq!(tail, b"6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n");
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
    let (token, mut signals) = unsafe { poll_request_with_signals(server) };
    let deadline = std::time::Instant::now() + Duration::from_secs(5);

    while !signals.iter().any(|event| event.path_len == 1) {
        assert!(
            std::time::Instant::now() < deadline,
            "terminal body signal did not arrive"
        );
        unsafe { gather_signals(server, &mut signals) };
    }

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
fn dispatch_body_round_trip_then_reuses_connection() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/upload", u64::MAX) };
    unsafe { register_managed(server, b"GET", b"/m", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\nhello")
        .expect("write post");
    let token = unsafe { poll_one(server) };
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"stored"))
        },
        VylResult::Ok
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"stored");
    stream
        .write_all(b"GET /m HTTP/1.1\r\nHost: test\r\n\r\n")
        .expect("write reuse");
    let token = unsafe { poll_one(server) };
    assert_ne!(token.request_seq, 0, "reused slot parks a fresh sequence");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"again"))
        },
        VylResult::Ok
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"again");
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
    let (head, tail) = split_head(&raw);
    head_with_date(
        head,
        b"HTTP/1.1 200 OK",
        b"\r\nTransfer-Encoding: chunked\r\n\r\n",
    );
    assert_eq!(
        tail,
        b"6\r\nhello \r\n5\r\nworld\r\n0\r\nx-checksum: abc\r\nx-tag: v1\r\n\r\n"
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

struct FixtureDir {
    path: std::path::PathBuf,
}

impl FixtureDir {
    fn new(name: &str) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "veylor-ffi-mount-{}-{}-{name}",
            std::process::id(),
            id
        ));
        std::fs::create_dir_all(&path).expect("create fixture dir");
        Self { path }
    }
    fn write(&self, name: &str, contents: &[u8]) {
        std::fs::write(self.path.join(name), contents).expect("write fixture file");
    }
}

impl Drop for FixtureDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

unsafe fn mount_raw(server: *mut VylManagedStaticServer, prefix: &[u8], root: &[u8]) -> VylResult {
    unsafe { super::vyl_managed_static_register_file_mount(server, bytes(prefix), bytes(root)) }
}

fn mount_root_bytes(path: &std::path::Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().to_vec()
    }
    #[cfg(not(unix))]
    {
        path.to_str().expect("utf8 temp path").as_bytes().to_vec()
    }
}

fn get_file(port: u16, path: &str) -> (u16, String, Vec<u8>) {
    let mut stream = connect_loopback(port);
    let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write request");
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read head byte");
        raw.push(byte[0]);
        if raw.ends_with(b"\r\n\r\n".as_slice()) {
            break;
        }
    }
    let head = String::from_utf8(raw).expect("utf8 head");
    let status = head
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status int");
    let length = head
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
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).expect("read body");
    (status, head, body)
}

#[test]
fn file_mount_serves_exact_bytes_and_content_type() {
    let fixture = FixtureDir::new("serve");
    fixture.write("hello.txt", b"file-bytes-123");
    let root = mount_root_bytes(&fixture.path);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let (status, head, body) = get_file(port, "/assets/hello.txt");
    assert_eq!(status, 200);
    assert_eq!(body, b"file-bytes-123");
    assert!(head.contains("Content-Type: text/plain"), "head: {head:?}");
    let (missing, _, _) = get_file(port, "/assets/missing.txt");
    assert_eq!(missing, 404);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 2);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn file_mount_rejects_traversal_as_404() {
    let fixture = FixtureDir::new("traversal");
    fixture.write("hello.txt", b"hello");
    let root = mount_root_bytes(&fixture.path);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    for probe in [
        "/assets/../hello.txt",
        "/assets/%2e%2e/hello.txt",
        "/assets/%2Fhello.txt",
        "/assets//hello.txt",
    ] {
        let (status, _, _) = get_file(port, probe);
        assert_eq!(status, 404, "probe {probe:?} must not escape");
    }
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 4);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn file_mount_rejects_duplicate_and_bad_roots() {
    let fixture = FixtureDir::new("dupe");
    fixture.write("hello.txt", b"hello");
    let root = mount_root_bytes(&fixture.path);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { mount_raw(server, b"/assets", &root) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { mount_raw(server, b"/other/", b"relative/path") },
        VylResult::InvalidArgument
    );
    let missing = mount_root_bytes(&fixture.path.join("absent"));
    assert_eq!(
        unsafe { mount_raw(server, b"/other/", &missing) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::vyl_managed_static_register_file_mount(
                std::ptr::null_mut(),
                bytes(b"/x/"),
                bytes(&root),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::vyl_managed_static_register_file_mount(
                server,
                VylBytes {
                    data: std::ptr::null(),
                    length: 3,
                },
                bytes(&root),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

fn file_header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        if key.trim().eq_ignore_ascii_case(name) {
            Some(value.trim())
        } else {
            None
        }
    })
}

fn file_exchange(
    stream: &mut TcpStream,
    request: &[u8],
    head_only: bool,
) -> (u16, String, Vec<u8>) {
    stream.write_all(request).expect("write request");
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
    let head = String::from_utf8(raw).expect("utf8 head");
    let status = head
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status int");
    if head_only {
        return (status, head, Vec::new());
    }
    let length = file_header(&head, "content-length")
        .expect("content length")
        .parse()
        .expect("content length int");
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).expect("read body");
    (status, head, body)
}

#[test]
fn file_mount_single_range_206_and_unsatisfiable_416() {
    let fixture = FixtureDir::new("range");
    fixture.write("data.txt", b"0123456789");
    let root = mount_root_bytes(&fixture.path);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    let (status, head, body) = file_exchange(
        &mut stream,
        b"GET /assets/data.txt HTTP/1.1\r\nHost: test\r\nRange: bytes=2-5\r\nConnection: close\r\n\r\n",
        false,
    );
    assert_eq!(status, 206);
    assert_eq!(body, b"2345");
    assert_eq!(file_header(&head, "content-range"), Some("bytes 2-5/10"));
    assert_eq!(file_header(&head, "accept-ranges"), Some("bytes"));
    drop(stream);
    let mut stream = connect_loopback(port);
    let (status, head, body) = file_exchange(
        &mut stream,
        b"GET /assets/data.txt HTTP/1.1\r\nHost: test\r\nRange: bytes=0-1, 4-5\r\nConnection: close\r\n\r\n",
        false,
    );
    assert_eq!(status, 206);
    assert!(
        file_header(&head, "content-type")
            .is_some_and(|ctype| ctype.starts_with("multipart/byteranges; boundary=")),
        "head: {head:?}"
    );
    let text = String::from_utf8(body).expect("multipart utf8");
    assert!(
        text.contains("Content-Range: bytes 0-1/10"),
        "body: {text:?}"
    );
    assert!(
        text.contains("Content-Range: bytes 4-5/10"),
        "body: {text:?}"
    );
    assert!(text.contains("01"), "body: {text:?}");
    assert!(text.contains("45"), "body: {text:?}");
    drop(stream);
    let mut stream = connect_loopback(port);
    let (status, head, body) = file_exchange(
        &mut stream,
        b"GET /assets/data.txt HTTP/1.1\r\nHost: test\r\nRange: bytes=99-\r\nConnection: close\r\n\r\n",
        false,
    );
    assert_eq!(status, 416);
    assert_eq!(file_header(&head, "content-range"), Some("bytes */10"));
    assert!(body.is_empty());
    assert!(head.contains("Connection: close"), "head: {head:?}");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 3);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn file_mount_conditional_round_trip_stale_and_garbage_dates() {
    let fixture = FixtureDir::new("conditional");
    fixture.write("data.txt", b"0123456789");
    let root = mount_root_bytes(&fixture.path);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    let (status, head, body) = file_exchange(
        &mut stream,
        b"GET /assets/data.txt HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n",
        false,
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"0123456789");
    let last_modified = file_header(&head, "last-modified")
        .expect("last-modified")
        .to_string();
    assert_eq!(last_modified.len(), 29, "IMF-fixdate: {last_modified:?}");
    assert!(
        last_modified.ends_with(" GMT")
            && last_modified.contains(", ")
            && last_modified.matches(':').count() == 2,
        "IMF-fixdate: {last_modified:?}"
    );
    let conditional = format!(
        "GET /assets/data.txt HTTP/1.1\r\nHost: test\r\nIf-Modified-Since: {last_modified}\r\nConnection: keep-alive\r\n\r\n"
    );
    let (status, head, body) = file_exchange(&mut stream, conditional.as_bytes(), true);
    assert_eq!(status, 304);
    assert!(body.is_empty(), "304 carries no body");
    assert_eq!(
        file_header(&head, "last-modified"),
        Some(last_modified.as_str())
    );
    let (status, _, body) = file_exchange(
        &mut stream,
        b"GET /assets/data.txt HTTP/1.1\r\nHost: test\r\nIf-Modified-Since: Thu, 01 Jan 1970 00:00:00 GMT\r\nConnection: keep-alive\r\n\r\n",
        false,
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"0123456789");
    let (status, _, body) = file_exchange(
        &mut stream,
        b"GET /assets/data.txt HTTP/1.1\r\nHost: test\r\nIf-Modified-Since: not-a-date\r\nConnection: keep-alive\r\n\r\n",
        false,
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"0123456789");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn file_mount_head_conditional_matches_get() {
    let fixture = FixtureDir::new("head-conditional");
    fixture.write("data.txt", b"0123456789");
    let root = mount_root_bytes(&fixture.path);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    let (status, head, body) = file_exchange(
        &mut stream,
        b"HEAD /assets/data.txt HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n",
        true,
    );
    assert_eq!(status, 200);
    assert!(body.is_empty());
    assert_eq!(file_header(&head, "content-length"), Some("10"));
    let last_modified = file_header(&head, "last-modified")
        .expect("last-modified")
        .to_string();
    let (status, head, _) = file_exchange(
        &mut stream,
        b"HEAD /assets/data.txt HTTP/1.1\r\nHost: test\r\nRange: bytes=0-1\r\nConnection: keep-alive\r\n\r\n",
        true,
    );
    assert_eq!(status, 200, "HEAD ignores Range: {head:?}");
    assert_eq!(file_header(&head, "content-range"), None);
    assert_eq!(file_header(&head, "content-length"), Some("10"));
    let conditional = format!(
        "HEAD /assets/data.txt HTTP/1.1\r\nHost: test\r\nIf-Modified-Since: {last_modified}\r\nConnection: keep-alive\r\n\r\n"
    );
    let (status, _, _) = file_exchange(&mut stream, conditional.as_bytes(), true);
    assert_eq!(status, 304);
    let (status, _, _) = file_exchange(
        &mut stream,
        b"HEAD /assets/data.txt HTTP/1.1\r\nHost: test\r\nIf-Modified-Since: Thu, 01 Jan 1970 00:00:00 GMT\r\nConnection: keep-alive\r\n\r\n",
        true,
    );
    assert_eq!(status, 200);
    let (status, _, body) = file_exchange(
        &mut stream,
        b"GET /assets/data.txt HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n",
        false,
    );
    assert_eq!(status, 200);
    assert_eq!(body, b"0123456789");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 0, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn file_mount_rejects_registration_after_start() {
    let fixture = FixtureDir::new("late");
    fixture.write("hello.txt", b"hello");
    let root = mount_root_bytes(&fixture.path);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { mount_raw(server, b"/late/", &root) },
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
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: native-route coverage. Each test owns its handler statics so parallel runs never share
// observations. Handlers never unwind (nounwind contract): they record, answer, and return.

use std::ffi::c_void;
use std::sync::Mutex;

struct NativeObserved {
    user_data: usize,
    method: Vec<u8>,
    path: Vec<u8>,
    has_body: u8,
    body_len: u64,
    body: Vec<u8>,
}

impl NativeObserved {
    const fn empty() -> Self {
        Self {
            user_data: 0,
            method: Vec::new(),
            path: Vec::new(),
            has_body: 0,
            body_len: 0,
            body: Vec::new(),
        }
    }
}

static NATIVE_TAG: u8 = 0x5E;

unsafe fn view_bytes(data: *const u8, len: usize) -> Vec<u8> {
    if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
    }
}

fn record_native(
    observed: &Mutex<NativeObserved>,
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
) {
    let request = unsafe { &*request };
    let method = unsafe { view_bytes(request.method, request.method_len) };
    let path = unsafe { view_bytes(request.path, request.path_len) };
    let body = if request.body.is_null() || request.body_len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(request.body, request.body_len as usize) }.to_vec()
    };
    *observed.lock().expect("observe lock") = NativeObserved {
        user_data: user_data as usize,
        method,
        path,
        has_body: request.has_body,
        body_len: request.body_len,
        body,
    };
}

fn answer_native(
    response: *mut VylManagedStaticNativeResponse,
    status: i32,
    reason: &[u8],
    body: &[u8],
) {
    unsafe {
        (*response).status = status;
        (*response).reason = reason.as_ptr();
        (*response).reason_len = reason.len();
        (*response).body = body.as_ptr();
        (*response).body_len = body.len();
    }
}

unsafe fn register_native(
    server: *mut VylManagedStaticServer,
    method: &[u8],
    path: &[u8],
    handler: VylManagedStaticNativeHandler,
    user_data: *mut c_void,
) -> VylResult {
    unsafe {
        vyl_managed_static_register_native_route(
            server,
            bytes(method),
            bytes(path),
            handler,
            user_data,
        )
    }
}

fn native_tag() -> *mut c_void {
    &NATIVE_TAG as *const u8 as *mut c_void
}

static NATIVE_GET_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn native_get_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    record_native(&NATIVE_GET_OBSERVED, user_data, request);
    answer_native(response, 200, b"OK", b"native-bytes");
    0
}

static NATIVE_POST_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn native_post_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    record_native(&NATIVE_POST_OBSERVED, user_data, request);
    answer_native(response, 200, b"OK", b"submit-ack");
    0
}

unsafe extern "C" fn native_failing_handler(
    _user_data: *mut c_void,
    _request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    answer_native(response, 200, b"OK", b"ignored");
    1
}

unsafe extern "C" fn native_panicking_handler(
    _user_data: *mut c_void,
    _request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    let survived = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        panic!("native handler boom");
    }));
    if survived.is_err() {
        answer_native(response, 200, b"OK", b"ignored");
        return 1;
    }
    0
}

#[test]
fn native_get_round_trip_reports_views_and_user_data() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/native", Some(native_get_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let head = get(port, "/native");
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {head:?}"
    );
    assert!(head.ends_with("native-bytes"), "unexpected body: {head:?}");
    let observed = NATIVE_GET_OBSERVED.lock().expect("observed");
    assert_eq!(observed.user_data, tag as usize);
    assert_eq!(observed.method, b"GET");
    assert_eq!(observed.path, b"/native");
    assert_eq!(observed.has_body, 0);
    assert_eq!(observed.body_len, 0);
    assert!(observed.body.is_empty());
    drop(observed);
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/late", Some(native_get_handler), tag) },
        VylResult::BadState
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn native_post_borrows_staged_body_bytes() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { register_native(server, b"POST", b"/submit", Some(native_post_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    let (status, body) = post_exchange(&mut stream, "/submit", b"hello");
    assert_eq!(status, 200);
    assert_eq!(body, b"submit-ack");
    let observed = NATIVE_POST_OBSERVED.lock().expect("observed");
    assert_eq!(observed.user_data, tag as usize);
    assert_eq!(observed.method, b"POST");
    assert_eq!(observed.path, b"/submit");
    assert_eq!(observed.has_body, 1);
    assert_eq!(observed.body_len, 5);
    assert_eq!(observed.body, b"hello");
    drop(observed);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

static NATIVE_ECHO_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn native_echo_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }
    record_native(&NATIVE_ECHO_OBSERVED, user_data, request);
    let observed = NATIVE_ECHO_OBSERVED.lock().expect("observed");
    unsafe {
        (*response).status = 200;
        (*response).reason = b"OK".as_ptr();
        (*response).reason_len = 2;
        (*response).body = observed.body.as_ptr();
        (*response).body_len = observed.body.len();
    }
    0
}

#[test]
fn native_echo_returns_borrowed_body_through_ffi_twin() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { register_native(server, b"POST", b"/echo", Some(native_echo_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    let (status, body) = post_exchange(&mut stream, "/echo", b"ffi-echo");
    assert_eq!(status, 200);
    assert_eq!(body, b"ffi-echo");
    let observed = NATIVE_ECHO_OBSERVED.lock().expect("observed");
    assert_eq!(observed.body, b"ffi-echo");
    assert_eq!(observed.body_len, 8);
    drop(observed);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn native_rejects_duplicates_overlaps_and_null() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/n", Some(native_get_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/n", Some(native_get_handler), tag) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route(
                server,
                bytes(b"GET"),
                bytes(b"/n"),
                200,
                bytes(b"OK"),
                bytes(b"x"),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_managed_route(server, bytes(b"GET"), bytes(b"/n"), u64::MAX)
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/null", None, tag) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_native_route(
                std::ptr::null_mut(),
                bytes(b"GET"),
                bytes(b"/null"),
                Some(native_get_handler),
                tag,
            )
        },
        VylResult::InvalidArgument
    );
    unsafe { register(server, b"POST", b"/s", b"x") };
    assert_eq!(
        unsafe { register_native(server, b"POST", b"/s", Some(native_post_handler), tag) },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

fn read_closed(stream: &mut TcpStream) -> Vec<u8> {
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).expect("read close");
    rest
}

#[test]
fn native_nonzero_return_closes_without_bytes() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            register_native(
                server,
                b"GET",
                b"/fail",
                Some(native_failing_handler),
                native_tag(),
            )
        },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /fail HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
        .expect("write request");
    assert!(
        read_closed(&mut stream).is_empty(),
        "nonzero return must close without bytes"
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.response_overflow, 1, "counts: {counts:?}");
    assert_eq!(counts_total(counts), 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn native_handler_catches_own_panic_and_fails_closed() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            register_native(
                server,
                b"GET",
                b"/boom",
                Some(native_panicking_handler),
                native_tag(),
            )
        },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /boom HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
        .expect("write request");
    assert!(
        read_closed(&mut stream).is_empty(),
        "caught panic must fail closed without bytes"
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.response_overflow, 1, "counts: {counts:?}");
    assert_eq!(counts_total(counts), 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: additive-slice coverage: H2C staging, worker-count staging, and join-busy. The H2C
// client is byte-level like h2_tests (raw preface plus empty SETTINGS, then one HPACK GET)
// without depending on the server crate; the join probe passes the handle via user_data,
// which is what the field is for.

use std::sync::atomic::{AtomicI32, Ordering};
use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM,
    DEFAULT_MAX_FRAME_SIZE, FrameHeader, FrameType, HEADERS_FLAG_END_HEADERS,
    HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackFieldToEncode, HpackHeader, HpackRepresentation,
    HpackTable, SETTINGS_ACK, decode_frame_header, decode_header_block, encode_frame_header,
    encode_header_block,
};
use veylor_server::MAX_WORKERS;

fn h2_write_frame(
    stream: &mut TcpStream,
    frame_type: FrameType,
    flags: u8,
    id: u32,
    payload: &[u8],
) {
    let header = FrameHeader {
        payload_length: payload.len() as u32,
        frame_type,
        flags,
        stream_id: id,
    };
    let mut encoded = [0; 9];
    encode_frame_header(header, &mut encoded, DEFAULT_MAX_FRAME_SIZE).expect("encode header");
    stream.write_all(&encoded).expect("write header");
    stream.write_all(payload).expect("write payload");
}

fn h2_read_frame(stream: &mut TcpStream) -> (FrameHeader, Vec<u8>) {
    let mut encoded = [0; 9];
    stream.read_exact(&mut encoded).expect("read header");
    let header = decode_frame_header(&encoded, DEFAULT_MAX_FRAME_SIZE).expect("decode header");
    let mut payload = vec![0; header.payload_length as usize];
    stream.read_exact(&mut payload).expect("read payload");
    (header, payload)
}

fn h2_field(name: &'static [u8], value: &'static [u8]) -> HpackFieldToEncode<'static> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

#[test]
fn h2c_enable_serves_handshake_and_static_route() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(std::ptr::null_mut(), 0) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::BadState
    );
    let h2c_port = {
        let mut port = 0;
        assert_eq!(
            unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
            VylResult::Ok
        );
        port
    };
    assert_ne!(h2c_port, 0);
    let mut stream = connect_loopback(h2c_port);
    stream
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write magic");
    h2_write_frame(&mut stream, FrameType::Settings, 0, 0, &[]);
    let (header, _) = h2_read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.stream_id, 0);
    h2_write_frame(&mut stream, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = h2_read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);
    let fields = [
        h2_field(b":method", b"GET"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/hello"),
        h2_field(b":authority", b"localhost"),
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    h2_write_frame(
        &mut stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        1,
        &block[..len],
    );
    let (header, payload) = h2_read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, 1);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (count, _) =
        decode_header_block(&payload, &mut decoder, &mut decoded).expect("decode block");
    let mut status = 0;
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    assert_eq!(status, 200);
    let (header, body) = h2_read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(header.stream_id, 1);
    assert_eq!(header.flags & DATA_FLAG_END_STREAM, DATA_FLAG_END_STREAM);
    assert_eq!(body, b"hello");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn worker_count_stages_rejects_bounds_and_serves() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { vyl_managed_static_set_worker_count(std::ptr::null_mut(), 2) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_set_worker_count(server, 0) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_set_worker_count(server, MAX_WORKERS + 1) },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
    for count in [2usize, 4usize] {
        let config = test_config(0);
        let server = unsafe { create(&config) };
        assert_eq!(
            unsafe { vyl_managed_static_set_worker_count(server, count) },
            VylResult::Ok
        );
        unsafe { register(server, b"GET", b"/w", b"workers") };
        assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
        assert_eq!(
            unsafe { vyl_managed_static_set_worker_count(server, 1) },
            VylResult::BadState
        );
        assert_eq!(
            unsafe { vyl_managed_static_enable_h2c(server, 0) },
            VylResult::BadState
        );
        let port = unsafe { local_port(server) };
        for _ in 0..count {
            let head = get(port, "/w");
            assert!(
                head.starts_with("HTTP/1.1 200 OK"),
                "unexpected head: {head:?}"
            );
            assert!(head.ends_with("workers"), "unexpected body: {head:?}");
        }
        let counts = unsafe { stop_join(server) };
        assert_eq!(counts_total(counts), count as u64, "counts: {counts:?}");
        assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
    }
}

static JOIN_BUSY_OBSERVED: AtomicI32 = AtomicI32::new(-1);

unsafe extern "C" fn native_join_busy_handler(
    user_data: *mut c_void,
    _request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    let server = user_data as *mut VylManagedStaticServer;
    let mut counts = VylManagedStaticCloseCounts::default();
    let result = unsafe { vyl_managed_static_join(server, &mut counts) };
    JOIN_BUSY_OBSERVED.store(result as i32, Ordering::Release);
    answer_native(response, 200, b"OK", b"join-busy");
    0
}

#[test]
fn join_from_native_handler_reports_busy_without_deadlock() {
    JOIN_BUSY_OBSERVED.store(-1, Ordering::Release);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            register_native(
                server,
                b"GET",
                b"/busy",
                Some(native_join_busy_handler),
                server as *mut c_void,
            )
        },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let head = get(port, "/busy");
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {head:?}"
    );
    assert!(head.ends_with("join-busy"), "unexpected body: {head:?}");
    assert_eq!(
        JOIN_BUSY_OBSERVED.load(Ordering::Acquire),
        VylResult::Busy as i32
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: native-over-H2C coverage through the managed C ABI. Every H2C configuration
// rebinds at start (api.rs gates the direct path on no-H2C plus single worker), so each
// test below exercises the ReadyRouteOp::Native replay into the rebound server; the H1
// reads on the same handles pin the direct path alongside. One stateful HPACK decoder
// rides each connection because response blocks index across streams.

unsafe fn h2c_port_of(server: *mut VylManagedStaticServer) -> u16 {
    let mut port = 0;
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::Ok
    );
    assert_ne!(port, 0);
    port
}

#[test]
fn h2c_addr_reports_bound_port_and_rejects_unusable_states() {
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(std::ptr::null_mut(), std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let mut port = 0xBEEFu16;
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(std::ptr::null_mut(), &mut port) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::BadState,
        "pre-start reports BadState: the H2C listener binds at start"
    );
    assert_eq!(port, 0);
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::BadState,
        "staged but unbound still reports BadState"
    );
    assert_eq!(port, 0);
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::Ok
    );
    assert_ne!(port, 0);
    assert_ne!(port, unsafe { local_port(server) });
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 0, "counts: {counts:?}");
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::BadState
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn h2c_addr_reports_disabled_and_shared_ports() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let mut port = 0xBEEFu16;
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::BadState,
        "H2C-disabled running handles report BadState"
    );
    assert_eq!(port, 0);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 0, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);

    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { vyl_managed_static_set_worker_count(server, 2) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let mut h2c = 0;
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut h2c) },
        VylResult::Ok
    );
    assert_ne!(h2c, 0);
    let head = get(unsafe { local_port(server) }, "/missing");
    assert!(
        head.starts_with("HTTP/1.1 404 Not Found"),
        "reuse-port H1 still serves: {head:?}"
    );
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

fn h2_handshake(stream: &mut TcpStream) {
    stream
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write magic");
    h2_write_frame(stream, FrameType::Settings, 0, 0, &[]);
    let (header, _) = h2_read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.stream_id, 0);
    h2_write_frame(stream, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = h2_read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);
}

fn h2_send_headers(stream: &mut TcpStream, id: u32, fields: &[HpackFieldToEncode<'_>]) {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(fields, &mut encoder, &mut block).expect("encode block");
    h2_write_frame(
        stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        id,
        &block[..len],
    );
}

fn h2_send_get(stream: &mut TcpStream, id: u32, path: &'static [u8]) {
    let fields = [
        h2_field(b":method", b"GET"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", path),
        h2_field(b":authority", b"localhost"),
    ];
    h2_send_headers(stream, id, &fields);
}

fn h2_send_lying_get(stream: &mut TcpStream, id: u32, path: &'static [u8]) {
    let fields = [
        h2_field(b":method", b"GET"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", path),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"5"),
    ];
    h2_send_headers(stream, id, &fields);
}

fn h2_status(
    stream: &mut TcpStream,
    decoder: &mut HpackTable<'_>,
    id: u32,
    decoded: &mut [HpackHeader; 32],
) -> u16 {
    let (header, payload) = h2_read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, id);
    let (count, _) = decode_header_block(&payload, decoder, decoded).expect("decode block");
    let mut status = 0;
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    status
}

fn h2_data(stream: &mut TcpStream, id: u32) -> Vec<u8> {
    let (header, body) = h2_read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(header.stream_id, id);
    assert_eq!(header.flags & DATA_FLAG_END_STREAM, DATA_FLAG_END_STREAM);
    body
}

fn h2_expect_rst(stream: &mut TcpStream, id: u32) {
    let (header, payload) = h2_read_frame(stream);
    assert_eq!(header.frame_type, FrameType::RstStream);
    assert_eq!(header.stream_id, id);
    assert_eq!(
        payload,
        1u32.to_be_bytes(),
        "framing lie must RST PROTOCOL_ERROR"
    );
}

fn h2_decoder<'a>(entries: &'a mut [HpackEntrySlot; 8], scratch: &'a mut [u8]) -> HpackTable<'a> {
    HpackTable::new(entries, scratch, 4_096)
}

static H2C_NATIVE_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn h2c_native_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    record_native(&H2C_NATIVE_OBSERVED, user_data, request);
    answer_native(response, 200, b"OK", b"h2c-native");
    0
}

#[test]
fn h2c_native_replays_through_rebind_with_static_alongside() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/native", Some(h2c_native_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h1 = unsafe { local_port(server) };
    let h2c = unsafe { h2c_port_of(server) };
    assert_ne!(h2c, 0);
    assert_ne!(h2c, h1);
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    h2_send_get(&mut stream, 1, b"/native");
    assert_eq!(h2_status(&mut stream, &mut decoder, 1, &mut decoded), 200);
    assert_eq!(h2_data(&mut stream, 1), b"h2c-native");
    let observed = H2C_NATIVE_OBSERVED.lock().expect("observed");
    assert_eq!(observed.user_data, tag as usize);
    assert_eq!(observed.method, b"GET");
    assert_eq!(observed.path, b"/native");
    assert_eq!(observed.has_body, 0);
    assert_eq!(observed.body_len, 0);
    assert!(observed.body.is_empty());
    drop(observed);
    h2_send_get(&mut stream, 3, b"/hello");
    assert_eq!(h2_status(&mut stream, &mut decoder, 3, &mut decoded), 200);
    assert_eq!(h2_data(&mut stream, 3), b"hello");
    h2_send_lying_get(&mut stream, 5, b"/native");
    h2_expect_rst(&mut stream, 5);
    h2_send_get(&mut stream, 7, b"/native");
    assert_eq!(h2_status(&mut stream, &mut decoder, 7, &mut decoded), 200);
    assert_eq!(h2_data(&mut stream, 7), b"h2c-native");
    let head = get(h1, "/native");
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {head:?}"
    );
    assert!(head.ends_with("h2c-native"), "unexpected body: {head:?}");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

static H2C_REBIND_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn h2c_rebind_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    record_native(&H2C_REBIND_OBSERVED, user_data, request);
    answer_native(response, 200, b"OK", b"rebind-native");
    0
}

#[test]
fn h2c_native_served_when_h2c_staged_before_register() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/native", Some(h2c_rebind_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h1 = unsafe { local_port(server) };
    let h2c = unsafe { h2c_port_of(server) };
    assert_ne!(h2c, 0);
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    h2_send_get(&mut stream, 1, b"/native");
    assert_eq!(h2_status(&mut stream, &mut decoder, 1, &mut decoded), 200);
    assert_eq!(h2_data(&mut stream, 1), b"rebind-native");
    let observed = H2C_REBIND_OBSERVED.lock().expect("observed");
    assert_eq!(observed.user_data, tag as usize);
    assert_eq!(observed.method, b"GET");
    assert_eq!(observed.path, b"/native");
    drop(observed);
    let head = get(h1, "/native");
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {head:?}"
    );
    assert!(head.ends_with("rebind-native"), "unexpected body: {head:?}");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

static H2C_WORKER_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn h2c_worker_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    record_native(&H2C_WORKER_OBSERVED, user_data, request);
    answer_native(response, 200, b"OK", b"worker-native");
    0
}

#[test]
fn h2c_native_served_with_two_workers() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { vyl_managed_static_set_worker_count(server, 2) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/native", Some(h2c_worker_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h1 = unsafe { local_port(server) };
    let h2c = unsafe { h2c_port_of(server) };
    assert_ne!(h2c, 0);
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    h2_send_get(&mut stream, 1, b"/native");
    assert_eq!(h2_status(&mut stream, &mut decoder, 1, &mut decoded), 200);
    assert_eq!(h2_data(&mut stream, 1), b"worker-native");
    let observed = H2C_WORKER_OBSERVED.lock().expect("observed");
    assert_eq!(observed.user_data, tag as usize);
    assert_eq!(observed.path, b"/native");
    drop(observed);
    let head = get(h1, "/native");
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {head:?}"
    );
    assert!(head.ends_with("worker-native"), "unexpected body: {head:?}");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn dispatch_body_available_signal_admits_pull_then_count() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/upload", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\nhello")
        .expect("write post");
    let (token, mut signals) = unsafe { poll_request_with_signals(server) };
    // NOTE: the terminal signal carries the full staged window: fixed-length admission stages
    // every buffered byte at park, so the first end-flagged signal already counts the body.
    // Segment splits may stage partial updates first; the loop skips them for the terminal one.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let signal = loop {
        if let Some(done) = signals.iter().find(|event| event.path_len == 1) {
            break *done;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "terminal body signal stalls"
        );
        unsafe { gather_signals(server, &mut signals) };
    };
    assert_eq!(signal.token, token);
    assert_eq!(
        signal.route_id, 0,
        "signals reuse route_id as zero, the token correlates"
    );
    assert_eq!(signal.method_len, 5);
    assert_eq!(signal.path_len, 1);
    assert_eq!((signal.method_offset, signal.path_offset), (0, 0));
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
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
fn dispatch_body_available_partial_signal_precedes_completion() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/upload", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 11\r\n\r\nhello")
        .expect("write head and first segment");
    let (token, mut signals) = unsafe { poll_request_with_signals(server) };
    // NOTE: a partial signal admits a partial pull without consuming the exchange: the event is
    // advisory, so the completion flag stays clear until the staged window covers the total.
    // The rest stays unsent until the full first segment stages, so the terminal signal cannot
    // arrive early; coalesced updates grow the count monotonically to exactly the bytes sent.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if signals
            .iter()
            .any(|event| event.method_len == 5 && event.path_len == 0)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "partial body signal stalls"
        );
        unsafe { gather_signals(server, &mut signals) };
    }
    for signal in &signals {
        assert_eq!(signal.token, token);
        assert_eq!(signal.path_len, 0, "no completion with 5 of 11 bytes sent");
        assert!(
            signal.method_len <= 5,
            "signal never over-counts staged bytes"
        );
    }
    let mut first = [0u8; 5];
    let (result, pulled, end) = unsafe { pull_into(server, token, &mut first) };
    assert_eq!((result, pulled, end), (VylResult::Ok, 5, 0));
    assert_eq!(&first, b"hello");
    stream.write_all(b" world").expect("write second segment");
    assert_eq!(unsafe { drain_body(server, token) }, b" world");
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
fn dispatch_event_kind_mapping_reuses_length_fields_without_growth() {
    assert_eq!(VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE, 2);
    assert_eq!(VYL_MANAGED_STATIC_EVENT_WRITABLE, 3);
    assert_eq!(std::mem::size_of::<VylManagedStaticEvent>(), 80);
    let token = VylManagedStaticToken {
        worker_id: 7,
        slot: 9,
        request_seq: 11,
        stream_id: 13,
        worker_generation: 17,
        generation: 19,
    };
    let server_token = veylor_runtime::ConnectionToken {
        worker_id: 7,
        slot: 9,
        request_seq: 11,
        stream_id: 13,
        worker_generation: 17,
        generation: 19,
    };
    let body = super::dispatch::event_from_slot(veylor_server::EventSlot {
        kind: veylor_server::HostEventKind::BodyAvailable,
        token: server_token,
        route_id: 0,
        method_len: 5,
        path_len: 1,
        method_offset: 0,
        path_offset: 0,
    });
    assert_eq!(body.kind, VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE);
    assert_eq!(body.token, token);
    assert_eq!(body.route_id, 0);
    assert_eq!(body.method_len, 5);
    assert_eq!(body.path_len, 1);
    assert_eq!((body.method_offset, body.path_offset), (0, 0));
    // NOTE: the writable mapping is pinned end to end: the stream re-arm seam stages it once
    // per release (see the WRITABLE emission sites), so this event arrives on the wire below.
    let writable = super::dispatch::event_from_slot(veylor_server::EventSlot {
        kind: veylor_server::HostEventKind::Writable,
        token: server_token,
        route_id: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    });
    assert_eq!(writable.kind, VYL_MANAGED_STATIC_EVENT_WRITABLE);
    assert_eq!(writable.token, token);
    assert_eq!(writable.route_id, 0);
    assert_eq!((writable.method_len, writable.path_len), (0, 0));
    assert_eq!((writable.method_offset, writable.path_offset), (0, 0));
}

unsafe fn wait_writable_event(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) -> VylManagedStaticEvent {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut events = [VylManagedStaticEvent::default(); 1];
        let mut methods = [0u8; 256];
        let mut paths = [0u8; 4096];
        let report =
            unsafe { poll_into(server, 100_000_000, &mut events, &mut methods, &mut paths) };
        for event in events.iter().take(report.copied) {
            if event.kind == VYL_MANAGED_STATIC_EVENT_WRITABLE && event.token == token {
                return *event;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "writable signal stalls"
        );
    }
}

// NOTE: WRITABLE dispatch pin: the flushed head and each consumed write queue exactly one
// coalesced event with zeroed fields, and each event admits exactly one write. The waiter
// keeps advisory signals instead of assuming batch shape, so REQUEST-only polls stay exact.
#[test]
fn dispatch_writable_event_per_release_with_zero_fields() {
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
    let first = unsafe { wait_writable_event(server, token) };
    assert_eq!(first.kind, VYL_MANAGED_STATIC_EVENT_WRITABLE);
    assert_eq!(first.token, token);
    assert_eq!(first.route_id, 0);
    assert_eq!((first.method_len, first.path_len), (0, 0));
    assert_eq!((first.method_offset, first.path_offset), (0, 0));
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"hello ")) },
        VylResult::Ok
    );
    let second = unsafe { wait_writable_event(server, token) };
    assert_eq!(second.token, token);
    assert_eq!(second.route_id, 0);
    assert_eq!((second.method_len, second.path_len), (0, 0));
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
    let (head, tail) = split_head(&raw);
    head_with_date(
        head,
        b"HTTP/1.1 200 OK",
        b"\r\nTransfer-Encoding: chunked\r\n\r\n",
    );
    assert_eq!(tail, b"6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

fn h2_send_headers_open(stream: &mut TcpStream, id: u32, fields: &[HpackFieldToEncode<'_>]) {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(fields, &mut encoder, &mut block).expect("encode block");
    h2_write_frame(
        stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS,
        id,
        &block[..len],
    );
}

fn h2_write_data(stream: &mut TcpStream, id: u32, bytes: &[u8], end: bool) {
    let flags = if end { DATA_FLAG_END_STREAM } else { 0 };
    h2_write_frame(stream, FrameType::Data, flags, id, bytes);
}

fn h2_next_non_grant(stream: &mut TcpStream) -> (FrameHeader, Vec<u8>) {
    loop {
        let (header, payload) = h2_read_frame(stream);
        if header.frame_type == FrameType::WindowUpdate {
            continue;
        }
        return (header, payload);
    }
}

// NOTE: H2 BODY_AVAILABLE dispatch pin through the managed C ABI: staged DATA carries ready
// bytes with the end flag in the reused length fields like H1, so the host observes the full
// window before pulling. Grant frames interleave freely and are skipped, never asserted.
#[test]
fn h2c_managed_body_available_signals_ready_and_end() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/upload", u64::MAX) };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"5"),
    ];
    h2_send_headers_open(&mut stream, 1, &fields);
    h2_write_data(&mut stream, 1, b"hello", true);
    let (token, mut signals) = unsafe { poll_request_with_signals(server) };
    assert_eq!(token.stream_id, 1);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let signal = loop {
        if let Some(done) = signals.iter().find(|event| {
            event.kind == VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE
                && event.token == token
                && event.path_len == 1
        }) {
            break *done;
        }
        unsafe { gather_signals(server, &mut signals) };
        signals.retain(|event| {
            event.kind != VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE || event.token == token
        });
        assert!(
            std::time::Instant::now() < deadline,
            "terminal body signal stalls"
        );
    };
    assert_eq!(signal.token, token);
    assert_eq!(signal.route_id, 0);
    assert_eq!(signal.method_len, 5);
    assert_eq!((signal.method_offset, signal.path_offset), (0, 0));
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (header, payload) = h2_next_non_grant(&mut stream);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, 1);
    let (count, _) = decode_header_block(&payload, &mut decoder, &mut decoded).expect("decode");
    let mut status = 0;
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    assert_eq!(status, 200);
    let (header, body) = h2_next_non_grant(&mut stream);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(header.stream_id, 1);
    assert_eq!(header.flags & DATA_FLAG_END_STREAM, DATA_FLAG_END_STREAM);
    assert_eq!(body, b"done");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: H2 DATA wake pin: staged DATA must wake a thread-blocked poll the way the H1
// push_body paths do. The waiter blocks in long-timeout polls after the REQUEST drain, so
// only a DATA-driven notify returns it promptly; without one it sleeps the full timeout.
#[test]
fn h2c_data_staged_body_available_wakes_blocked_poll() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/upload", u64::MAX) };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"5"),
    ];
    h2_send_headers_open(&mut stream, 1, &fields);
    let token = unsafe { poll_one(server) };
    assert_eq!(token.stream_id, 1);
    let raw = server as usize;
    let waiter = std::thread::spawn(move || {
        let server = raw as *mut VylManagedStaticServer;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let start = std::time::Instant::now();
        let mut found = false;
        while !found && std::time::Instant::now() < deadline {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            let mut events = [VylManagedStaticEvent::default(); 8];
            let mut methods = [0u8; 256];
            let mut paths = [0u8; 4096];
            let report = unsafe {
                poll_into(
                    server,
                    left.as_nanos() as u64,
                    &mut events,
                    &mut methods,
                    &mut paths,
                )
            };
            found = events.iter().take(report.copied).any(|event| {
                event.kind == VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE && event.token == token
            });
        }
        (start.elapsed(), found)
    });
    std::thread::sleep(Duration::from_millis(200));
    h2_write_data(&mut stream, 1, b"hello", true);
    let (elapsed, found) = waiter.join().expect("waiter joins");
    assert!(found, "blocked poll never observed the H2 DATA body signal");
    assert!(
        elapsed < Duration::from_secs(4),
        "blocked poll slept {elapsed:?}: H2 DATA staged no wake"
    );
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (header, payload) = h2_next_non_grant(&mut stream);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, 1);
    let (count, _) = decode_header_block(&payload, &mut decoder, &mut decoded).expect("decode");
    let mut status = 0;
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    assert_eq!(status, 200);
    let (header, body) = h2_next_non_grant(&mut stream);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(header.stream_id, 1);
    assert_eq!(header.flags & DATA_FLAG_END_STREAM, DATA_FLAG_END_STREAM);
    assert_eq!(body, b"done");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

fn h2_send_trailers(stream: &mut TcpStream, id: u32, fields: &[HpackFieldToEncode<'_>]) {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(fields, &mut encoder, &mut block).expect("encode block");
    h2_write_frame(
        stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        id,
        &block[..len],
    );
}

fn h2_decode_status(
    decoder: &mut HpackTable<'_>,
    decoded: &mut [HpackHeader; 32],
    block: &[u8],
) -> u16 {
    let (count, _) = decode_header_block(block, decoder, decoded).expect("decode block");
    let mut status = 0;
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    status
}

fn h2_decode_pairs(
    decoder: &mut HpackTable<'_>,
    decoded: &mut [HpackHeader; 32],
    block: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (count, _) = decode_header_block(block, decoder, decoded).expect("decode block");
    decoded
        .iter()
        .take(count)
        .map(|header| {
            let name = decoder.resolve(header.field.name).expect("name").to_vec();
            let value = decoder.resolve(header.field.value).expect("value").to_vec();
            (name, value)
        })
        .collect()
}

fn h2_read_block_skipping_grants(stream: &mut TcpStream, id: u32) -> (Vec<u8>, bool) {
    let (header, payload) = h2_next_non_grant(stream);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, id);
    let mut block = payload;
    let ended = header.flags & HEADERS_FLAG_END_STREAM != 0;
    let mut flags = header.flags;
    while flags & HEADERS_FLAG_END_HEADERS == 0 {
        let (next, payload) = h2_next_non_grant(stream);
        assert_eq!(next.frame_type, FrameType::Continuation);
        assert_eq!(next.stream_id, id);
        flags = next.flags;
        block.extend_from_slice(&payload);
        if flags & CONTINUATION_FLAG_END_HEADERS != 0 {
            break;
        }
    }
    (block, ended)
}

fn h2c_read_simple(
    stream: &mut TcpStream,
    decoder: &mut HpackTable<'_>,
    decoded: &mut [HpackHeader; 32],
    id: u32,
) -> (u16, Vec<u8>) {
    let (block, ended) = h2_read_block_skipping_grants(stream, id);
    let status = h2_decode_status(decoder, decoded, &block);
    let mut body = Vec::new();
    if !ended {
        loop {
            let (header, payload) = h2_next_non_grant(stream);
            assert_eq!(header.stream_id, id);
            assert_eq!(header.frame_type, FrameType::Data);
            body.extend_from_slice(&payload);
            if header.flags & DATA_FLAG_END_STREAM != 0 {
                break;
            }
        }
    }
    (status, body)
}

fn h2c_read_streamed_with_trailers(
    stream: &mut TcpStream,
    decoder: &mut HpackTable<'_>,
    decoded: &mut [HpackHeader; 32],
    id: u32,
) -> H2TraileredResponse {
    let (block, ended) = h2_read_block_skipping_grants(stream, id);
    assert!(!ended, "response HEADERS must not end a trailered stream");
    let status = h2_decode_status(decoder, decoded, &block);
    let mut body = Vec::new();
    let mut trailers = Vec::new();
    loop {
        let (header, payload) = h2_next_non_grant(stream);
        assert_eq!(header.stream_id, id);
        match header.frame_type {
            FrameType::Data => {
                body.extend_from_slice(&payload);
                if header.flags & DATA_FLAG_END_STREAM != 0 {
                    panic!("DATA must not end a trailered stream");
                }
            }
            FrameType::Headers => {
                let mut block = payload;
                let mut flags = header.flags;
                while flags & HEADERS_FLAG_END_HEADERS == 0 {
                    let (next, payload) = h2_next_non_grant(stream);
                    assert_eq!(next.frame_type, FrameType::Continuation);
                    assert_eq!(next.stream_id, id);
                    flags = next.flags;
                    block.extend_from_slice(&payload);
                }
                let end = header.flags & HEADERS_FLAG_END_STREAM != 0;
                trailers.extend(h2_decode_pairs(decoder, decoded, &block));
                assert!(end, "terminal HEADERS must end the stream");
                break;
            }
            found => panic!("unexpected frame: {found:?}"),
        }
    }
    (status, body, trailers)
}

type H2TraileredResponse = (u16, Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>);

fn h2_expect_rst_h2(stream: &mut TcpStream, id: u32, code: u32) {
    loop {
        let (header, payload) = h2_read_frame(stream);
        if header.frame_type == FrameType::WindowUpdate {
            continue;
        }
        assert_eq!(header.frame_type, FrameType::RstStream);
        assert_eq!(header.stream_id, id);
        assert_eq!(payload, code.to_be_bytes(), "unexpected RST code");
        return;
    }
}

// NOTE: H2C response-trailer pin through the managed C ABI: HEADERS-open POST plus DATA-end
// stages the body, the empty-set trailer pull proves the staged whole-set discipline over
// H2C, and end-with-trailers lands terminal HEADERS with exact fields while the conn
// survives for a sibling. Plain managed registration; allowlist-registered routes serve
// H2C too, covered by h2c_managed_with_trailers_mirror_serves_and_enforces below.
#[test]
fn h2c_managed_stream_end_with_trailers_exact_fields() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/upload", u64::MAX) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"5"),
    ];
    h2_send_headers_open(&mut stream, 1, &fields);
    h2_write_data(&mut stream, 1, b"hello", true);
    let token = unsafe { poll_one(server) };
    assert_eq!(token.stream_id, 1);
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    // NOTE: no request trailers were sent, so no pull: only the H1 chunked terminal path
    // initializes the staged set (mark_chunked_done); a DATA-ended body stays NotReady
    // engine-wide. Whole-set request-trailer discipline is pinned by the next test.
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"chunk-one")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"chunk-two")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    let trailers = [field(b"x-checksum", b"abc"), field(b"x-tag", b"v1")];
    assert_eq!(
        unsafe { end_with_trailers(server, token, &trailers) },
        VylResult::Ok
    );
    let (status, body, got) =
        h2c_read_streamed_with_trailers(&mut stream, &mut decoder, &mut decoded, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"chunk-onechunk-two");
    assert_eq!(
        got,
        vec![
            (b"x-checksum".to_vec(), b"abc".to_vec()),
            (b"x-tag".to_vec(), b"v1".to_vec()),
        ]
    );
    assert_eq!(
        unsafe { end_with_trailers(server, token, &trailers) },
        VylResult::Gone
    );
    h2_send_get(&mut stream, 3, b"/hello");
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 3);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: H2C request-trailer pin through the managed C ABI: open POST plus DATA stages ready
// bytes without completion, the trailer HEADERS completes the set for a whole-set pull with
// capacity retry, and a prohibited set on the next stream RSTs while the conn survives for
// a sibling. Plain managed registration (the engine H2 trailer tests stage without an
// allowlist; the allowlist gate lives on the H1 park path only).
#[test]
fn h2c_managed_request_trailers_stage_and_pull() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    // NOTE: empty-allowlist routes RST trailer sets over H2C (same one-predicate rule as
    // H1), so this whole-set discipline pins against a listed allowlist.
    let allow = trailer_bytes(&[b"x-checksum", b"x-count"]);
    unsafe { register_managed_with_trailers(server, b"POST", b"/upload", u64::MAX, &allow) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"4"),
    ];
    h2_send_headers_open(&mut stream, 1, &fields);
    h2_write_data(&mut stream, 1, b"data", false);
    let token = unsafe { poll_one(server) };
    assert_eq!(token.stream_id, 1);
    let mut names = [0xAAu8; 32];
    let mut values = [0xAAu8; 16];
    let (early, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(early, VylResult::WouldBlock);
    assert_eq!(report, VylManagedStaticTrailerReport::default());
    h2_send_trailers(
        &mut stream,
        1,
        &[h2_field(b"x-checksum", b"abc"), h2_field(b"x-count", b"2")],
    );
    assert_eq!(unsafe { drain_body(server, token) }, b"data");
    // NOTE: trailer readiness can lag body-end by a turn (the engine pin spins too), so
    // the short-buffer probe retries WouldBlock until the whole set stages at once.
    let report = {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut names = [0u8; 2];
            let mut values = [0u8; 2];
            let (short, report) =
                unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
            assert_eq!((names, values), ([0u8; 2], [0u8; 2]));
            if short == VylResult::Ok {
                break report;
            }
            assert_eq!(short, VylResult::WouldBlock);
            assert_eq!(report, VylManagedStaticTrailerReport::default());
            assert!(
                std::time::Instant::now() < deadline,
                "staged trailer set never became ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    assert_eq!(report.fields, 2);
    assert_eq!((report.names_copied, report.values_copied), (0, 0));
    assert_eq!((report.names_required, report.values_required), (18, 5));
    let mut names = [0u8; 18];
    let mut values = [0u8; 5];
    let (full, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(full, VylResult::Ok);
    assert_eq!(report.fields, 2);
    assert_eq!((report.names_copied, report.values_copied), (18, 5));
    assert_eq!((report.names_required, report.values_required), (0, 0));
    assert_eq!(&names, b"x-checksum\x00x-count");
    assert_eq!(&values, b"abc\x002");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"done");
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"4"),
    ];
    h2_send_headers_open(&mut stream, 3, &fields);
    let refused = unsafe { poll_one(server) };
    assert_eq!(refused.stream_id, 3);
    h2_write_data(&mut stream, 3, b"data", false);
    h2_send_trailers(&mut stream, 3, &[h2_field(b"connection", b"close")]);
    h2_expect_rst_h2(&mut stream, 3, 1);
    h2_send_get(&mut stream, 5, b"/hello");
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 5);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: file-mount-over-H2C coverage through the managed C ABI. Enabling H2C forces the
// start-time rebind (api.rs gates the direct path on no-H2C plus single worker), so the
// mount below exercises the ReadyRouteOp::FileMount replay (mod.rs ReadyRouteOp::apply
// into Server::register_file_mount); every stream then pins the H2 file dispatch shapes
// (h2/file.rs plan_file/emit_file, h2/routes.rs send_file_head) against the H1 matrix
// above. One stateful HPACK decoder rides the connection because response blocks index.

fn h2_file_field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

fn h2_send_file(
    stream: &mut TcpStream,
    id: u32,
    method: &[u8],
    path: &[u8],
    extra: &[(&[u8], &[u8])],
) {
    let mut fields = Vec::with_capacity(4 + extra.len());
    fields.push(h2_file_field(b":method", method));
    fields.push(h2_file_field(b":scheme", b"http"));
    fields.push(h2_file_field(b":path", path));
    fields.push(h2_file_field(b":authority", b"localhost"));
    for (name, value) in extra {
        fields.push(h2_file_field(name, value));
    }
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 1024];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    h2_write_frame(
        stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        id,
        &block[..len],
    );
}

type H2FileResponse = (u16, Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>);

fn h2c_read_full(
    stream: &mut TcpStream,
    decoder: &mut HpackTable<'_>,
    decoded: &mut [HpackHeader; 32],
    id: u32,
) -> H2FileResponse {
    let (block, ended) = h2_read_block_skipping_grants(stream, id);
    let pairs = h2_decode_pairs(decoder, decoded, &block);
    let status = pairs
        .iter()
        .find(|(name, _)| name == b":status")
        .map(|(_, value)| {
            std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int")
        })
        .expect(":status");
    let mut body = Vec::new();
    if !ended {
        loop {
            let (header, payload) = h2_next_non_grant(stream);
            assert_eq!(header.stream_id, id);
            assert_eq!(header.frame_type, FrameType::Data);
            body.extend_from_slice(&payload);
            if header.flags & DATA_FLAG_END_STREAM != 0 {
                break;
            }
        }
    }
    (status, pairs, body)
}

fn h2_header<'a>(headers: &'a [(Vec<u8>, Vec<u8>)], name: &str) -> Option<&'a str> {
    headers.iter().find_map(|(found, value)| {
        (found == name.as_bytes()).then(|| std::str::from_utf8(value).expect("header utf8"))
    })
}

#[test]
fn file_mount_h2c_serves_full_range_conditional_matrix() {
    let fixture = FixtureDir::new("h2c-file");
    fixture.write("data.txt", b"0123456789");
    let root = mount_root_bytes(&fixture.path);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    // NOTE: 9 date-carrying responses need 521 scratch bytes; 512 predates H2 Date.
    let mut scratch = [0; 1024];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];

    h2_send_file(&mut stream, 1, b"GET", b"/assets/data.txt", &[]);
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"0123456789");
    assert_eq!(h2_header(&headers, "content-type"), Some("text/plain"));
    assert_eq!(h2_header(&headers, "accept-ranges"), Some("bytes"));
    assert_eq!(h2_header(&headers, "content-length"), Some("10"));
    let last_modified = h2_header(&headers, "last-modified")
        .expect("last-modified")
        .to_string();
    assert_eq!(last_modified.len(), 29, "IMF-fixdate: {last_modified:?}");

    h2_send_file(&mut stream, 3, b"HEAD", b"/assets/data.txt", &[]);
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 3);
    assert_eq!(status, 200);
    assert!(body.is_empty(), "HEAD carries no body");
    assert_eq!(h2_header(&headers, "content-length"), Some("10"));
    assert_eq!(h2_header(&headers, "content-type"), Some("text/plain"));
    assert_eq!(h2_header(&headers, "content-range"), None);

    h2_send_file(
        &mut stream,
        5,
        b"HEAD",
        b"/assets/data.txt",
        &[(b"range" as &[u8], b"bytes=0-1" as &[u8])],
    );
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 5);
    assert_eq!(status, 200, "HEAD ignores Range");
    assert!(body.is_empty(), "HEAD carries no body");
    assert_eq!(h2_header(&headers, "content-range"), None);
    assert_eq!(h2_header(&headers, "content-length"), Some("10"));

    h2_send_file(
        &mut stream,
        7,
        b"GET",
        b"/assets/data.txt",
        &[(b"range" as &[u8], b"bytes=2-5" as &[u8])],
    );
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 7);
    assert_eq!(status, 206);
    assert_eq!(body, b"2345");
    assert_eq!(h2_header(&headers, "content-range"), Some("bytes 2-5/10"));
    assert_eq!(h2_header(&headers, "accept-ranges"), Some("bytes"));
    assert_eq!(h2_header(&headers, "content-length"), Some("4"));

    h2_send_file(
        &mut stream,
        9,
        b"GET",
        b"/assets/data.txt",
        &[(b"range" as &[u8], b"bytes=0-1, 4-5" as &[u8])],
    );
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 9);
    assert_eq!(status, 206);
    let ctype = h2_header(&headers, "content-type").expect("multipart content-type");
    assert!(
        ctype.starts_with("multipart/byteranges; boundary="),
        "ctype: {ctype:?}"
    );
    assert_eq!(h2_header(&headers, "content-range"), None);
    let framed: usize = h2_header(&headers, "content-length")
        .expect("multipart length")
        .parse()
        .expect("length int");
    assert_eq!(framed, body.len(), "multipart length frames the whole body");
    let text = String::from_utf8(body).expect("multipart utf8");
    assert!(
        text.contains("Content-Range: bytes 0-1/10"),
        "body: {text:?}"
    );
    assert!(
        text.contains("Content-Range: bytes 4-5/10"),
        "body: {text:?}"
    );
    assert!(text.contains("01"), "body: {text:?}");
    assert!(text.contains("45"), "body: {text:?}");

    h2_send_file(
        &mut stream,
        11,
        b"GET",
        b"/assets/data.txt",
        &[(b"range" as &[u8], b"bytes=99-" as &[u8])],
    );
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 11);
    assert_eq!(status, 416);
    assert_eq!(h2_header(&headers, "content-range"), Some("bytes */10"));
    assert_eq!(h2_header(&headers, "content-type"), None);
    assert!(body.is_empty(), "416 carries no body");

    h2_send_file(
        &mut stream,
        13,
        b"GET",
        b"/assets/data.txt",
        &[(b"if-modified-since" as &[u8], last_modified.as_bytes())],
    );
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 13);
    assert_eq!(status, 304);
    assert!(body.is_empty(), "304 carries no body");
    assert_eq!(
        h2_header(&headers, "last-modified"),
        Some(last_modified.as_str())
    );

    h2_send_file(
        &mut stream,
        15,
        b"GET",
        b"/assets/data.txt",
        &[(
            b"if-modified-since" as &[u8],
            b"Thu, 01 Jan 1970 00:00:00 GMT" as &[u8],
        )],
    );
    let (status, _, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 15);
    assert_eq!(status, 200);
    assert_eq!(body, b"0123456789");

    h2_send_file(&mut stream, 17, b"GET", b"/assets/missing.txt", &[]);
    let (status, _, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 17);
    assert_eq!(status, 404);
    assert_eq!(body, b"Not Found");

    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: with_trailers registration mirrors to H2C like every other managed route: the
// projection carries the same allowlist H1 reads at park, so a route registered only this
// way still parks, and unlisted trailer sets RST the stream while the connection survives.
#[test]
// NOTE: allowlist routes mirror to H2C and enforce there: the listed set stages whole,
// the unlisted set RSTs the stream, the connection survives.
fn h2c_managed_with_trailers_mirror_serves_and_enforces() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let allow = trailer_bytes(&[b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/upload", u64::MAX, &allow) },
        VylResult::Ok
    );
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"4"),
    ];
    h2_send_headers_open(&mut stream, 1, &fields);
    h2_write_data(&mut stream, 1, b"data", true);
    let token = unsafe { poll_one(server) };
    assert_eq!(token.stream_id, 1);
    assert_eq!(unsafe { drain_body(server, token) }, b"data");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"done");
    h2_send_headers_open(&mut stream, 3, &fields);
    let refused = unsafe { poll_one(server) };
    assert_eq!(refused.stream_id, 3);
    h2_write_data(&mut stream, 3, b"data", false);
    h2_send_trailers(&mut stream, 3, &[h2_field(b"x-evil", b"0")]);
    h2_expect_rst_h2(&mut stream, 3, 1);
    h2_send_headers_open(&mut stream, 5, &fields);
    let listed = unsafe { poll_one(server) };
    assert_eq!(listed.stream_id, 5);
    h2_write_data(&mut stream, 5, b"data", false);
    h2_send_trailers(&mut stream, 5, &[h2_field(b"x-tag", b"1")]);
    assert_eq!(unsafe { drain_body(server, listed) }, b"data");
    let report = {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut names = [0u8; 1];
            let mut values = [0u8; 1];
            let (short, report) =
                unsafe { pull_trailers_into(server, listed, &mut names, &mut values) };
            if short == VylResult::Ok {
                break report;
            }
            assert_eq!(short, VylResult::WouldBlock);
            assert!(
                std::time::Instant::now() < deadline,
                "listed trailer set never became ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    assert_eq!(report.fields, 1);
    let mut names = [0u8; 5];
    let mut values = [0u8; 1];
    let (full, report) = unsafe { pull_trailers_into(server, listed, &mut names, &mut values) };
    assert_eq!(full, VylResult::Ok);
    assert_eq!(report.fields, 1);
    assert_eq!(&names, b"x-tag");
    assert_eq!(&values, b"1");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, listed, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 5);
    assert_eq!(status, 200);
    assert_eq!(body, b"done");
    h2_send_get(&mut stream, 7, b"/hello");
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 7);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

const ROTATE_CERT1: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_cert.der");
const ROTATE_KEY1: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_key.der");
const ROTATE_CERT2: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_cert2.der");
const ROTATE_KEY2: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_key2.der");

unsafe fn enable_tls(server: *mut VylManagedStaticServer, cert: &[u8], key: &[u8]) {
    assert_eq!(
        unsafe {
            vyl_managed_static_enable_tls(
                server,
                cert.as_ptr(),
                cert.len(),
                key.as_ptr(),
                key.len(),
                16,
            )
        },
        VylResult::Ok
    );
}

#[test]
fn rotate_tls_validates_state_and_applies_on_running_server() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            vyl_managed_static_rotate_tls(
                server,
                ROTATE_CERT2.as_ptr(),
                ROTATE_CERT2.len(),
                ROTATE_KEY2.as_ptr(),
                ROTATE_KEY2.len(),
            )
        },
        VylResult::BadState
    );
    unsafe { enable_tls(server, ROTATE_CERT1, ROTATE_KEY1) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe {
            vyl_managed_static_rotate_tls(
                server,
                ROTATE_CERT2.as_ptr(),
                ROTATE_CERT2.len(),
                ROTATE_KEY2.as_ptr(),
                ROTATE_KEY2.len(),
            )
        },
        VylResult::Ok
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_rotate_tls(
                server,
                b"-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----".as_ptr(),
                55,
                ROTATE_KEY2.as_ptr(),
                ROTATE_KEY2.len(),
            )
        },
        VylResult::InvalidArgument
    );
    let _ = unsafe { stop_join(server) };
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

static NATIVE_GZIP_BODY: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();

fn native_gzip_body() -> &'static [u8] {
    NATIVE_GZIP_BODY.get_or_init(|| b"The quick brown fox jumps over the lazy dog. ".repeat(50))
}

unsafe extern "C" fn native_gzip_handler(
    _user_data: *mut c_void,
    _request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    answer_native(response, 200, b"OK", native_gzip_body());
    0
}

#[test]
fn gzip_native_route_round_trips_through_ffi() {
    let plain = b"The quick brown fox jumps over the lazy dog. ".repeat(50);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            vyl_managed_static_register_native_route_with_gzip(
                server,
                bytes(b"GET"),
                bytes(b"/gzip-native"),
                Some(native_gzip_handler),
                std::ptr::null_mut(),
            )
        },
        VylResult::Ok
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_native_route_with_gzip(
                server,
                bytes(b"GET"),
                bytes(b"/gzip-native"),
                Some(native_gzip_handler),
                std::ptr::null_mut(),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_native_route_with_gzip(
                std::ptr::null_mut(),
                bytes(b"GET"),
                bytes(b"/gzip-native"),
                Some(native_gzip_handler),
                std::ptr::null_mut(),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    assert_eq!(
        unsafe {
            vyl_managed_static_register_native_route_with_gzip(
                server,
                bytes(b"GET"),
                bytes(b"/late"),
                Some(native_gzip_handler),
                std::ptr::null_mut(),
            )
        },
        VylResult::BadState
    );

    let (status, head, gzipped) = get_raw(port, "/gzip-native", "Accept-Encoding: gzip\r\n");
    assert_eq!(status, 200);
    assert!(
        head.contains("Content-Encoding: gzip"),
        "encoding: {head:?}"
    );
    assert!(head.contains("Vary: Accept-Encoding"), "vary: {head:?}");
    assert_eq!(&gzipped[..2], &[0x1f, 0x8b], "gzip magic");
    assert!(gzipped.len() < plain.len());

    let (plain_status, plain_head, identity) = get_raw(port, "/gzip-native", "");
    assert_eq!(plain_status, 200);
    assert_eq!(identity, plain);
    assert!(
        !plain_head.to_ascii_lowercase().contains("content-encoding"),
        "no encoding: {plain_head:?}"
    );

    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 2);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

fn managed_respond_headers(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    status: u16,
    reason: &[u8],
    headers: &[(&[u8], &[u8])],
    body: &[u8],
) -> VylResult {
    let names: Vec<VylBytes> = headers.iter().map(|(name, _)| bytes(name)).collect();
    let values: Vec<VylBytes> = headers.iter().map(|(_, value)| bytes(value)).collect();
    unsafe {
        vyl_managed_static_host_respond_with_headers(
            server,
            token,
            status,
            bytes(reason),
            names.as_ptr(),
            values.as_ptr(),
            headers.len(),
            bytes(body),
        )
    }
}

#[test]
fn managed_respond_with_headers_serves_them_after_date() {
    let plain: Vec<u8> = (0..3000).map(|i| (i % 251) as u8).collect();
    let gzipped = {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plain).expect("gzip body");
        encoder.finish().expect("finish gzip")
    };
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"GET", b"/work", u64::MAX) };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let send_get = |extra: &str| {
        let mut stream = connect_loopback(port);
        let request =
            format!("GET /work HTTP/1.1\r\nHost: test\r\n{extra}Connection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).expect("write request");
        stream
    };
    let read_response = |stream: &mut TcpStream| {
        let mut raw = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            stream.read_exact(&mut byte).expect("read head byte");
            raw.push(byte[0]);
            if raw.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let head = String::from_utf8(raw).expect("utf8 head");
        let status: u16 = head
            .split_whitespace()
            .nth(1)
            .expect("status")
            .parse()
            .expect("status int");
        let length: usize = head
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
        let mut body = vec![0u8; length];
        stream.read_exact(&mut body).expect("read body");
        (status, head, body)
    };
    let mut stream = send_get("");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        managed_respond_headers(
            server,
            token,
            200,
            b"OK",
            &[(b"content-type".as_slice(), b"text/plain".as_slice())],
            b"plain",
        ),
        VylResult::Ok
    );
    let (status, head, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"plain");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: text/plain"),
        "headers: {head}"
    );
    let mut stream = send_get("");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        managed_respond_headers(
            server,
            token,
            200,
            b"OK",
            &[(b"content-encoding".as_slice(), b"gzip".as_slice())],
            &gzipped,
        ),
        VylResult::Ok
    );
    let (gzip_status, gzip_head, gzip_body) = read_response(&mut stream);
    assert_eq!(gzip_status, 200);
    let mut decoder = flate2::read::GzDecoder::new(&gzip_body[..]);
    let mut plain_back = Vec::new();
    decoder.read_to_end(&mut plain_back).expect("gunzip body");
    assert_eq!(plain_back, plain);
    assert!(
        gzip_head
            .to_ascii_lowercase()
            .contains("content-encoding: gzip")
    );
    let mut stream = send_get("");
    let token = unsafe { poll_one(server) };
    assert_eq!(
        managed_respond_headers(
            server,
            token,
            200,
            b"OK",
            &[(b"content-length".as_slice(), b"5".as_slice())],
            b"plain",
        ),
        VylResult::InvalidArgument
    );
    assert_eq!(
        managed_respond_headers(server, token, 200, b"OK", &[], b"plain"),
        VylResult::Ok
    );
    let (status, _, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"plain");
    let counts = unsafe { stop_join(server) };
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
    let _ = counts;
}

#[test]
fn managed_stream_begin_with_headers_streams_them() {
    let config = test_config(0);
    let idle = unsafe { create(&config) };
    unsafe { register_managed(idle, b"GET", b"/s", u64::MAX) };
    assert_eq!(
        unsafe {
            vyl_managed_static_host_begin_stream_with_headers(
                idle,
                VylManagedStaticToken::default(),
                200,
                bytes(b"OK"),
                std::ptr::null(),
                std::ptr::null(),
                0,
            )
        },
        VylResult::BadState
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(idle) }, VylResult::Ok);
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
    let names = [bytes(b"content-type"), bytes(b"x-test")];
    let values = [bytes(b"text/plain"), bytes(b"yes")];
    assert_eq!(
        unsafe {
            vyl_managed_static_host_begin_stream_with_headers(
                server,
                token,
                200,
                bytes(b"OK"),
                names.as_ptr(),
                values.as_ptr(),
                names.len(),
            )
        },
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
    assert_eq!(
        unsafe { vyl_managed_static_host_end_stream(server, token) },
        VylResult::Ok
    );
    let (status, raw, body) = read_streamed(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello world");
    let (head, tail) = split_head(&raw);
    head_with_date(
        head,
        b"HTTP/1.1 200 OK",
        b"\r\ncontent-type: text/plain\r\nx-test: yes\r\nTransfer-Encoding: chunked\r\n\r\n",
    );
    assert_eq!(tail, b"6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn config_binds_address_and_applies_limits() {
    let mut config = test_config(0);
    let address = b"0.0.0.0";
    config.address = bytes(address);
    config.max_connections = 1;
    config.worker_count = 2;

    let server = unsafe { create(&config) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let response = get(port, "/hello");
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 1);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn config_rejects_invalid_address_and_limits() {
    let mut config = test_config(0);
    let bad_address = b"not-an-ip";
    config.address = bytes(bad_address);
    let mut server = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_managed_static_new(&config, &mut server) },
        VylResult::InvalidArgument
    );

    let mut config = test_config(0);
    config.max_connections = 1025;
    let mut server = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_managed_static_new(&config, &mut server) },
        VylResult::InvalidArgument
    );

    let mut config = test_config(0);
    config.worker_count = MAX_WORKERS as u64 + 1;
    let mut server = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_managed_static_new(&config, &mut server) },
        VylResult::InvalidArgument
    );
}
