use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::time::Duration;

use super::*;
use crate::abi::{VylBytes, VylResult};

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
