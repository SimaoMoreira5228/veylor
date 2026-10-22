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
