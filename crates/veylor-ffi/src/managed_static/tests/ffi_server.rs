use super::super::*;
use super::managed_peer::*;
use crate::abi::{VylBytes, VylField, VylResult};
use parking_lot::Mutex;
use std::ffi::c_void;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::time::Duration;

pub(crate) fn test_config(port: u16) -> VylManagedStaticConfig {
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

pub(crate) fn bytes(data: &[u8]) -> VylBytes {
    VylBytes {
        data: data.as_ptr(),
        length: data.len(),
    }
}

pub(crate) unsafe fn create(config: &VylManagedStaticConfig) -> *mut VylManagedStaticServer {
    let mut server = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_managed_static_new(config, &mut server) },
        VylResult::Ok
    );
    assert!(!server.is_null());
    server
}

pub(crate) unsafe fn register(
    server: *mut VylManagedStaticServer,
    method: &[u8],
    path: &[u8],
    body: &[u8],
) {
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

pub(crate) unsafe fn register_capped(
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

pub(crate) unsafe fn register_managed(
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

pub(crate) unsafe fn local_port(server: *mut VylManagedStaticServer) -> u16 {
    let mut port = 0;
    assert_eq!(
        unsafe { vyl_managed_static_local_addr(server, &mut port) },
        VylResult::Ok
    );
    assert_ne!(port, 0);
    port
}

pub(crate) fn get(port: u16, path: &str) -> String {
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

pub(crate) fn get_raw(port: u16, path: &str, extra: &str) -> (u16, String, Vec<u8>) {
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

pub(crate) unsafe fn stop_join(server: *mut VylManagedStaticServer) -> VylManagedStaticCloseCounts {
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

pub(crate) fn counts_total(counts: VylManagedStaticCloseCounts) -> u64 {
    counts.completed
        + counts.peer_closed
        + counts.reset
        + counts.timeout
        + counts.protocol_error
        + counts.response_overflow
        + counts.transport_error
        + counts.shutdown
}

pub(crate) fn read_response(stream: &mut TcpStream) -> (u16, Vec<u8>) {
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

pub(crate) fn field(name: &[u8], value: &[u8]) -> VylField {
    VylField {
        name: bytes(name),
        value: bytes(value),
    }
}

pub(crate) unsafe fn end_with_trailers(
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

pub(crate) struct FixtureDir {
    pub(crate) path: std::path::PathBuf,
}

impl FixtureDir {
    pub(crate) fn new(name: &str) -> Self {
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
    pub(crate) fn write(&self, name: &str, contents: &[u8]) {
        std::fs::write(self.path.join(name), contents).expect("write fixture file");
    }
}

impl Drop for FixtureDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub(crate) unsafe fn mount_raw(
    server: *mut VylManagedStaticServer,
    prefix: &[u8],
    root: &[u8],
) -> VylResult {
    unsafe {
        super::super::vyl_managed_static_register_file_mount(server, bytes(prefix), bytes(root))
    }
}

pub(crate) fn mount_root_bytes(path: &std::path::Path) -> Vec<u8> {
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

pub(crate) struct NativeObserved {
    pub(crate) user_data: usize,
    pub(crate) method: Vec<u8>,
    pub(crate) path: Vec<u8>,
    pub(crate) has_body: u8,
    pub(crate) body_len: u64,
    pub(crate) body: Vec<u8>,
}

impl NativeObserved {
    pub(crate) const fn empty() -> Self {
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

pub(crate) static NATIVE_TAG: u8 = 0x5E;

pub(crate) unsafe fn view_bytes(data: *const u8, len: usize) -> Vec<u8> {
    if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
    }
}

pub(crate) fn record_native(
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
    *observed.lock() = NativeObserved {
        user_data: user_data as usize,
        method,
        path,
        has_body: request.has_body,
        body_len: request.body_len,
        body,
    };
}

pub(crate) fn answer_native(
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

pub(crate) unsafe fn register_native(
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

pub(crate) fn native_tag() -> *mut c_void {
    &NATIVE_TAG as *const u8 as *mut c_void
}

// NOTE: native-over-H2C coverage through the managed C ABI. Every H2C configuration
// rebinds at start (api.rs gates the direct path on no-H2C plus single worker), so each
// test below exercises the ReadyRouteOp::Native replay into the rebound server; the H1
// reads on the same handles pin the direct path alongside. One stateful HPACK decoder
// rides each connection because response blocks index across streams.

pub(crate) unsafe fn h2c_port_of(server: *mut VylManagedStaticServer) -> u16 {
    let mut port = 0;
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::Ok
    );
    assert_ne!(port, 0);
    port
}
