use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use veylor_runtime::ConnectionTimeouts;
use veylor_server::{
    MAX_RESPONSE_BODY, MAX_STATIC_BODY_BYTES, NativeRequestView, NativeResponseOut, RunningServer,
    Server, ServerConfig,
};

pub(crate) const DEADLINE: Duration = Duration::from_secs(5);

pub(crate) fn test_server() -> RunningServer {
    test_server_with_config(ServerConfig::default())
}

pub(crate) fn test_server_with_config(config: ServerConfig) -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
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
    server.start().expect("start server")
}

pub(crate) fn connect(server: &RunningServer) -> TcpStream {
    let stream = TcpStream::connect(server.addr()).expect("connect loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
}

pub(crate) struct ClientResponse {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

pub(crate) fn read_response(stream: &mut TcpStream) -> ClientResponse {
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
    let status_line = lines.next().expect("status line");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status int");
    let mut headers = Vec::new();
    let mut content_length = 0;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').expect("header colon");
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().expect("content length int");
        }
        headers.push((name.to_string(), value.trim().to_string()));
    }
    let mut body = vec![0; content_length];
    stream.read_exact(&mut body).expect("read body");
    ClientResponse {
        status,
        headers,
        body,
    }
}

pub(crate) fn read_eof(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        other => panic!("expected EOF, got {other:?}"),
    }
}

pub(crate) fn read_closed(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Err(_) => {}
        Ok(_) => panic!("expected close, got data"),
    }
}

pub(crate) fn get(stream: &mut TcpStream, path: &str) {
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n");
    stream.write_all(request.as_bytes()).expect("write request");
}

pub(crate) fn tight_config(
    head_ns: u64,
    body_ns: u64,
    write_ns: u64,
    idle_ns: u64,
) -> ServerConfig {
    ServerConfig {
        timeouts: ConnectionTimeouts {
            request_head_ns: head_ns,
            body_progress_ns: body_ns,
            response_write_ns: write_ns,
            keep_alive_idle_ns: idle_ns,
        },
        ..ServerConfig::default()
    }
}

pub(crate) fn capped_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_route(b"POST", b"/tight", 200, b"OK", b"tight", 16)
        .expect("tight");
    server
        .register_route(b"POST", b"/zero", 200, b"OK", b"zero", 0)
        .expect("zero");
    server
        .register_route(b"POST", b"/huge", 200, b"OK", b"huge", u64::MAX)
        .expect("huge");
    server
        .register_managed(b"POST", b"/mupload", 16)
        .expect("managed");
    server.start().expect("start server")
}

pub(crate) static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) const FILE_BLOB_LEN: usize = 1 << 20;

pub(crate) struct FileFixture {
    pub(crate) dir: PathBuf,
    pub(crate) root: PathBuf,
    pub(crate) blob: Vec<u8>,
}

impl FileFixture {
    pub(crate) fn build() -> Self {
        let id = NEXT_FILE_ID.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("veylor-srv-file-{}-{id}", std::process::id()));
        let root = dir.join("root");
        std::fs::create_dir_all(&root).expect("fixture root");
        std::fs::write(root.join("hello.txt"), b"hello file").expect("hello file");
        std::fs::write(root.join("empty.txt"), b"").expect("empty file");
        std::fs::write(dir.join("outside.txt"), b"outside secret").expect("outside file");
        let mut blob = Vec::with_capacity(FILE_BLOB_LEN);
        for i in 0..FILE_BLOB_LEN {
            blob.push((i % 251) as u8);
        }
        std::fs::write(root.join("blob.bin"), &blob).expect("blob file");
        Self { dir, root, blob }
    }
}

impl Drop for FileFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub(crate) fn file_server() -> (RunningServer, FileFixture) {
    let fixture = FileFixture::build();
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
    server
        .register_file_mount(b"/assets/", &fixture.root)
        .expect("register assets");
    (server.start().expect("start server"), fixture)
}

pub(crate) fn header<'a>(response: &'a ClientResponse, name: &str) -> &'a str {
    response
        .headers
        .iter()
        .find(|(field, _)| field.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
        .expect("header present")
}

pub(crate) fn get_range(stream: &mut TcpStream, path: &str, range: &str) {
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nRange: {range}\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .expect("write ranged request");
}

pub(crate) fn read_head(stream: &mut TcpStream) -> String {
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
    String::from_utf8(head).expect("head utf8")
}

// NOTE: native-handler suite: handlers below are extern "C" to prove the C-ABI boundary.
// A panicking handler aborts the process (nounwind frame, uncatchable engine-side), so
// native_boom contains its own panic and reports failure — the contractual handler pattern
// — which the engine fails closed without a partial write.
#[allow(unsafe_code)]
pub(crate) unsafe extern "C" fn native_fixed(
    _user_data: *mut std::ffi::c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }

    let view = unsafe { &*request };
    let out = unsafe { &mut *response };
    if view.method_len == 0 || view.path_len == 0 {
        return 1;
    }
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = b"native-hello".as_ptr();
    out.body_len = 12;
    0
}

#[allow(unsafe_code)]
pub(crate) unsafe extern "C" fn native_stray_304(
    _user_data: *mut std::ffi::c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if response.is_null() {
        return 1;
    }

    let out = unsafe { &mut *response };
    out.status = 304;
    out.reason = b"Not Modified".as_ptr();
    out.reason_len = 12;
    out.body = b"stray-body".as_ptr();
    out.body_len = 10;
    0
}

#[allow(unsafe_code)]
pub(crate) unsafe extern "C" fn native_boom(
    _user_data: *mut std::ffi::c_void,
    _request: *const NativeRequestView,
    _response: *mut NativeResponseOut,
) -> i32 {
    let contained = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        panic!("native handler must not unwind across the boundary");
    }));
    i32::from(contained.is_ok())
}

pub(crate) static NATIVE_REENTER_REJECTED: AtomicBool = AtomicBool::new(false);

#[allow(unsafe_code)]
pub(crate) unsafe extern "C" fn native_reenter(
    _user_data: *mut std::ffi::c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut nested = Server::bind(addr, ServerConfig::default()).expect("nested bind");
    let rejected = nested
        .register_route(b"GET", b"/nested", 200, b"OK", b"x", MAX_STATIC_BODY_BYTES)
        .is_err();
    NATIVE_REENTER_REJECTED.store(rejected, Ordering::SeqCst);
    if response.is_null() {
        return 1;
    }

    let out = unsafe { &mut *response };
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = b"reentered".as_ptr();
    out.body_len = 9;
    0
}

pub(crate) static NATIVE_LARGE_BODY: [u8; MAX_RESPONSE_BODY] = [b'N'; MAX_RESPONSE_BODY];

// NOTE: echo/record statics below are per-handler (never shared across handlers), so parallel
// tests recording through different routes never observe each other. The echo buffer is static
// storage the engine copies before return; the default single-worker pump invokes handlers
// sequentially, so no concurrent invoke can realloc it mid-copy.
pub(crate) static NATIVE_ECHO_BODY: parking_lot::Mutex<Vec<u8>> =
    parking_lot::Mutex::new(Vec::new());

pub(crate) static NATIVE_RECORD_BODY_NULL: AtomicBool = AtomicBool::new(false);

pub(crate) static NATIVE_RECORD_HAS_BODY: AtomicBool = AtomicBool::new(false);

pub(crate) static NATIVE_RECORD_BODY_LEN: AtomicU64 = AtomicU64::new(0);

#[allow(unsafe_code)]
pub(crate) unsafe extern "C" fn native_echo(
    _user_data: *mut std::ffi::c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }

    let view = unsafe { &*request };
    let out = unsafe { &mut *response };
    let mut staged = NATIVE_ECHO_BODY.lock();
    staged.clear();
    if !view.body.is_null() && view.body_len > 0 {
        let bytes = unsafe { std::slice::from_raw_parts(view.body, view.body_len as usize) };
        staged.extend_from_slice(bytes);
    }
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = staged.as_ptr();
    out.body_len = staged.len();
    0
}

#[allow(unsafe_code)]
pub(crate) unsafe extern "C" fn native_record(
    _user_data: *mut std::ffi::c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }

    let view = unsafe { &*request };
    let out = unsafe { &mut *response };
    NATIVE_RECORD_BODY_NULL.store(view.body.is_null(), Ordering::SeqCst);
    NATIVE_RECORD_HAS_BODY.store(view.has_body != 0, Ordering::SeqCst);
    NATIVE_RECORD_BODY_LEN.store(view.body_len, Ordering::SeqCst);
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = b"recorded".as_ptr();
    out.body_len = 8;
    0
}

#[allow(unsafe_code)]
pub(crate) unsafe extern "C" fn native_large(
    _user_data: *mut std::ffi::c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if response.is_null() {
        return 1;
    }

    let out = unsafe { &mut *response };
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = NATIVE_LARGE_BODY.as_ptr();
    out.body_len = NATIVE_LARGE_BODY.len();
    0
}

pub(crate) fn native_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_native(b"GET", b"/fixed", Some(native_fixed), 0)
        .expect("register fixed");
    server
        .register_native(b"GET", b"/boom", Some(native_boom), 0)
        .expect("register boom");
    server
        .register_native(b"GET", b"/reenter", Some(native_reenter), 0)
        .expect("register reenter");
    server
        .register_native(b"POST", b"/large", Some(native_large), 0)
        .expect("register large");
    server
        .register_native(b"POST", b"/echo", Some(native_echo), 0)
        .expect("register echo");
    server
        .register_native(b"POST", b"/record", Some(native_record), 0)
        .expect("register record");
    server
        .register_native(b"GET", b"/stray", Some(native_stray_304), 0)
        .expect("register stray");
    server.start().expect("start server")
}

pub(crate) fn date_shape(response: &ClientResponse) -> String {
    let seen = header(response, "date").to_string();
    assert_eq!(seen.len(), 29, "IMF-fixdate width: {seen}");
    assert!(seen.ends_with(" GMT"), "GMT only: {seen}");
    assert_eq!(seen.as_bytes()[3], b',', "weekday comma: {seen}");
    let count = response
        .headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("date"))
        .count();
    assert_eq!(count, 1, "exactly one Date");
    seen
}

pub(crate) fn connect_addr(addr: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).expect("connect loopback");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    stream
}

pub(crate) fn gzip_body() -> Vec<u8> {
    b"The quick brown fox jumps over the lazy dog. ".repeat(50)
}

pub(crate) fn gzip_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_gzip_route(
            b"GET",
            b"/gzip",
            200,
            b"OK",
            &gzip_body(),
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register gzip");
    server.start().expect("start server")
}
