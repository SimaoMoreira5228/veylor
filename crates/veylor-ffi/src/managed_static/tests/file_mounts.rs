use super::super::*;
use super::ffi_server::*;
use super::managed_peer::*;
use crate::abi::{VylBytes, VylResult};
use std::io::{Read, Write};
use std::net::TcpStream;

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
            super::super::vyl_managed_static_register_file_mount(
                std::ptr::null_mut(),
                bytes(b"/x/"),
                bytes(&root),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::super::vyl_managed_static_register_file_mount(
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
