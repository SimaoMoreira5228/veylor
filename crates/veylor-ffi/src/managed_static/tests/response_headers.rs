use super::super::*;
use super::ffi_server::*;
use super::managed_peer::*;
use crate::abi::{VylBytes, VylResult};
use std::io::{Read, Write};
use std::net::TcpStream;

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
