use super::super::*;
use super::ffi_server::*;
use super::managed_peer::*;
use crate::abi::{VylField, VylResult};
use std::io::{Read, Write};
use std::net::TcpStream;

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
