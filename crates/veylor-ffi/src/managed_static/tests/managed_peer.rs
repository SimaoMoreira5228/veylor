use super::super::*;
use super::ffi_server::*;
use crate::abi::{VylBytes, VylResult};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::time::Duration;

pub(crate) fn post_exchange(stream: &mut TcpStream, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
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

pub(crate) fn connect_loopback(port: u16) -> TcpStream {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    stream
}

pub(crate) const DISPATCH_TIMEOUT_NS: u64 = 5_000_000_000;

pub(crate) unsafe fn poll_into(
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

pub(crate) unsafe fn register_managed_with_trailers(
    server: *mut VylManagedStaticServer,
    method: &[u8],
    path: &[u8],
    cap: u64,
    trailers: &[VylBytes],
) -> VylResult {
    unsafe {
        super::super::vyl_managed_static_register_managed_route_with_trailers(
            server,
            bytes(method),
            bytes(path),
            cap,
            trailers.as_ptr(),
            trailers.len(),
        )
    }
}

pub(crate) unsafe fn pull_trailers_into(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    names: &mut [u8],
    values: &mut [u8],
) -> (VylResult, VylManagedStaticTrailerReport) {
    let mut report = VylManagedStaticTrailerReport::default();
    let result = unsafe {
        super::super::vyl_managed_static_host_pull_trailers(
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

pub(crate) fn trailer_bytes(names: &[&[u8]]) -> Vec<VylBytes> {
    names
        .iter()
        .map(|name| VylBytes {
            data: name.as_ptr(),
            length: name.len(),
        })
        .collect()
}

pub(crate) fn write_chunked(
    stream: &mut TcpStream,
    path: &str,
    chunks: &[&[u8]],
    trailers: &[(&str, &str)],
) {
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

pub(crate) unsafe fn pull_into(
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

pub(crate) unsafe fn drain_body(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) -> Vec<u8> {
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

pub(crate) unsafe fn poll_one(server: *mut VylManagedStaticServer) -> VylManagedStaticToken {
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
pub(crate) unsafe fn poll_request_with_signals(
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
pub(crate) unsafe fn gather_signals(
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

pub(crate) unsafe fn is_writable(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) -> (VylResult, u32) {
    let mut writable = 0u32;
    let result = unsafe { vyl_managed_static_host_is_writable(server, token, &mut writable) };
    (result, writable)
}

pub(crate) unsafe fn wait_stream_writable(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) {
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

pub(crate) fn split_head(raw: &[u8]) -> (&[u8], &[u8]) {
    let end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("head terminator")
        + 4;
    raw.split_at(end)
}

pub(crate) fn head_with_date(head: &[u8], status_line: &[u8], rest: &[u8]) {
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

pub(crate) fn read_streamed(stream: &mut TcpStream) -> (u16, Vec<u8>, Vec<u8>) {
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

pub(crate) fn read_stream_head(stream: &mut TcpStream) -> Vec<u8> {
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

pub(crate) fn read_stream_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
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
