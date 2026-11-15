use super::super::*;
use super::ffi_server::*;
use super::managed_peer::*;
use crate::abi::VylResult;
use std::io::Write;
use std::time::Duration;

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
    assert_eq!(std::mem::size_of::<VylManagedStaticEvent>(), 88);
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
    let body = super::super::dispatch::event_from_slot(veylor_server::EventSlot {
        kind: veylor_server::HostEventKind::BodyAvailable,
        token: server_token,
        route_id: 0,
        route_token: 0,
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
    let writable = super::super::dispatch::event_from_slot(veylor_server::EventSlot {
        kind: veylor_server::HostEventKind::Writable,
        token: server_token,
        route_id: 0,
        route_token: 0,
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
