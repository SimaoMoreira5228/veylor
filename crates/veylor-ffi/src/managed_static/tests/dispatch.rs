use super::super::*;
use super::ffi_server::*;
use super::managed_peer::*;
use crate::abi::VylResult;
use std::io::{Read, Write};
use std::time::Duration;

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
    let oversized = vec![b'x'; 15 * 1024 + 1];
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
