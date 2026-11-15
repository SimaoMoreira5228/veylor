use super::super::*;
use super::ffi_server::*;
use super::h2c_peer::*;
use super::managed_peer::*;
use crate::abi::VylResult;
use std::time::Duration;
use veylor_http2::{
    DATA_FLAG_END_STREAM, FrameType, HpackEntrySlot, HpackHeader, HpackTable, decode_header_block,
};

// NOTE: H2 BODY_AVAILABLE dispatch pin through the managed C ABI: staged DATA carries ready
// bytes with the end flag in the reused length fields like H1, so the host observes the full
// window before pulling. Grant frames interleave freely and are skipped, never asserted.
#[test]
fn h2c_managed_body_available_signals_ready_and_end() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/upload", u64::MAX) };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"5"),
    ];
    h2_send_headers_open(&mut stream, 1, &fields);
    h2_write_data(&mut stream, 1, b"hello", true);
    let (token, mut signals) = unsafe { poll_request_with_signals(server) };
    assert_eq!(token.stream_id, 1);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let signal = loop {
        if let Some(done) = signals.iter().find(|event| {
            event.kind == VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE
                && event.token == token
                && event.path_len == 1
        }) {
            break *done;
        }
        unsafe { gather_signals(server, &mut signals) };
        signals.retain(|event| {
            event.kind != VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE || event.token == token
        });
        assert!(
            std::time::Instant::now() < deadline,
            "terminal body signal stalls"
        );
    };
    assert_eq!(signal.token, token);
    assert_eq!(signal.route_id, 0);
    assert_eq!(signal.method_len, 5);
    assert_eq!((signal.method_offset, signal.path_offset), (0, 0));
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (header, payload) = h2_next_non_grant(&mut stream);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, 1);
    let (count, _) = decode_header_block(&payload, &mut decoder, &mut decoded).expect("decode");
    let mut status = 0;
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    assert_eq!(status, 200);
    let (header, body) = h2_next_non_grant(&mut stream);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(header.stream_id, 1);
    assert_eq!(header.flags & DATA_FLAG_END_STREAM, DATA_FLAG_END_STREAM);
    assert_eq!(body, b"done");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: H2 DATA wake pin: staged DATA must wake a thread-blocked poll the way the H1
// push_body paths do. The waiter blocks in long-timeout polls after the REQUEST drain, so
// only a DATA-driven notify returns it promptly; without one it sleeps the full timeout.
#[test]
fn h2c_data_staged_body_available_wakes_blocked_poll() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/upload", u64::MAX) };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"5"),
    ];
    h2_send_headers_open(&mut stream, 1, &fields);
    let token = unsafe { poll_one(server) };
    assert_eq!(token.stream_id, 1);
    let raw = server as usize;
    let waiter = std::thread::spawn(move || {
        let server = raw as *mut VylManagedStaticServer;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let start = std::time::Instant::now();
        let mut found = false;
        while !found && std::time::Instant::now() < deadline {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            let mut events = [VylManagedStaticEvent::default(); 8];
            let mut methods = [0u8; 256];
            let mut paths = [0u8; 4096];
            let report = unsafe {
                poll_into(
                    server,
                    left.as_nanos() as u64,
                    &mut events,
                    &mut methods,
                    &mut paths,
                )
            };
            found = events.iter().take(report.copied).any(|event| {
                event.kind == VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE && event.token == token
            });
        }
        (start.elapsed(), found)
    });
    std::thread::sleep(Duration::from_millis(200));
    h2_write_data(&mut stream, 1, b"hello", true);
    let (elapsed, found) = waiter.join().expect("waiter joins");
    assert!(found, "blocked poll never observed the H2 DATA body signal");
    assert!(
        elapsed < Duration::from_secs(4),
        "blocked poll slept {elapsed:?}: H2 DATA staged no wake"
    );
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (header, payload) = h2_next_non_grant(&mut stream);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, 1);
    let (count, _) = decode_header_block(&payload, &mut decoder, &mut decoded).expect("decode");
    let mut status = 0;
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    assert_eq!(status, 200);
    let (header, body) = h2_next_non_grant(&mut stream);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(header.stream_id, 1);
    assert_eq!(header.flags & DATA_FLAG_END_STREAM, DATA_FLAG_END_STREAM);
    assert_eq!(body, b"done");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: H2C response-trailer pin through the managed C ABI: HEADERS-open POST plus DATA-end
// stages the body, the empty-set trailer pull proves the staged whole-set discipline over
// H2C, and end-with-trailers lands terminal HEADERS with exact fields while the conn
// survives for a sibling. Plain managed registration; allowlist-registered routes serve
// H2C too, covered by h2c_managed_with_trailers_mirror_serves_and_enforces below.
#[test]
fn h2c_managed_stream_end_with_trailers_exact_fields() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register_managed(server, b"POST", b"/upload", u64::MAX) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"5"),
    ];
    h2_send_headers_open(&mut stream, 1, &fields);
    h2_write_data(&mut stream, 1, b"hello", true);
    let token = unsafe { poll_one(server) };
    assert_eq!(token.stream_id, 1);
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    // NOTE: no request trailers were sent, so no pull: only the H1 chunked terminal path
    // initializes the staged set (mark_chunked_done); a DATA-ended body stays NotReady
    // engine-wide. Whole-set request-trailer discipline is pinned by the next test.
    assert_eq!(
        unsafe { vyl_managed_static_host_begin_stream(server, token, 200, bytes(b"OK")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"chunk-one")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    assert_eq!(
        unsafe { vyl_managed_static_host_write_stream(server, token, bytes(b"chunk-two")) },
        VylResult::Ok
    );
    unsafe { wait_stream_writable(server, token) };
    let trailers = [field(b"x-checksum", b"abc"), field(b"x-tag", b"v1")];
    assert_eq!(
        unsafe { end_with_trailers(server, token, &trailers) },
        VylResult::Ok
    );
    let (status, body, got) =
        h2c_read_streamed_with_trailers(&mut stream, &mut decoder, &mut decoded, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"chunk-onechunk-two");
    assert_eq!(
        got,
        vec![
            (b"x-checksum".to_vec(), b"abc".to_vec()),
            (b"x-tag".to_vec(), b"v1".to_vec()),
        ]
    );
    assert_eq!(
        unsafe { end_with_trailers(server, token, &trailers) },
        VylResult::Gone
    );
    h2_send_get(&mut stream, 3, b"/hello");
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 3);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: H2C request-trailer pin through the managed C ABI: open POST plus DATA stages ready
// bytes without completion, the trailer HEADERS completes the set for a whole-set pull with
// capacity retry, and a prohibited set on the next stream RSTs while the conn survives for
// a sibling. Plain managed registration (the engine H2 trailer tests stage without an
// allowlist; the allowlist gate lives on the H1 park path only).
#[test]
fn h2c_managed_request_trailers_stage_and_pull() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    // NOTE: empty-allowlist routes RST trailer sets over H2C (same one-predicate rule as
    // H1), so this whole-set discipline pins against a listed allowlist.
    let allow = trailer_bytes(&[b"x-checksum", b"x-count"]);
    unsafe { register_managed_with_trailers(server, b"POST", b"/upload", u64::MAX, &allow) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"4"),
    ];
    h2_send_headers_open(&mut stream, 1, &fields);
    h2_write_data(&mut stream, 1, b"data", false);
    let token = unsafe { poll_one(server) };
    assert_eq!(token.stream_id, 1);
    let mut names = [0xAAu8; 32];
    let mut values = [0xAAu8; 16];
    let (early, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(early, VylResult::WouldBlock);
    assert_eq!(report, VylManagedStaticTrailerReport::default());
    h2_send_trailers(
        &mut stream,
        1,
        &[h2_field(b"x-checksum", b"abc"), h2_field(b"x-count", b"2")],
    );
    assert_eq!(unsafe { drain_body(server, token) }, b"data");
    // NOTE: trailer readiness can lag body-end by a turn (the engine pin spins too), so
    // the short-buffer probe retries WouldBlock until the whole set stages at once.
    let report = {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut names = [0u8; 2];
            let mut values = [0u8; 2];
            let (short, report) =
                unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
            assert_eq!((names, values), ([0u8; 2], [0u8; 2]));
            if short == VylResult::Ok {
                break report;
            }
            assert_eq!(short, VylResult::WouldBlock);
            assert_eq!(report, VylManagedStaticTrailerReport::default());
            assert!(
                std::time::Instant::now() < deadline,
                "staged trailer set never became ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    assert_eq!(report.fields, 2);
    assert_eq!((report.names_copied, report.values_copied), (0, 0));
    assert_eq!((report.names_required, report.values_required), (18, 5));
    let mut names = [0u8; 18];
    let mut values = [0u8; 5];
    let (full, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(full, VylResult::Ok);
    assert_eq!(report.fields, 2);
    assert_eq!((report.names_copied, report.values_copied), (18, 5));
    assert_eq!((report.names_required, report.values_required), (0, 0));
    assert_eq!(&names, b"x-checksum\x00x-count");
    assert_eq!(&values, b"abc\x002");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"done");
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"4"),
    ];
    h2_send_headers_open(&mut stream, 3, &fields);
    let refused = unsafe { poll_one(server) };
    assert_eq!(refused.stream_id, 3);
    h2_write_data(&mut stream, 3, b"data", false);
    h2_send_trailers(&mut stream, 3, &[h2_field(b"connection", b"close")]);
    h2_expect_rst_h2(&mut stream, 3, 1);
    h2_send_get(&mut stream, 5, b"/hello");
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 5);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

// NOTE: with_trailers registration mirrors to H2C like every other managed route: the
// projection carries the same allowlist H1 reads at park, so a route registered only this
// way still parks, and unlisted trailer sets RST the stream while the connection survives.
#[test]
// NOTE: allowlist routes mirror to H2C and enforce there: the listed set stages whole,
// the unlisted set RSTs the stream, the connection survives.
fn h2c_managed_with_trailers_mirror_serves_and_enforces() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let allow = trailer_bytes(&[b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/upload", u64::MAX, &allow) },
        VylResult::Ok
    );
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let fields = [
        h2_field(b":method", b"POST"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/upload"),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"4"),
    ];
    h2_send_headers_open(&mut stream, 1, &fields);
    h2_write_data(&mut stream, 1, b"data", true);
    let token = unsafe { poll_one(server) };
    assert_eq!(token.stream_id, 1);
    assert_eq!(unsafe { drain_body(server, token) }, b"data");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"done");
    h2_send_headers_open(&mut stream, 3, &fields);
    let refused = unsafe { poll_one(server) };
    assert_eq!(refused.stream_id, 3);
    h2_write_data(&mut stream, 3, b"data", false);
    h2_send_trailers(&mut stream, 3, &[h2_field(b"x-evil", b"0")]);
    h2_expect_rst_h2(&mut stream, 3, 1);
    h2_send_headers_open(&mut stream, 5, &fields);
    let listed = unsafe { poll_one(server) };
    assert_eq!(listed.stream_id, 5);
    h2_write_data(&mut stream, 5, b"data", false);
    h2_send_trailers(&mut stream, 5, &[h2_field(b"x-tag", b"1")]);
    assert_eq!(unsafe { drain_body(server, listed) }, b"data");
    let report = {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut names = [0u8; 1];
            let mut values = [0u8; 1];
            let (short, report) =
                unsafe { pull_trailers_into(server, listed, &mut names, &mut values) };
            if short == VylResult::Ok {
                break report;
            }
            assert_eq!(short, VylResult::WouldBlock);
            assert!(
                std::time::Instant::now() < deadline,
                "listed trailer set never became ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    assert_eq!(report.fields, 1);
    let mut names = [0u8; 5];
    let mut values = [0u8; 1];
    let (full, report) = unsafe { pull_trailers_into(server, listed, &mut names, &mut values) };
    assert_eq!(full, VylResult::Ok);
    assert_eq!(report.fields, 1);
    assert_eq!(&names, b"x-tag");
    assert_eq!(&values, b"1");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, listed, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 5);
    assert_eq!(status, 200);
    assert_eq!(body, b"done");
    h2_send_get(&mut stream, 7, b"/hello");
    let (status, body) = h2c_read_simple(&mut stream, &mut decoder, &mut decoded, 7);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}
