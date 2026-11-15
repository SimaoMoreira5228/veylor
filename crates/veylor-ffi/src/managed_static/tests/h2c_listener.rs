use super::super::*;
use super::ffi_server::*;
use super::h2c_peer::*;
use super::managed_peer::*;
use crate::abi::VylResult;
use parking_lot::Mutex;
use std::ffi::c_void;
use std::io::Write;
use std::sync::atomic::{AtomicI32, Ordering};
use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, DATA_FLAG_END_STREAM, FrameType, HEADERS_FLAG_END_HEADERS,
    HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackHeader, HpackTable, SETTINGS_ACK,
    decode_header_block, encode_header_block,
};
use veylor_server::MAX_WORKERS;

// NOTE: additive-slice coverage: H2C staging, worker-count staging, and join-busy. The H2C
// client is byte-level like h2_tests (raw preface plus empty SETTINGS, then one HPACK GET)
// without depending on the server crate; the join probe passes the handle via user_data,
// which is what the field is for.

#[test]
fn h2c_enable_serves_handshake_and_static_route() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(std::ptr::null_mut(), 0) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::BadState
    );
    let h2c_port = {
        let mut port = 0;
        assert_eq!(
            unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
            VylResult::Ok
        );
        port
    };
    assert_ne!(h2c_port, 0);
    let mut stream = connect_loopback(h2c_port);
    stream
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write magic");
    h2_write_frame(&mut stream, FrameType::Settings, 0, 0, &[]);
    let (header, _) = h2_read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.stream_id, 0);
    h2_write_frame(&mut stream, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = h2_read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);
    let fields = [
        h2_field(b":method", b"GET"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", b"/hello"),
        h2_field(b":authority", b"localhost"),
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    h2_write_frame(
        &mut stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        1,
        &block[..len],
    );
    let (header, payload) = h2_read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, 1);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (count, _) =
        decode_header_block(&payload, &mut decoder, &mut decoded).expect("decode block");
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
    let (header, body) = h2_read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(header.stream_id, 1);
    assert_eq!(header.flags & DATA_FLAG_END_STREAM, DATA_FLAG_END_STREAM);
    assert_eq!(body, b"hello");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn worker_count_stages_rejects_bounds_and_serves() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { vyl_managed_static_set_worker_count(std::ptr::null_mut(), 2) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_set_worker_count(server, 0) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_set_worker_count(server, MAX_WORKERS + 1) },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
    for count in [2usize, 4usize] {
        let config = test_config(0);
        let server = unsafe { create(&config) };
        assert_eq!(
            unsafe { vyl_managed_static_set_worker_count(server, count) },
            VylResult::Ok
        );
        unsafe { register(server, b"GET", b"/w", b"workers") };
        assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
        assert_eq!(
            unsafe { vyl_managed_static_set_worker_count(server, 1) },
            VylResult::BadState
        );
        assert_eq!(
            unsafe { vyl_managed_static_enable_h2c(server, 0) },
            VylResult::BadState
        );
        let port = unsafe { local_port(server) };
        for _ in 0..count {
            let head = get(port, "/w");
            assert!(
                head.starts_with("HTTP/1.1 200 OK"),
                "unexpected head: {head:?}"
            );
            assert!(head.ends_with("workers"), "unexpected body: {head:?}");
        }
        let counts = unsafe { stop_join(server) };
        assert_eq!(counts_total(counts), count as u64, "counts: {counts:?}");
        assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
    }
}

static JOIN_BUSY_OBSERVED: AtomicI32 = AtomicI32::new(-1);

unsafe extern "C" fn native_join_busy_handler(
    user_data: *mut c_void,
    _request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    let server = user_data as *mut VylManagedStaticServer;
    let mut counts = VylManagedStaticCloseCounts::default();
    let result = unsafe { vyl_managed_static_join(server, &mut counts) };
    JOIN_BUSY_OBSERVED.store(result as i32, Ordering::Release);
    answer_native(response, 200, b"OK", b"join-busy");
    0
}

#[test]
fn join_from_native_handler_reports_busy_without_deadlock() {
    JOIN_BUSY_OBSERVED.store(-1, Ordering::Release);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            register_native(
                server,
                b"GET",
                b"/busy",
                Some(native_join_busy_handler),
                server as *mut c_void,
            )
        },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let head = get(port, "/busy");
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {head:?}"
    );
    assert!(head.ends_with("join-busy"), "unexpected body: {head:?}");
    assert_eq!(
        JOIN_BUSY_OBSERVED.load(Ordering::Acquire),
        VylResult::Busy as i32
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn h2c_addr_reports_bound_port_and_rejects_unusable_states() {
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(std::ptr::null_mut(), std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let mut port = 0xBEEFu16;
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(std::ptr::null_mut(), &mut port) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::BadState,
        "pre-start reports BadState: the H2C listener binds at start"
    );
    assert_eq!(port, 0);
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::BadState,
        "staged but unbound still reports BadState"
    );
    assert_eq!(port, 0);
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::Ok
    );
    assert_ne!(port, 0);
    assert_ne!(port, unsafe { local_port(server) });
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 0, "counts: {counts:?}");
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::BadState
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn h2c_addr_reports_disabled_and_shared_ports() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let mut port = 0xBEEFu16;
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut port) },
        VylResult::BadState,
        "H2C-disabled running handles report BadState"
    );
    assert_eq!(port, 0);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 0, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);

    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { vyl_managed_static_set_worker_count(server, 2) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let mut h2c = 0;
    assert_eq!(
        unsafe { vyl_managed_static_h2c_addr(server, &mut h2c) },
        VylResult::Ok
    );
    assert_ne!(h2c, 0);
    let head = get(unsafe { local_port(server) }, "/missing");
    assert!(
        head.starts_with("HTTP/1.1 404 Not Found"),
        "reuse-port H1 still serves: {head:?}"
    );
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

static H2C_NATIVE_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn h2c_native_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    record_native(&H2C_NATIVE_OBSERVED, user_data, request);
    answer_native(response, 200, b"OK", b"h2c-native");
    0
}

#[test]
fn h2c_native_replays_through_rebind_with_static_alongside() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/native", Some(h2c_native_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h1 = unsafe { local_port(server) };
    let h2c = unsafe { h2c_port_of(server) };
    assert_ne!(h2c, 0);
    assert_ne!(h2c, h1);
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    h2_send_get(&mut stream, 1, b"/native");
    assert_eq!(h2_status(&mut stream, &mut decoder, 1, &mut decoded), 200);
    assert_eq!(h2_data(&mut stream, 1), b"h2c-native");
    let observed = H2C_NATIVE_OBSERVED.lock();
    assert_eq!(observed.user_data, tag as usize);
    assert_eq!(observed.method, b"GET");
    assert_eq!(observed.path, b"/native");
    assert_eq!(observed.has_body, 0);
    assert_eq!(observed.body_len, 0);
    assert!(observed.body.is_empty());
    drop(observed);
    h2_send_get(&mut stream, 3, b"/hello");
    assert_eq!(h2_status(&mut stream, &mut decoder, 3, &mut decoded), 200);
    assert_eq!(h2_data(&mut stream, 3), b"hello");
    h2_send_lying_get(&mut stream, 5, b"/native");
    h2_expect_rst(&mut stream, 5);
    h2_send_get(&mut stream, 7, b"/native");
    assert_eq!(h2_status(&mut stream, &mut decoder, 7, &mut decoded), 200);
    assert_eq!(h2_data(&mut stream, 7), b"h2c-native");
    let head = get(h1, "/native");
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {head:?}"
    );
    assert!(head.ends_with("h2c-native"), "unexpected body: {head:?}");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

static H2C_REBIND_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn h2c_rebind_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    record_native(&H2C_REBIND_OBSERVED, user_data, request);
    answer_native(response, 200, b"OK", b"rebind-native");
    0
}

#[test]
fn h2c_native_served_when_h2c_staged_before_register() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/native", Some(h2c_rebind_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h1 = unsafe { local_port(server) };
    let h2c = unsafe { h2c_port_of(server) };
    assert_ne!(h2c, 0);
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    h2_send_get(&mut stream, 1, b"/native");
    assert_eq!(h2_status(&mut stream, &mut decoder, 1, &mut decoded), 200);
    assert_eq!(h2_data(&mut stream, 1), b"rebind-native");
    let observed = H2C_REBIND_OBSERVED.lock();
    assert_eq!(observed.user_data, tag as usize);
    assert_eq!(observed.method, b"GET");
    assert_eq!(observed.path, b"/native");
    drop(observed);
    let head = get(h1, "/native");
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {head:?}"
    );
    assert!(head.ends_with("rebind-native"), "unexpected body: {head:?}");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

static H2C_WORKER_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn h2c_worker_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    record_native(&H2C_WORKER_OBSERVED, user_data, request);
    answer_native(response, 200, b"OK", b"worker-native");
    0
}

#[test]
fn h2c_native_served_with_two_workers() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { vyl_managed_static_set_worker_count(server, 2) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/native", Some(h2c_worker_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h1 = unsafe { local_port(server) };
    let h2c = unsafe { h2c_port_of(server) };
    assert_ne!(h2c, 0);
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];
    h2_send_get(&mut stream, 1, b"/native");
    assert_eq!(h2_status(&mut stream, &mut decoder, 1, &mut decoded), 200);
    assert_eq!(h2_data(&mut stream, 1), b"worker-native");
    let observed = H2C_WORKER_OBSERVED.lock();
    assert_eq!(observed.user_data, tag as usize);
    assert_eq!(observed.path, b"/native");
    drop(observed);
    let head = get(h1, "/native");
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {head:?}"
    );
    assert!(head.ends_with("worker-native"), "unexpected body: {head:?}");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}
