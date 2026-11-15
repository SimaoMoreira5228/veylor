use super::super::*;
use super::ffi_server::*;
use super::managed_peer::*;
use crate::abi::VylResult;
use parking_lot::Mutex;
use std::ffi::c_void;
use std::io::{Read, Write};
use std::net::TcpStream;

// NOTE: native-route coverage. Each test owns its handler statics so parallel runs never share
// observations. Handlers never unwind (nounwind contract): they record, answer, and return.

static NATIVE_GET_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn native_get_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    record_native(&NATIVE_GET_OBSERVED, user_data, request);
    answer_native(response, 200, b"OK", b"native-bytes");
    0
}

static NATIVE_POST_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn native_post_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    record_native(&NATIVE_POST_OBSERVED, user_data, request);
    answer_native(response, 200, b"OK", b"submit-ack");
    0
}

unsafe extern "C" fn native_failing_handler(
    _user_data: *mut c_void,
    _request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    answer_native(response, 200, b"OK", b"ignored");
    1
}

unsafe extern "C" fn native_panicking_handler(
    _user_data: *mut c_void,
    _request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    let survived = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        panic!("native handler boom");
    }));
    if survived.is_err() {
        answer_native(response, 200, b"OK", b"ignored");
        return 1;
    }
    0
}

#[test]
fn native_get_round_trip_reports_views_and_user_data() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/native", Some(native_get_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let head = get(port, "/native");
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {head:?}"
    );
    assert!(head.ends_with("native-bytes"), "unexpected body: {head:?}");
    let observed = NATIVE_GET_OBSERVED.lock();
    assert_eq!(observed.user_data, tag as usize);
    assert_eq!(observed.method, b"GET");
    assert_eq!(observed.path, b"/native");
    assert_eq!(observed.has_body, 0);
    assert_eq!(observed.body_len, 0);
    assert!(observed.body.is_empty());
    drop(observed);
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/late", Some(native_get_handler), tag) },
        VylResult::BadState
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn native_post_borrows_staged_body_bytes() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { register_native(server, b"POST", b"/submit", Some(native_post_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    let (status, body) = post_exchange(&mut stream, "/submit", b"hello");
    assert_eq!(status, 200);
    assert_eq!(body, b"submit-ack");
    let observed = NATIVE_POST_OBSERVED.lock();
    assert_eq!(observed.user_data, tag as usize);
    assert_eq!(observed.method, b"POST");
    assert_eq!(observed.path, b"/submit");
    assert_eq!(observed.has_body, 1);
    assert_eq!(observed.body_len, 5);
    assert_eq!(observed.body, b"hello");
    drop(observed);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

static NATIVE_ECHO_OBSERVED: Mutex<NativeObserved> = Mutex::new(NativeObserved::empty());

unsafe extern "C" fn native_echo_handler(
    user_data: *mut c_void,
    request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }
    record_native(&NATIVE_ECHO_OBSERVED, user_data, request);
    let observed = NATIVE_ECHO_OBSERVED.lock();
    unsafe {
        (*response).status = 200;
        (*response).reason = b"OK".as_ptr();
        (*response).reason_len = 2;
        (*response).body = observed.body.as_ptr();
        (*response).body_len = observed.body.len();
    }
    0
}

#[test]
fn native_echo_returns_borrowed_body_through_ffi_twin() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { register_native(server, b"POST", b"/echo", Some(native_echo_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    let (status, body) = post_exchange(&mut stream, "/echo", b"ffi-echo");
    assert_eq!(status, 200);
    assert_eq!(body, b"ffi-echo");
    let observed = NATIVE_ECHO_OBSERVED.lock();
    assert_eq!(observed.body, b"ffi-echo");
    assert_eq!(observed.body_len, 8);
    drop(observed);
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn native_rejects_duplicates_overlaps_and_null() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let tag = native_tag();
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/n", Some(native_get_handler), tag) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/n", Some(native_get_handler), tag) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route(
                server,
                bytes(b"GET"),
                bytes(b"/n"),
                200,
                bytes(b"OK"),
                bytes(b"x"),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_managed_route(server, bytes(b"GET"), bytes(b"/n"), u64::MAX)
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { register_native(server, b"GET", b"/null", None, tag) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_native_route(
                std::ptr::null_mut(),
                bytes(b"GET"),
                bytes(b"/null"),
                Some(native_get_handler),
                tag,
            )
        },
        VylResult::InvalidArgument
    );
    unsafe { register(server, b"POST", b"/s", b"x") };
    assert_eq!(
        unsafe { register_native(server, b"POST", b"/s", Some(native_post_handler), tag) },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

fn read_closed(stream: &mut TcpStream) -> Vec<u8> {
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).expect("read close");
    rest
}

#[test]
fn native_nonzero_return_closes_without_bytes() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            register_native(
                server,
                b"GET",
                b"/fail",
                Some(native_failing_handler),
                native_tag(),
            )
        },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /fail HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
        .expect("write request");
    assert!(
        read_closed(&mut stream).is_empty(),
        "nonzero return must close without bytes"
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.response_overflow, 1, "counts: {counts:?}");
    assert_eq!(counts_total(counts), 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn native_handler_catches_own_panic_and_fails_closed() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            register_native(
                server,
                b"GET",
                b"/boom",
                Some(native_panicking_handler),
                native_tag(),
            )
        },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"GET /boom HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
        .expect("write request");
    assert!(
        read_closed(&mut stream).is_empty(),
        "caught panic must fail closed without bytes"
    );
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.response_overflow, 1, "counts: {counts:?}");
    assert_eq!(counts_total(counts), 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}
