use super::super::*;
use super::ffi_server::*;
use crate::abi::VylResult;
use std::ffi::c_void;

#[test]
fn gzip_route_serves_precompressed_and_identity_fallback() {
    let plain = b"The quick brown fox jumps over the lazy dog. ".repeat(50);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route_with_gzip(
                server,
                bytes(b"GET"),
                bytes(b"/gzip"),
                200,
                bytes(b"OK"),
                bytes(&plain),
            )
        },
        VylResult::Ok
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route_with_gzip(
                server,
                bytes(b"GET"),
                bytes(b"/gzip"),
                200,
                bytes(b"OK"),
                bytes(&plain),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };

    let (status, head, gzipped) = get_raw(port, "/gzip", "Accept-Encoding: gzip\r\n");
    assert_eq!(status, 200);
    assert!(
        head.contains("Content-Encoding: gzip"),
        "encoding: {head:?}"
    );
    assert!(head.contains("Vary: Accept-Encoding"), "vary: {head:?}");
    assert_eq!(&gzipped[..2], &[0x1f, 0x8b], "gzip magic");
    assert!(gzipped.len() < plain.len());
    let framed: usize = head
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
    assert_eq!(framed, gzipped.len());

    let (plain_status, plain_head, identity) = get_raw(port, "/gzip", "");
    assert_eq!(plain_status, 200);
    assert_eq!(identity, plain);
    assert!(
        !plain_head.to_ascii_lowercase().contains("content-encoding"),
        "no content-encoding: {plain_head:?}"
    );
    assert!(
        plain_head.contains("Vary: Accept-Encoding"),
        "vary: {plain_head:?}"
    );

    let (_, _, again) = get_raw(port, "/gzip", "Accept-Encoding: gzip\r\n");
    assert_eq!(again, gzipped);

    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 3);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

static NATIVE_GZIP_BODY: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();

fn native_gzip_body() -> &'static [u8] {
    NATIVE_GZIP_BODY.get_or_init(|| b"The quick brown fox jumps over the lazy dog. ".repeat(50))
}

unsafe extern "C" fn native_gzip_handler(
    _user_data: *mut c_void,
    _request: *const VylManagedStaticNativeRequest,
    response: *mut VylManagedStaticNativeResponse,
) -> i32 {
    answer_native(response, 200, b"OK", native_gzip_body());
    0
}

#[test]
fn gzip_native_route_round_trips_through_ffi() {
    let plain = b"The quick brown fox jumps over the lazy dog. ".repeat(50);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            vyl_managed_static_register_native_route_with_gzip(
                server,
                bytes(b"GET"),
                bytes(b"/gzip-native"),
                Some(native_gzip_handler),
                std::ptr::null_mut(),
            )
        },
        VylResult::Ok
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_native_route_with_gzip(
                server,
                bytes(b"GET"),
                bytes(b"/gzip-native"),
                Some(native_gzip_handler),
                std::ptr::null_mut(),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_register_native_route_with_gzip(
                std::ptr::null_mut(),
                bytes(b"GET"),
                bytes(b"/gzip-native"),
                Some(native_gzip_handler),
                std::ptr::null_mut(),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    assert_eq!(
        unsafe {
            vyl_managed_static_register_native_route_with_gzip(
                server,
                bytes(b"GET"),
                bytes(b"/late"),
                Some(native_gzip_handler),
                std::ptr::null_mut(),
            )
        },
        VylResult::BadState
    );

    let (status, head, gzipped) = get_raw(port, "/gzip-native", "Accept-Encoding: gzip\r\n");
    assert_eq!(status, 200);
    assert!(
        head.contains("Content-Encoding: gzip"),
        "encoding: {head:?}"
    );
    assert!(head.contains("Vary: Accept-Encoding"), "vary: {head:?}");
    assert_eq!(&gzipped[..2], &[0x1f, 0x8b], "gzip magic");
    assert!(gzipped.len() < plain.len());

    let (plain_status, plain_head, identity) = get_raw(port, "/gzip-native", "");
    assert_eq!(plain_status, 200);
    assert_eq!(identity, plain);
    assert!(
        !plain_head.to_ascii_lowercase().contains("content-encoding"),
        "no encoding: {plain_head:?}"
    );

    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 2);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}
