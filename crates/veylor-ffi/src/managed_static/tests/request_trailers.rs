use super::super::*;
use super::ffi_server::*;
use super::managed_peer::*;
use crate::abi::VylResult;
use std::io::{Read, Write};
use std::time::Duration;

#[test]
fn trailers_rejects_bad_allowlists_before_start() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let many = vec![bytes(b"x"); 33];
    assert_eq!(
        unsafe {
            super::super::vyl_managed_static_register_managed_route_with_trailers(
                server,
                bytes(b"POST"),
                bytes(b"/many"),
                u64::MAX,
                many.as_ptr(),
                many.len(),
            )
        },
        VylResult::InvalidArgument
    );
    let long = vec![b'x'; 129];
    let overlong = trailer_bytes(&[&long]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/long", u64::MAX, &overlong) },
        VylResult::InvalidArgument
    );
    let empty = trailer_bytes(&[b""]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/empty", u64::MAX, &empty) },
        VylResult::InvalidArgument
    );
    let dupe = trailer_bytes(&[b"X-Tag", b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/dupe", u64::MAX, &dupe) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::super::vyl_managed_static_register_managed_route_with_trailers(
                server,
                bytes(b"POST"),
                bytes(b"/null"),
                u64::MAX,
                std::ptr::null(),
                1,
            )
        },
        VylResult::InvalidArgument
    );
    let one = trailer_bytes(&[b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/t", u64::MAX, &one) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/t", u64::MAX, &one) },
        VylResult::InvalidArgument
    );
    unsafe { register_managed(server, b"POST", b"/plain", u64::MAX) };
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/plain", u64::MAX, &one) },
        VylResult::InvalidArgument
    );
    unsafe { register(server, b"POST", b"/static", b"x") };
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/static", u64::MAX, &one) },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/late", u64::MAX, &one) },
        VylResult::BadState
    );
    let port = unsafe { local_port(server) };
    let _ = connect_loopback(port);
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
fn trailers_copy_whole_set_with_capacity_retry() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let allow = trailer_bytes(&[b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/t", u64::MAX, &allow) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    write_chunked(&mut stream, "/t", &[b"hello"], &[("X-Tag", "v1")]);
    let token = unsafe { poll_one(server) };
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    let mut names = [0u8; 2];
    let mut values = [0u8; 2];
    let (short, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(short, VylResult::Ok);
    assert_eq!(report.fields, 1);
    assert_eq!((report.names_copied, report.values_copied), (0, 0));
    assert_eq!((report.names_required, report.values_required), (5, 2));
    assert_eq!((names, values), ([0u8; 2], [0u8; 2]));
    let mut names = [0u8; 5];
    let mut values = [0u8; 2];
    let (full, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(full, VylResult::Ok);
    assert_eq!(report.fields, 1);
    assert_eq!((report.names_copied, report.values_copied), (5, 2));
    assert_eq!((report.names_required, report.values_required), (0, 0));
    assert_eq!(&names, b"X-Tag");
    assert_eq!(&values, b"v1");
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
fn trailers_report_would_block_before_end_of_body() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let allow = trailer_bytes(&[b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/n", u64::MAX, &allow) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(
            b"POST /n HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n",
        )
        .expect("write partial");
    let token = unsafe { poll_one(server) };
    let mut names = [0xAAu8; 8];
    let mut values = [0xAAu8; 8];
    let (early, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(early, VylResult::WouldBlock);
    assert_eq!(report, VylManagedStaticTrailerReport::default());
    assert_eq!((names, values), ([0xAAu8; 8], [0xAAu8; 8]));
    stream
        .write_all(b"0\r\nX-Tag: v1\r\n\r\n")
        .expect("write terminal");
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    let (ready, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(ready, VylResult::Ok);
    assert_eq!(report.fields, 1);
    assert_eq!(&names[..report.names_copied], b"X-Tag");
    assert_eq!(&values[..report.values_copied], b"v1");
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"done"))
        },
        VylResult::Ok
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"done");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn trailers_empty_allowlist_reports_empty_set() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            super::super::vyl_managed_static_register_managed_route_with_trailers(
                server,
                bytes(b"POST"),
                bytes(b"/e"),
                u64::MAX,
                std::ptr::null(),
                0,
            )
        },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    write_chunked(&mut stream, "/e", &[b"hello"], &[]);
    let token = unsafe { poll_one(server) };
    assert_eq!(unsafe { drain_body(server, token) }, b"hello");
    let mut names = [0u8; 8];
    let mut values = [0u8; 8];
    let (result, report) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
    assert_eq!(result, VylResult::Ok);
    assert_eq!(report.fields, 0);
    assert_eq!((report.names_copied, report.values_copied), (0, 0));
    assert_eq!((report.names_required, report.values_required), (0, 0));
    assert_eq!(
        unsafe {
            vyl_managed_static_host_respond(server, token, 200, bytes(b"OK"), bytes(b"empty"))
        },
        VylResult::Ok
    );
    let (status, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"empty");
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn trailers_unlisted_field_closes_connection() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let allow = trailer_bytes(&[b"x-ok"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/u", u64::MAX, &allow) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let mut stream = connect_loopback(port);
    stream
        .write_all(b"POST /u HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n")
        .expect("write head");
    let token = unsafe { poll_one(server) };
    stream
        .write_all(b"5\r\nhello\r\n0\r\nX-Evil: 1\r\n\r\n")
        .expect("write unlisted");
    let mut observed = VylResult::Ok;
    for _ in 0..250 {
        let mut names = [0u8; 8];
        let mut values = [0u8; 8];
        let (result, _) = unsafe { pull_trailers_into(server, token, &mut names, &mut values) };
        if result == VylResult::Gone {
            observed = result;
            break;
        }
        assert_eq!(result, VylResult::WouldBlock);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(observed, VylResult::Gone);
    let mut scratch = [0u8; 8];
    let (pull_result, _, _) = unsafe { pull_into(server, token, &mut scratch) };
    assert_eq!(pull_result, VylResult::Gone);
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).expect("read close");
    assert!(
        rest.is_empty(),
        "refused trailer must close without bytes: {rest:?}"
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
fn trailers_rejects_bad_pull_inputs_and_state() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    let allow = trailer_bytes(&[b"x-tag"]);
    assert_eq!(
        unsafe { register_managed_with_trailers(server, b"POST", b"/t", u64::MAX, &allow) },
        VylResult::Ok
    );
    let zero_token = VylManagedStaticToken::default();
    let mut names = [0u8; 8];
    let mut values = [0u8; 8];
    let mut report = VylManagedStaticTrailerReport::default();
    assert_eq!(
        unsafe {
            super::super::vyl_managed_static_host_pull_trailers(
                server,
                zero_token,
                names.as_mut_ptr(),
                names.len(),
                values.as_mut_ptr(),
                values.len(),
                &mut report,
            )
        },
        VylResult::BadState
    );
    assert_eq!(
        unsafe {
            super::super::vyl_managed_static_host_pull_trailers(
                std::ptr::null_mut(),
                zero_token,
                names.as_mut_ptr(),
                names.len(),
                values.as_mut_ptr(),
                values.len(),
                &mut report,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::super::vyl_managed_static_host_pull_trailers(
                server,
                zero_token,
                names.as_mut_ptr(),
                names.len(),
                values.as_mut_ptr(),
                values.len(),
                std::ptr::null_mut(),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::super::vyl_managed_static_host_pull_trailers(
                server,
                zero_token,
                std::ptr::null_mut(),
                1,
                values.as_mut_ptr(),
                values.len(),
                &mut report,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            super::super::vyl_managed_static_host_pull_trailers(
                server,
                zero_token,
                names.as_mut_ptr(),
                names.len(),
                std::ptr::null_mut(),
                1,
                &mut report,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}
