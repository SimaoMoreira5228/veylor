use super::super::*;
use super::endpoint_io::*;
use crate::abi::{VylBytes, VylH2ResponseClass, VylH2Setting, VylResult};

#[test]
fn rejects_invalid_arguments_and_reports_state() {
    assert_eq!(
        unsafe { vyl_h2_client_new(std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_h2_client_destroy(std::ptr::null_mut()) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_h2_client_cancel(std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    let client = new_client();
    let mut output = [0; 64];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_connection_preface(output.as_mut_ptr(), 0, &mut written, &mut required)
        },
        VylResult::BufferTooSmall
    );
    assert_eq!(required, 24);
    let mut consumed = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_poll_feed(
                std::ptr::null_mut(),
                output.as_ptr(),
                0,
                &mut consumed,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        },
        VylResult::InvalidArgument
    );
    let push_enabled = [VylH2Setting { id: 2, value: 1 }];
    assert_eq!(
        unsafe {
            vyl_h2_client_send_settings(
                client,
                push_enabled.as_ptr(),
                push_enabled.len(),
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    let oversized_header_list = [VylH2Setting {
        id: 6,
        value: crate::abi::VYL_H2_MAX_HEADER_LIST_SIZE + 1,
    }];
    assert_eq!(
        unsafe {
            vyl_h2_client_send_settings(
                client,
                oversized_header_list.as_ptr(),
                oversized_header_list.len(),
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    let fields = [encode_field(b":method", b"GET")];
    let mut stream_id: u32 = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_start_request(
                client,
                fields.as_ptr(),
                fields.len(),
                1,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut stream_id,
                &mut required,
            )
        },
        VylResult::BadState
    );
    let mut pending = 0;
    assert_eq!(
        unsafe { vyl_h2_client_acknowledge(client, 1, &mut pending) },
        VylResult::BadState
    );
    let bad_settings = [VylH2Setting { id: 0, value: 0 }];
    assert_eq!(
        unsafe {
            vyl_h2_client_send_settings(
                client,
                bad_settings.as_ptr(),
                bad_settings.len(),
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    let bad_frame_size = [VylH2Setting { id: 5, value: 1 }];
    assert_eq!(
        unsafe {
            vyl_h2_client_send_settings(
                client,
                bad_frame_size.as_ptr(),
                bad_frame_size.len(),
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    let _ = client_settings(client);
    let mut consumed = 0;
    let mut end_stream: u32 = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_send_data(
                client,
                1,
                VylBytes {
                    data: std::ptr::null(),
                    length: 0,
                },
                0,
                output.as_mut_ptr(),
                output.len(),
                &mut consumed,
                &mut written,
                &mut end_stream,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    let mut class = VylH2ResponseClass::default();
    assert_eq!(
        unsafe { vyl_h2_client_classify_headers(client, 1, 0, 0, 0, &mut class) },
        VylResult::InvalidArgument
    );
    let mut phase: u32 = 0;
    assert_eq!(
        unsafe { vyl_h2_client_account_data(client, 1, 0, 0, &mut phase) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_h2_client_send_window_update(
                client,
                1,
                0,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_h2_client_cancel(client) }, VylResult::Ok);
    assert_eq!(
        unsafe {
            vyl_h2_client_send_settings(
                client,
                std::ptr::null(),
                0,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::Cancelled
    );
    destroy_client(client);
}

#[test]
fn reports_overrun_and_pending_writes() {
    let client = new_client();
    let settings = client_settings(client);
    assert!(!settings.is_empty());
    let mut pending = 0;
    assert_eq!(
        unsafe { vyl_h2_client_acknowledge(client, usize::MAX, &mut pending) },
        VylResult::BadState
    );
    let fields = [
        encode_field(b":method", b"GET"),
        encode_field(b":scheme", b"https"),
        encode_field(b":path", b"/"),
        encode_field(b":authority", b"example.com"),
    ];
    let mut request = [0; 1_024];
    let mut written = 0;
    let mut stream_id: u32 = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_start_request(
                client,
                fields.as_ptr(),
                fields.len(),
                1,
                request.as_mut_ptr(),
                request.len(),
                &mut written,
                &mut stream_id,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_h2_client_acknowledge(client, written + 1, &mut pending) },
        VylResult::WriteOverrun
    );
    let first_written = written;
    let mut second_stream: u32 = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_start_request(
                client,
                fields.as_ptr(),
                fields.len(),
                1,
                request.as_mut_ptr(),
                request.len(),
                &mut written,
                &mut second_stream,
                &mut required,
            )
        },
        VylResult::WouldBlock
    );
    assert_eq!(ack_client(client, first_written), 0);
    destroy_client(client);
}
