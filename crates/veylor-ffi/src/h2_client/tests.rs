use super::*;
use crate::abi::{
    VylBytes, VylH2Action, VylH2ActionKind, VylH2Field, VylH2FieldToEncode, VylH2ResponseClass,
    VylH2Setting, VylResult,
};
use crate::h2_server;

pub(super) fn new_client() -> *mut VylH2Client {
    let mut handle = std::ptr::null_mut();
    assert_eq!(unsafe { vyl_h2_client_new(&mut handle) }, VylResult::Ok);
    assert!(!handle.is_null());
    handle
}

pub(super) fn new_server() -> *mut h2_server::VylH2Server {
    let mut handle = std::ptr::null_mut();
    assert_eq!(
        unsafe { h2_server::vyl_h2_server_new(&mut handle) },
        VylResult::Ok
    );
    assert!(!handle.is_null());
    handle
}

pub(super) fn destroy_client(handle: *mut VylH2Client) {
    assert_eq!(unsafe { vyl_h2_client_destroy(handle) }, VylResult::Ok);
}

pub(super) fn destroy_server(handle: *mut h2_server::VylH2Server) {
    assert_eq!(
        unsafe { h2_server::vyl_h2_server_destroy(handle) },
        VylResult::Ok
    );
}

pub(super) fn ack_client(handle: *mut VylH2Client, transferred: usize) -> usize {
    let mut pending = usize::MAX;
    assert_eq!(
        unsafe { vyl_h2_client_acknowledge(handle, transferred, &mut pending) },
        VylResult::Ok
    );
    pending
}

pub(super) fn ack_server(handle: *mut h2_server::VylH2Server, transferred: usize) -> usize {
    let mut pending = usize::MAX;
    assert_eq!(
        unsafe { h2_server::vyl_h2_server_acknowledge(handle, transferred, &mut pending) },
        VylResult::Ok
    );
    pending
}

pub(super) fn client_settings(handle: *mut VylH2Client) -> Vec<u8> {
    let mut output = [0; 128];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_send_settings(
                handle,
                std::ptr::null(),
                0,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_client(handle, written), 0);
    output[..written].to_vec()
}

pub(super) fn server_settings(handle: *mut h2_server::VylH2Server) -> Vec<u8> {
    let mut output = [0; 128];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            h2_server::vyl_h2_server_send_settings(
                handle,
                std::ptr::null(),
                0,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_server(handle, written), 0);
    output[..written].to_vec()
}

pub(super) struct PollOut {
    pub(super) consumed: usize,
    pub(super) actions: Vec<VylH2Action>,
    pub(super) fields: Vec<VylH2Field>,
    pub(super) data: Vec<u8>,
}

pub(super) fn poll_client(handle: *mut VylH2Client, input: &[u8]) -> (VylResult, PollOut) {
    let mut actions = [VylH2Action::default(); crate::abi::VYL_H2_MAX_ACTIONS];
    let mut fields = [VylH2Field::default(); crate::abi::VYL_H2_MAX_FIELDS];
    let mut data = [0; 16_384];
    let mut consumed = 0;
    let mut out_actions = 0;
    let mut out_fields = 0;
    let mut actions_required = 0;
    let mut fields_required = 0;
    let mut data_required = 0;
    let mut error = 0;
    let result = unsafe {
        vyl_h2_client_poll_feed(
            handle,
            input.as_ptr(),
            input.len(),
            &mut consumed,
            actions.as_mut_ptr(),
            actions.len(),
            fields.as_mut_ptr(),
            fields.len(),
            data.as_mut_ptr(),
            data.len(),
            &mut out_actions,
            &mut out_fields,
            &mut actions_required,
            &mut fields_required,
            &mut data_required,
            &mut error,
        )
    };
    let _ = (actions_required, fields_required, data_required, error);
    (
        result,
        PollOut {
            consumed,
            actions: actions[..out_actions].to_vec(),
            fields: fields[..out_fields].to_vec(),
            data: data.to_vec(),
        },
    )
}

pub(super) fn poll_server(
    handle: *mut h2_server::VylH2Server,
    input: &[u8],
) -> (VylResult, PollOut) {
    let mut actions = [VylH2Action::default(); crate::abi::VYL_H2_MAX_ACTIONS];
    let mut fields = [VylH2Field::default(); crate::abi::VYL_H2_MAX_FIELDS];
    let mut data = [0; 16_384];
    let mut consumed = 0;
    let mut out_actions = 0;
    let mut out_fields = 0;
    let mut actions_required = 0;
    let mut fields_required = 0;
    let mut data_required = 0;
    let mut error = 0;
    let result = unsafe {
        h2_server::vyl_h2_server_poll_feed(
            handle,
            input.as_ptr(),
            input.len(),
            &mut consumed,
            actions.as_mut_ptr(),
            actions.len(),
            fields.as_mut_ptr(),
            fields.len(),
            data.as_mut_ptr(),
            data.len(),
            &mut out_actions,
            &mut out_fields,
            &mut actions_required,
            &mut fields_required,
            &mut data_required,
            &mut error,
        )
    };
    let _ = (actions_required, fields_required, data_required, error);
    (
        result,
        PollOut {
            consumed,
            actions: actions[..out_actions].to_vec(),
            fields: fields[..out_fields].to_vec(),
            data: data.to_vec(),
        },
    )
}

pub(super) fn encode_field(name: &[u8], value: &[u8]) -> VylH2FieldToEncode {
    VylH2FieldToEncode {
        name: VylBytes {
            data: name.as_ptr(),
            length: name.len(),
        },
        value: VylBytes {
            data: value.as_ptr(),
            length: value.len(),
        },
        representation: 2,
        huffman_name: 0,
        huffman_value: 0,
    }
}

pub(super) fn field_text(out: &PollOut, field: VylH2Field) -> (Vec<u8>, Vec<u8>) {
    let name = out.data[field.name_offset..field.name_offset + field.name_length].to_vec();
    let value = out.data[field.value_offset..field.value_offset + field.value_length].to_vec();
    (name, value)
}

pub(super) fn answer_settings_ack_client(handle: *mut VylH2Client, bytes: &[u8]) {
    let (result, out) = poll_client(handle, bytes);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::SettingsAckRequired);
    assert_eq!(out.consumed, bytes.len());
    let mut output = [0; 32];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_send_settings_ack(
                handle,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_client(handle, written), 0);
}

pub(super) fn answer_settings_ack_server(handle: *mut h2_server::VylH2Server, bytes: &[u8]) {
    let (result, out) = poll_server(handle, bytes);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::SettingsAckRequired);
    assert_eq!(out.consumed, bytes.len());
    let mut output = [0; 32];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            h2_server::vyl_h2_server_send_settings_ack(
                handle,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_server(handle, written), 0);
}

pub(super) fn drain_ack_client(handle: *mut VylH2Client, bytes: &[u8]) {
    let (result, out) = poll_client(handle, bytes);
    assert_eq!(result, VylResult::NeedMore);
    assert_eq!(out.consumed, bytes.len());
}

pub(super) fn drain_ack_server(handle: *mut h2_server::VylH2Server, bytes: &[u8]) {
    let (result, out) = poll_server(handle, bytes);
    assert_eq!(result, VylResult::NeedMore);
    assert_eq!(out.consumed, bytes.len());
}

pub(super) fn send_client_ack_bytes(handle: *mut VylH2Client) -> Vec<u8> {
    let mut output = [0; 32];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_send_settings_ack(
                handle,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_client(handle, written), 0);
    output[..written].to_vec()
}

pub(super) fn send_server_ack_bytes(handle: *mut h2_server::VylH2Server) -> Vec<u8> {
    let mut output = [0; 32];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            h2_server::vyl_h2_server_send_settings_ack(
                handle,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_server(handle, written), 0);
    output[..written].to_vec()
}

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
