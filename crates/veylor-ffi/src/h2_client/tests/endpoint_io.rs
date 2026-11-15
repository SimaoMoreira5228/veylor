use super::super::*;
use crate::abi::{
    VylBytes, VylH2Action, VylH2ActionKind, VylH2Field, VylH2FieldToEncode, VylResult,
};
use crate::h2_server;

pub(crate) fn new_client() -> *mut VylH2Client {
    let mut handle = std::ptr::null_mut();
    assert_eq!(unsafe { vyl_h2_client_new(&mut handle) }, VylResult::Ok);
    assert!(!handle.is_null());
    handle
}

pub(crate) fn new_server() -> *mut h2_server::VylH2Server {
    let mut handle = std::ptr::null_mut();
    assert_eq!(
        unsafe { h2_server::vyl_h2_server_new(&mut handle) },
        VylResult::Ok
    );
    assert!(!handle.is_null());
    handle
}

pub(crate) fn destroy_client(handle: *mut VylH2Client) {
    assert_eq!(unsafe { vyl_h2_client_destroy(handle) }, VylResult::Ok);
}

pub(crate) fn destroy_server(handle: *mut h2_server::VylH2Server) {
    assert_eq!(
        unsafe { h2_server::vyl_h2_server_destroy(handle) },
        VylResult::Ok
    );
}

pub(crate) fn ack_client(handle: *mut VylH2Client, transferred: usize) -> usize {
    let mut pending = usize::MAX;
    assert_eq!(
        unsafe { vyl_h2_client_acknowledge(handle, transferred, &mut pending) },
        VylResult::Ok
    );
    pending
}

pub(crate) fn ack_server(handle: *mut h2_server::VylH2Server, transferred: usize) -> usize {
    let mut pending = usize::MAX;
    assert_eq!(
        unsafe { h2_server::vyl_h2_server_acknowledge(handle, transferred, &mut pending) },
        VylResult::Ok
    );
    pending
}

pub(crate) fn client_settings(handle: *mut VylH2Client) -> Vec<u8> {
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

pub(crate) fn server_settings(handle: *mut h2_server::VylH2Server) -> Vec<u8> {
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

pub(crate) struct PollOut {
    pub(crate) consumed: usize,
    pub(crate) actions: Vec<VylH2Action>,
    pub(crate) fields: Vec<VylH2Field>,
    pub(crate) data: Vec<u8>,
}

pub(crate) fn poll_client(handle: *mut VylH2Client, input: &[u8]) -> (VylResult, PollOut) {
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

pub(crate) fn poll_server(
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

pub(crate) fn encode_field(name: &[u8], value: &[u8]) -> VylH2FieldToEncode {
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

pub(crate) fn field_text(out: &PollOut, field: VylH2Field) -> (Vec<u8>, Vec<u8>) {
    let name = out.data[field.name_offset..field.name_offset + field.name_length].to_vec();
    let value = out.data[field.value_offset..field.value_offset + field.value_length].to_vec();
    (name, value)
}

pub(crate) fn answer_settings_ack_client(handle: *mut VylH2Client, bytes: &[u8]) {
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

pub(crate) fn answer_settings_ack_server(handle: *mut h2_server::VylH2Server, bytes: &[u8]) {
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

pub(crate) fn drain_ack_client(handle: *mut VylH2Client, bytes: &[u8]) {
    let (result, out) = poll_client(handle, bytes);
    assert_eq!(result, VylResult::NeedMore);
    assert_eq!(out.consumed, bytes.len());
}

pub(crate) fn drain_ack_server(handle: *mut h2_server::VylH2Server, bytes: &[u8]) {
    let (result, out) = poll_server(handle, bytes);
    assert_eq!(result, VylResult::NeedMore);
    assert_eq!(out.consumed, bytes.len());
}

pub(crate) fn send_client_ack_bytes(handle: *mut VylH2Client) -> Vec<u8> {
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

pub(crate) fn send_server_ack_bytes(handle: *mut h2_server::VylH2Server) -> Vec<u8> {
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
