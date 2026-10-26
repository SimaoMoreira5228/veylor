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

// NOTE: scripted-peer wire helpers hand-frame bytes with no live server involved.
// NOTE: the harness feeds byte vectors straight into poll_feed, matching poll_client above:
// NOTE: deterministic, single-shot, and bounded by fixed caller buffers, so no socket timeouts apply.
fn peer_header(payload_length: usize, frame_type: u8, flags: u8, stream_id: u32) -> [u8; 9] {
    let mut header = [0; 9];
    header[0] = ((payload_length >> 16) & 0xFF) as u8;
    header[1] = ((payload_length >> 8) & 0xFF) as u8;
    header[2] = (payload_length & 0xFF) as u8;
    header[3] = frame_type;
    header[4] = flags;
    header[5] = ((stream_id >> 24) & 0x7F) as u8;
    header[6] = ((stream_id >> 16) & 0xFF) as u8;
    header[7] = ((stream_id >> 8) & 0xFF) as u8;
    header[8] = (stream_id & 0xFF) as u8;
    header
}

// NOTE: HPACK literal without indexing; prefix integers use the RFC 7541 variable encoding.
fn peer_push_prefix(block: &mut Vec<u8>, first: u8, bits: u8, value: usize) {
    let max = (1usize << bits) - 1;
    if value < max {
        block.push(first | (value as u8));
        return;
    }
    block.push(first | (max as u8));
    let mut rest = value - max;
    while rest >= 128 {
        block.push(((rest & 0x7F) as u8) | 0x80);
        rest >>= 7;
    }
    block.push(rest as u8);
}

fn peer_string(block: &mut Vec<u8>, text: &[u8]) {
    peer_push_prefix(block, 0x00, 7, text.len());
    block.extend_from_slice(text);
}

fn peer_literal(block: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    block.push(0x00);
    peer_string(block, name);
    peer_string(block, value);
}

fn peer_headers(stream_id: u32, end_stream: bool, fields: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut block = Vec::new();
    for (name, value) in fields {
        peer_literal(&mut block, name, value);
    }
    let mut flags = 0x04;
    if end_stream {
        flags |= 0x01;
    }
    let mut frame = peer_header(block.len(), 0x01, flags, stream_id).to_vec();
    frame.extend_from_slice(&block);
    frame
}

fn peer_data(stream_id: u32, end_stream: bool, payload: &[u8]) -> Vec<u8> {
    let mut flags = 0x00;
    if end_stream {
        flags = 0x01;
    }
    let mut frame = peer_header(payload.len(), 0x00, flags, stream_id).to_vec();
    frame.extend_from_slice(payload);
    frame
}

fn peer_rst(stream_id: u32, code: u32) -> Vec<u8> {
    let mut frame = peer_header(4, 0x03, 0, stream_id).to_vec();
    frame.extend_from_slice(&code.to_be_bytes());
    frame
}

fn peer_goaway(last_stream_id: u32, code: u32) -> Vec<u8> {
    let mut frame = peer_header(8, 0x07, 0, 0).to_vec();
    frame.extend_from_slice(&(last_stream_id & 0x7FFF_FFFF).to_be_bytes());
    frame.extend_from_slice(&code.to_be_bytes());
    frame
}

fn peer_window_update(stream_id: u32, increment: u32) -> Vec<u8> {
    let mut frame = peer_header(4, 0x08, 0, stream_id).to_vec();
    frame.extend_from_slice(&(increment & 0x7FFF_FFFF).to_be_bytes());
    frame
}

fn peer_settings(entries: &[(u16, u32)]) -> Vec<u8> {
    let mut frame = peer_header(entries.len() * 6, 0x04, 0, 0).to_vec();
    for (id, value) in entries {
        frame.extend_from_slice(&id.to_be_bytes());
        frame.extend_from_slice(&value.to_be_bytes());
    }
    frame
}

fn peer_settings_ack() -> Vec<u8> {
    peer_header(0, 0x04, 0x01, 0).to_vec()
}

fn peer_field_text(data: &[u8], field: VylH2Field) -> (Vec<u8>, Vec<u8>) {
    let name = data[field.name_offset..field.name_offset + field.name_length].to_vec();
    let value = data[field.value_offset..field.value_offset + field.value_length].to_vec();
    (name, value)
}

struct PeerOut {
    result: VylResult,
    consumed: usize,
    actions: Vec<VylH2Action>,
    fields: Vec<VylH2Field>,
    data: Vec<u8>,
    connection_error: u32,
}

fn poll_peer(handle: *mut VylH2Client, input: &[u8]) -> PeerOut {
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
    let _ = (actions_required, fields_required, data_required);
    PeerOut {
        result,
        consumed,
        actions: actions[..out_actions].to_vec(),
        fields: fields[..out_fields].to_vec(),
        data: data.to_vec(),
        connection_error: error,
    }
}

fn peer_answer_settings(handle: *mut VylH2Client, bytes: &[u8]) {
    let out = poll_peer(handle, bytes);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.consumed, bytes.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::SettingsAckRequired);
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

fn scripted_handshake(handle: *mut VylH2Client) {
    let settings = client_settings(handle);
    assert!(!settings.is_empty());
    let empty = peer_header(0, 0x04, 0, 0);
    peer_answer_settings(handle, &empty);
    let ack = peer_settings_ack();
    let out = poll_peer(handle, &ack);
    assert_eq!(out.result, VylResult::NeedMore);
    assert_eq!(out.consumed, ack.len());
}

fn scripted_request(handle: *mut VylH2Client, method: &[u8], path: &[u8], end_stream: u32) -> u32 {
    let fields = [
        encode_field(b":method", method),
        encode_field(b":scheme", b"https"),
        encode_field(b":path", path),
        encode_field(b":authority", b"example.com"),
    ];
    let mut request = [0; 1_024];
    let mut written = 0;
    let mut stream_id = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_start_request(
                handle,
                fields.as_ptr(),
                fields.len(),
                end_stream,
                request.as_mut_ptr(),
                request.len(),
                &mut written,
                &mut stream_id,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_client(handle, written), 0);
    stream_id
}

fn scripted_get(handle: *mut VylH2Client) -> u32 {
    scripted_request(handle, b"GET", b"/", 1)
}

fn scripted_classify(
    handle: *mut VylH2Client,
    stream_id: u32,
    action: &VylH2Action,
) -> VylH2ResponseClass {
    let mut class = VylH2ResponseClass::default();
    assert_eq!(
        unsafe {
            vyl_h2_client_classify_headers(
                handle,
                stream_id,
                action.field_start,
                action.field_count,
                action.flags,
                &mut class,
            )
        },
        VylResult::Ok
    );
    class
}

fn scripted_account(handle: *mut VylH2Client, action: &VylH2Action) -> u32 {
    let mut phase = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_account_data(
                handle,
                action.stream_id,
                action.data_length,
                action.flags,
                &mut phase,
            )
        },
        VylResult::Ok
    );
    phase
}

fn scripted_send(
    handle: *mut VylH2Client,
    stream_id: u32,
    input: &[u8],
    end_stream: u32,
) -> (VylResult, usize, usize, u32) {
    let mut output = [0; 1_024];
    let mut consumed = 0;
    let mut written = 0;
    let mut end_flag = 0;
    let mut required = 0;
    let result = unsafe {
        vyl_h2_client_send_data(
            handle,
            stream_id,
            VylBytes {
                data: input.as_ptr(),
                length: input.len(),
            },
            end_stream,
            output.as_mut_ptr(),
            output.len(),
            &mut consumed,
            &mut written,
            &mut end_flag,
            &mut required,
        )
    };
    (result, consumed, written, end_flag)
}

#[test]
fn scripted_get_200_data_trailers() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let headers = peer_headers(1, false, &[(b":status".as_slice(), b"200".as_slice())]);
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.consumed, headers.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Headers);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!((class.kind, class.status, class.phase), (1, 200, 1));
    let body = peer_data(1, false, b"hello");
    let out = poll_peer(client, &body);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Data);
    assert_eq!(out.actions[0].data_length, 5);
    let payload = &out.data[out.actions[0].data_offset..out.actions[0].data_offset + 5];
    assert_eq!(payload, b"hello");
    assert_eq!(scripted_account(client, &out.actions[0]), 1);
    let trailers = peer_headers(1, true, &[(b"x-sum".as_slice(), b"abc".as_slice())]);
    let out = poll_peer(client, &trailers);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Headers);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!((class.kind, class.phase), (2, 2));
    assert_eq!(out.fields.len(), 1);
    let (name, value) = peer_field_text(&out.data, out.fields[0]);
    assert_eq!((name, value), (b"x-sum".to_vec(), b"abc".to_vec()));
    destroy_client(client);
}

#[test]
fn scripted_informational_then_final() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let interim = peer_headers(1, false, &[(b":status".as_slice(), b"103".as_slice())]);
    let out = poll_peer(client, &interim);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Headers);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!((class.kind, class.status, class.phase), (0, 103, 0));
    let end = peer_headers(1, true, &[(b":status".as_slice(), b"200".as_slice())]);
    let out = poll_peer(client, &end);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!((class.kind, class.status, class.phase), (1, 200, 2));
    destroy_client(client);
}

#[test]
fn scripted_204_no_body() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let headers = peer_headers(1, true, &[(b":status".as_slice(), b"204".as_slice())]);
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!((class.kind, class.status, class.phase), (1, 204, 2));
    let stray = peer_data(1, true, b"x");
    let out = poll_peer(client, &stray);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.consumed, stray.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::SendReset);
    assert_eq!(out.actions[0].stream_id, 1);
    assert_eq!(out.actions[0].error_code, 5);
    destroy_client(client);
}

#[test]
fn scripted_content_length_dispute_errors() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let headers = peer_headers(
        1,
        false,
        &[
            (b":status".as_slice(), b"200".as_slice()),
            (b"content-length".as_slice(), b"11".as_slice()),
        ],
    );
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!(
        (class.kind, class.content_length, class.has_content_length),
        (1, 11, 1)
    );
    let overflow = peer_data(1, true, b"01234567890123456789");
    let out = poll_peer(client, &overflow);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Data);
    assert_eq!(out.actions[0].data_length, 20);
    let mut phase = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_account_data(
                client,
                1,
                out.actions[0].data_length,
                out.actions[0].flags,
                &mut phase,
            )
        },
        VylResult::ProtocolError
    );
    let second = scripted_request(client, b"GET", b"/short", 1);
    assert_eq!(second, 3);
    let headers = peer_headers(
        3,
        false,
        &[
            (b":status".as_slice(), b"200".as_slice()),
            (b"content-length".as_slice(), b"11".as_slice()),
        ],
    );
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    let class = scripted_classify(client, 3, &out.actions[0]);
    assert_eq!(class.phase, 1);
    let short = peer_data(3, true, b"hello");
    let out = poll_peer(client, &short);
    assert_eq!(out.result, VylResult::Ok);
    let mut phase = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_account_data(
                client,
                3,
                out.actions[0].data_length,
                out.actions[0].flags,
                &mut phase,
            )
        },
        VylResult::ProtocolError
    );
    let third = scripted_request(client, b"GET", b"/ok", 1);
    assert_eq!(third, 5);
    let headers = peer_headers(5, true, &[(b":status".as_slice(), b"200".as_slice())]);
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    let class = scripted_classify(client, 5, &out.actions[0]);
    assert_eq!((class.kind, class.phase), (1, 2));
    destroy_client(client);
}

#[test]
fn scripted_rst_mid_body_keeps_connection() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let headers = peer_headers(
        1,
        false,
        &[
            (b":status".as_slice(), b"200".as_slice()),
            (b"content-length".as_slice(), b"11".as_slice()),
        ],
    );
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!(class.phase, 1);
    let chunk = peer_data(1, false, b"hello ");
    let out = poll_peer(client, &chunk);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(scripted_account(client, &out.actions[0]), 1);
    let reset = peer_rst(1, 8);
    let out = poll_peer(client, &reset);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.consumed, reset.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::StreamReset);
    assert_eq!(out.actions[0].stream_id, 1);
    assert_eq!(out.actions[0].error_code, 8);
    let late = peer_data(1, true, b"x");
    let out = poll_peer(client, &late);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.consumed, late.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::SendReset);
    assert_eq!(out.actions[0].stream_id, 1);
    assert_eq!(out.actions[0].error_code, 5);
    let second = scripted_request(client, b"GET", b"/again", 1);
    assert_eq!(second, 3);
    let headers = peer_headers(3, true, &[(b":status".as_slice(), b"200".as_slice())]);
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    let class = scripted_classify(client, 3, &out.actions[0]);
    assert_eq!((class.kind, class.status, class.phase), (1, 200, 2));
    destroy_client(client);
}

#[test]
fn scripted_goaway_drains_new_streams() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let headers = peer_headers(1, false, &[(b":status".as_slice(), b"200".as_slice())]);
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!(class.phase, 1);
    let goaway = peer_goaway(1, 0);
    let out = poll_peer(client, &goaway);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.consumed, goaway.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::GoAway);
    assert_eq!(out.actions[0].stream_id, 1);
    assert_eq!(out.actions[0].error_code, 0);
    let tail = peer_data(1, true, b"hi");
    let out = poll_peer(client, &tail);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Data);
    assert_eq!(scripted_account(client, &out.actions[0]), 2);
    let fields = [
        encode_field(b":method", b"GET"),
        encode_field(b":scheme", b"https"),
        encode_field(b":path", b"/"),
        encode_field(b":authority", b"example.com"),
    ];
    let mut request = [0; 1_024];
    let mut written = 0;
    let mut stream_id = 0;
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
        VylResult::BadState
    );
    destroy_client(client);
}

#[test]
fn scripted_send_honors_peer_window() {
    let client = new_client();
    scripted_handshake(client);
    peer_answer_settings(client, &peer_settings(&[(4, 16)]));
    let stream = scripted_request(client, b"POST", b"/upload", 0);
    assert_eq!(stream, 1);
    let input = [7; 100];
    let (result, consumed, written, end_flag) = scripted_send(client, 1, &input, 0);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(consumed, 16);
    assert_eq!(written, 25);
    assert_eq!(end_flag, 0);
    assert_eq!(ack_client(client, written), 0);
    let out = poll_peer(client, &peer_window_update(1, 100));
    assert_eq!(out.result, VylResult::NeedMore);
    let out = poll_peer(client, &peer_window_update(0, 100));
    assert_eq!(out.result, VylResult::NeedMore);
    let (result, consumed, written, end_flag) = scripted_send(client, 1, &input[16..], 1);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(consumed, 84);
    assert_eq!(written, 93);
    assert_eq!(end_flag, 1);
    assert_eq!(ack_client(client, written), 0);
    peer_answer_settings(client, &peer_settings(&[(4, 0)]));
    let starved = scripted_request(client, b"POST", b"/starved", 0);
    assert_eq!(starved, 3);
    let (result, _, _, _) = scripted_send(client, 3, &input[..10], 0);
    assert_eq!(result, VylResult::WouldBlock);
    let out = poll_peer(client, &peer_window_update(3, 10));
    assert_eq!(out.result, VylResult::NeedMore);
    let out = poll_peer(client, &peer_window_update(0, 10));
    assert_eq!(out.result, VylResult::NeedMore);
    let (result, consumed, written, end_flag) = scripted_send(client, 3, &input[..10], 1);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(consumed, 10);
    assert_eq!(written, 19);
    assert_eq!(end_flag, 1);
    assert_eq!(ack_client(client, written), 0);
    destroy_client(client);
}

#[test]
fn scripted_oversized_headers_fails_closed() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let payload = [0; 20_000];
    let mut frame = peer_header(payload.len(), 0x01, 0x04, 1).to_vec();
    frame.extend_from_slice(&payload);
    let out = poll_peer(client, &frame);
    assert_eq!(out.result, VylResult::ProtocolError);
    assert_eq!(out.consumed, 0);
    assert_eq!(out.connection_error, 6);
    let out = poll_peer(client, &[]);
    assert_eq!(out.result, VylResult::ProtocolError);
    assert_eq!(out.connection_error, 6);
    let fields = [
        encode_field(b":method", b"GET"),
        encode_field(b":scheme", b"https"),
        encode_field(b":path", b"/"),
        encode_field(b":authority", b"example.com"),
    ];
    let mut request = [0; 1_024];
    let mut written = 0;
    let mut stream_id = 0;
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
        VylResult::BadState
    );
    destroy_client(client);
}

#[test]
fn scripted_zero_length_data_end_stream() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let headers = peer_headers(1, false, &[(b":status".as_slice(), b"200".as_slice())]);
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!(class.phase, 1);
    let empty = peer_data(1, true, b"");
    let out = poll_peer(client, &empty);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.consumed, empty.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Data);
    assert_eq!(out.actions[0].data_length, 0);
    assert_eq!(scripted_account(client, &out.actions[0]), 2);
    destroy_client(client);
}

#[test]
fn scripted_even_stream_id_fails_connection() {
    let client = new_client();
    scripted_handshake(client);
    let push = peer_headers(2, true, &[(b":status".as_slice(), b"200".as_slice())]);
    let out = poll_peer(client, &push);
    assert_eq!(out.result, VylResult::ProtocolError);
    assert_eq!(out.consumed, push.len());
    assert_eq!(out.connection_error, 1);
    let out = poll_peer(client, &[]);
    assert_eq!(out.result, VylResult::ProtocolError);
    assert_eq!(out.connection_error, 1);
    let (result, _, _, _) = scripted_send(client, 1, b"x", 0);
    assert_eq!(result, VylResult::BadState);
    let fields = [
        encode_field(b":method", b"GET"),
        encode_field(b":scheme", b"https"),
        encode_field(b":path", b"/"),
        encode_field(b":authority", b"example.com"),
    ];
    let mut request = [0; 1_024];
    let mut written = 0;
    let mut stream_id = 0;
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
        VylResult::BadState
    );
    destroy_client(client);
}
