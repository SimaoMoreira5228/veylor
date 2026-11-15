use super::super::*;
use super::endpoint_io::*;
use crate::abi::{
    VylBytes, VylH2Action, VylH2ActionKind, VylH2Field, VylH2ResponseClass, VylResult,
};

// NOTE: scripted-peer wire helpers hand-frame bytes with no live server involved.
// NOTE: the harness feeds byte vectors straight into poll_feed, matching poll_client above:
// NOTE: deterministic, single-shot, and bounded by fixed caller buffers, so no socket timeouts apply.
pub(crate) fn peer_header(
    payload_length: usize,
    frame_type: u8,
    flags: u8,
    stream_id: u32,
) -> [u8; 9] {
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
pub(crate) fn peer_push_prefix(block: &mut Vec<u8>, first: u8, bits: u8, value: usize) {
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

pub(crate) fn peer_string(block: &mut Vec<u8>, text: &[u8]) {
    peer_push_prefix(block, 0x00, 7, text.len());
    block.extend_from_slice(text);
}

pub(crate) fn peer_literal(block: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    block.push(0x00);
    peer_string(block, name);
    peer_string(block, value);
}

pub(crate) fn peer_headers(stream_id: u32, end_stream: bool, fields: &[(&[u8], &[u8])]) -> Vec<u8> {
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

pub(crate) fn peer_data(stream_id: u32, end_stream: bool, payload: &[u8]) -> Vec<u8> {
    let mut flags = 0x00;
    if end_stream {
        flags = 0x01;
    }
    let mut frame = peer_header(payload.len(), 0x00, flags, stream_id).to_vec();
    frame.extend_from_slice(payload);
    frame
}

pub(crate) fn peer_rst(stream_id: u32, code: u32) -> Vec<u8> {
    let mut frame = peer_header(4, 0x03, 0, stream_id).to_vec();
    frame.extend_from_slice(&code.to_be_bytes());
    frame
}

pub(crate) fn peer_goaway(last_stream_id: u32, code: u32) -> Vec<u8> {
    let mut frame = peer_header(8, 0x07, 0, 0).to_vec();
    frame.extend_from_slice(&(last_stream_id & 0x7FFF_FFFF).to_be_bytes());
    frame.extend_from_slice(&code.to_be_bytes());
    frame
}

pub(crate) fn peer_window_update(stream_id: u32, increment: u32) -> Vec<u8> {
    let mut frame = peer_header(4, 0x08, 0, stream_id).to_vec();
    frame.extend_from_slice(&(increment & 0x7FFF_FFFF).to_be_bytes());
    frame
}

pub(crate) fn peer_settings(entries: &[(u16, u32)]) -> Vec<u8> {
    let mut frame = peer_header(entries.len() * 6, 0x04, 0, 0).to_vec();
    for (id, value) in entries {
        frame.extend_from_slice(&id.to_be_bytes());
        frame.extend_from_slice(&value.to_be_bytes());
    }
    frame
}

pub(crate) fn peer_settings_ack() -> Vec<u8> {
    peer_header(0, 0x04, 0x01, 0).to_vec()
}

pub(crate) fn peer_field_text(data: &[u8], field: VylH2Field) -> (Vec<u8>, Vec<u8>) {
    let name = data[field.name_offset..field.name_offset + field.name_length].to_vec();
    let value = data[field.value_offset..field.value_offset + field.value_length].to_vec();
    (name, value)
}

pub(crate) struct PeerOut {
    pub(crate) result: VylResult,
    pub(crate) consumed: usize,
    pub(crate) actions: Vec<VylH2Action>,
    pub(crate) fields: Vec<VylH2Field>,
    pub(crate) data: Vec<u8>,
    pub(crate) connection_error: u32,
}

pub(crate) fn poll_peer(handle: *mut VylH2Client, input: &[u8]) -> PeerOut {
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

pub(crate) fn peer_answer_settings(handle: *mut VylH2Client, bytes: &[u8]) {
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

pub(crate) fn scripted_handshake(handle: *mut VylH2Client) {
    let settings = client_settings(handle);
    assert_eq!(
        settings,
        [
            0, 0, 12, 0x04, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 6, 0, 0, 8, 64,
        ]
    );
    let empty = peer_header(0, 0x04, 0, 0);
    peer_answer_settings(handle, &empty);
    let ack = peer_settings_ack();
    let out = poll_peer(handle, &ack);
    assert_eq!(out.result, VylResult::NeedMore);
    assert_eq!(out.consumed, ack.len());
}

pub(crate) fn scripted_request(
    handle: *mut VylH2Client,
    method: &[u8],
    path: &[u8],
    end_stream: u32,
) -> u32 {
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

pub(crate) fn scripted_get(handle: *mut VylH2Client) -> u32 {
    scripted_request(handle, b"GET", b"/", 1)
}

pub(crate) fn scripted_classify(
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

pub(crate) fn scripted_account(handle: *mut VylH2Client, action: &VylH2Action) -> u32 {
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

pub(crate) fn scripted_send(
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
