use super::*;
use crate::abi::{
    VylBytes, VylH2Action, VylH2ActionKind, VylH2Field, VylH2FieldToEncode, VylH2ValidatedRequest,
    VylResult,
};
use crate::h2_client;

fn new_server() -> *mut VylH2Server {
    let mut handle = std::ptr::null_mut();
    assert_eq!(unsafe { vyl_h2_server_new(&mut handle) }, VylResult::Ok);
    assert!(!handle.is_null());
    handle
}

fn new_client() -> *mut h2_client::VylH2Client {
    let mut handle = std::ptr::null_mut();
    assert_eq!(
        unsafe { h2_client::vyl_h2_client_new(&mut handle) },
        VylResult::Ok
    );
    assert!(!handle.is_null());
    handle
}

fn destroy_server(handle: *mut VylH2Server) {
    assert_eq!(unsafe { vyl_h2_server_destroy(handle) }, VylResult::Ok);
}

fn destroy_client(handle: *mut h2_client::VylH2Client) {
    assert_eq!(
        unsafe { h2_client::vyl_h2_client_destroy(handle) },
        VylResult::Ok
    );
}

fn ack_server(handle: *mut VylH2Server, transferred: usize) -> usize {
    let mut pending = usize::MAX;
    assert_eq!(
        unsafe { vyl_h2_server_acknowledge(handle, transferred, &mut pending) },
        VylResult::Ok
    );
    pending
}

fn ack_client(handle: *mut h2_client::VylH2Client, transferred: usize) -> usize {
    let mut pending = usize::MAX;
    assert_eq!(
        unsafe { h2_client::vyl_h2_client_acknowledge(handle, transferred, &mut pending) },
        VylResult::Ok
    );
    pending
}

fn encode_field(name: &[u8], value: &[u8]) -> VylH2FieldToEncode {
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

struct PollOut {
    consumed: usize,
    actions: Vec<VylH2Action>,
    fields: Vec<VylH2Field>,
    data: Vec<u8>,
}

fn poll_server(handle: *mut VylH2Server, input: &[u8]) -> (VylResult, PollOut) {
    poll_server_caps(
        handle,
        input,
        crate::abi::VYL_H2_MAX_ACTIONS,
        crate::abi::VYL_H2_MAX_FIELDS,
        16_384,
    )
}

fn poll_server_caps(
    handle: *mut VylH2Server,
    input: &[u8],
    action_cap: usize,
    field_cap: usize,
    data_cap: usize,
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
        vyl_h2_server_poll_feed(
            handle,
            input.as_ptr(),
            input.len(),
            &mut consumed,
            actions.as_mut_ptr(),
            action_cap.min(actions.len()),
            fields.as_mut_ptr(),
            field_cap.min(fields.len()),
            data.as_mut_ptr(),
            data_cap.min(data.len()),
            &mut out_actions,
            &mut out_fields,
            &mut actions_required,
            &mut fields_required,
            &mut data_required,
            &mut error,
        )
    };
    let _ = error;
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

fn poll_client_caps(
    handle: *mut h2_client::VylH2Client,
    input: &[u8],
    action_cap: usize,
    field_cap: usize,
    data_cap: usize,
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
        h2_client::vyl_h2_client_poll_feed(
            handle,
            input.as_ptr(),
            input.len(),
            &mut consumed,
            actions.as_mut_ptr(),
            action_cap.min(actions.len()),
            fields.as_mut_ptr(),
            field_cap.min(fields.len()),
            data.as_mut_ptr(),
            data_cap.min(data.len()),
            &mut out_actions,
            &mut out_fields,
            &mut actions_required,
            &mut fields_required,
            &mut data_required,
            &mut error,
        )
    };
    let _ = error;
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

fn field_text(out: &PollOut, field: VylH2Field) -> (Vec<u8>, Vec<u8>) {
    let name = out.data[field.name_offset..field.name_offset + field.name_length].to_vec();
    let value = out.data[field.value_offset..field.value_offset + field.value_length].to_vec();
    (name, value)
}

fn answer_ack_server(handle: *mut VylH2Server, bytes: &[u8]) {
    let (result, out) = poll_server_caps(handle, bytes, 16, 64, 16_384);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.consumed, bytes.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::SettingsAckRequired);
    let mut output = [0; 32];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_server_send_settings_ack(
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

fn answer_ack_client(handle: *mut h2_client::VylH2Client, bytes: &[u8]) {
    let (result, out) = poll_client_caps(handle, bytes, 16, 64, 16_384);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.consumed, bytes.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::SettingsAckRequired);
    let mut output = [0; 32];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            h2_client::vyl_h2_client_send_settings_ack(
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

fn drain_client(handle: *mut h2_client::VylH2Client, bytes: &[u8]) {
    let (result, out) = poll_client_caps(handle, bytes, 16, 64, 16_384);
    assert_eq!(result, VylResult::NeedMore);
    assert_eq!(out.consumed, bytes.len());
}

fn drain_server(handle: *mut VylH2Server, bytes: &[u8]) {
    let (result, out) = poll_server_caps(handle, bytes, 16, 64, 16_384);
    assert_eq!(result, VylResult::NeedMore);
    assert_eq!(out.consumed, bytes.len());
}

fn ack_bytes_client(handle: *mut h2_client::VylH2Client) -> Vec<u8> {
    let mut output = [0; 32];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            h2_client::vyl_h2_client_send_settings_ack(
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

fn ack_bytes_server(handle: *mut VylH2Server) -> Vec<u8> {
    let mut output = [0; 32];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_server_send_settings_ack(
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

fn handshake(client: *mut h2_client::VylH2Client, server: *mut VylH2Server) {
    let mut preface = [0; 32];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            h2_client::vyl_h2_client_connection_preface(
                preface.as_mut_ptr(),
                preface.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    drain_server(server, &preface[..written]);
    let mut settings = [0; 128];
    let mut settings_written = 0;
    let mut settings_required = 0;
    assert_eq!(
        unsafe {
            h2_client::vyl_h2_client_send_settings(
                client,
                std::ptr::null(),
                0,
                settings.as_mut_ptr(),
                settings.len(),
                &mut settings_written,
                &mut settings_required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_client(client, settings_written), 0);
    answer_ack_server(server, &settings[..settings_written]);
    assert_eq!(
        unsafe {
            vyl_h2_server_send_settings(
                server,
                std::ptr::null(),
                0,
                settings.as_mut_ptr(),
                settings.len(),
                &mut settings_written,
                &mut settings_required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_server(server, settings_written), 0);
    answer_ack_client(client, &settings[..settings_written]);
    drain_server(server, &ack_bytes_client(client));
    drain_client(client, &ack_bytes_server(server));
}

fn client_request_bytes(client: *mut h2_client::VylH2Client) -> (Vec<u8>, u32) {
    let fields = [
        encode_field(b":method", b"POST"),
        encode_field(b":scheme", b"https"),
        encode_field(b":path", b"/upload"),
        encode_field(b":authority", b"example.com"),
    ];
    let mut output = [0; 1_024];
    let mut written = 0;
    let mut stream_id = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            h2_client::vyl_h2_client_start_request(
                client,
                fields.as_ptr(),
                fields.len(),
                0,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut stream_id,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_client(client, written), 0);
    (output[..written].to_vec(), stream_id)
}

#[test]
fn accept_response_refuse_and_window_update_round_trip() {
    let client = new_client();
    let server = new_server();
    handshake(client, server);
    let (request, stream_id) = client_request_bytes(client);
    assert_eq!(stream_id, 1);
    let (result, out) = poll_server(server, &request);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.consumed, request.len());
    assert_eq!(out.fields.len(), 4);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Headers);
    assert_eq!(out.actions[0].flags, 0);
    let mut validated = VylH2ValidatedRequest::default();
    assert_eq!(
        unsafe {
            vyl_h2_server_accept_request(
                server,
                stream_id,
                out.actions[0].field_start,
                out.actions[0].field_count,
                &mut validated,
            )
        },
        VylResult::Ok
    );
    assert_eq!(validated.has_content_length, 0);
    assert!(validated.authority_field != crate::abi::VYL_H2_NO_FIELD);
    let (_, method) = field_text(&out, out.fields[validated.method_field]);
    assert_eq!(method, b"POST");
    let (_, path) = field_text(&out, out.fields[validated.path_field]);
    assert_eq!(path, b"/upload");
    let mut update = [0; 32];
    let mut update_written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_server_send_window_update(
                server,
                stream_id,
                1_024,
                update.as_mut_ptr(),
                update.len(),
                &mut update_written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_server(server, update_written), 0);
    let response_fields = [encode_field(b":status", b"200")];
    let mut response = [0; 1_024];
    let mut response_written = 0;
    assert_eq!(
        unsafe {
            vyl_h2_server_send_response(
                server,
                stream_id,
                response_fields.as_ptr(),
                response_fields.len(),
                1,
                response.as_mut_ptr(),
                response.len(),
                &mut response_written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_server(server, response_written), 0);
    let (result, out) = poll_client_caps(client, &response[..response_written], 16, 64, 16_384);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.consumed, response_written);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Headers);
    assert_eq!(out.actions[0].flags, crate::abi::VYL_H2_END_STREAM);
    let (request2, stream3) = client_request_bytes(client);
    assert_eq!(stream3, 3);
    let (result, out) = poll_server(server, &request2);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.consumed, request2.len());
    let mut refused = [0; 32];
    let mut refused_written = 0;
    assert_eq!(
        unsafe {
            vyl_h2_server_refuse(
                server,
                stream3,
                refused.as_mut_ptr(),
                refused.len(),
                &mut refused_written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_server(server, refused_written), 0);
    let (result, out) = poll_client_caps(client, &refused[..refused_written], 16, 64, 16_384);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.consumed, refused_written);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::StreamReset);
    assert_eq!(out.actions[0].stream_id, stream3);
    assert_eq!(out.actions[0].error_code, 7);
    destroy_client(client);
    destroy_server(server);
}

fn ping_frame(opaque: &[u8; 8]) -> [u8; 17] {
    let mut frame = [0; 17];
    frame[2] = 8;
    frame[3] = 0x06;
    frame[9..].copy_from_slice(opaque);
    frame
}

#[test]
fn ping_ack_and_goaway_drain_round_trip() {
    let client = new_client();
    let server = new_server();
    handshake(client, server);
    let opaque = [1, 2, 3, 4, 5, 6, 7, 8];
    let mut ping = [0; 32];
    let mut ping_written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_server_send_ping_ack(
                server,
                opaque.as_ptr(),
                ping.as_mut_ptr(),
                ping.len(),
                &mut ping_written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ping_written, 17);
    assert_eq!(ack_server(server, ping_written), 0);
    let (result, out) = poll_client_caps(client, &ping[..ping_written], 16, 64, 16_384);
    assert_eq!(result, VylResult::NeedMore);
    assert_eq!(out.consumed, ping_written);
    let request_ping = ping_frame(&opaque);
    let (result, out) = poll_server(server, &request_ping);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.consumed, request_ping.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::PingAck);
    assert_eq!(out.actions[0].ping, opaque);
    let mut goaway = [0; 32];
    let mut goaway_written = 0;
    assert_eq!(
        unsafe {
            vyl_h2_server_goaway_drain(
                server,
                0,
                goaway.as_mut_ptr(),
                goaway.len(),
                &mut goaway_written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(goaway_written, 17);
    assert_eq!(ack_server(server, goaway_written), 0);
    let (result, out) = poll_client_caps(client, &goaway[..goaway_written], 16, 64, 16_384);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.consumed, goaway_written);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::GoAway);
    assert_eq!(out.actions[0].stream_id, 0);
    destroy_client(client);
    destroy_server(server);
}

#[test]
fn retries_small_poll_buffers_with_required_capacity() {
    let client = new_client();
    let server = new_server();
    handshake(client, server);
    let (request, _) = client_request_bytes(client);
    let (result, out) = poll_server_caps(server, &request, 0, 0, 0);
    assert_eq!(result, VylResult::BufferTooSmall);
    assert_eq!(out.consumed, request.len());
    let (result, out) = poll_server(server, &request);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.consumed, 0);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].field_count, 4);
    let response_fields = [encode_field(b":status", b"200")];
    let mut response = [0; 1_024];
    let mut response_written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_server_send_response(
                server,
                1,
                response_fields.as_ptr(),
                response_fields.len(),
                1,
                response.as_mut_ptr(),
                response.len(),
                &mut response_written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_server(server, response_written), 0);
    let (result, out) = poll_client_caps(client, &response[..response_written], 0, 0, 0);
    assert_eq!(result, VylResult::BufferTooSmall);
    assert_eq!(out.consumed, response_written);
    let (result, out) = poll_client_caps(client, &response[..response_written], 16, 64, 16_384);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.consumed, 0);
    assert_eq!(out.actions.len(), 1);
    destroy_client(client);
    destroy_server(server);
}

#[test]
fn rejects_invalid_server_arguments() {
    assert_eq!(
        unsafe { vyl_h2_server_new(std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_h2_server_destroy(std::ptr::null_mut()) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_h2_server_cancel(std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    let server = new_server();
    let mut validated = VylH2ValidatedRequest::default();
    assert_eq!(
        unsafe { vyl_h2_server_accept_request(server, 1, 0, 1, &mut validated) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_h2_server_accept_request(server, 1, usize::MAX, 1, &mut validated) },
        VylResult::InvalidArgument
    );
    let fields = [encode_field(b":status", b"200")];
    let mut output = [0; 64];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_server_send_response(
                server,
                1,
                fields.as_ptr(),
                fields.len(),
                0,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    let bad_representation = [VylH2FieldToEncode {
        representation: 9,
        ..encode_field(b":status", b"200")
    }];
    assert_eq!(
        unsafe {
            vyl_h2_server_send_response(
                server,
                1,
                bad_representation.as_ptr(),
                bad_representation.len(),
                0,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_h2_server_refuse(
                server,
                99,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_h2_server_send_reset(
                server,
                1,
                5,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe {
            vyl_h2_server_goaway_drain(
                server,
                0,
                output.as_mut_ptr(),
                0,
                &mut written,
                &mut required,
            )
        },
        VylResult::BufferTooSmall
    );
    assert_eq!(required, 17);
    assert_eq!(
        unsafe {
            vyl_h2_server_send_window_update(
                server,
                0,
                0,
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::InvalidArgument
    );
    let mut pending = 0;
    assert_eq!(
        unsafe { vyl_h2_server_acknowledge(server, 0, &mut pending) },
        VylResult::BadState
    );
    assert_eq!(unsafe { vyl_h2_server_cancel(server) }, VylResult::Ok);
    assert_eq!(
        unsafe {
            vyl_h2_server_send_settings(
                server,
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
    destroy_server(server);
}
