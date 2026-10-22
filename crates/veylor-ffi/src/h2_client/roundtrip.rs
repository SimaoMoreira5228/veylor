use super::tests::{
    ack_client, ack_server, answer_settings_ack_client, answer_settings_ack_server,
    client_settings, destroy_client, destroy_server, drain_ack_client, drain_ack_server,
    encode_field, field_text, new_client, new_server, poll_client, poll_server,
    send_client_ack_bytes, send_server_ack_bytes, server_settings,
};
use super::*;
use crate::abi::{
    VYL_H2_END_STREAM, VylBytes, VylH2ActionKind, VylH2ResponseClass, VylH2ValidatedRequest,
    VylResult,
};
use crate::h2_server;

#[test]
fn handshake_request_response_data_reset_goaway_round_trip() {
    let client = new_client();
    let server = new_server();
    let mut output = [0; 1_024];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_connection_preface(
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(written, 24);
    let (result, _) = poll_server(server, &output[..written]);
    assert_eq!(result, VylResult::NeedMore);
    let client_settings = client_settings(client);
    answer_settings_ack_server(server, &client_settings);
    let server_settings = server_settings(server);
    answer_settings_ack_client(client, &server_settings);
    drain_ack_server(server, &send_client_ack_bytes(client));
    drain_ack_client(client, &send_server_ack_bytes(server));
    let fields = [
        encode_field(b":method", b"GET"),
        encode_field(b":scheme", b"https"),
        encode_field(b":path", b"/"),
        encode_field(b":authority", b"example.com"),
    ];
    let mut request = [0; 1_024];
    let mut request_written = 0;
    let mut stream_id = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_start_request(
                client,
                fields.as_ptr(),
                fields.len(),
                1,
                request.as_mut_ptr(),
                request.len(),
                &mut request_written,
                &mut stream_id,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(stream_id, 1);
    assert_eq!(ack_client(client, request_written), 0);
    let (result, out) = poll_server(server, &request[..request_written]);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Headers);
    assert_eq!(out.actions[0].stream_id, 1);
    assert_eq!(out.actions[0].flags, VYL_H2_END_STREAM);
    assert_eq!(out.actions[0].field_count, 4);
    let mut validated = VylH2ValidatedRequest::default();
    assert_eq!(
        unsafe {
            h2_server::vyl_h2_server_accept_request(
                server,
                1,
                out.actions[0].field_start,
                out.actions[0].field_count,
                &mut validated,
            )
        },
        VylResult::Ok
    );
    let (_, method) = field_text(&out, out.fields[validated.method_field]);
    assert_eq!(method, b"GET");
    let (_, path) = field_text(&out, out.fields[validated.path_field]);
    assert_eq!(path, b"/");
    let response_fields = [
        encode_field(b":status", b"200"),
        encode_field(b"content-length", b"11"),
    ];
    let mut response = [0; 1_024];
    let mut response_written = 0;
    assert_eq!(
        unsafe {
            h2_server::vyl_h2_server_send_response(
                server,
                1,
                response_fields.as_ptr(),
                response_fields.len(),
                0,
                response.as_mut_ptr(),
                response.len(),
                &mut response_written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_server(server, response_written), 0);
    let (result, out) = poll_client(client, &response[..response_written]);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Headers);
    let mut class = VylH2ResponseClass::default();
    assert_eq!(
        unsafe {
            vyl_h2_client_classify_headers(
                client,
                1,
                out.actions[0].field_start,
                out.actions[0].field_count,
                0,
                &mut class,
            )
        },
        VylResult::Ok
    );
    assert_eq!(class.kind, 1);
    assert_eq!(class.status, 200);
    assert_eq!(class.phase, 1);
    assert_eq!(class.content_length, 11);
    assert_eq!(class.has_content_length, 1);
    let mut body = Vec::new();
    for (chunk, end) in [(b"hello ".as_slice(), 0), (b"world".as_slice(), 1)] {
        let mut data_frame = [0; 1_024];
        let mut consumed = 0;
        let mut data_written = 0;
        let mut end_applied: u32 = 0;
        assert_eq!(
            unsafe {
                h2_server::vyl_h2_server_send_data(
                    server,
                    1,
                    VylBytes {
                        data: chunk.as_ptr(),
                        length: chunk.len(),
                    },
                    end,
                    data_frame.as_mut_ptr(),
                    data_frame.len(),
                    &mut consumed,
                    &mut data_written,
                    &mut end_applied,
                    &mut required,
                )
            },
            VylResult::Ok
        );
        assert_eq!(consumed, chunk.len());
        assert_eq!(end_applied, end);
        assert_eq!(ack_server(server, data_written), 0);
        let (result, out) = poll_client(client, &data_frame[..data_written]);
        assert_eq!(result, VylResult::Ok);
        assert_eq!(out.actions.len(), 1);
        assert_eq!(out.actions[0].kind, VylH2ActionKind::Data);
        assert_eq!(out.actions[0].stream_id, 1);
        let payload = &out.data
            [out.actions[0].data_offset..out.actions[0].data_offset + out.actions[0].data_length];
        body.extend_from_slice(payload);
        let mut phase: u32 = 0;
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
            VylResult::Ok
        );
        assert_eq!(phase, if end == 0 { 1 } else { 2 });
    }
    assert_eq!(body, b"hello world");
    let second = [
        encode_field(b":method", b"GET"),
        encode_field(b":scheme", b"https"),
        encode_field(b":path", b"/second"),
        encode_field(b":authority", b"example.com"),
    ];
    let mut request2 = [0; 1_024];
    let mut request2_written = 0;
    let mut stream3 = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_start_request(
                client,
                second.as_ptr(),
                second.len(),
                0,
                request2.as_mut_ptr(),
                request2.len(),
                &mut request2_written,
                &mut stream3,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(stream3, 3);
    assert_eq!(ack_client(client, request2_written), 0);
    let (result, _) = poll_server(server, &request2[..request2_written]);
    assert_eq!(result, VylResult::Ok);
    let mut reset = [0; 32];
    let mut reset_written = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_cancel_stream(
                client,
                3,
                reset.as_mut_ptr(),
                reset.len(),
                &mut reset_written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_client(client, reset_written), 0);
    let (result, out) = poll_server(server, &reset[..reset_written]);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::StreamReset);
    assert_eq!(out.actions[0].stream_id, 3);
    assert_eq!(out.actions[0].error_code, 8);
    let mut goaway = [0; 32];
    let mut goaway_written = 0;
    assert_eq!(
        unsafe {
            h2_server::vyl_h2_server_goaway_drain(
                server,
                3,
                goaway.as_mut_ptr(),
                goaway.len(),
                &mut goaway_written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(ack_server(server, goaway_written), 0);
    let (result, out) = poll_client(client, &goaway[..goaway_written]);
    assert_eq!(result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::GoAway);
    assert_eq!(out.actions[0].stream_id, 3);
    assert_eq!(out.actions[0].error_code, 0);
    let mut request_written = 0;
    let mut stream_id = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_start_request(
                client,
                fields.as_ptr(),
                fields.len(),
                1,
                request.as_mut_ptr(),
                request.len(),
                &mut request_written,
                &mut stream_id,
                &mut required,
            )
        },
        VylResult::BadState
    );
    destroy_client(client);
    destroy_server(server);
}
