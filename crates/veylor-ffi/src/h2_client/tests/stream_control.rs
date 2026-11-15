use super::super::*;
use super::endpoint_io::*;
use super::scripted_peer::*;
use crate::abi::{VylH2ActionKind, VylResult};

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
    assert_eq!(out.actions.len(), 2);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::SendReset);
    assert_eq!(out.actions[0].stream_id, 1);
    assert_eq!(out.actions[0].error_code, 5);
    assert_eq!(out.actions[1].kind, VylH2ActionKind::ConnectionCredit);
    assert_eq!(out.actions[1].data_length, 1);
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
fn scripted_ping_ack_round_trip() {
    let client = new_client();
    scripted_handshake(client);
    let opaque = *b"12345678";
    let mut frame = peer_header(8, 0x06, 0x00, 0).to_vec();
    frame.extend_from_slice(&opaque);
    let out = poll_peer(client, &frame);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.consumed, frame.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::PingAck);
    assert_eq!(out.actions[0].ping, opaque);
    let echoed = out.actions[0].ping;
    let mut output = [0; 32];
    let mut written = 0;
    let mut required = 0;
    assert_eq!(
        unsafe {
            vyl_h2_client_send_ping_ack(
                client,
                echoed.as_ptr(),
                output.as_mut_ptr(),
                output.len(),
                &mut written,
                &mut required,
            )
        },
        VylResult::Ok
    );
    assert_eq!(written, 17);
    assert_eq!(&output[..9], &peer_header(8, 0x06, 0x01, 0));
    assert_eq!(&output[9..written], &opaque);
    assert_eq!(ack_client(client, written), 0);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let headers = peer_headers(1, true, &[(b":status".as_slice(), b"200".as_slice())]);
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!((class.kind, class.status, class.phase), (1, 200, 2));
    destroy_client(client);
}
