use super::super::*;
use super::endpoint_io::*;
use super::scripted_peer::*;
use crate::abi::{VylH2ActionKind, VylResult};

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
    assert_eq!(out.actions.len(), 2);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::SendReset);
    assert_eq!(out.actions[0].stream_id, 1);
    assert_eq!(out.actions[0].error_code, 5);
    assert_eq!(out.actions[1].kind, VylH2ActionKind::ConnectionCredit);
    assert_eq!(out.actions[1].data_length, 1);
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
fn scripted_padded_data_reports_padding_to_credit() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let headers = peer_headers(1, false, &[(b":status".as_slice(), b"200".as_slice())]);
    let out = poll_peer(client, &headers);
    assert_eq!(out.result, VylResult::Ok);
    let mut padded = vec![200_u8];
    padded.extend_from_slice(b"abc");
    padded.extend_from_slice(&[0_u8; 200]);
    let mut frame = peer_header(padded.len(), 0x00, 0x08, 1).to_vec();
    frame.extend_from_slice(&padded);
    let out = poll_peer(client, &frame);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Data);
    assert_eq!(out.actions[0].data_length, 3);
    assert_eq!(out.actions[0].padding_length, 201);
    destroy_client(client);
}
