use super::super::*;
use super::endpoint_io::*;
use super::scripted_peer::*;
use crate::abi::{VylH2ActionKind, VylResult};

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

#[test]
fn scripted_push_promise_fails_connection() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let mut block = Vec::new();
    peer_literal(&mut block, b":method", b"GET");
    peer_literal(&mut block, b":scheme", b"https");
    peer_literal(&mut block, b":path", b"/pushed");
    peer_literal(&mut block, b":authority", b"example.com");
    let mut frame = peer_header(block.len() + 4, 0x05, 0x04, 1).to_vec();
    frame.extend_from_slice(&2u32.to_be_bytes());
    frame.extend_from_slice(&block);
    let out = poll_peer(client, &frame);
    assert_eq!(out.result, VylResult::ProtocolError);
    assert_eq!(out.consumed, frame.len());
    assert_eq!(out.connection_error, 1);
    let out = poll_peer(client, &[]);
    assert_eq!(out.result, VylResult::ProtocolError);
    assert_eq!(out.connection_error, 1);
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

fn peer_fragmented_headers(stream_id: u32, block: &[u8], first_len: usize) -> Vec<u8> {
    let first_len = first_len.min(block.len());
    let mut wire = peer_header(first_len, 0x01, 0x00, stream_id).to_vec();
    wire.extend_from_slice(&block[..first_len]);
    let rest = &block[first_len..];
    if !rest.is_empty() {
        wire.extend_from_slice(&peer_header(rest.len(), 0x09, 0x04, stream_id));
        wire.extend_from_slice(rest);
    }
    wire
}

fn peer_padded_block(status: &[u8], pad_value_len: usize) -> Vec<u8> {
    let mut block = Vec::new();
    peer_literal(&mut block, b":status", status);
    let pad_value = vec![b'p'; pad_value_len];
    for _ in 0..3 {
        peer_literal(&mut block, b"x-pad", &pad_value);
    }
    block
}

#[test]
fn scripted_continuation_within_limit_succeeds() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let block = peer_padded_block(b"200", 500);
    assert!(block.len() < 4_096);
    let wire = peer_fragmented_headers(1, &block, 1_000);
    let out = poll_peer(client, &wire);
    assert_eq!(out.result, VylResult::Ok);
    assert_eq!(out.consumed, wire.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::Headers);
    let class = scripted_classify(client, 1, &out.actions[0]);
    assert_eq!((class.kind, class.status, class.phase), (1, 200, 1));
    assert_eq!(out.fields.len(), 4);
    destroy_client(client);
}

#[test]
fn scripted_continuation_over_limit_fails_connection_before_reuse() {
    let client = new_client();
    scripted_handshake(client);
    let stream = scripted_get(client);
    assert_eq!(stream, 1);
    let block = peer_padded_block(b"200", 1_500);
    assert!(block.len() > 4_096);
    let wire = peer_fragmented_headers(1, &block, 1_000);
    let out = poll_peer(client, &wire);
    assert_eq!(out.result, VylResult::ProtocolError);
    assert_eq!(out.consumed, wire.len());
    assert_eq!(out.actions.len(), 1);
    assert_eq!(out.actions[0].kind, VylH2ActionKind::ConnectionError);
    assert_eq!(out.actions[0].error_code, 9);
    assert_eq!(out.connection_error, 9);
    let closed = poll_peer(client, &[]);
    assert_eq!(closed.result, VylResult::ProtocolError);
    assert_eq!(closed.connection_error, 9);
    destroy_client(client);
}
