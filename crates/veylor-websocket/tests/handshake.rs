use veylor_http1::ResponseRequestMethod;
use veylor_http1::{Header, Limits, Parser, ResponseParser};
use veylor_websocket::{
    HandshakeError, accept_key, client_accept, encode_client_handshake, encode_server_handshake,
    server_accept,
};

const REQUEST: &[u8] = b"GET /chat HTTP/1.1\r\nHost: server.example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: chat, superchat\r\nSec-WebSocket-Version: 13\r\n\r\n";
const RESPONSE: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
const ACCEPT: &[u8; 28] = b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

fn request_head(input: &[u8], headers: &mut [Header]) -> veylor_http1::RequestHead {
    let mut parser = Parser::new();
    parser
        .parse(input, headers, Limits::default())
        .unwrap()
        .unwrap()
}

fn response_head(input: &[u8], headers: &mut [Header]) -> veylor_http1::ResponseHead {
    let mut parser = ResponseParser::new(ResponseRequestMethod::Other, b"websocket");
    parser
        .parse(input, headers, Limits::default())
        .unwrap()
        .unwrap()
}

#[test]
fn rfc_6455_accept_vector() {
    let mut key = [0u8; 28];
    accept_key(b"dGhlIHNhbXBsZSBub25jZQ==", &mut key).unwrap();
    assert_eq!(&key, ACCEPT);
}

#[test]
fn server_handshake_accepts_rfc_request_and_encodes_101() {
    let mut headers = [Header::default(); 8];
    let head = request_head(REQUEST, &mut headers);
    let handshake = server_accept(head, REQUEST, &headers[..head.header_count]).unwrap();
    assert_eq!(handshake.accept(), ACCEPT);
    assert_eq!(handshake.protocol(), Some(&b"chat, superchat"[..]));

    let mut out = [0u8; 256];
    let written = encode_server_handshake(&handshake, &mut out).unwrap();
    assert_eq!(
        &out[..written],
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\nSec-WebSocket-Protocol: chat, superchat\r\n\r\n"
    );
}

#[test]
fn client_handshake_accepts_rfc_response() {
    let mut headers = [Header::default(); 8];
    let head = response_head(RESPONSE, &mut headers);
    let handshake = client_accept(head, RESPONSE, &headers[..head.header_count], ACCEPT).unwrap();
    assert_eq!(handshake.protocol(), None);
}

#[test]
fn server_rejects_missing_or_wrong_handshake_fields() {
    let cases: &[(&[u8], HandshakeError)] = &[
        (
            b"POST /chat HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
            HandshakeError::InvalidMethod,
        ),
        (
            b"GET /chat HTTP/1.0\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
            HandshakeError::InvalidVersion,
        ),
        (
            b"GET /chat HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\r\n",
            HandshakeError::MissingKey,
        ),
        (
            b"GET /chat HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 12\r\n\r\n",
            HandshakeError::UnsupportedVersion,
        ),
        (
            b"GET /chat HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
            HandshakeError::MissingConnectionUpgrade,
        ),
    ];

    for (request, expected) in cases {
        let mut headers = [Header::default(); 8];
        let head = request_head(request, &mut headers);
        assert_eq!(
            server_accept(head, request, &headers[..head.header_count]).unwrap_err(),
            *expected
        );
    }
}

#[test]
fn client_rejects_wrong_accept_and_extensions() {
    let mut headers = [Header::default(); 8];
    let bad = RESPONSE
        .iter()
        .copied()
        .map(|byte| if byte == b'O' { b'P' } else { byte })
        .collect::<Vec<_>>();
    let head = response_head(&bad, &mut headers);
    assert_eq!(
        client_accept(head, &bad, &headers[..head.header_count], ACCEPT).unwrap_err(),
        HandshakeError::AcceptMismatch
    );

    let extended = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n";
    let mut headers = [Header::default(); 8];
    let head = response_head(extended, &mut headers);
    assert_eq!(
        client_accept(head, extended, &headers[..head.header_count], ACCEPT).unwrap_err(),
        HandshakeError::UnexpectedExtensions
    );
}

#[test]
fn client_encoder_emits_valid_request() {
    let mut out = [0u8; 512];
    let written = encode_client_handshake(
        b"server.example.com",
        b"/chat",
        b"dGhlIHNhbXBsZSBub25jZQ==",
        &[b"chat", b"superchat"],
        &mut out,
    )
    .unwrap();
    assert_eq!(
        &out[..written],
        b"GET /chat HTTP/1.1\r\nHost: server.example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: chat, superchat\r\n\r\n"
    );
}

#[test]
fn small_buffers_report_required_capacity() {
    let mut headers = [Header::default(); 8];
    let head = request_head(REQUEST, &mut headers);
    let handshake = server_accept(head, REQUEST, &headers[..head.header_count]).unwrap();
    let mut out = [0u8; 8];
    assert!(matches!(
        encode_server_handshake(&handshake, &mut out),
        Err(HandshakeError::BufferTooSmall { required }) if required > out.len()
    ));
}
