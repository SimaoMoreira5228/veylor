use veylor_server_conn::{
    Admission, Field, Header, ResponseFraming, ResponseHeadParams, ServerConnection,
};
use veylor_websocket::{Event, Limits, OpCode, WebSocket, encode_frame, server_accept};

const REQUEST: &[u8] = b"GET /chat HTTP/1.1\r\nHost: server.example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
const MASK: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];

#[test]
fn h1_upgrade_hands_off_to_websocket() {
    let mut conn = ServerConnection::new();
    let mut headers = [Header::default(); 8];
    let head = conn.parse_head(REQUEST, &mut headers).unwrap();
    assert_eq!(
        conn.admit_body(REQUEST, &headers, b"", &[], true).unwrap(),
        Admission::Ready
    );

    let handshake = server_accept(head, REQUEST, &headers[..head.header_count]).unwrap();
    let fields = [
        Field {
            name: b"Upgrade",
            value: b"websocket",
        },
        Field {
            name: b"Sec-WebSocket-Accept",
            value: handshake.accept(),
        },
    ];
    let params = ResponseHeadParams {
        status: 101,
        reason: b"Switching Protocols",
        fields: &fields,
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        request_body_complete: true,
        response_close: false,
    };

    let mut response = [0u8; 256];
    let written = conn
        .encode_response_head(REQUEST, &headers, params, &mut response)
        .unwrap();
    conn.advance_response_write(written.written).unwrap();
    assert!(!conn.can_reuse());
    assert!(response[..written.written].starts_with(b"HTTP/1.1 101 Switching Protocols\r\n"));

    let mut websocket = WebSocket::server(Limits::default());
    let mut frame = [0u8; 32];
    let length = encode_frame(true, OpCode::Text, Some(MASK), b"hello", &mut frame).unwrap();
    let feed = websocket.feed(&mut frame[..length]).unwrap();
    assert!(matches!(
        feed.event.unwrap(),
        Event::Text {
            payload: b"hello",
            fin: true
        }
    ));
}
