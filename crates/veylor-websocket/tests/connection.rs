use veylor_websocket::{
    Error, Event, Limits, OpCode, Role, WebSocket, encode_close_payload, encode_frame,
};

const MASK: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];

fn frame(fin: bool, opcode: OpCode, mask: Option<[u8; 4]>, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; payload.len() + 14];
    let written = encode_frame(fin, opcode, mask, payload, &mut out).unwrap();
    out.truncate(written);
    out
}

fn feed<'a>(ws: &mut WebSocket, bytes: &'a mut [u8]) -> Result<Event<'a>, Error> {
    ws.feed(bytes)?.event.ok_or(Error::Closed)
}

#[test]
fn server_accepts_masked_text() {
    let mut ws = WebSocket::server(Limits::default());
    let mut bytes = frame(true, OpCode::Text, Some(MASK), b"Hello");
    let length = bytes.len();
    let feed = ws.feed(&mut bytes).unwrap();
    assert_eq!(feed.consumed, length);
    assert!(!feed.need_more);
    assert!(matches!(
        feed.event.unwrap(),
        Event::Text {
            payload: b"Hello",
            fin: true
        }
    ));
}

#[test]
fn client_accepts_unmasked_text() {
    let mut ws = WebSocket::client(Limits::default());
    let mut bytes = frame(true, OpCode::Text, None, b"Hello");
    assert!(matches!(
        feed(&mut ws, &mut bytes).unwrap(),
        Event::Text {
            payload: b"Hello",
            fin: true
        }
    ));
}

#[test]
fn fragmented_text_validates_split_utf8() {
    let mut ws = WebSocket::server(Limits::default());
    let mut first = frame(false, OpCode::Text, Some(MASK), &[0xc3]);
    assert!(matches!(
        feed(&mut ws, &mut first).unwrap(),
        Event::Text {
            payload: [0xc3],
            fin: false
        }
    ));

    let mut second = frame(true, OpCode::Continuation, Some(MASK), &[0xa9]);
    assert!(matches!(
        feed(&mut ws, &mut second).unwrap(),
        Event::Continuation {
            payload: [0xa9],
            fin: true
        }
    ));
}

#[test]
fn control_frames_may_interleave() {
    let mut ws = WebSocket::server(Limits::default());

    let mut first = frame(false, OpCode::Text, Some(MASK), b"He");
    assert!(matches!(
        feed(&mut ws, &mut first).unwrap(),
        Event::Text {
            payload: b"He",
            fin: false
        }
    ));

    let mut ping = frame(true, OpCode::Ping, Some(MASK), b"p");
    assert!(matches!(
        feed(&mut ws, &mut ping).unwrap(),
        Event::Ping(b"p")
    ));

    let mut last = frame(true, OpCode::Continuation, Some(MASK), b"llo");
    assert!(matches!(
        feed(&mut ws, &mut last).unwrap(),
        Event::Continuation {
            payload: b"llo",
            fin: true
        }
    ));
}

#[test]
fn rejects_invalid_utf8() {
    let mut ws = WebSocket::server(Limits::default());
    let mut bytes = frame(true, OpCode::Text, Some(MASK), &[0xc3]);
    assert_eq!(ws.feed(&mut bytes).unwrap_err(), Error::InvalidUtf8);
}

#[test]
fn rejects_wrong_masking_and_fragmentation() {
    let mut server = WebSocket::server(Limits::default());
    let mut unmasked = frame(true, OpCode::Text, None, b"x");
    assert_eq!(
        server.feed(&mut unmasked).unwrap_err(),
        Error::Protocol("client frame is not masked")
    );

    let mut client = WebSocket::client(Limits::default());
    let mut masked = frame(true, OpCode::Text, Some(MASK), b"x");
    assert_eq!(
        client.feed(&mut masked).unwrap_err(),
        Error::Protocol("server frame is masked")
    );

    let mut continuation = frame(true, OpCode::Continuation, Some(MASK), b"x");
    assert_eq!(
        server.feed(&mut continuation).unwrap_err(),
        Error::Protocol("unexpected continuation frame")
    );

    let mut first = frame(false, OpCode::Text, Some(MASK), b"x");
    feed(&mut server, &mut first).unwrap();
    let mut second = frame(true, OpCode::Text, Some(MASK), b"y");
    assert_eq!(
        server.feed(&mut second).unwrap_err(),
        Error::Protocol("expected a continuation frame")
    );
}

#[test]
fn enforces_frame_and_message_limits() {
    let mut ws = WebSocket::server(Limits::new(2, 4));
    let mut large = frame(true, OpCode::Binary, Some(MASK), b"abc");
    assert_eq!(ws.feed(&mut large).unwrap_err(), Error::FrameTooLarge);

    let mut ws = WebSocket::server(Limits::new(8, 4));
    let mut first = frame(false, OpCode::Binary, Some(MASK), b"abc");
    feed(&mut ws, &mut first).unwrap();
    let mut second = frame(true, OpCode::Continuation, Some(MASK), b"de");
    assert_eq!(ws.feed(&mut second).unwrap_err(), Error::MessageTooLarge);
}

#[test]
fn close_frame_validates_payload_and_closes() {
    let mut ws = WebSocket::server(Limits::default());
    let mut payload = [0u8; 125];
    let length = encode_close_payload(Some(1000), b"bye", &mut payload).unwrap();
    let mut bytes = frame(true, OpCode::Close, Some(MASK), &payload[..length]);

    let feed = ws.feed(&mut bytes).unwrap();
    assert!(matches!(
        feed.event.unwrap(),
        Event::Close(close) if close.code == Some(1000) && close.reason == b"bye"
    ));
    assert!(ws.is_closed());
    assert_eq!(ws.feed(&mut []).unwrap_err(), Error::Closed);
}

#[test]
fn incomplete_frame_returns_need_more_without_consuming() {
    let mut ws = WebSocket::server(Limits::default());
    let mut full = frame(true, OpCode::Text, Some(MASK), b"Hello");
    let length = full.len();
    let header_len = 6;

    let feed = ws.feed(&mut full[..header_len]).unwrap();
    assert_eq!(feed.consumed, 0);
    assert!(feed.need_more);
    assert!(feed.event.is_none());

    let feed = ws.feed(&mut full).unwrap();
    assert_eq!(feed.consumed, length);
    assert!(matches!(
        feed.event.unwrap(),
        Event::Text {
            payload: b"Hello",
            fin: true
        }
    ));
}

#[test]
fn encode_and_feed_round_trip_both_roles() {
    let mut client = WebSocket::client(Limits::default());
    let mut server = WebSocket::server(Limits::default());

    let mut client_bytes = [0u8; 32];
    let written = client
        .encode(true, OpCode::Text, Some(MASK), b"ping", &mut client_bytes)
        .unwrap();
    let feed = server.feed(&mut client_bytes[..written]).unwrap();
    assert!(matches!(
        feed.event.unwrap(),
        Event::Text {
            payload: b"ping",
            fin: true
        }
    ));

    let mut server_bytes = [0u8; 32];
    let written = server
        .encode(true, OpCode::Text, None, b"pong", &mut server_bytes)
        .unwrap();
    let feed = client.feed(&mut server_bytes[..written]).unwrap();
    assert!(matches!(
        feed.event.unwrap(),
        Event::Text {
            payload: b"pong",
            fin: true
        }
    ));
}

#[test]
fn role_is_exposed() {
    assert_eq!(WebSocket::server(Limits::default()).role(), Role::Server);
    assert_eq!(WebSocket::client(Limits::default()).role(), Role::Client);
}
