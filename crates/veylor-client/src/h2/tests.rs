use veylor_http2::{
    ClientResponsePhase, H2FrameAction, HpackFieldToEncode, HpackHeader, HpackRepresentation,
    ResponseSectionKind, SETTINGS_ENABLE_PUSH, SETTINGS_MAX_HEADER_LIST_SIZE, SettingsParameter,
};

use super::{
    BatchStatus, FeedBatch, FeedScratch, H2ClientConnection, H2ClientError, H2ClientTables,
    HEADER_BLOCK, HPACK_ENTRIES, HPACK_SCRATCH, MAX_ACTIONS, MAX_FIELDS, SETTINGS_ACKS,
    STREAM_SLOTS,
};

fn field<'f>(name: &'f [u8], value: &'f [u8]) -> HpackFieldToEncode<'f> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

fn get_fields<'f>() -> [HpackFieldToEncode<'f>; 4] {
    [
        field(b":method", b"GET"),
        field(b":scheme", b"https"),
        field(b":path", b"/"),
        field(b":authority", b"example.com"),
    ]
}

macro_rules! fixture {
    ($tables:ident, $conn:ident) => {
        let mut slots = [veylor_http2::StreamSlot::EMPTY; STREAM_SLOTS];
        let mut header_block = [0; HEADER_BLOCK];
        let mut decoder_entries = [veylor_http2::HpackEntrySlot::EMPTY; HPACK_ENTRIES];
        let mut decoder_scratch = [0; HPACK_SCRATCH];
        let mut encoder_entries = [veylor_http2::HpackEntrySlot::EMPTY; HPACK_ENTRIES];
        let mut encoder_scratch = [0; HPACK_SCRATCH];
        let mut ack_limits = [0; SETTINGS_ACKS];
        let mut $tables = H2ClientTables::new(
            &mut slots,
            &mut header_block,
            &mut decoder_entries,
            &mut decoder_scratch,
            &mut encoder_entries,
            &mut encoder_scratch,
            &mut ack_limits,
        );
        let mut $conn = $tables.connect().expect("connect");
    };
}

fn poll<'i, 's>(
    conn: &mut H2ClientConnection<'_, '_>,
    input: &'i [u8],
    scratch: &'s mut FeedScratch<'i>,
) -> FeedBatch<'s, 'i> {
    conn.poll_feed(input, scratch).expect("poll")
}

fn peer_header(payload_length: usize, frame_type: u8, flags: u8, stream_id: u32) -> [u8; 9] {
    [
        ((payload_length >> 16) & 0xFF) as u8,
        ((payload_length >> 8) & 0xFF) as u8,
        (payload_length & 0xFF) as u8,
        frame_type,
        flags,
        ((stream_id >> 24) & 0x7F) as u8,
        ((stream_id >> 16) & 0xFF) as u8,
        ((stream_id >> 8) & 0xFF) as u8,
        (stream_id & 0xFF) as u8,
    ]
}

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
    let flags = if end_stream { 0x01 } else { 0x00 };
    let mut frame = peer_header(payload.len(), 0x00, flags, stream_id).to_vec();
    frame.extend_from_slice(payload);
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

fn peer_window_update(stream_id: u32, increment: u32) -> Vec<u8> {
    let mut frame = peer_header(4, 0x08, 0, stream_id).to_vec();
    frame.extend_from_slice(&(increment & 0x7FFF_FFFF).to_be_bytes());
    frame
}

fn peer_goaway(last_stream_id: u32, code: u32) -> Vec<u8> {
    let mut frame = peer_header(8, 0x07, 0, 0).to_vec();
    frame.extend_from_slice(&(last_stream_id & 0x7FFF_FFFF).to_be_bytes());
    frame.extend_from_slice(&code.to_be_bytes());
    frame
}

fn handshake(conn: &mut H2ClientConnection<'_, '_>) {
    let mut output = [0; 128];
    let written = conn
        .send_settings(&[], &mut output)
        .expect("settings")
        .written;
    assert_eq!(
        &output[..written],
        &[
            0, 0, 12, 0x04, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 6, 0, 0, 8, 64,
        ]
    );
    assert_eq!(conn.acknowledge(written).expect("ack"), 0);
    let mut scratch = FeedScratch::new();
    let empty = peer_header(0, 0x04, 0, 0);
    let batch = poll(conn, &empty, &mut scratch);
    assert_eq!(batch.status, BatchStatus::Ready);
    assert_eq!(batch.action_count, 1);
    assert!(matches!(
        batch.actions[0],
        Some(H2FrameAction::SettingsAckRequired)
    ));
    conn.finish_batch();
    let written = conn
        .send_settings_ack(&mut output)
        .expect("ack frame")
        .written;
    assert_eq!(conn.acknowledge(written).expect("ack"), 0);
    let ack = peer_settings_ack();
    let batch = poll(conn, &ack, &mut scratch);
    assert_eq!(batch.status, BatchStatus::NeedMore);
    assert_eq!(batch.consumed, ack.len());
}

#[test]
fn settings_keep_push_disabled_and_header_storage_bounded() {
    fixture!(tables, conn);
    let _ = tables;
    let mut output = [0; 128];
    let push_enabled = [SettingsParameter {
        id: SETTINGS_ENABLE_PUSH,
        value: 1,
    }];
    assert_eq!(
        conn.send_settings(&push_enabled, &mut output),
        Err(H2ClientError::InvalidArgument)
    );

    let excessive_header_limit = [SettingsParameter {
        id: SETTINGS_MAX_HEADER_LIST_SIZE,
        value: super::MAX_HEADER_LIST_SIZE + 1,
    }];
    assert_eq!(
        conn.send_settings(&excessive_header_limit, &mut output),
        Err(H2ClientError::InvalidArgument)
    );

    let bounded = [
        SettingsParameter {
            id: SETTINGS_ENABLE_PUSH,
            value: 0,
        },
        SettingsParameter {
            id: SETTINGS_MAX_HEADER_LIST_SIZE,
            value: 1_024,
        },
    ];
    let written = conn
        .send_settings(&bounded, &mut output)
        .expect("bounded settings")
        .written;
    assert_eq!(written, 21);
    assert_eq!(conn.acknowledge(written), Ok(0));
    assert_eq!(
        conn.driver.driver().streams().local_settings().enable_push,
        0
    );
    assert_eq!(
        conn.driver
            .driver()
            .streams()
            .local_settings()
            .max_header_list_size,
        1_024
    );

    let full = [SettingsParameter {
        id: veylor_http2::SETTINGS_HEADER_TABLE_SIZE,
        value: 4_096,
    }; super::MAX_SETTINGS];
    let written = conn
        .send_settings(&full, &mut output)
        .expect("full caller settings plus defaults")
        .written;
    assert_eq!(written, 69);
    assert_eq!(conn.acknowledge(written), Ok(0));
}

fn start_get(conn: &mut H2ClientConnection<'_, '_>) -> u32 {
    let fields = get_fields();
    let mut output = [0; 1_024];
    let started = conn
        .start_request(&fields, &mut output, true)
        .expect("start");
    assert_eq!(conn.acknowledge(started.written).expect("ack"), 0);
    started.stream_id
}

#[test]
fn settings_must_precede_new_streams() {
    fixture!(tables, conn);
    let _ = tables;
    let fields = get_fields();
    let mut output = [0; 1_024];
    assert_eq!(
        conn.start_request(&fields, &mut output, true)
            .map(|_| ())
            .unwrap_err(),
        H2ClientError::BadState
    );
    handshake(&mut conn);
    assert_eq!(start_get(&mut conn), 1);
    assert_eq!(start_get(&mut conn), 3);
}

#[test]
fn pending_write_blocks_new_frames() {
    fixture!(tables, conn);
    let _ = tables;
    handshake(&mut conn);
    let fields = get_fields();
    let mut output = [0; 1_024];
    let started = conn
        .start_request(&fields, &mut output, true)
        .expect("start");
    assert_eq!(
        conn.start_request(&fields, &mut output, true)
            .map(|_| ())
            .unwrap_err(),
        H2ClientError::WouldBlock
    );
    assert_eq!(
        conn.acknowledge(started.written + 1)
            .map(|_| ())
            .unwrap_err(),
        H2ClientError::WriteOverrun
    );
    assert_eq!(conn.acknowledge(started.written).expect("ack"), 0);
    assert_eq!(
        conn.acknowledge(0).map(|_| ()).unwrap_err(),
        H2ClientError::BadState
    );
}

#[test]
fn get_200_data_trailers() {
    fixture!(tables, conn);
    let _ = tables;
    handshake(&mut conn);
    assert_eq!(start_get(&mut conn), 1);
    let mut scratch = FeedScratch::new();
    let headers = peer_headers(1, false, &[(b":status".as_slice(), b"200".as_slice())]);
    let batch = poll(&mut conn, &headers, &mut scratch);
    assert_eq!(batch.status, BatchStatus::Ready);
    assert_eq!(batch.action_count, 1);
    let (stream_id, end_stream) = match batch.actions[0] {
        Some(H2FrameAction::Headers {
            stream_id,
            end_stream,
            ..
        }) => (stream_id, end_stream),
        ref other => panic!("headers action, got {other:?}"),
    };
    assert_eq!(stream_id, 1);
    let class = conn
        .classify_headers(1, &batch.headers[..batch.header_count], end_stream)
        .expect("classify");
    assert_eq!(class.kind, ResponseSectionKind::Final);
    assert_eq!(class.status, 200);
    assert_eq!(class.phase, ClientResponsePhase::Content);
    conn.finish_batch();
    let body = peer_data(1, false, b"hello");
    let batch = poll(&mut conn, &body, &mut scratch);
    let (stream_id, payload_len, end_stream) = match batch.actions[0] {
        Some(H2FrameAction::Data {
            stream_id,
            data,
            end_stream,
        }) => (stream_id, data.len(), end_stream),
        ref other => panic!("data action, got {other:?}"),
    };
    assert_eq!((stream_id, payload_len), (1, 5));
    let payload: Vec<u8> = match batch.actions[0] {
        Some(H2FrameAction::Data { data, .. }) => data.to_vec(),
        _ => unreachable!(),
    };
    assert_eq!(payload, b"hello");
    let phase = conn
        .account_data(1, payload_len, end_stream)
        .expect("account")
        .phase;
    assert_eq!(phase, ClientResponsePhase::Content);
    conn.finish_batch();
    let trailers = peer_headers(1, true, &[(b"x-sum".as_slice(), b"abc".as_slice())]);
    let batch = poll(&mut conn, &trailers, &mut scratch);
    let end_stream = match batch.actions[0] {
        Some(H2FrameAction::Headers { end_stream, .. }) => end_stream,
        ref other => panic!("trailers action, got {other:?}"),
    };
    let class = conn
        .classify_headers(1, &batch.headers[..batch.header_count], end_stream)
        .expect("classify trailers");
    assert_eq!(class.kind, ResponseSectionKind::Trailers);
    assert_eq!(class.phase, ClientResponsePhase::Complete);
    conn.finish_batch();
}

#[test]
fn informational_then_final() {
    fixture!(tables, conn);
    let _ = tables;
    handshake(&mut conn);
    assert_eq!(start_get(&mut conn), 1);
    let mut scratch = FeedScratch::new();
    let interim = peer_headers(1, false, &[(b":status".as_slice(), b"103".as_slice())]);
    let batch = poll(&mut conn, &interim, &mut scratch);
    let end_stream = match batch.actions[0] {
        Some(H2FrameAction::Headers { end_stream, .. }) => end_stream,
        ref other => panic!("headers action, got {other:?}"),
    };
    let class = conn
        .classify_headers(1, &batch.headers[..batch.header_count], end_stream)
        .expect("classify");
    assert_eq!(class.kind, ResponseSectionKind::Informational);
    assert_eq!(class.status, 103);
    assert_eq!(class.phase, ClientResponsePhase::AwaitingFinalHeaders);
    conn.finish_batch();
    let end = peer_headers(1, true, &[(b":status".as_slice(), b"200".as_slice())]);
    let batch = poll(&mut conn, &end, &mut scratch);
    let end_stream = match batch.actions[0] {
        Some(H2FrameAction::Headers { end_stream, .. }) => end_stream,
        ref other => panic!("headers action, got {other:?}"),
    };
    let class = conn
        .classify_headers(1, &batch.headers[..batch.header_count], end_stream)
        .expect("classify");
    assert_eq!(class.kind, ResponseSectionKind::Final);
    assert_eq!(class.phase, ClientResponsePhase::Complete);
    conn.finish_batch();
}

#[test]
fn content_length_dispute_errors() {
    fixture!(tables, conn);
    let _ = tables;
    handshake(&mut conn);
    assert_eq!(start_get(&mut conn), 1);
    let mut scratch = FeedScratch::new();
    let headers = peer_headers(
        1,
        false,
        &[
            (b":status".as_slice(), b"200".as_slice()),
            (b"content-length".as_slice(), b"11".as_slice()),
        ],
    );
    let batch = poll(&mut conn, &headers, &mut scratch);
    let end_stream = match batch.actions[0] {
        Some(H2FrameAction::Headers { end_stream, .. }) => end_stream,
        ref other => panic!("headers action, got {other:?}"),
    };
    let class = conn
        .classify_headers(1, &batch.headers[..batch.header_count], end_stream)
        .expect("classify");
    assert_eq!(class.content_length, Some(11));
    conn.finish_batch();
    let overflow = peer_data(1, true, b"01234567890123456789");
    let batch = poll(&mut conn, &overflow, &mut scratch);
    let (payload_len, end_stream) = match batch.actions[0] {
        Some(H2FrameAction::Data {
            data, end_stream, ..
        }) => (data.len(), end_stream),
        ref other => panic!("data action, got {other:?}"),
    };
    assert_eq!(payload_len, 20);
    assert_eq!(
        conn.account_data(1, payload_len, end_stream)
            .map(|_| ())
            .unwrap_err(),
        H2ClientError::ProtocolError
    );
    conn.finish_batch();
}

#[test]
fn send_honors_peer_window() {
    fixture!(tables, conn);
    let _ = tables;
    handshake(&mut conn);
    let mut scratch = FeedScratch::new();
    let small = peer_settings(&[(4, 16)]);
    let batch = poll(&mut conn, &small, &mut scratch);
    assert_eq!(batch.status, BatchStatus::Ready);
    assert_eq!(batch.action_count, 1);
    assert!(matches!(
        batch.actions[0],
        Some(H2FrameAction::SettingsAckRequired)
    ));
    conn.finish_batch();
    let mut output = [0; 1_024];
    let written = conn
        .send_settings_ack(&mut output)
        .expect("settings ack")
        .written;
    assert_eq!(conn.acknowledge(written).expect("ack"), 0);
    let fields = [
        field(b":method", b"POST"),
        field(b":scheme", b"https"),
        field(b":path", b"/upload"),
        field(b":authority", b"example.com"),
    ];
    let started = conn
        .start_request(&fields, &mut output, false)
        .expect("start");
    assert_eq!(started.stream_id, 1);
    assert_eq!(conn.acknowledge(started.written).expect("ack"), 0);
    let input = [7; 100];
    let sent = conn.send_data(1, &input, false, &mut output).expect("send");
    assert_eq!(
        (sent.consumed, sent.written, sent.end_stream),
        (16, 25, false)
    );
    assert_eq!(conn.acknowledge(sent.written).expect("ack"), 0);
    let updates = [peer_window_update(1, 100), peer_window_update(0, 100)];
    for update in &updates {
        let batch = poll(&mut conn, update, &mut scratch);
        assert_eq!(batch.status, BatchStatus::NeedMore);
    }
    let sent = conn
        .send_data(1, &input[16..], true, &mut output)
        .expect("send");
    assert_eq!(
        (sent.consumed, sent.written, sent.end_stream),
        (84, 93, true)
    );
    assert_eq!(conn.acknowledge(sent.written).expect("ack"), 0);
    conn.cancel();
    assert_eq!(
        conn.send_data(1, &input, false, &mut output)
            .map(|_| ())
            .unwrap_err(),
        H2ClientError::Cancelled
    );
}

#[test]
fn even_stream_id_fails_connection() {
    fixture!(tables, conn);
    let _ = tables;
    handshake(&mut conn);
    assert_eq!(start_get(&mut conn), 1);
    let mut scratch = FeedScratch::new();
    let push = peer_headers(2, true, &[(b":status".as_slice(), b"200".as_slice())]);
    let batch = poll(&mut conn, &push, &mut scratch);
    assert_eq!(batch.status, BatchStatus::Ready);
    assert_eq!(batch.consumed, push.len());
    assert!(batch.connection_failed);
    assert_eq!(
        batch.connection_error,
        Some(veylor_http2::ErrorCode::ProtocolError)
    );
    let fields = get_fields();
    let mut output = [0; 1_024];
    assert_eq!(
        conn.start_request(&fields, &mut output, true)
            .map(|_| ())
            .unwrap_err(),
        H2ClientError::BadState
    );
}

#[test]
fn goaway_drains_new_streams() {
    fixture!(tables, conn);
    let _ = tables;
    handshake(&mut conn);
    assert_eq!(start_get(&mut conn), 1);
    let mut scratch = FeedScratch::new();
    let goaway = peer_goaway(1, 0);
    let batch = poll(&mut conn, &goaway, &mut scratch);
    assert_eq!(batch.status, BatchStatus::Ready);
    assert!(matches!(
        batch.actions[0],
        Some(H2FrameAction::GoAway { .. })
    ));
    conn.finish_batch();
    let fields = get_fields();
    let mut output = [0; 1_024];
    assert_eq!(
        conn.start_request(&fields, &mut output, true)
            .map(|_| ())
            .unwrap_err(),
        H2ClientError::BadState
    );
    let mut goaway_out = [0; 32];
    let written = conn.goaway(&mut goaway_out).expect("goaway").written;
    assert_eq!(conn.acknowledge(written).expect("ack"), 0);
}

#[test]
fn retry_replays_pending_batch() {
    fixture!(tables, conn);
    let _ = tables;
    handshake(&mut conn);
    assert_eq!(start_get(&mut conn), 1);
    let mut scratch = FeedScratch::new();
    let headers = peer_headers(1, true, &[(b":status".as_slice(), b"200".as_slice())]);
    let batch = poll(&mut conn, &headers, &mut scratch);
    assert_eq!(batch.status, BatchStatus::Ready);
    let (action_count, header_count, consumed) =
        (batch.action_count, batch.header_count, batch.consumed);
    let replay = conn.poll_retry(&mut scratch).expect("retry");
    assert_eq!(replay.action_count, action_count);
    assert_eq!(replay.header_count, header_count);
    assert_eq!(replay.consumed, consumed);
    conn.finish_batch();
    assert_eq!(
        conn.poll_retry(&mut scratch).map(|_| ()).unwrap_err(),
        H2ClientError::BadState
    );
}

#[test]
fn scratch_capacity_matches_abi_limits() {
    assert_eq!(MAX_ACTIONS, 16);
    assert_eq!(MAX_FIELDS, 64);
    let scratch = FeedScratch::new();
    let batch_size = std::mem::size_of_val(&scratch);
    assert!(batch_size > 0);
    let headers = [HpackHeader::EMPTY; MAX_FIELDS];
    assert_eq!(headers.len(), 64);
}
