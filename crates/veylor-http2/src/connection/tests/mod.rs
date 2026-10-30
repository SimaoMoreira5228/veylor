mod control;
mod header_block;
mod settings;

use super::*;
use crate::flow_control::{FlowWindowError, MAX_FLOW_WINDOW_SIZE};
use crate::frame::{
    DATA_FLAG_END_STREAM, DATA_FLAG_PADDED, DEFAULT_MAX_FRAME_SIZE, FRAME_HEADER_LEN, FrameHeader,
    FrameType, HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, SETTINGS_ACK, SETTINGS_ENTRY_LEN,
    SettingsParameter, encode_frame_header,
};
use crate::hpack::HpackHeader;
use crate::hpack::{HpackEntrySlot, HpackTable};
use crate::stream::{FrameDirection, H2Role, StreamActionKind, StreamSlot, StreamTable};

fn frame(
    output: &mut [u8],
    frame_type: FrameType,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> usize {
    let header = FrameHeader {
        payload_length: u32::try_from(payload.len()).unwrap(),
        frame_type,
        flags,
        stream_id,
    };
    encode_frame_header(
        header,
        &mut output[..FRAME_HEADER_LEN],
        DEFAULT_MAX_FRAME_SIZE,
    )
    .unwrap();
    output[FRAME_HEADER_LEN..FRAME_HEADER_LEN + payload.len()].copy_from_slice(payload);
    FRAME_HEADER_LEN + payload.len()
}

fn establish(streams: &mut StreamTable<'_>) {
    if streams.role() == H2Role::Server {
        streams
            .receive_client_preface_fragment(crate::stream::CLIENT_CONNECTION_PREFACE)
            .unwrap();
    }

    let settings = FrameHeader {
        payload_length: 0,
        frame_type: FrameType::Settings,
        flags: 0,
        stream_id: 0,
    };
    let sent = streams.on_frame(settings, FrameDirection::Sent, &[]);
    assert_eq!(sent.kind, StreamActionKind::Accept);
    let received = streams.on_frame(settings, FrameDirection::Received, &[]);
    assert_eq!(received.kind, StreamActionKind::Accept);
    let ack = FrameHeader {
        flags: SETTINGS_ACK,
        ..settings
    };
    let sent_ack = streams.on_frame(ack, FrameDirection::Sent, &[]);
    assert_eq!(sent_ack.kind, StreamActionKind::Accept);
    let received_ack = streams.on_frame(ack, FrameDirection::Received, &[]);
    assert_eq!(received_ack.kind, StreamActionKind::Accept);
}

fn open_stream(driver: &mut H2FrameDriver<'_, '_, '_, '_>, headers: &mut [HpackHeader]) {
    let mut input = [0; FRAME_HEADER_LEN + 3];
    let length = frame(
        &mut input,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS,
        1,
        &[0x82, 0x86, 0x84],
    );
    let mut actions = [None; 1];
    let result = driver.feed(&input[..length], &mut actions, headers);
    assert_eq!(result.status, FeedStatus::NeedMoreInput);
    assert_eq!(result.error, None);
    assert_eq!(result.consumed, length);
    assert_eq!(result.actions, 1);
}

#[test]
fn feed_preserves_partial_frames_and_consumes_read_ahead_as_whole_frames() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut header_block = [0; 64];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 64);
    establish(&mut streams);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 256];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    let mut input = [0; 64];
    let first = frame(
        &mut input,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS,
        1,
        &[0x82, 0x86, 0x84],
    );
    let second = frame(
        &mut input[first..],
        FrameType::Data,
        DATA_FLAG_END_STREAM,
        1,
        b"body",
    );
    let total = first + second;
    let mut actions = [None; 4];
    let mut headers = [HpackHeader::EMPTY; 8];

    let short = driver.feed(&input[..7], &mut actions, &mut headers);
    assert_eq!(short.status, FeedStatus::NeedMoreInput);
    assert_eq!(short.consumed, 0);
    assert_eq!(short.actions, 0);

    let partial_payload = driver.feed(&input[..FRAME_HEADER_LEN + 1], &mut actions, &mut headers);
    assert_eq!(partial_payload.status, FeedStatus::NeedMoreInput);
    assert_eq!(partial_payload.consumed, 0);
    assert_eq!(partial_payload.actions, 0);

    let result = driver.feed(&input[..total], &mut actions, &mut headers);
    assert_eq!(result.status, FeedStatus::NeedMoreInput);
    assert_eq!(result.consumed, total);
    assert_eq!(result.actions, 2);
    assert_eq!(result.headers, 3);
    assert_eq!(
        actions[0],
        Some(H2FrameAction::Headers {
            stream_id: 1,
            headers: HeaderRange { start: 0, count: 3 },
            end_stream: false,
        })
    );
    assert_eq!(
        actions[1],
        Some(H2FrameAction::Data {
            stream_id: 1,
            data: b"body",
            end_stream: true,
        })
    );
    assert_eq!(
        driver.resolve_header(headers[0]),
        Some((&b":method"[..], &b"GET"[..]))
    );
    assert_eq!(
        driver.resolve_header(headers[1]),
        Some((&b":scheme"[..], &b"http"[..]))
    );
    assert_eq!(
        driver.resolve_header(headers[2]),
        Some((&b":path"[..], &b"/"[..]))
    );
}

#[test]
fn fragmented_client_preface_and_first_settings_are_driven_incrementally() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut header_block = [0; 64];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 64);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 256];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    let mut input = [0; 64];
    input[..crate::stream::CLIENT_CONNECTION_PREFACE.len()]
        .copy_from_slice(crate::stream::CLIENT_CONNECTION_PREFACE);
    let settings_end = crate::stream::CLIENT_CONNECTION_PREFACE.len()
        + frame(
            &mut input[crate::stream::CLIENT_CONNECTION_PREFACE.len()..],
            FrameType::Settings,
            0,
            0,
            &[],
        );
    let mut actions = [None; 4];
    let mut headers = [HpackHeader::EMPTY; 8];

    let prefix = driver.feed(&input[..10], &mut actions, &mut headers);
    assert_eq!(prefix.status, FeedStatus::NeedMoreInput);
    assert_eq!(prefix.consumed, 10);
    assert_eq!(prefix.actions, 0);

    let rest = driver.feed(&input[10..settings_end], &mut actions, &mut headers);
    assert_eq!(rest.status, FeedStatus::NeedMoreInput);
    assert_eq!(rest.consumed, settings_end - 10);
    assert_eq!(rest.actions, 1);
    assert_eq!(actions[0], Some(H2FrameAction::SettingsAckRequired));
}

#[test]
fn headers_and_continuations_decode_into_caller_descriptors() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut header_block = [0; 64];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 64);
    establish(&mut streams);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 256];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    let mut input = [0; 64];
    let head = frame(&mut input, FrameType::Headers, 0, 1, &[0x82]);
    let tail = frame(
        &mut input[head..],
        FrameType::Continuation,
        crate::frame::CONTINUATION_FLAG_END_HEADERS,
        1,
        &[0x86, 0x84],
    );
    let mut actions = [None; 2];
    let mut headers = [HpackHeader::EMPTY; 8];
    let result = driver.feed(&input[..head + tail], &mut actions, &mut headers);

    assert_eq!(result.status, FeedStatus::NeedMoreInput);
    assert_eq!(result.consumed, head + tail);
    assert_eq!(result.actions, 1);
    assert_eq!(result.headers, 3);
    assert_eq!(
        actions[0],
        Some(H2FrameAction::Headers {
            stream_id: 1,
            headers: HeaderRange { start: 0, count: 3 },
            end_stream: false,
        })
    );
}

#[test]
fn data_delivery_borrows_payload_and_charges_padding_to_both_windows() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 32);
    establish(&mut streams);
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();
    let mut headers = [HpackHeader::EMPTY; 4];
    open_stream(&mut driver, &mut headers);
    assert_eq!(driver.streams().active_count(), 1);

    driver.connection_receive_window.take(65_530).unwrap();
    driver
        .streams
        .receive_window_mut(1)
        .unwrap()
        .take(65_530)
        .unwrap();
    let mut input = [0; FRAME_HEADER_LEN + 5];
    let length = frame(
        &mut input,
        FrameType::Data,
        DATA_FLAG_PADDED | DATA_FLAG_END_STREAM,
        1,
        &[2, b'a', b'b', 0, 0],
    );
    let mut actions = [None; 1];
    let result = driver.feed(&input[..length], &mut actions, &mut headers);
    assert_eq!(result.status, FeedStatus::NeedMoreInput);
    assert_eq!(result.actions, 1);
    assert_eq!(driver.connection_receive_window().value(), 0);
    assert_eq!(driver.streams().find(1).unwrap().receive_window.value(), 0);
    assert_eq!(
        actions[0],
        Some(H2FrameAction::Data {
            stream_id: 1,
            data: b"ab",
            end_stream: true,
        })
    );
}

#[test]
fn application_credit_restores_both_receive_windows_without_partial_updates() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 32);
    establish(&mut streams);
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();
    let mut headers = [HpackHeader::EMPTY; 4];
    open_stream(&mut driver, &mut headers);

    let mut input = [0; FRAME_HEADER_LEN + 5];
    let length = frame(&mut input, FrameType::Data, 0, 1, b"bytes");
    let mut actions = [None; 1];
    assert_eq!(
        driver
            .feed(&input[..length], &mut actions, &mut headers)
            .actions,
        1
    );
    assert_eq!(driver.connection_receive_window().value(), 65_530);
    assert_eq!(
        driver.streams().find(1).unwrap().receive_window.value(),
        65_530
    );

    assert_eq!(
        driver.add_stream_receive_credit(1, 0),
        Err(FlowWindowError::InvalidIncrement)
    );
    assert_eq!(
        driver.add_connection_receive_credit(0),
        Err(FlowWindowError::InvalidIncrement)
    );
    assert_eq!(driver.connection_receive_window().value(), 65_530);
    assert_eq!(
        driver.streams().find(1).unwrap().receive_window.value(),
        65_530
    );

    driver.add_stream_receive_credit(1, 5).unwrap();
    driver.add_connection_receive_credit(5).unwrap();
    assert_eq!(driver.connection_receive_window().value(), 65_535);
    assert_eq!(
        driver.streams().find(1).unwrap().receive_window.value(),
        65_535
    );
}

#[test]
fn stream_flow_error_still_charges_connection_credit_without_partial_stream_credit() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 32);
    establish(&mut streams);
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();
    let mut headers = [HpackHeader::EMPTY; 4];
    open_stream(&mut driver, &mut headers);
    driver
        .streams
        .receive_window_mut(1)
        .unwrap()
        .take(65_533)
        .unwrap();

    let mut input = [0; FRAME_HEADER_LEN + 5];
    let length = frame(&mut input, FrameType::Data, 0, 1, b"12345");
    let mut actions = [None; 1];
    let result = driver.feed(&input[..length], &mut actions, &mut headers);
    assert_eq!(result.status, FeedStatus::NeedMoreInput);
    assert_eq!(driver.connection_receive_window().value(), 65_530);
    assert_eq!(driver.streams().find(1).unwrap().receive_window.value(), 2);
    assert_eq!(
        actions[0],
        Some(H2FrameAction::SendReset {
            stream_id: 1,
            error_code: ErrorCode::FlowControlError,
        })
    );
    assert_eq!(driver.streams().active_count(), 0);

    let mut next_input = [0; FRAME_HEADER_LEN + 3];
    let next_length = frame(
        &mut next_input,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS,
        3,
        &[0x82, 0x86, 0x84],
    );
    let mut next_actions = [None; 1];
    let next = driver.feed(&next_input[..next_length], &mut next_actions, &mut headers);
    assert_eq!(next.status, FeedStatus::NeedMoreInput);
    assert_eq!(next.actions, 1);
    assert_eq!(driver.streams().find(1), None);
    assert_eq!(
        driver.streams().find(3).unwrap().state,
        crate::stream::StreamState::Open
    );
    assert_eq!(driver.streams().active_count(), 1);
}

#[test]
fn headers_priority_self_dependency_resets_stream() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 32);
    establish(&mut streams);
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();
    let mut input = [0; FRAME_HEADER_LEN + 6];
    let length = frame(
        &mut input,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | crate::frame::HEADERS_FLAG_PRIORITY,
        1,
        &[0, 0, 0, 1, 0, 0x82],
    );
    let mut actions = [None; 1];
    let mut headers = [HpackHeader::EMPTY; 1];
    let result = driver.feed(&input[..length], &mut actions, &mut headers);
    assert_eq!(result.status, FeedStatus::NeedMoreInput);
    assert_eq!(
        actions[0],
        Some(H2FrameAction::SendReset {
            stream_id: 1,
            error_code: ErrorCode::ProtocolError,
        })
    );
}

#[test]
fn window_update_overflow_resets_stream_and_releases_concurrency() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 32);
    establish(&mut streams);
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();
    let mut headers = [HpackHeader::EMPTY; 4];
    open_stream(&mut driver, &mut headers);
    driver
        .streams
        .send_window_mut(1)
        .unwrap()
        .add((MAX_FLOW_WINDOW_SIZE - 65_535) as u32)
        .unwrap();

    let mut input = [0; FRAME_HEADER_LEN + 4];
    let length = frame(&mut input, FrameType::WindowUpdate, 0, 1, &[0, 0, 0, 1]);
    let mut actions = [None; 1];
    let result = driver.feed(&input[..length], &mut actions, &mut headers);
    assert_eq!(result.status, FeedStatus::NeedMoreInput);
    assert_eq!(
        actions[0],
        Some(H2FrameAction::SendReset {
            stream_id: 1,
            error_code: ErrorCode::FlowControlError,
        })
    );
    assert_eq!(driver.streams().active_count(), 0);
}

#[test]
fn header_stream_error_decodes_hpack_then_closes_the_stream() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 32);
    establish(&mut streams);
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();
    let mut headers = [HpackHeader::EMPTY; 8];

    let mut input = [0; FRAME_HEADER_LEN + 3];
    let length = frame(
        &mut input,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        1,
        &[0x82, 0x86, 0x84],
    );
    let mut actions = [None; 1];
    assert_eq!(
        driver
            .feed(&input[..length], &mut actions, &mut headers)
            .actions,
        1
    );
    assert_eq!(driver.streams().active_count(), 1);

    let mut trailers = [0; FRAME_HEADER_LEN + 1];
    let trailer_length = frame(
        &mut trailers,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS,
        1,
        &[0x82],
    );
    let mut trailer_actions = [None; 1];
    let result = driver.feed(
        &trailers[..trailer_length],
        &mut trailer_actions,
        &mut headers,
    );
    assert_eq!(result.status, FeedStatus::NeedMoreInput);
    assert_eq!(result.headers, 1);
    assert_eq!(driver.streams().active_count(), 0);
    assert_eq!(
        trailer_actions[0],
        Some(H2FrameAction::SendReset {
            stream_id: 1,
            error_code: ErrorCode::StreamClosed,
        })
    );
}

#[test]
fn connection_flow_error_does_not_decrement_either_window() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 32);
    establish(&mut streams);
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();
    let mut headers = [HpackHeader::EMPTY; 4];
    open_stream(&mut driver, &mut headers);
    driver.connection_receive_window.take(65_531).unwrap();

    let mut input = [0; FRAME_HEADER_LEN + 5];
    let length = frame(&mut input, FrameType::Data, 0, 1, b"12345");
    let mut actions = [None; 1];
    let result = driver.feed(&input[..length], &mut actions, &mut headers);
    assert_eq!(result.status, FeedStatus::ConnectionFailed);
    assert_eq!(result.error, Some(ErrorCode::FlowControlError));
    assert_eq!(driver.connection_receive_window().value(), 4);
    assert_eq!(
        driver.streams().find(1).unwrap().receive_window.value(),
        65_535
    );
    assert_eq!(
        actions[0],
        Some(H2FrameAction::ConnectionError {
            error_code: ErrorCode::FlowControlError,
        })
    );
}

#[test]
fn action_capacity_stops_before_the_next_complete_frame() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut header_block = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 32);
    establish(&mut streams);
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let mut hpack = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut hpack_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut hpack,
        &mut hpack_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();
    let mut headers = [HpackHeader::EMPTY; 4];
    let mut input = [0; 64];
    let first = frame(&mut input, FrameType::Ping, 0, 0, b"pingpong");
    let second = frame(&mut input[first..], FrameType::Ping, 0, 0, b"pingpong");
    let mut actions = [None; 1];
    let result = driver.feed(&input[..first + second], &mut actions, &mut headers);
    assert_eq!(result.status, FeedStatus::OutputFull);
    assert_eq!(result.consumed, first);
    assert_eq!(result.actions, 1);
    assert_eq!(
        actions[0],
        Some(H2FrameAction::PingAck {
            opaque_data: *b"pingpong"
        })
    );
}
