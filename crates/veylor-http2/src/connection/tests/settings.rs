use super::{establish, frame};
use crate::connection::H2FrameDriver;
use crate::frame::{
    FRAME_HEADER_LEN, FrameHeader, FrameType, HEADERS_FLAG_END_HEADERS, SETTINGS_ACK,
    SETTINGS_ENABLE_PUSH, SETTINGS_ENTRY_LEN, SETTINGS_HEADER_TABLE_SIZE, SettingsParameter,
};
use crate::hpack::{HpackEntrySlot, HpackHeader, HpackTable, encode_table_size_update};
use crate::stream::{FrameDirection, H2Role, StreamActionKind, StreamSlot, StreamTable};

#[test]
fn peer_settings_limit_encoder_without_changing_decoder_limit() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block = [0; 8];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 0);
    establish(&mut streams);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut decoder_scratch = [0; 128];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut encoder_scratch = [0; 128];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    let peer_settings = [0, SETTINGS_HEADER_TABLE_SIZE as u8, 0, 0, 0, 128];
    let mut input = [0; FRAME_HEADER_LEN + SETTINGS_ENTRY_LEN];
    let length = frame(&mut input, FrameType::Settings, 0, 0, &peer_settings);
    let mut actions = [None; 1];
    let mut headers = [HpackHeader::EMPTY; 1];
    let result = driver.feed(&input[..length], &mut actions, &mut headers);

    assert_eq!(result.error, None);
    assert_eq!(result.actions, 1);
    assert_eq!(driver.streams().remote_settings().header_table_size, 128);
    assert_eq!(driver.hpack_encoder_mut().protocol_max_size(), 128);
    assert_eq!(driver.hpack_encoder_mut().max_size(), 128);
    assert_eq!(driver.hpack_encoder_mut().pending_min_size(), Some(128));

    let mut header_block = [0; 8];
    let update_length = encode_table_size_update(256, &mut header_block).unwrap();
    header_block[update_length..update_length + 3].copy_from_slice(&[0x82, 0x86, 0x84]);
    let mut input = [0; FRAME_HEADER_LEN + 8];
    let length = frame(
        &mut input,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS,
        1,
        &header_block[..update_length + 3],
    );
    let mut actions = [None; 1];
    let mut headers = [HpackHeader::EMPTY; 3];
    let result = driver.feed(&input[..length], &mut actions, &mut headers);

    assert_eq!(result.error, None);
    assert_eq!(result.headers, 3);

    let invalid_peer_settings = [0, SETTINGS_ENABLE_PUSH as u8, 0, 0, 0, 2];
    let mut input = [0; FRAME_HEADER_LEN + SETTINGS_ENTRY_LEN];
    let length = frame(
        &mut input,
        FrameType::Settings,
        0,
        0,
        &invalid_peer_settings,
    );
    let result = driver.feed(&input[..length], &mut actions, &mut headers);

    assert_eq!(result.error, Some(crate::frame::ErrorCode::ProtocolError));
    assert_eq!(driver.streams().remote_settings().header_table_size, 128);
    assert_eq!(driver.hpack_encoder_mut().protocol_max_size(), 128);
}

#[test]
fn local_header_table_limits_follow_settings_ack_order() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block_storage = [0; 8];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block_storage, 0);
    establish(&mut streams);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut decoder_scratch = [0; 256];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    decoder.insert_bytes(b"x", &[b'a'; 128]).unwrap();
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut encoder_scratch = [0; 128];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    let first = [SettingsParameter {
        id: SETTINGS_HEADER_TABLE_SIZE,
        value: 2_048,
    }];
    driver
        .register_fully_written_local_settings(&first)
        .unwrap();
    driver.register_fully_written_local_settings(&[]).unwrap();
    let third = [SettingsParameter {
        id: SETTINGS_HEADER_TABLE_SIZE,
        value: 4_096,
    }];
    driver
        .register_fully_written_local_settings(&third)
        .unwrap();
    let fourth = [SettingsParameter {
        id: SETTINGS_HEADER_TABLE_SIZE,
        value: 128,
    }];
    driver
        .register_fully_written_local_settings(&fourth)
        .unwrap();

    assert_eq!(driver.streams().unacked_settings(), 4);
    assert_eq!(driver.hpack_decoder().protocol_max_size(), 4_096);

    for (expected_limit, remaining_acks) in [(2_048, 3), (2_048, 2), (4_096, 1), (128, 0)] {
        let mut input = [0; FRAME_HEADER_LEN];
        let length = frame(&mut input, FrameType::Settings, SETTINGS_ACK, 0, &[]);
        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 1];
        let result = driver.feed(&input[..length], &mut actions, &mut headers);

        assert_eq!(result.error, None);
        assert_eq!(driver.hpack_decoder().protocol_max_size(), expected_limit);
        assert_eq!(driver.streams().unacked_settings(), remaining_acks);
    }
    assert_eq!(driver.hpack_decoder().required_size_update(), Some(128));
}

#[test]
fn local_settings_capacity_failure_leaves_stream_and_decoder_state_unchanged() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block_storage = [0; 8];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block_storage, 0);
    establish(&mut streams);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut decoder_scratch = [0; 128];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut encoder_scratch = [0; 128];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 1];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    let first = [SettingsParameter {
        id: SETTINGS_HEADER_TABLE_SIZE,
        value: 128,
    }];
    driver
        .register_fully_written_local_settings(&first)
        .unwrap();
    let second = [SettingsParameter {
        id: SETTINGS_HEADER_TABLE_SIZE,
        value: 64,
    }];

    assert_eq!(
        driver.register_fully_written_local_settings(&second),
        Err(crate::stream::SettingsError::Capacity)
    );
    assert_eq!(driver.streams().local_settings().header_table_size, 128);
    assert_eq!(driver.streams().unacked_settings(), 1);
    assert_eq!(driver.hpack_decoder().protocol_max_size(), 4_096);
    assert_eq!(driver.hpack_decoder().required_size_update(), None);

    let mut input = [0; FRAME_HEADER_LEN];
    let length = frame(&mut input, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let mut actions = [None; 1];
    let mut headers = [HpackHeader::EMPTY; 1];
    let result = driver.feed(&input[..length], &mut actions, &mut headers);

    assert_eq!(result.error, None);
    assert_eq!(driver.hpack_decoder().protocol_max_size(), 128);
    assert_eq!(driver.hpack_decoder().required_size_update(), None);
    assert_eq!(driver.streams().unacked_settings(), 0);
}

#[test]
fn untracked_settings_ack_fails_without_changing_decoder_limit() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block_storage = [0; 8];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block_storage, 0);
    establish(&mut streams);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut decoder_scratch = [0; 128];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut encoder_scratch = [0; 128];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut settings_ack_limits = [0; 2];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    let sent_settings = FrameHeader {
        payload_length: 0,
        frame_type: FrameType::Settings,
        flags: 0,
        stream_id: 0,
    };
    assert_eq!(
        driver
            .streams_mut()
            .on_frame(sent_settings, FrameDirection::Sent, &[])
            .kind,
        StreamActionKind::Accept
    );

    let mut input = [0; FRAME_HEADER_LEN];
    let length = frame(&mut input, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let mut actions = [None; 1];
    let mut headers = [HpackHeader::EMPTY; 1];
    let result = driver.feed(&input[..length], &mut actions, &mut headers);

    assert_eq!(result.error, Some(crate::frame::ErrorCode::InternalError));
    assert_eq!(driver.hpack_decoder().protocol_max_size(), 4_096);
}
