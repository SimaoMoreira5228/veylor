use super::*;

#[test]
fn settings_ping_window_updates_rst_and_goaway_dispatch_control_actions() {
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
    open_stream(&mut driver, &mut headers);

    let mut input = [0; 128];
    let setting = SettingsParameter {
        id: crate::frame::SETTINGS_MAX_CONCURRENT_STREAMS,
        value: 7,
    };
    let mut setting_payload = [0; SETTINGS_ENTRY_LEN];
    crate::frame::encode_settings_payload(&[setting], &mut setting_payload).unwrap();
    let mut offset = frame(&mut input, FrameType::Settings, 0, 0, &setting_payload);
    offset += frame(&mut input[offset..], FrameType::Ping, 0, 0, b"pingpong");
    offset += frame(
        &mut input[offset..],
        FrameType::WindowUpdate,
        0,
        0,
        &[0, 0, 0, 9],
    );
    offset += frame(
        &mut input[offset..],
        FrameType::WindowUpdate,
        0,
        1,
        &[0, 0, 0, 11],
    );
    offset += frame(
        &mut input[offset..],
        FrameType::RstStream,
        0,
        1,
        &[0, 0, 0, 8],
    );
    let goaway_payload = [0, 0, 0, 1, 0, 0, 0, 0];
    offset += frame(
        &mut input[offset..],
        FrameType::GoAway,
        0,
        0,
        &goaway_payload,
    );

    let mut actions = [None; 8];
    let result = driver.feed(&input[..offset], &mut actions, &mut headers);
    assert_eq!(result.status, FeedStatus::NeedMoreInput);
    assert_eq!(result.consumed, offset);
    assert_eq!(result.actions, 4);
    assert_eq!(actions[0], Some(H2FrameAction::SettingsAckRequired));
    assert_eq!(driver.streams().pending_settings_acks(), 1);
    let settings_ack = FrameHeader {
        payload_length: 0,
        frame_type: FrameType::Settings,
        flags: SETTINGS_ACK,
        stream_id: 0,
    };
    assert_eq!(
        driver
            .streams_mut()
            .on_frame(settings_ack, FrameDirection::Sent, &[])
            .kind,
        StreamActionKind::Accept
    );
    assert_eq!(driver.streams().pending_settings_acks(), 0);
    assert_eq!(
        actions[1],
        Some(H2FrameAction::PingAck {
            opaque_data: *b"pingpong",
        })
    );
    assert!(matches!(
        actions[2],
        Some(H2FrameAction::StreamReset { stream_id: 1, .. })
    ));
    assert!(matches!(
        actions[3],
        Some(H2FrameAction::GoAway {
            last_stream_id: 1,
            ..
        })
    ));
    assert_eq!(driver.connection_send_window().value(), 65_544);
    assert_eq!(
        driver.streams().find(1).unwrap().send_window.value(),
        65_546
    );
}
