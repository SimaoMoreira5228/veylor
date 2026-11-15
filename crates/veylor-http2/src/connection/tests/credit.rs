use super::*;

#[test]
fn data_on_reset_stream_emits_connection_credit_for_every_frame() {
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
    let mut input = [0; 64];
    let reset = frame(&mut input, FrameType::RstStream, 0, 1, &[0, 0, 0, 8]);
    let mut actions = [None; 4];
    let result = driver.feed(&input[..reset], &mut actions, &mut headers);
    assert_eq!(result.error, None);

    let mut credited = 0u32;
    for _ in 0..8 {
        let mut input = [0; FRAME_HEADER_LEN + 5];
        let length = frame(&mut input, FrameType::Data, 0, 1, b"12345");
        let mut actions = [None; 4];
        let result = driver.feed(&input[..length], &mut actions, &mut headers);
        assert_eq!(result.error, None);
        for action in actions.iter().flatten() {
            if let H2FrameAction::ConnectionCredit { amount } = action {
                credited += amount;
            }
        }
    }
    assert_eq!(credited, 40);
    driver.add_connection_receive_credit(credited).unwrap();
    assert_eq!(driver.connection_receive_window().value(), 65_535);
}

#[test]
fn credit_without_free_action_slot_is_owed_to_the_next_feed() {
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
    driver.feed(&input[..length], &mut actions, &mut headers);
    assert!(matches!(actions[0], Some(H2FrameAction::SendReset { .. })));

    let mut ping = [0; 32];
    let ping_length = frame(&mut ping, FrameType::Ping, 0, 0, b"pingpong");
    let mut next = [None; 1];
    driver.feed(&ping[..ping_length], &mut next, &mut headers);
    assert_eq!(next[0], Some(H2FrameAction::ConnectionCredit { amount: 5 }));
}

#[test]
fn data_after_a_local_reset_is_ignored_without_answering_with_another_reset() {
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
    assert!(driver.streams.close_stream(1));

    for _ in 0..8 {
        let mut input = [0; FRAME_HEADER_LEN + 5];
        let length = frame(&mut input, FrameType::Data, 0, 1, b"12345");
        let mut actions = [None; 4];
        let result = driver.feed(&input[..length], &mut actions, &mut headers);
        assert_eq!(result.error, None);
        for action in actions.iter().flatten() {
            assert!(!matches!(action, H2FrameAction::SendReset { .. }));
        }
    }
}
