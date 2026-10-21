use super::*;
use crate::frame::{
    DEFAULT_MAX_FRAME_SIZE, SETTINGS_ACK, SETTINGS_ENABLE_PUSH, SETTINGS_ENTRY_LEN,
    SETTINGS_INITIAL_WINDOW_SIZE, SETTINGS_MAX_CONCURRENT_STREAMS, SETTINGS_MAX_FRAME_SIZE,
    SETTINGS_MAX_FRAME_SIZE_MIN, SettingsParameter,
};

fn setting(id: u16, value: u32) -> [u8; SETTINGS_ENTRY_LEN] {
    let mut bytes = [0; SETTINGS_ENTRY_LEN];
    bytes[..2].copy_from_slice(&id.to_be_bytes());
    bytes[2..].copy_from_slice(&value.to_be_bytes());
    bytes
}

fn settings_header(flags: u8, payload_length: usize) -> FrameHeader {
    header(
        FrameType::Settings,
        flags,
        0,
        u32::try_from(payload_length).unwrap(),
    )
}

#[test]
fn endpoint_settings_defaults_are_role_specific_and_directional() {
    let mut client_slots = [StreamSlot::EMPTY; 1];
    let mut client_storage = [0; 8];
    let client = StreamTable::new(&mut client_slots, H2Role::Client, &mut client_storage, 0);
    assert_eq!(client.local_settings().enable_push, 1);
    assert_eq!(client.remote_settings().enable_push, 0);
    assert_eq!(client.local_settings().initial_window_size, 65_535);
    assert_eq!(
        client.remote_settings().max_frame_size,
        DEFAULT_MAX_FRAME_SIZE
    );

    let mut server_slots = [StreamSlot::EMPTY; 1];
    let mut server_storage = [0; 8];
    let server = StreamTable::new(&mut server_slots, H2Role::Server, &mut server_storage, 0);
    assert_eq!(server.local_settings().enable_push, 0);
    assert_eq!(server.remote_settings().enable_push, 1);
}

#[test]
fn settings_are_applied_in_order_and_update_existing_and_future_windows() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut storage = [0; 8];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut storage, 0);
    establish(&mut table);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Received,
            &[],
        ),
        StreamAction::ACCEPT
    );

    let parameters = [
        SettingsParameter {
            id: SETTINGS_MAX_CONCURRENT_STREAMS,
            value: 2,
        },
        SettingsParameter {
            id: SETTINGS_MAX_CONCURRENT_STREAMS,
            value: 7,
        },
        SettingsParameter {
            id: SETTINGS_INITIAL_WINDOW_SIZE,
            value: 100_000,
        },
        SettingsParameter {
            id: 0xff00,
            value: 9,
        },
        SettingsParameter {
            id: SETTINGS_INITIAL_WINDOW_SIZE,
            value: 120_000,
        },
    ];
    assert_eq!(
        table.apply_remote_settings(&parameters),
        StreamAction::ACCEPT
    );
    assert_eq!(table.remote_max_concurrent(), 7);
    assert_eq!(table.remote_settings().initial_window_size, 120_000);
    assert_eq!(table.find(1).unwrap().send_window.value(), 120_000);

    let new_stream = header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 3, 0);
    assert_eq!(
        table.on_frame(new_stream, FrameDirection::Received, &[]),
        StreamAction::ACCEPT
    );
    assert_eq!(table.find(3).unwrap().send_window.value(), 120_000);

    let local = [
        SettingsParameter {
            id: SETTINGS_INITIAL_WINDOW_SIZE,
            value: 64_000,
        },
        SettingsParameter {
            id: SETTINGS_MAX_CONCURRENT_STREAMS,
            value: 1,
        },
    ];
    assert_eq!(table.apply_local_settings(&local), Ok(()));
    assert_eq!(table.find(1).unwrap().receive_window.value(), 64_000);
    assert_eq!(table.find(3).unwrap().receive_window.value(), 64_000);

    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 5, 0),
            FrameDirection::Received,
            &[],
        ),
        StreamAction::send_rst(ErrorCode::RefusedStream)
    );

    let mut client_slots = [StreamSlot::EMPTY; 1];
    let mut client_storage = [0; 8];
    let mut client = StreamTable::new(&mut client_slots, H2Role::Client, &mut client_storage, 0);
    establish(&mut client);
    let remote_limit = [SettingsParameter {
        id: SETTINGS_MAX_CONCURRENT_STREAMS,
        value: 0,
    }];
    assert_eq!(
        client.apply_remote_settings(&remote_limit),
        StreamAction::ACCEPT
    );
    assert_eq!(
        client.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Sent,
            &[],
        ),
        StreamAction::REJECT
    );
}

#[test]
fn enable_push_validation_observes_sender_role() {
    let mut server_slots = [StreamSlot::EMPTY; 1];
    let mut server_storage = [0; 8];
    let mut server = StreamTable::new(&mut server_slots, H2Role::Server, &mut server_storage, 0);
    let enabled = [SettingsParameter {
        id: SETTINGS_ENABLE_PUSH,
        value: 1,
    }];
    assert_eq!(
        server.apply_local_settings(&enabled),
        Err(SettingsError::Protocol)
    );
    assert_eq!(server.local_settings().enable_push, 0);
    assert_eq!(server.apply_remote_settings(&enabled), StreamAction::ACCEPT);
    assert_eq!(server.remote_settings().enable_push, 1);

    let mut client_slots = [StreamSlot::EMPTY; 1];
    let mut client_storage = [0; 8];
    let mut client = StreamTable::new(&mut client_slots, H2Role::Client, &mut client_storage, 0);
    assert_eq!(client.apply_local_settings(&enabled), Ok(()));
    assert_eq!(
        client.apply_remote_settings(&enabled),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );

    let invalid = [SettingsParameter {
        id: SETTINGS_ENABLE_PUSH,
        value: 2,
    }];
    assert_eq!(
        server.apply_remote_settings(&invalid),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
}

#[test]
fn settings_frame_acknowledgements_pair_and_reject_unsolicited_ack() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut storage = [0; 8];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut storage, 0);
    table
        .receive_client_preface_fragment(CLIENT_CONNECTION_PREFACE)
        .unwrap();

    assert_eq!(
        table.on_frame(settings_header(0, 0), FrameDirection::Sent, &[]),
        StreamAction::ACCEPT
    );
    assert_eq!(table.unacked_settings(), 1);

    let received_setting = setting(SETTINGS_MAX_CONCURRENT_STREAMS, 11);
    assert_eq!(
        table.on_frame(
            settings_header(0, received_setting.len()),
            FrameDirection::Received,
            &received_setting,
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(table.pending_settings_acks(), 1);
    assert_eq!(table.remote_max_concurrent(), 11);

    assert_eq!(
        table.on_frame(
            settings_header(SETTINGS_ACK, 0),
            FrameDirection::Received,
            &[],
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(table.unacked_settings(), 0);
    assert_eq!(
        table.on_frame(settings_header(SETTINGS_ACK, 0), FrameDirection::Sent, &[]),
        StreamAction::ACCEPT
    );
    assert_eq!(table.pending_settings_acks(), 0);
    assert_eq!(
        table.on_frame(
            settings_header(SETTINGS_ACK, 0),
            FrameDirection::Received,
            &[],
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
}

#[test]
fn malformed_settings_payloads_leave_endpoint_state_unchanged() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut storage = [0; 8];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut storage, 0);
    establish(&mut table);

    assert_eq!(
        table.on_frame(settings_header(0, 5), FrameDirection::Received, &[0; 5],),
        StreamAction::connection_error(ErrorCode::FrameSizeError)
    );
    let enable_push = setting(SETTINGS_ENABLE_PUSH, 2);
    assert_eq!(
        table.on_frame(
            settings_header(0, enable_push.len()),
            FrameDirection::Received,
            &enable_push,
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
    assert_eq!(table.remote_settings().enable_push, 1);

    let wrong_length = setting(SETTINGS_MAX_CONCURRENT_STREAMS, 4);
    assert_eq!(
        table.on_frame(
            settings_header(0, 0),
            FrameDirection::Received,
            &wrong_length,
        ),
        StreamAction::connection_error(ErrorCode::FrameSizeError)
    );
    assert_eq!(table.remote_max_concurrent(), u32::MAX);

    assert_eq!(
        table.on_frame(
            settings_header(SETTINGS_ACK, 6),
            FrameDirection::Received,
            &[0; 6],
        ),
        StreamAction::connection_error(ErrorCode::FrameSizeError)
    );

    let bad_frame_size = [SettingsParameter {
        id: SETTINGS_MAX_FRAME_SIZE,
        value: SETTINGS_MAX_FRAME_SIZE_MIN - 1,
    }];
    assert_eq!(
        table.apply_remote_settings(&bad_frame_size),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
}

#[test]
fn settings_ack_counters_reject_overflow_without_applying_the_frame() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut storage = [0; 8];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut storage, 0);
    establish(&mut table);
    let parameter = setting(SETTINGS_MAX_CONCURRENT_STREAMS, 17);

    table.pending_settings_acks = u32::MAX;
    assert_eq!(
        table.on_frame(
            settings_header(0, parameter.len()),
            FrameDirection::Received,
            &parameter,
        ),
        StreamAction::connection_error(ErrorCode::InternalError)
    );
    assert_eq!(table.remote_max_concurrent(), u32::MAX);

    table.pending_settings_acks = 0;
    table.unacked_settings = u32::MAX;
    assert_eq!(
        table.on_frame(
            settings_header(0, parameter.len()),
            FrameDirection::Sent,
            &parameter,
        ),
        StreamAction::REJECT
    );
    assert_eq!(table.local_max_concurrent(), u32::MAX);
}

#[test]
fn settings_window_overflow_is_atomic() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut storage = [0; 8];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut storage, 0);
    establish(&mut table);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Received,
            &[],
        ),
        StreamAction::ACCEPT
    );
    table
        .send_window_mut(1)
        .unwrap()
        .add((MAX_FLOW_WINDOW_SIZE - 65_535) as u32)
        .unwrap();

    let initial_window = setting(SETTINGS_INITIAL_WINDOW_SIZE, 65_536);
    assert_eq!(
        table.on_frame(
            settings_header(0, initial_window.len()),
            FrameDirection::Received,
            &initial_window,
        ),
        StreamAction::connection_error(ErrorCode::FlowControlError)
    );
    assert_eq!(table.remote_settings().initial_window_size, 65_535);
    assert_eq!(
        table.find(1).unwrap().send_window.value(),
        MAX_FLOW_WINDOW_SIZE
    );
}

#[test]
fn duplicate_initial_windows_check_each_ordered_intermediate_value() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut storage = [0; 8];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut storage, 0);
    establish(&mut table);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Received,
            &[],
        ),
        StreamAction::ACCEPT
    );
    table
        .send_window_mut(1)
        .unwrap()
        .add((MAX_FLOW_WINDOW_SIZE - 65_535) as u32)
        .unwrap();

    let parameters = [
        SettingsParameter {
            id: SETTINGS_INITIAL_WINDOW_SIZE,
            value: 65_536,
        },
        SettingsParameter {
            id: SETTINGS_INITIAL_WINDOW_SIZE,
            value: 65_535,
        },
    ];
    assert_eq!(
        table.apply_remote_settings(&parameters),
        StreamAction::connection_error(ErrorCode::FlowControlError)
    );
    assert_eq!(table.remote_settings().initial_window_size, 65_535);
    assert_eq!(
        table.find(1).unwrap().send_window.value(),
        MAX_FLOW_WINDOW_SIZE
    );
}
