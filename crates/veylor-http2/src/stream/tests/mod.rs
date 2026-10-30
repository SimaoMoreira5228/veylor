use super::*;
use crate::frame::SETTINGS_ACK;

mod preface;
mod settings;
mod state;

pub(super) fn establish(table: &mut StreamTable<'_>) {
    if table.role() == H2Role::Server {
        assert_eq!(
            table.receive_client_preface_fragment(CLIENT_CONNECTION_PREFACE),
            Ok(CLIENT_CONNECTION_PREFACE.len())
        );
    }

    let settings = header(FrameType::Settings, 0, 0, 0);
    assert_eq!(
        table.on_frame(settings, FrameDirection::Sent, &[]),
        StreamAction::ACCEPT
    );
    assert_eq!(
        table.on_frame(settings, FrameDirection::Received, &[]),
        StreamAction::ACCEPT
    );
}

pub(super) fn header(
    frame_type: FrameType,
    flags: u8,
    stream_id: u32,
    payload_length: u32,
) -> FrameHeader {
    FrameHeader {
        payload_length,
        frame_type,
        flags,
        stream_id,
    }
}

#[test]
fn half_closed_and_closed_streams_enforce_direction_and_late_frames() {
    let local_send = stream_state_on_frame(
        StreamState::HalfClosedLocal,
        FrameType::Data,
        0,
        FrameDirection::Sent,
    );
    assert_eq!(local_send.action.kind, StreamActionKind::SendRst);
    assert_eq!(local_send.action.code, ErrorCode::StreamClosed);

    let remote_receive = stream_state_on_frame(
        StreamState::HalfClosedRemote,
        FrameType::Headers,
        0,
        FrameDirection::Received,
    );
    assert_eq!(remote_receive.action.kind, StreamActionKind::SendRst);

    let closed_data = stream_state_on_frame(
        StreamState::Closed,
        FrameType::Data,
        0,
        FrameDirection::Received,
    );
    assert_eq!(closed_data.action.kind, StreamActionKind::SendRst);

    let closed_reset = stream_state_on_frame(
        StreamState::Closed,
        FrameType::RstStream,
        0,
        FrameDirection::Received,
    );
    assert_eq!(closed_reset.action.kind, StreamActionKind::Ignore);

    let closed_priority = stream_state_on_frame(
        StreamState::Closed,
        FrameType::Priority,
        0,
        FrameDirection::Received,
    );
    assert_eq!(closed_priority.action.kind, StreamActionKind::Accept);
}

#[test]
fn unknown_frames_are_ignored_and_idle_stream_rules_are_strict() {
    assert_eq!(
        stream_state_on_frame(
            StreamState::Idle,
            FrameType::Unknown(0x0b),
            0,
            FrameDirection::Received,
        )
        .action
        .kind,
        StreamActionKind::Ignore
    );
    assert_eq!(
        stream_state_on_frame(
            StreamState::Idle,
            FrameType::Priority,
            0,
            FrameDirection::Received,
        )
        .action
        .kind,
        StreamActionKind::Accept
    );
    for kind in [
        FrameType::Data,
        FrameType::RstStream,
        FrameType::WindowUpdate,
    ] {
        let result = stream_state_on_frame(StreamState::Idle, kind, 0, FrameDirection::Received);
        assert_eq!(result.action.kind, StreamActionKind::ConnectionError);
        assert_eq!(result.action.code, ErrorCode::ProtocolError);
    }
}

#[test]
fn role_parity_monotonic_ids_and_stream_reuse_are_enforced() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut table_header_storage = [0; 256];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut table_header_storage, 0);
    establish(&mut table);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(table.find(1).unwrap().state, StreamState::Open);
    assert_eq!(table.highest_remote(), 1);
    assert_eq!(table.active_count(), 1);

    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 3, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::send_rst(ErrorCode::RefusedStream)
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::RstStream, 0, 1, 4),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(table.find(1).unwrap().state, StreamState::Closed);
    assert_eq!(table.active_count(), 0);

    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 5, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(table.find(1), None);
    assert_eq!(table.find(5).unwrap().state, StreamState::Open);
    assert_eq!(table.highest_remote(), 5);
    assert_eq!(table.active_count(), 1);

    let old = table.on_frame(
        header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
        FrameDirection::Received,
        &[],
    );
    assert_eq!(old.kind, StreamActionKind::SendRst);

    let bad_parity = table.on_frame(
        header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 2, 0),
        FrameDirection::Received,
        &[],
    );
    assert_eq!(bad_parity.kind, StreamActionKind::ConnectionError);
}

#[test]
fn concurrent_limits_are_directional_and_reserved_streams_do_not_count() {
    let mut slots = [StreamSlot::EMPTY; 4];
    let mut table_header_storage = [0; 256];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut table_header_storage, 0);
    establish(&mut table);
    table.set_local_max_concurrent(1);
    assert_eq!(table.reserve_local(2), Ok(()));
    assert_eq!(table.active_count(), 0);

    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(table.active_count(), 1);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 3, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::send_rst(ErrorCode::RefusedStream)
    );

    let mut client_slots = [StreamSlot::EMPTY; 2];
    let mut client_header_storage = [0; 256];
    let mut client = StreamTable::new(
        &mut client_slots,
        H2Role::Client,
        &mut client_header_storage,
        0,
    );
    establish(&mut client);
    client.set_remote_max_concurrent(0);
    assert_eq!(
        client.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Sent,
            &[]
        ),
        StreamAction::REJECT
    );
    assert_eq!(client.highest_local(), 0);
    client.set_remote_max_concurrent(1);
    assert_eq!(
        client.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Sent,
            &[],
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(client.highest_local(), 1);
}

#[test]
fn slot_capacity_bounds_concurrent_opens_regardless_of_advertised_max() {
    let mut slots = [StreamSlot::EMPTY; 4];
    let mut table_header_storage = [0; 256];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut table_header_storage, 0);
    establish(&mut table);
    assert_eq!(table.slot_capacity(), 4);
    assert_eq!(table.local_max_concurrent(), u32::MAX);
    for id in [1, 3, 5, 7] {
        assert_eq!(
            table.on_frame(
                header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, id, 0),
                FrameDirection::Received,
                &[]
            ),
            StreamAction::ACCEPT
        );
    }
    assert_eq!(table.active_count(), 4);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 9, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::send_rst(ErrorCode::RefusedStream)
    );
    assert_eq!(table.highest_remote(), 9);
    assert_eq!(
        table.on_frame(
            header(FrameType::RstStream, 0, 1, 4),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(table.active_count(), 3);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 11, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(table.find(1), None);
    assert_eq!(table.active_count(), 4);
}

#[test]
fn client_and_server_stream_opening_respects_initiator_role() {
    let mut client_slots = [StreamSlot::EMPTY; 2];
    let mut client_header_storage = [0; 256];
    let mut client = StreamTable::new(
        &mut client_slots,
        H2Role::Client,
        &mut client_header_storage,
        0,
    );
    establish(&mut client);
    assert_eq!(
        client.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Sent,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(client.highest_local(), 1);
    assert_eq!(
        client.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 2, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );

    let mut server_slots = [StreamSlot::EMPTY; 2];
    let mut server_header_storage = [0; 256];
    let mut server = StreamTable::new(
        &mut server_slots,
        H2Role::Server,
        &mut server_header_storage,
        0,
    );
    establish(&mut server);
    assert_eq!(
        server.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 2, 0),
            FrameDirection::Sent,
            &[]
        ),
        StreamAction::REJECT
    );
    assert_eq!(server.reserve_local(2), Ok(()));
    assert_eq!(
        server.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 2, 0),
            FrameDirection::Sent,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(server.find(2).unwrap().state, StreamState::HalfClosedRemote);
}

#[test]
fn goaway_limits_streams_and_rejects_increasing_last_ids() {
    let mut slots = [StreamSlot::EMPTY; 4];
    let mut table_header_storage = [0; 256];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut table_header_storage, 0);
    establish(&mut table);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 3, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(table.on_goaway_sent(1), Ok(()));
    assert_eq!(table.find(1).unwrap().state, StreamState::Open);
    assert_eq!(table.find(3).unwrap().state, StreamState::Closed);
    assert_eq!(table.active_count(), 1);
    assert_eq!(
        table.on_goaway_sent(3),
        Err(StreamTableError::GoAwayLastStreamIdIncreased)
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::Data, 0, 3, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::IGNORE
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 5, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::IGNORE
    );

    let payload = [0, 0, 0, 1, 0, 0, 0, 0];
    let frame = header(FrameType::GoAway, 0, 0, payload.len() as u32);
    assert_eq!(
        table.on_goaway_received(frame, &payload),
        StreamAction::ACCEPT
    );
    assert_eq!(table.goaway_recv_last(), 1);
    assert_eq!(table.goaway_recv_code(), ErrorCode::NoError);
    let increased_payload = [0, 0, 0, 3, 0, 0, 0, 0];
    let increased = header(FrameType::GoAway, 0, 0, increased_payload.len() as u32);
    assert_eq!(
        table.on_goaway_received(increased, &increased_payload),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
    assert_eq!(table.goaway_recv_last(), 1);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 2, 0),
            FrameDirection::Sent,
            &[]
        ),
        StreamAction::REJECT
    );
}

#[test]
fn connection_and_frame_length_rules_are_enforced() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut table_header_storage = [0; 256];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut table_header_storage, 0);
    establish(&mut table);
    assert_eq!(
        table.on_frame(
            header(FrameType::Ping, 0, 0, 7),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::connection_error(ErrorCode::FrameSizeError)
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::Settings, 0, 0, 7),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::connection_error(ErrorCode::FrameSizeError)
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::Settings, SETTINGS_ACK, 0, 6),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::connection_error(ErrorCode::FrameSizeError)
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::GoAway, 0, 1, 8),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::RstStream, 0, 1, 3),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::connection_error(ErrorCode::FrameSizeError)
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::Data, 0, 0, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
}

#[test]
fn initial_window_adjustments_are_transactional_and_bounded() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut table_header_storage = [0; 256];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut table_header_storage, 0);
    establish(&mut table);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(table.set_remote_initial_window(100_000), Ok(()));
    assert_eq!(table.find(1).unwrap().send_window.value(), 100_000);
    table
        .send_window_mut(1)
        .unwrap()
        .add((MAX_FLOW_WINDOW_SIZE - 100_000) as u32)
        .unwrap();
    assert_eq!(
        table.set_remote_initial_window(100_001),
        Err(ErrorCode::FlowControlError)
    );
    assert_eq!(
        table.find(1).unwrap().send_window.value(),
        MAX_FLOW_WINDOW_SIZE
    );
    assert_eq!(table.remote_settings.initial_window_size, 100_000);

    let mut next_slots = [StreamSlot::EMPTY; 2];
    let mut next_header_storage = [0; 256];
    let mut next = StreamTable::new(&mut next_slots, H2Role::Server, &mut next_header_storage, 0);
    establish(&mut next);
    for id in [1, 3] {
        assert_eq!(
            next.on_frame(
                header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, id, 0),
                FrameDirection::Received,
                &[],
            ),
            StreamAction::ACCEPT
        );
    }
    next.send_window_mut(3)
        .unwrap()
        .adjust(-MAX_FLOW_WINDOW_SIZE)
        .unwrap();
    next.send_window_mut(3).unwrap().adjust(-65_535).unwrap();
    assert_eq!(
        next.set_remote_initial_window(0),
        Err(ErrorCode::FlowControlError)
    );
    assert_eq!(next.find(1).unwrap().send_window.value(), 65_535);
    assert_eq!(next.remote_settings.initial_window_size, 65_535);
}

#[test]
fn continuation_fragments_are_copied_and_reassembled_for_the_matching_stream() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut storage = [0; 16];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut storage, 16);
    establish(&mut table);
    let mut first_fragment = [0x82, 0x86];
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, 0, 1, 2),
            FrameDirection::Received,
            &first_fragment,
        ),
        StreamAction::ACCEPT
    );
    first_fragment.fill(0xff);
    assert!(table.is_assembling_headers());
    assert_eq!(table.assembled_header_block(), None);

    assert_eq!(
        table.on_frame(
            header(FrameType::Continuation, 0, 1, 1),
            FrameDirection::Received,
            &[0x84],
        ),
        StreamAction::ACCEPT
    );
    assert!(table.is_assembling_headers());
    assert_eq!(
        table.on_frame(
            header(
                FrameType::Continuation,
                crate::frame::CONTINUATION_FLAG_END_HEADERS,
                1,
                1,
            ),
            FrameDirection::Received,
            &[0x88],
        ),
        StreamAction::ACCEPT
    );
    assert!(!table.is_assembling_headers());
    assert_eq!(
        table.assembled_header_block(),
        Some((1, &[0x82, 0x86, 0x84, 0x88][..]))
    );
}

#[test]
fn continuation_gate_rejects_strays_wrong_streams_and_interleaving() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut storage = [0; 16];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut storage, 16);
    establish(&mut table);
    assert_eq!(
        table.on_frame(
            header(
                FrameType::Continuation,
                crate::frame::CONTINUATION_FLAG_END_HEADERS,
                1,
                1,
            ),
            FrameDirection::Received,
            &[0x88],
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );

    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, 0, 1, 1),
            FrameDirection::Received,
            &[0x82]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(
        table.on_frame(
            header(
                FrameType::Continuation,
                crate::frame::CONTINUATION_FLAG_END_HEADERS,
                3,
                1,
            ),
            FrameDirection::Received,
            &[0x86],
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::Ping, 0, 0, 8),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
    assert_eq!(
        table.on_frame(
            header(
                FrameType::Continuation,
                crate::frame::CONTINUATION_FLAG_END_HEADERS,
                1,
                1,
            ),
            FrameDirection::Received,
            &[0x86],
        ),
        StreamAction::ACCEPT
    );

    let mut next_slots = [StreamSlot::EMPTY; 1];
    let mut next_storage = [0; 8];
    let mut next = StreamTable::new(&mut next_slots, H2Role::Server, &mut next_storage, 8);
    establish(&mut next);
    assert_eq!(
        next.on_frame(
            header(FrameType::Headers, 0, 1, 1),
            FrameDirection::Received,
            &[0x82]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(
        next.on_frame(
            header(
                FrameType::Continuation,
                crate::frame::CONTINUATION_FLAG_END_HEADERS,
                1,
                2,
            ),
            FrameDirection::Received,
            &[0x86],
        ),
        StreamAction::connection_error(ErrorCode::FrameSizeError)
    );
}

#[test]
fn continuation_assembly_resets_stream_on_storage_or_header_limit_overflow() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut storage = [0; 4];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut storage, 8);
    establish(&mut table);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, 0, 1, 5),
            FrameDirection::Received,
            &[1, 2, 3, 4, 5]
        ),
        StreamAction::send_rst(ErrorCode::RefusedStream)
    );
    assert!(!table.is_assembling_headers());
    assert_eq!(table.assembled_header_block(), None);

    let mut next_slots = [StreamSlot::EMPTY; 2];
    let mut limited_storage = [0; 16];
    let mut limited = StreamTable::new(&mut next_slots, H2Role::Server, &mut limited_storage, 4);
    establish(&mut limited);
    assert_eq!(
        limited.on_frame(
            header(FrameType::Headers, 0, 1, 3),
            FrameDirection::Received,
            &[1, 2, 3]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(
        limited.on_frame(
            header(
                FrameType::Continuation,
                crate::frame::CONTINUATION_FLAG_END_HEADERS,
                1,
                2,
            ),
            FrameDirection::Received,
            &[4, 5],
        ),
        StreamAction::send_rst(ErrorCode::RefusedStream)
    );
    assert!(!limited.is_assembling_headers());
    assert_eq!(limited.assembled_header_block(), None);
}

#[test]
fn stray_continuation_on_closed_stream_resets_instead_of_failing_the_connection() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut storage = [0; 64];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut storage, 64);
    establish(&mut table);
    assert_eq!(
        table.on_frame(
            header(FrameType::Headers, crate::HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Received,
            &[]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::RstStream, 0, 1, 4),
            FrameDirection::Received,
            &[0, 0, 0, 0]
        ),
        StreamAction::ACCEPT
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::Continuation, 0, 1, 1),
            FrameDirection::Received,
            &[0]
        ),
        StreamAction::send_rst(ErrorCode::StreamClosed)
    );
    assert_eq!(
        table.on_frame(
            header(FrameType::Continuation, 0, 99, 1),
            FrameDirection::Received,
            &[0]
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
}
