use super::*;

#[test]
fn stream_state_transitions_cover_open_half_close_and_closed_states() {
    let cases = [
        (
            StreamState::Idle,
            FrameType::Headers,
            FrameDirection::Sent,
            0,
            StreamActionKind::Accept,
            StreamState::Open,
        ),
        (
            StreamState::Idle,
            FrameType::Headers,
            FrameDirection::Received,
            1,
            StreamActionKind::Accept,
            StreamState::HalfClosedRemote,
        ),
        (
            StreamState::Open,
            FrameType::Data,
            FrameDirection::Sent,
            1,
            StreamActionKind::Accept,
            StreamState::HalfClosedLocal,
        ),
        (
            StreamState::Open,
            FrameType::Headers,
            FrameDirection::Received,
            1,
            StreamActionKind::Accept,
            StreamState::HalfClosedRemote,
        ),
        (
            StreamState::HalfClosedLocal,
            FrameType::Data,
            FrameDirection::Received,
            1,
            StreamActionKind::Accept,
            StreamState::Closed,
        ),
        (
            StreamState::HalfClosedRemote,
            FrameType::Headers,
            FrameDirection::Sent,
            1,
            StreamActionKind::Accept,
            StreamState::Closed,
        ),
        (
            StreamState::Open,
            FrameType::RstStream,
            FrameDirection::Received,
            0,
            StreamActionKind::Accept,
            StreamState::Closed,
        ),
    ];

    for (state, kind, direction, flags, action, next_state) in cases {
        let result = stream_state_on_frame(state, kind, flags, direction);
        assert_eq!(result.action.kind, action);
        assert_eq!(result.next_state, next_state);
    }
}

#[test]
fn reserved_states_apply_directional_frame_rules() {
    let local_headers = stream_state_on_frame(
        StreamState::ReservedLocal,
        FrameType::Headers,
        0,
        FrameDirection::Sent,
    );
    assert_eq!(local_headers.action, StreamAction::ACCEPT);
    assert_eq!(local_headers.next_state, StreamState::HalfClosedRemote);

    let local_end = stream_state_on_frame(
        StreamState::ReservedLocal,
        FrameType::Headers,
        1,
        FrameDirection::Sent,
    );
    assert_eq!(local_end.next_state, StreamState::Closed);

    let remote_headers = stream_state_on_frame(
        StreamState::ReservedRemote,
        FrameType::Headers,
        0,
        FrameDirection::Received,
    );
    assert_eq!(remote_headers.next_state, StreamState::HalfClosedLocal);

    for (state, direction) in [
        (StreamState::ReservedLocal, FrameDirection::Sent),
        (StreamState::ReservedRemote, FrameDirection::Received),
    ] {
        let invalid_data = stream_state_on_frame(state, FrameType::Data, 0, direction);
        assert_eq!(invalid_data.action.kind, StreamActionKind::ConnectionError);
        assert_eq!(invalid_data.action.code, ErrorCode::ProtocolError);
    }

    let local_received_window = stream_state_on_frame(
        StreamState::ReservedLocal,
        FrameType::WindowUpdate,
        0,
        FrameDirection::Received,
    );
    assert_eq!(local_received_window.action.kind, StreamActionKind::Accept);

    let remote_sent_window = stream_state_on_frame(
        StreamState::ReservedRemote,
        FrameType::WindowUpdate,
        0,
        FrameDirection::Sent,
    );
    assert_eq!(remote_sent_window.action.kind, StreamActionKind::Accept);
}
