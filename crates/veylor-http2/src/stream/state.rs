use super::{
    ErrorCode, FrameDirection, FrameType, StreamAction, StreamState, StreamTransition, transition,
};

pub fn stream_state_on_frame(
    state: StreamState,
    frame_type: FrameType,
    flags: u8,
    direction: FrameDirection,
) -> StreamTransition {
    if matches!(frame_type, FrameType::Unknown(_)) {
        return transition(StreamAction::IGNORE, state);
    }
    if frame_type == FrameType::Priority {
        return transition(StreamAction::ACCEPT, state);
    }
    if matches!(
        frame_type,
        FrameType::Settings | FrameType::Ping | FrameType::GoAway
    ) {
        return transition(StreamAction::IGNORE, state);
    }
    if frame_type == FrameType::PushPromise {
        return transition(
            StreamAction::connection_error(ErrorCode::ProtocolError),
            state,
        );
    }
    if frame_type == FrameType::RstStream {
        return match state {
            StreamState::Idle => transition(
                StreamAction::connection_error(ErrorCode::ProtocolError),
                state,
            ),
            StreamState::Closed => transition(StreamAction::IGNORE, state),
            _ => transition(StreamAction::ACCEPT, StreamState::Closed),
        };
    }
    if frame_type == FrameType::WindowUpdate {
        return match state {
            StreamState::Idle => transition(
                StreamAction::connection_error(ErrorCode::ProtocolError),
                state,
            ),
            StreamState::Closed => transition(StreamAction::IGNORE, state),
            StreamState::ReservedLocal if direction == FrameDirection::Sent => transition(
                StreamAction::connection_error(ErrorCode::ProtocolError),
                state,
            ),
            StreamState::ReservedRemote if direction == FrameDirection::Received => transition(
                StreamAction::connection_error(ErrorCode::ProtocolError),
                state,
            ),
            _ => transition(StreamAction::ACCEPT, state),
        };
    }

    let end_stream = flags & 0x1 != 0;
    match state {
        StreamState::Idle if frame_type != FrameType::Headers => transition(
            StreamAction::connection_error(ErrorCode::ProtocolError),
            state,
        ),
        StreamState::Idle => {
            let next = if end_stream {
                half_closed_state(direction)
            } else {
                StreamState::Open
            };
            transition(StreamAction::ACCEPT, next)
        }
        StreamState::ReservedLocal => {
            if direction != FrameDirection::Sent || frame_type != FrameType::Headers {
                return transition(
                    StreamAction::connection_error(ErrorCode::ProtocolError),
                    state,
                );
            }
            let next = if end_stream {
                StreamState::Closed
            } else {
                StreamState::HalfClosedRemote
            };
            transition(StreamAction::ACCEPT, next)
        }
        StreamState::ReservedRemote => {
            if direction != FrameDirection::Received || frame_type != FrameType::Headers {
                return transition(
                    StreamAction::connection_error(ErrorCode::ProtocolError),
                    state,
                );
            }
            let next = if end_stream {
                StreamState::Closed
            } else {
                StreamState::HalfClosedLocal
            };
            transition(StreamAction::ACCEPT, next)
        }
        StreamState::Open => {
            let next = if end_stream {
                half_closed_state(direction)
            } else {
                state
            };
            transition(StreamAction::ACCEPT, next)
        }
        StreamState::HalfClosedLocal => {
            if direction == FrameDirection::Sent {
                return transition(StreamAction::send_rst(ErrorCode::StreamClosed), state);
            }
            let next = if end_stream {
                StreamState::Closed
            } else {
                state
            };
            transition(StreamAction::ACCEPT, next)
        }
        StreamState::HalfClosedRemote => {
            if direction == FrameDirection::Received {
                return transition(StreamAction::send_rst(ErrorCode::StreamClosed), state);
            }
            let next = if end_stream {
                StreamState::Closed
            } else {
                state
            };
            transition(StreamAction::ACCEPT, next)
        }
        StreamState::Closed => match (direction, frame_type) {
            (FrameDirection::Received, FrameType::Headers | FrameType::Continuation) => transition(
                StreamAction::connection_error(ErrorCode::StreamClosed),
                state,
            ),
            (FrameDirection::Received, _) => {
                transition(StreamAction::send_rst(ErrorCode::StreamClosed), state)
            }
            (FrameDirection::Sent, _) => transition(StreamAction::REJECT, state),
        },
    }
}

const fn half_closed_state(direction: FrameDirection) -> StreamState {
    match direction {
        FrameDirection::Received => StreamState::HalfClosedRemote,
        FrameDirection::Sent => StreamState::HalfClosedLocal,
    }
}

pub(super) const fn is_active(state: StreamState) -> bool {
    matches!(
        state,
        StreamState::Open | StreamState::HalfClosedLocal | StreamState::HalfClosedRemote
    )
}
