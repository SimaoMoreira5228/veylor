use crate::flow_control::{FlowWindow, MAX_FLOW_WINDOW_SIZE};
use crate::frame::{ErrorCode, FrameHeader, FrameType, MAX_STREAM_ID};

use super::{
    FrameDirection, H2Role, Stream, StreamAction, StreamActionKind, StreamState, StreamTable,
    StreamTableError, is_active, stream_is_local_id, stream_state_on_frame,
};

impl<'a> StreamTable<'a> {
    pub(super) fn on_new_stream(
        &mut self,
        header: FrameHeader,
        direction: FrameDirection,
        is_local: bool,
    ) -> StreamAction {
        if header.frame_type == FrameType::Priority {
            return StreamAction::ACCEPT;
        }

        if is_local && direction == FrameDirection::Received {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }
        if !is_local && direction == FrameDirection::Sent {
            return StreamAction::REJECT;
        }

        let highest = if is_local {
            self.highest_local
        } else {
            self.highest_remote
        };
        if header.stream_id <= highest {
            if self.find(header.stream_id).is_some() {
                return stream_state_on_frame(
                    StreamState::Closed,
                    header.frame_type,
                    header.flags,
                    direction,
                )
                .action;
            }
            if header.frame_type == FrameType::Headers {
                return StreamAction::connection_error(ErrorCode::ProtocolError);
            }
            return stream_state_on_frame(
                StreamState::Closed,
                header.frame_type,
                header.flags,
                direction,
            )
            .action;
        }

        if header.frame_type != FrameType::Headers {
            return if is_local {
                StreamAction::REJECT
            } else {
                StreamAction::connection_error(ErrorCode::ProtocolError)
            };
        }

        if !is_local && self.role == H2Role::Client {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }
        if is_local && self.role == H2Role::Server {
            return StreamAction::REJECT;
        }
        if is_local && self.goaway_received {
            return StreamAction::REJECT;
        }

        if !is_local {
            self.highest_remote = header.stream_id;
        }

        let maximum = if is_local {
            self.remote_settings.max_concurrent_streams
        } else {
            self.local_settings.max_concurrent_streams
        };
        if self.active_count() >= usize::try_from(maximum).unwrap_or(usize::MAX) {
            return if is_local {
                StreamAction::REJECT
            } else {
                StreamAction::send_rst(ErrorCode::RefusedStream)
            };
        }

        let transition = stream_state_on_frame(
            StreamState::Idle,
            header.frame_type,
            header.flags,
            direction,
        );
        if transition.action.kind != StreamActionKind::Accept {
            return transition.action;
        }
        if self
            .insert(header.stream_id, transition.next_state)
            .is_err()
        {
            return if is_local {
                StreamAction::REJECT
            } else {
                StreamAction::send_rst(ErrorCode::RefusedStream)
            };
        }
        if is_local {
            self.highest_local = header.stream_id;
        }
        transition.action
    }

    pub(super) fn reserve(&mut self, id: u32, local: bool) -> Result<(), StreamTableError> {
        if id == 0 || id > MAX_STREAM_ID {
            return Err(StreamTableError::StreamIdOutOfRange);
        }
        if stream_is_local_id(self.role, id) != local {
            return Err(StreamTableError::StreamIdParity);
        }
        if (self.role == H2Role::Client && local) || (self.role == H2Role::Server && !local) {
            return Err(StreamTableError::StreamIdParity);
        }
        let highest = if local {
            self.highest_local
        } else {
            self.highest_remote
        };
        if id <= highest {
            return Err(StreamTableError::StreamIdNotIncreasing);
        }
        let state = if local {
            StreamState::ReservedLocal
        } else {
            StreamState::ReservedRemote
        };
        self.insert(id, state)?;
        if local {
            self.highest_local = id;
        } else {
            self.highest_remote = id;
        }
        Ok(())
    }

    pub(super) fn insert(&mut self, id: u32, state: StreamState) -> Result<(), StreamTableError> {
        let send_window = FlowWindow::new(i64::from(self.remote_settings.initial_window_size))
            .map_err(|_| StreamTableError::SlotCapacity)?;
        let receive_window = FlowWindow::new(i64::from(self.local_settings.initial_window_size))
            .map_err(|_| StreamTableError::SlotCapacity)?;
        let stream = Stream {
            id,
            state,
            send_window,
            receive_window,
            locally_reset: false,
        };

        if self.used < self.slots.len() {
            self.slots[self.used].stream = Some(stream);
            self.used += 1;
            if is_active(state) {
                self.active_streams += 1;
            }
            return Ok(());
        }
        let Some(slot) = self.slots[..self.used].iter_mut().find(|slot| {
            slot.stream
                .is_some_and(|stream| stream.state == StreamState::Closed)
        }) else {
            return Err(StreamTableError::SlotCapacity);
        };
        slot.stream = Some(stream);
        if is_active(state) {
            self.active_streams += 1;
        }
        Ok(())
    }

    pub(super) fn find_index(&self, id: u32) -> Option<usize> {
        self.slots[..self.used]
            .iter()
            .position(|slot| slot.stream.is_some_and(|stream| stream.id == id))
    }

    pub(super) fn adjust_stream_windows(
        &mut self,
        window: u32,
        local: bool,
    ) -> Result<(), ErrorCode> {
        let requested = window;
        let window = i64::from(window);
        if window > MAX_FLOW_WINDOW_SIZE {
            return Err(ErrorCode::FlowControlError);
        }
        let current = if local {
            i64::from(self.local_settings.initial_window_size)
        } else {
            i64::from(self.remote_settings.initial_window_size)
        };
        let delta = window - current;
        for slot in &self.slots[..self.used] {
            let Some(stream) = slot.stream else {
                continue;
            };
            if stream.state == StreamState::Closed {
                continue;
            }
            let current = if local {
                stream.receive_window.value()
            } else {
                stream.send_window.value()
            };
            let Some(next) = current.checked_add(delta) else {
                return Err(ErrorCode::FlowControlError);
            };
            if !(-MAX_FLOW_WINDOW_SIZE..=MAX_FLOW_WINDOW_SIZE).contains(&next) {
                return Err(ErrorCode::FlowControlError);
            }
        }
        for slot in &mut self.slots[..self.used] {
            let Some(stream) = slot.stream.as_mut() else {
                continue;
            };
            if stream.state == StreamState::Closed {
                continue;
            }
            let window = if local {
                &mut stream.receive_window
            } else {
                &mut stream.send_window
            };
            window
                .adjust(delta)
                .map_err(|_| ErrorCode::FlowControlError)?;
        }
        if local {
            self.local_settings.initial_window_size = requested;
        } else {
            self.remote_settings.initial_window_size = requested;
        }
        Ok(())
    }
}
