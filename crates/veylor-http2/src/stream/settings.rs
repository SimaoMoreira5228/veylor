use super::{FrameDirection, H2Role, StreamAction, StreamTable};
use crate::flow_control::{DEFAULT_FLOW_WINDOW_SIZE, MAX_FLOW_WINDOW_SIZE};
use crate::frame::{
    DEFAULT_MAX_FRAME_SIZE, ErrorCode, FrameHeader, FrameType, SETTINGS_ACK,
    SETTINGS_ENABLE_CONNECT_PROTOCOL, SETTINGS_ENABLE_PUSH, SETTINGS_ENABLE_PUSH_MAX,
    SETTINGS_ENTRY_LEN, SETTINGS_HEADER_TABLE_SIZE, SETTINGS_INITIAL_WINDOW_SIZE,
    SETTINGS_MAX_CONCURRENT_STREAMS, SETTINGS_MAX_FRAME_SIZE, SETTINGS_MAX_HEADER_LIST_SIZE,
    SettingsParameter, settings_parameter_valid,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointSettings {
    pub header_table_size: u32,
    pub enable_push: u32,
    pub max_concurrent_streams: u32,
    pub initial_window_size: u32,
    pub max_frame_size: u32,
    pub max_header_list_size: u32,
    pub enable_connect_protocol: u32,
}

// NOTE: RFC 9113 §6.5.2 protocol defaults, named so deployments can cite them:
// the table size matches the HPACK default dynamic-table budget, and the two
// u32::MAX entries mean "no protocol-level cap" (bounded downstream instead:
// streams by slot tables, header lists by caller HPACK buffers).
pub const DEFAULT_HEADER_TABLE_SIZE: u32 = 4_096;
pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = u32::MAX;
pub const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = u32::MAX;

impl EndpointSettings {
    pub const fn for_role(role: H2Role) -> Self {
        // NOTE: max_header_list_size keeps the RFC 9113 default until we advertise a
        // downstream cap. The compressed-block assembler has a separate bound; exceeding
        // it closes the connection because the HPACK state cannot otherwise stay in sync.
        Self {
            header_table_size: DEFAULT_HEADER_TABLE_SIZE,
            enable_push: match role {
                H2Role::Client => 1,
                H2Role::Server => 0,
            },
            max_concurrent_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
            initial_window_size: DEFAULT_FLOW_WINDOW_SIZE as u32,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_header_list_size: DEFAULT_MAX_HEADER_LIST_SIZE,
            enable_connect_protocol: match role {
                H2Role::Client => 0,
                H2Role::Server => 1,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettingsError {
    Protocol,
    FlowControl,
    Capacity,
    UntrackedSettings,
}

impl SettingsError {
    const fn code(self) -> ErrorCode {
        match self {
            Self::Protocol => ErrorCode::ProtocolError,
            Self::FlowControl => ErrorCode::FlowControlError,
            Self::Capacity | Self::UntrackedSettings => ErrorCode::InternalError,
        }
    }
}

impl<'a> StreamTable<'a> {
    pub const fn local_max_concurrent(&self) -> u32 {
        self.local_settings.max_concurrent_streams
    }

    pub const fn remote_max_concurrent(&self) -> u32 {
        self.remote_settings.max_concurrent_streams
    }

    pub const fn local_settings(&self) -> EndpointSettings {
        self.local_settings
    }

    pub const fn remote_settings(&self) -> EndpointSettings {
        self.remote_settings
    }

    pub const fn unacked_settings(&self) -> u32 {
        self.unacked_settings
    }

    pub const fn pending_settings_acks(&self) -> u32 {
        self.pending_settings_acks
    }

    pub const fn first_settings_received(&self) -> bool {
        self.first_settings_received
    }

    pub const fn first_settings_sent(&self) -> bool {
        self.first_settings_sent
    }

    pub fn set_local_max_concurrent(&mut self, maximum: u32) {
        self.local_settings.max_concurrent_streams = maximum;
    }

    pub fn set_remote_max_concurrent(&mut self, maximum: u32) {
        self.remote_settings.max_concurrent_streams = maximum;
    }

    pub fn set_local_initial_window(&mut self, window: u32) -> Result<(), ErrorCode> {
        self.adjust_stream_windows(window, true)
    }

    pub fn set_remote_initial_window(&mut self, window: u32) -> Result<(), ErrorCode> {
        self.adjust_stream_windows(window, false)
    }

    pub fn apply_local_settings(
        &mut self,
        parameters: &[SettingsParameter],
    ) -> Result<(), SettingsError> {
        self.apply_settings(parameters.iter().copied(), true)
    }

    pub(crate) fn record_local_settings_sent(
        &mut self,
        parameters: &[SettingsParameter],
    ) -> Result<(), SettingsError> {
        if self.unacked_settings == u32::MAX {
            return Err(SettingsError::Capacity);
        }
        self.apply_settings(parameters.iter().copied(), true)?;
        self.unacked_settings += 1;
        self.first_settings_sent = true;
        Ok(())
    }

    pub fn apply_remote_settings(&mut self, parameters: &[SettingsParameter]) -> StreamAction {
        match self.apply_settings(parameters.iter().copied(), false) {
            Ok(()) => StreamAction::ACCEPT,
            Err(error) => StreamAction::connection_error(error.code()),
        }
    }

    pub(super) fn process_settings_frame(
        &mut self,
        header: FrameHeader,
        payload: &[u8],
        direction: FrameDirection,
    ) -> StreamAction {
        if header.frame_type != FrameType::Settings {
            return StreamAction::REJECT;
        }
        if header.stream_id != 0 {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }
        if usize::try_from(header.payload_length).ok() != Some(payload.len()) {
            return StreamAction::connection_error(ErrorCode::FrameSizeError);
        }
        if header.flags & SETTINGS_ACK != 0 && !payload.is_empty() {
            return StreamAction::connection_error(ErrorCode::FrameSizeError);
        }
        if header.flags & SETTINGS_ACK == 0 && !payload.len().is_multiple_of(SETTINGS_ENTRY_LEN) {
            return StreamAction::connection_error(ErrorCode::FrameSizeError);
        }

        let first_frame = match direction {
            FrameDirection::Received => !self.first_settings_received,
            FrameDirection::Sent => !self.first_settings_sent,
        };
        if first_frame && header.flags & SETTINGS_ACK != 0 {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }

        if header.flags & SETTINGS_ACK == 0 {
            match direction {
                FrameDirection::Received if self.pending_settings_acks == u32::MAX => {
                    return StreamAction::connection_error(ErrorCode::InternalError);
                }
                FrameDirection::Sent if self.unacked_settings == u32::MAX => {
                    return StreamAction::REJECT;
                }
                _ => {}
            }
        }

        if header.flags & SETTINGS_ACK != 0 {
            let pending = match direction {
                FrameDirection::Received => &mut self.unacked_settings,
                FrameDirection::Sent => &mut self.pending_settings_acks,
            };
            if *pending == 0 {
                return StreamAction::connection_error(ErrorCode::ProtocolError);
            }
            *pending -= 1;
            return StreamAction::ACCEPT;
        }

        let parameters = payload
            .as_chunks::<SETTINGS_ENTRY_LEN>()
            .0
            .iter()
            .map(|entry| SettingsParameter {
                id: u16::from_be_bytes([entry[0], entry[1]]),
                value: u32::from_be_bytes([entry[2], entry[3], entry[4], entry[5]]),
            });
        let local = direction == FrameDirection::Sent;
        if let Err(error) = self.apply_settings(parameters, local) {
            return StreamAction::connection_error(error.code());
        }

        match direction {
            FrameDirection::Received => {
                self.pending_settings_acks += 1;
                self.first_settings_received = true;
            }
            FrameDirection::Sent => {
                self.unacked_settings += 1;
                self.first_settings_sent = true;
            }
        }
        StreamAction::ACCEPT
    }

    fn apply_settings(
        &mut self,
        parameters: impl IntoIterator<Item = SettingsParameter>,
        local: bool,
    ) -> Result<(), SettingsError> {
        let mut next = if local {
            self.local_settings
        } else {
            self.remote_settings
        };
        let mut window_setting = i64::from(if local {
            self.local_settings.initial_window_size
        } else {
            self.remote_settings.initial_window_size
        });
        let (mut minimum_window, mut maximum_window) = self.stream_window_bounds(local);

        for parameter in parameters {
            let id = parameter.id;
            let value = parameter.value;
            if !settings_parameter_valid(id, value) {
                return Err(if id == SETTINGS_INITIAL_WINDOW_SIZE {
                    SettingsError::FlowControl
                } else {
                    SettingsError::Protocol
                });
            }
            if id == SETTINGS_ENABLE_PUSH
                && ((local && self.role == H2Role::Server && value == SETTINGS_ENABLE_PUSH_MAX)
                    || (!local && self.role == H2Role::Client && value == SETTINGS_ENABLE_PUSH_MAX))
            {
                return Err(SettingsError::Protocol);
            }

            if id == SETTINGS_INITIAL_WINDOW_SIZE {
                let delta = i64::from(value) - window_setting;
                let Some(next_minimum) = minimum_window.checked_add(delta) else {
                    return Err(SettingsError::FlowControl);
                };
                let Some(next_maximum) = maximum_window.checked_add(delta) else {
                    return Err(SettingsError::FlowControl);
                };
                if next_minimum < -MAX_FLOW_WINDOW_SIZE || next_maximum > MAX_FLOW_WINDOW_SIZE {
                    return Err(SettingsError::FlowControl);
                }
                minimum_window = next_minimum;
                maximum_window = next_maximum;
                window_setting = i64::from(value);
            }

            match id {
                SETTINGS_HEADER_TABLE_SIZE => next.header_table_size = value,
                SETTINGS_ENABLE_PUSH => next.enable_push = value,
                SETTINGS_MAX_CONCURRENT_STREAMS => next.max_concurrent_streams = value,
                SETTINGS_INITIAL_WINDOW_SIZE => next.initial_window_size = value,
                SETTINGS_MAX_FRAME_SIZE => next.max_frame_size = value,
                SETTINGS_MAX_HEADER_LIST_SIZE => next.max_header_list_size = value,
                SETTINGS_ENABLE_CONNECT_PROTOCOL => next.enable_connect_protocol = value,
                _ => {}
            }
        }

        if local {
            self.set_local_initial_window(next.initial_window_size)
                .map_err(|_| SettingsError::FlowControl)?;
            self.local_settings = next;
        } else {
            self.set_remote_initial_window(next.initial_window_size)
                .map_err(|_| SettingsError::FlowControl)?;
            self.remote_settings = next;
        }
        Ok(())
    }

    fn stream_window_bounds(&self, local: bool) -> (i64, i64) {
        let mut minimum = i64::MAX;
        let mut maximum = i64::MIN;
        for slot in &self.slots[..self.used] {
            let Some(stream) = slot.stream else {
                continue;
            };
            if stream.state == super::StreamState::Closed {
                continue;
            }
            let value = if local {
                stream.receive_window.value()
            } else {
                stream.send_window.value()
            };
            minimum = minimum.min(value);
            maximum = maximum.max(value);
        }
        if minimum == i64::MAX {
            return (0, 0);
        }
        (minimum, maximum)
    }
}
