mod admission;
mod assembler;
mod preface;
mod settings;
mod state;
#[cfg(test)]
mod tests;

use assembler::HeadersAssembler;
pub use preface::{CLIENT_CONNECTION_PREFACE, ClientPrefaceError};
pub use settings::{
    DEFAULT_HEADER_TABLE_SIZE, DEFAULT_MAX_CONCURRENT_STREAMS, DEFAULT_MAX_HEADER_LIST_SIZE,
    EndpointSettings, SettingsError,
};
use state::is_active;
pub use state::stream_state_on_frame;

use crate::flow_control::FlowWindow;
use crate::frame::{
    ErrorCode, FrameHeader, FrameType, GOAWAY_FIXED_LEN, HEADERS_FLAG_END_HEADERS, MAX_STREAM_ID,
    PING_PAYLOAD_LEN, PRIORITY_PAYLOAD_LEN, RST_STREAM_PAYLOAD_LEN, decode_goaway_payload,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum H2Role {
    Client,
    Server,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameDirection {
    Received,
    Sent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamState {
    Idle,
    ReservedLocal,
    ReservedRemote,
    Open,
    HalfClosedLocal,
    HalfClosedRemote,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamActionKind {
    Accept,
    Ignore,
    RejectProtocolError,
    SendRst,
    ConnectionError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamAction {
    pub kind: StreamActionKind,
    pub code: ErrorCode,
}

impl StreamAction {
    const ACCEPT: Self = Self {
        kind: StreamActionKind::Accept,
        code: ErrorCode::NoError,
    };

    const IGNORE: Self = Self {
        kind: StreamActionKind::Ignore,
        code: ErrorCode::NoError,
    };

    const REJECT: Self = Self {
        kind: StreamActionKind::RejectProtocolError,
        code: ErrorCode::ProtocolError,
    };

    const fn send_rst(code: ErrorCode) -> Self {
        Self {
            kind: StreamActionKind::SendRst,
            code,
        }
    }

    const fn connection_error(code: ErrorCode) -> Self {
        Self {
            kind: StreamActionKind::ConnectionError,
            code,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stream {
    pub id: u32,
    pub state: StreamState,
    pub send_window: FlowWindow,
    pub receive_window: FlowWindow,
    pub locally_reset: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamSlot {
    stream: Option<Stream>,
}

impl StreamSlot {
    pub const EMPTY: Self = Self { stream: None };

    pub const fn stream(&self) -> Option<&Stream> {
        self.stream.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamTransition {
    pub action: StreamAction,
    pub next_state: StreamState,
}

const fn transition(action: StreamAction, next_state: StreamState) -> StreamTransition {
    StreamTransition { action, next_state }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamTableError {
    StreamIdOutOfRange,
    StreamIdNotIncreasing,
    StreamIdParity,
    SlotCapacity,
    GoAwayLastStreamIdIncreased,
}

pub struct StreamTable<'a> {
    slots: &'a mut [StreamSlot],
    used: usize,
    active_streams: usize,
    role: H2Role,
    highest_local: u32,
    highest_remote: u32,
    pub(super) local_settings: EndpointSettings,
    pub(super) remote_settings: EndpointSettings,
    pub(super) unacked_settings: u32,
    pub(super) pending_settings_acks: u32,
    pub(super) first_settings_received: bool,
    pub(super) first_settings_sent: bool,
    pub(super) client_preface_bytes: usize,
    pub(super) client_preface_failed: bool,
    goaway_sent: bool,
    goaway_send_last: u32,
    goaway_received: bool,
    goaway_recv_last: u32,
    goaway_recv_code: ErrorCode,
    assembler: HeadersAssembler<'a>,
}

impl<'a> StreamTable<'a> {
    pub fn new(
        slots: &'a mut [StreamSlot],
        role: H2Role,
        header_block_storage: &'a mut [u8],
        max_header_block_size: u32,
    ) -> Self {
        slots.fill(StreamSlot::EMPTY);
        Self {
            slots,
            used: 0,
            active_streams: 0,
            role,
            highest_local: 0,
            highest_remote: 0,
            local_settings: EndpointSettings::for_role(role),
            remote_settings: EndpointSettings::for_role(peer_role(role)),
            unacked_settings: 0,
            pending_settings_acks: 0,
            first_settings_received: false,
            first_settings_sent: false,
            client_preface_bytes: if role == H2Role::Server {
                0
            } else {
                CLIENT_CONNECTION_PREFACE.len()
            },
            client_preface_failed: false,
            goaway_sent: false,
            goaway_send_last: 0,
            goaway_received: false,
            goaway_recv_last: 0,
            goaway_recv_code: ErrorCode::NoError,
            assembler: HeadersAssembler {
                buffer: header_block_storage,
                used: 0,
                stream_id: 0,
                active: false,
                complete: false,
                limit: if max_header_block_size == 0 {
                    16_384
                } else {
                    usize::try_from(max_header_block_size).unwrap_or(usize::MAX)
                },
            },
        }
    }

    pub const fn role(&self) -> H2Role {
        self.role
    }

    pub const fn highest_local(&self) -> u32 {
        self.highest_local
    }

    pub const fn highest_remote(&self) -> u32 {
        self.highest_remote
    }

    pub const fn goaway_sent(&self) -> bool {
        self.goaway_sent
    }

    pub const fn goaway_send_last(&self) -> u32 {
        self.goaway_send_last
    }

    pub const fn goaway_received(&self) -> bool {
        self.goaway_received
    }

    pub const fn goaway_recv_last(&self) -> u32 {
        self.goaway_recv_last
    }

    pub const fn goaway_recv_code(&self) -> ErrorCode {
        self.goaway_recv_code
    }

    pub const fn used_slots(&self) -> usize {
        self.used
    }

    // NOTE: a worker pool recycles the slot and block storage between connections; this
    // hand-off is only legal once no stream owns a slot and no header block is in flight,
    // so the array can be re-filled for its next owner without touching peer-visible state.
    pub fn take_storage(&mut self) -> Option<(&'a mut [StreamSlot], &'a mut [u8])> {
        if self.active_streams != 0 || self.assembler.active || self.assembler.complete {
            return None;
        }
        let slots = core::mem::take(&mut self.slots);
        let header_block = core::mem::take(&mut self.assembler.buffer);
        self.used = 0;
        self.assembler.reset();
        Some((slots, header_block))
    }

    pub fn install_storage(&mut self, slots: &'a mut [StreamSlot], header_block: &'a mut [u8]) {
        slots.fill(StreamSlot::EMPTY);
        self.slots = slots;
        self.used = 0;
        self.active_streams = 0;
        self.assembler.buffer = header_block;
        self.assembler.reset();
    }

    pub const fn slot_capacity(&self) -> usize {
        self.slots.len()
    }

    pub const fn is_assembling_headers(&self) -> bool {
        self.assembler.active
    }

    pub fn assembled_header_block(&self) -> Option<(u32, &[u8])> {
        if !self.assembler.complete {
            return None;
        }
        Some((
            self.assembler.stream_id,
            &self.assembler.buffer[..self.assembler.used],
        ))
    }

    pub fn reset_header_assembler(&mut self) {
        self.assembler.reset();
    }

    pub fn active_count(&self) -> usize {
        self.active_streams
    }

    pub fn find(&self, id: u32) -> Option<&Stream> {
        self.slots[..self.used]
            .iter()
            .filter_map(StreamSlot::stream)
            .find(|stream| stream.id == id)
    }

    pub fn send_window_mut(&mut self, id: u32) -> Option<&mut FlowWindow> {
        let index = self.find_index(id)?;
        Some(&mut self.slots[index].stream.as_mut()?.send_window)
    }

    pub fn receive_window_mut(&mut self, id: u32) -> Option<&mut FlowWindow> {
        let index = self.find_index(id)?;
        Some(&mut self.slots[index].stream.as_mut()?.receive_window)
    }

    pub fn close_stream(&mut self, id: u32) -> bool {
        let Some(index) = self.find_index(id) else {
            return false;
        };
        let stream = self.slots[index].stream.as_mut().unwrap();
        if is_active(stream.state) {
            self.active_streams -= 1;
        }
        stream.state = StreamState::Closed;
        stream.locally_reset = true;
        true
    }

    pub fn reserve_local(&mut self, id: u32) -> Result<(), StreamTableError> {
        self.reserve(id, true)
    }

    pub fn reserve_remote(&mut self, id: u32) -> Result<(), StreamTableError> {
        self.reserve(id, false)
    }

    pub fn on_frame(
        &mut self,
        header: FrameHeader,
        direction: FrameDirection,
        fragment: &[u8],
    ) -> StreamAction {
        let kind = header.frame_type;
        if self.client_preface_failed {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }
        if direction == FrameDirection::Received {
            if self.role == H2Role::Server && !self.client_preface_complete() {
                return StreamAction::connection_error(ErrorCode::ProtocolError);
            }
            if !self.first_settings_received && kind != FrameType::Settings {
                return StreamAction::connection_error(ErrorCode::ProtocolError);
            }
        } else if !self.first_settings_sent && kind != FrameType::Settings {
            return StreamAction::REJECT;
        }

        if direction == FrameDirection::Received {
            if self.assembler.active {
                if kind != FrameType::Continuation || header.stream_id != self.assembler.stream_id {
                    return StreamAction::connection_error(ErrorCode::ProtocolError);
                }
                if usize::try_from(header.payload_length).ok() != Some(fragment.len()) {
                    return StreamAction::connection_error(ErrorCode::FrameSizeError);
                }
                if !self.assembler.append(
                    header.stream_id,
                    fragment,
                    header.flags & crate::frame::CONTINUATION_FLAG_END_HEADERS != 0,
                ) {
                    return StreamAction::connection_error(ErrorCode::CompressionError);
                }
                return StreamAction::ACCEPT;
            }
            if kind == FrameType::Continuation {
                return StreamAction::connection_error(ErrorCode::ProtocolError);
            }
            if kind == FrameType::Headers {
                self.assembler.reset();
            }
        }

        if matches!(kind, FrameType::Unknown(_)) {
            return StreamAction::IGNORE;
        }

        if kind == FrameType::Settings {
            return self.process_settings_frame(header, fragment, direction);
        }

        if matches!(
            kind,
            FrameType::Settings | FrameType::Ping | FrameType::GoAway
        ) {
            return self.on_connection_frame(header);
        }

        if kind == FrameType::WindowUpdate && header.stream_id == 0 {
            return if header.payload_length == 4 {
                StreamAction::ACCEPT
            } else {
                StreamAction::connection_error(ErrorCode::FrameSizeError)
            };
        }

        if header.stream_id == 0 || header.stream_id > MAX_STREAM_ID {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }

        if !self.frame_length_valid(header) {
            return StreamAction::connection_error(ErrorCode::FrameSizeError);
        }

        if kind == FrameType::PushPromise {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }

        let is_local = stream_is_local_id(self.role, header.stream_id);
        if self.goaway_sent
            && direction == FrameDirection::Received
            && !is_local
            && header.stream_id > self.goaway_send_last
        {
            if kind == FrameType::Headers
                && header.flags & HEADERS_FLAG_END_HEADERS == 0
                && !self.assembler.begin(header.stream_id, fragment)
            {
                return StreamAction::connection_error(ErrorCode::CompressionError);
            }
            return StreamAction::IGNORE;
        }

        let action = if let Some(index) = self.find_index(header.stream_id) {
            let current = self.slots[index].stream.unwrap();
            if current.locally_reset
                && direction == FrameDirection::Received
                && !matches!(kind, FrameType::Headers | FrameType::Continuation)
            {
                return StreamAction::IGNORE;
            }
            let transition = stream_state_on_frame(current.state, kind, header.flags, direction);
            if transition.action.kind == StreamActionKind::Accept {
                if is_active(current.state) != is_active(transition.next_state) {
                    if is_active(transition.next_state) {
                        self.active_streams += 1;
                    } else {
                        self.active_streams -= 1;
                    }
                }
                self.slots[index].stream.as_mut().unwrap().state = transition.next_state;
            }
            transition.action
        } else {
            self.on_new_stream(header, direction, is_local)
        };

        if matches!(
            action.kind,
            StreamActionKind::Accept | StreamActionKind::SendRst | StreamActionKind::Ignore
        ) && direction == FrameDirection::Received
            && kind == FrameType::Headers
            && header.flags & HEADERS_FLAG_END_HEADERS == 0
            && !self.assembler.begin(header.stream_id, fragment)
        {
            return StreamAction::connection_error(ErrorCode::CompressionError);
        }
        action
    }

    pub fn on_goaway_sent(&mut self, last_stream_id: u32) -> Result<(), StreamTableError> {
        if last_stream_id > MAX_STREAM_ID {
            return Err(StreamTableError::StreamIdOutOfRange);
        }
        if self.goaway_sent && last_stream_id > self.goaway_send_last {
            return Err(StreamTableError::GoAwayLastStreamIdIncreased);
        }

        self.goaway_sent = true;
        self.goaway_send_last = last_stream_id;
        for slot in &mut self.slots[..self.used] {
            let Some(stream) = slot.stream.as_mut() else {
                continue;
            };
            let remote = !stream_is_local_id(self.role, stream.id);
            if remote && stream.id > last_stream_id {
                if is_active(stream.state) {
                    self.active_streams -= 1;
                }
                stream.state = StreamState::Closed;
            }
        }
        Ok(())
    }

    pub fn on_goaway_received(&mut self, header: FrameHeader, payload: &[u8]) -> StreamAction {
        if header.frame_type != FrameType::GoAway {
            return StreamAction::REJECT;
        }
        if header.stream_id != 0 {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }
        if usize::try_from(header.payload_length).ok() != Some(payload.len())
            || payload.len() < GOAWAY_FIXED_LEN
        {
            return StreamAction::connection_error(ErrorCode::FrameSizeError);
        }
        let Ok(decoded) = decode_goaway_payload(header, payload) else {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        };
        if decoded.last_stream_id > MAX_STREAM_ID {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }
        if self.goaway_received && decoded.last_stream_id > self.goaway_recv_last {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }

        self.goaway_received = true;
        self.goaway_recv_last = decoded.last_stream_id;
        self.goaway_recv_code = decoded.error_code;
        StreamAction::ACCEPT
    }

    fn on_connection_frame(&self, header: FrameHeader) -> StreamAction {
        if header.stream_id != 0 {
            return StreamAction::connection_error(ErrorCode::ProtocolError);
        }
        match header.frame_type {
            FrameType::Ping if header.payload_length != PING_PAYLOAD_LEN as u32 => {
                StreamAction::connection_error(ErrorCode::FrameSizeError)
            }
            FrameType::GoAway if header.payload_length < GOAWAY_FIXED_LEN as u32 => {
                StreamAction::connection_error(ErrorCode::FrameSizeError)
            }
            _ => StreamAction::ACCEPT,
        }
    }

    fn frame_length_valid(&self, header: FrameHeader) -> bool {
        match header.frame_type {
            FrameType::Priority => header.payload_length == PRIORITY_PAYLOAD_LEN as u32,
            FrameType::RstStream => header.payload_length == RST_STREAM_PAYLOAD_LEN as u32,
            FrameType::WindowUpdate => header.payload_length == 4,
            _ => true,
        }
    }
}

const fn peer_role(role: H2Role) -> H2Role {
    match role {
        H2Role::Client => H2Role::Server,
        H2Role::Server => H2Role::Client,
    }
}

pub fn stream_is_local_id(role: H2Role, id: u32) -> bool {
    match role {
        H2Role::Client => id & 1 == 1,
        H2Role::Server => id & 1 == 0,
    }
}
