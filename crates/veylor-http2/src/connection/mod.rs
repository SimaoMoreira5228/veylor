mod client;
mod client_request;
mod client_response;
mod frames;
mod output;
mod server;
mod server_request;
#[cfg(test)]
mod tests;

pub use client::{ClientError, H2ClientDriver, SentData, StartedRequest};
pub use client_request::{RequestFieldSectionError, encode_request_field_section};
pub use client_response::{
    ClientResponseHeaders, ClientResponsePhase, ResponseFieldSection, ResponseSectionError,
    ResponseSectionKind,
};
pub use output::{
    ControlFrameWriter, DataFrameWriter, DataWriteError, FrameWriteError, HeadersFrameWriter,
    HeadersWriteError, OutputProgress, QueuedDataFrame, QueuedHeadersBlock,
};
pub use server::{
    H2ServerDriver, SentResponse, SentServerData, ServerError, ServerResponseError,
    status_sheds_length,
};
pub use server_request::{
    ServerRequestError, ValidatedServerRequest, validate_server_request_fields,
};

use crate::flow_control::{DEFAULT_FLOW_WINDOW_SIZE, FlowWindow, FlowWindowError};
use crate::frame::{
    ErrorCode, FRAME_HEADER_LEN, FrameHeaderError, SettingsParameter, decode_frame_header,
};
use crate::hpack::{HpackHeader, HpackTable};
use crate::stream::{ClientPrefaceError, SettingsError, StreamTable};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeaderRange {
    pub start: usize,
    pub count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum H2FrameAction<'input> {
    Headers {
        stream_id: u32,
        headers: HeaderRange,
        end_stream: bool,
    },
    Data {
        stream_id: u32,
        data: &'input [u8],
        end_stream: bool,
    },
    StreamReset {
        stream_id: u32,
        error_code: ErrorCode,
    },
    GoAway {
        last_stream_id: u32,
        error_code: ErrorCode,
        debug_data: &'input [u8],
    },
    SettingsAckRequired,
    PingAck {
        opaque_data: [u8; 8],
    },
    SendReset {
        stream_id: u32,
        error_code: ErrorCode,
    },
    ConnectionError {
        error_code: ErrorCode,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeedStatus {
    NeedMoreInput,
    OutputFull,
    ConnectionFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeedResult {
    pub consumed: usize,
    pub actions: usize,
    pub headers: usize,
    pub status: FeedStatus,
    pub error: Option<ErrorCode>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeaderDisposition {
    Deliver,
    Reset(ErrorCode),
    Ignore,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingHeaders {
    stream_id: u32,
    end_stream: bool,
    disposition: HeaderDisposition,
}

pub struct H2FrameDriver<'stream, 'streams, 'decoder, 'encoder> {
    streams: &'stream mut StreamTable<'streams>,
    hpack_decoder: &'stream mut HpackTable<'decoder>,
    hpack_encoder: &'stream mut HpackTable<'encoder>,
    settings_ack_limits: &'stream mut [u32],
    settings_ack_head: usize,
    settings_ack_len: usize,
    connection_receive_window: FlowWindow,
    connection_send_window: FlowWindow,
    pending_headers: Option<PendingHeaders>,
    failed: Option<ErrorCode>,
}

impl<'stream, 'streams, 'decoder, 'encoder> H2FrameDriver<'stream, 'streams, 'decoder, 'encoder> {
    pub fn new(
        streams: &'stream mut StreamTable<'streams>,
        hpack_decoder: &'stream mut HpackTable<'decoder>,
        hpack_encoder: &'stream mut HpackTable<'encoder>,
        settings_ack_limits: &'stream mut [u32],
    ) -> Result<Self, FlowWindowError> {
        let connection_receive_window = FlowWindow::new(DEFAULT_FLOW_WINDOW_SIZE)?;
        let connection_send_window = FlowWindow::new(DEFAULT_FLOW_WINDOW_SIZE)?;

        hpack_encoder.set_peer_max_size(streams.remote_settings().header_table_size);

        Ok(Self {
            streams,
            hpack_decoder,
            hpack_encoder,
            settings_ack_limits,
            settings_ack_head: 0,
            settings_ack_len: 0,
            connection_receive_window,
            connection_send_window,
            pending_headers: None,
            failed: None,
        })
    }

    pub const fn connection_receive_window(&self) -> FlowWindow {
        self.connection_receive_window
    }

    pub const fn connection_send_window(&self) -> FlowWindow {
        self.connection_send_window
    }

    pub fn add_connection_receive_credit(&mut self, increment: u32) -> Result<(), FlowWindowError> {
        self.connection_receive_window.add(increment)
    }

    pub fn add_stream_receive_credit(
        &mut self,
        stream_id: u32,
        increment: u32,
    ) -> Result<(), FlowWindowError> {
        self.streams
            .receive_window_mut(stream_id)
            .ok_or(FlowWindowError::InvalidAmount)?
            .add(increment)
    }

    pub fn take_send_credit(&mut self, stream_id: u32, amount: i64) -> Result<(), FlowWindowError> {
        if amount < 0 {
            return Err(FlowWindowError::InvalidAmount);
        }
        if amount > self.connection_send_window.value() {
            return Err(FlowWindowError::InsufficientWindow);
        }
        let stream_window = self
            .streams
            .find(stream_id)
            .ok_or(FlowWindowError::InvalidAmount)?
            .send_window;
        if amount > stream_window.value() {
            return Err(FlowWindowError::InsufficientWindow);
        }
        self.connection_send_window.take(amount)?;
        self.streams
            .send_window_mut(stream_id)
            .ok_or(FlowWindowError::InvalidAmount)?
            .take(amount)
    }

    pub fn streams(&self) -> &StreamTable<'streams> {
        self.streams
    }

    pub fn streams_mut(&mut self) -> &mut StreamTable<'streams> {
        self.streams
    }

    pub fn hpack_encoder_mut(&mut self) -> &mut HpackTable<'encoder> {
        self.hpack_encoder
    }

    pub fn hpack_decoder(&self) -> &HpackTable<'decoder> {
        self.hpack_decoder
    }

    pub fn register_fully_written_local_settings(
        &mut self,
        parameters: &[SettingsParameter],
    ) -> Result<(), SettingsError> {
        if self.settings_ack_len == self.settings_ack_limits.len() {
            return Err(SettingsError::Capacity);
        }
        if usize::try_from(self.streams.unacked_settings()).ok() != Some(self.settings_ack_len) {
            return Err(SettingsError::UntrackedSettings);
        }

        self.streams.record_local_settings_sent(parameters)?;
        let tail =
            (self.settings_ack_head + self.settings_ack_len) % self.settings_ack_limits.len();
        self.settings_ack_limits[tail] = self.streams.local_settings().header_table_size;
        self.settings_ack_len += 1;
        Ok(())
    }

    pub fn resolve_header(&self, header: HpackHeader) -> Option<(&[u8], &[u8])> {
        Some((
            self.hpack_decoder.resolve(header.field.name)?,
            self.hpack_decoder.resolve(header.field.value)?,
        ))
    }

    pub fn failed(&self) -> Option<ErrorCode> {
        self.failed
    }

    pub fn feed<'input>(
        &mut self,
        input: &'input [u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        headers: &mut [HpackHeader],
    ) -> FeedResult {
        let mut result = FeedResult {
            consumed: 0,
            actions: 0,
            headers: 0,
            status: FeedStatus::NeedMoreInput,
            error: self.failed,
        };
        if let Some(error_code) = self.failed {
            result.status = FeedStatus::ConnectionFailed;
            result.error = Some(error_code);
            return result;
        }

        if !self.streams.client_preface_complete() {
            let preface_progress = self.streams.client_preface_progress();
            match self.streams.receive_client_preface_fragment(input) {
                Ok(consumed) => {
                    result.consumed = consumed;
                    if !self.streams.client_preface_complete() {
                        return result;
                    }
                }
                Err(error) => {
                    let error_code = match error {
                        ClientPrefaceError::WrongRole
                        | ClientPrefaceError::InvalidByte { .. }
                        | ClientPrefaceError::AlreadyComplete => ErrorCode::ProtocolError,
                    };
                    if let ClientPrefaceError::InvalidByte { offset } = error {
                        result.consumed = offset.saturating_sub(preface_progress).min(input.len());
                    }
                    return self.fail(error_code, actions, result);
                }
            }
        }

        while result.consumed < input.len() {
            if result.actions == actions.len() {
                result.status = FeedStatus::OutputFull;
                return result;
            }
            let frame_start = result.consumed;
            let remaining = &input[frame_start..];
            if remaining.len() < FRAME_HEADER_LEN {
                result.status = FeedStatus::NeedMoreInput;
                return result;
            }
            let max_frame_size = self.streams.local_settings().max_frame_size;
            let header = match decode_frame_header(remaining, max_frame_size) {
                Ok(header) => header,
                Err(FrameHeaderError::InputTooShort) => {
                    result.status = FeedStatus::NeedMoreInput;
                    return result;
                }
                Err(FrameHeaderError::FrameTooLarge) => {
                    return self.fail(ErrorCode::FrameSizeError, actions, result);
                }
                Err(FrameHeaderError::InvalidMaxFrameSize) => {
                    return self.fail(ErrorCode::InternalError, actions, result);
                }
                Err(_) => return self.fail(ErrorCode::ProtocolError, actions, result),
            };
            let payload_length = match usize::try_from(header.payload_length) {
                Ok(length) => length,
                Err(_) => return self.fail(ErrorCode::FrameSizeError, actions, result),
            };
            let Some(frame_length) = FRAME_HEADER_LEN.checked_add(payload_length) else {
                return self.fail(ErrorCode::FrameSizeError, actions, result);
            };
            if remaining.len() < frame_length {
                result.status = FeedStatus::NeedMoreInput;
                return result;
            }
            let payload = &remaining[FRAME_HEADER_LEN..frame_length];
            match self.process_frame(header, payload, actions, &mut result, headers) {
                Ok(()) => {
                    result.consumed = frame_start + frame_length;
                }
                Err(error_code) => {
                    result.consumed = frame_start + frame_length;
                    return self.fail(error_code, actions, result);
                }
            }
        }

        result.status = FeedStatus::NeedMoreInput;
        result
    }

    fn push_action<'input>(
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
        action: H2FrameAction<'input>,
    ) {
        actions[result.actions] = Some(action);
        result.actions += 1;
    }

    fn queue_reset<'input>(
        &mut self,
        stream_id: u32,
        error_code: ErrorCode,
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
    ) {
        self.streams.close_stream(stream_id);
        Self::push_action(
            actions,
            result,
            H2FrameAction::SendReset {
                stream_id,
                error_code,
            },
        );
    }

    fn fail<'input>(
        &mut self,
        error_code: ErrorCode,
        actions: &mut [Option<H2FrameAction<'input>>],
        mut result: FeedResult,
    ) -> FeedResult {
        self.failed = Some(error_code);
        if result.actions < actions.len() {
            Self::push_action(
                actions,
                &mut result,
                H2FrameAction::ConnectionError { error_code },
            );
        }
        result.status = FeedStatus::ConnectionFailed;
        result.error = Some(error_code);
        result
    }
}
