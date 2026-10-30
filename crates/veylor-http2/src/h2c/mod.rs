#[cfg(test)]
mod tests;

use super::connection::{
    ControlFrameWriter, FeedStatus, FrameWriteError, H2FrameAction, H2ServerDriver,
};
use super::flow_control::FlowWindowError;
use super::frame::{
    ErrorCode, FRAME_HEADER_LEN, GOAWAY_FIXED_LEN, SETTINGS_ENTRY_LEN, SETTINGS_HEADER_TABLE_SIZE,
    SETTINGS_MAX_CONCURRENT_STREAMS, SETTINGS_MAX_FRAME_SIZE, SETTINGS_MAX_HEADER_LIST_SIZE,
    SettingsParameter,
};
use super::hpack::{HpackHeader, HpackTable};
use super::stream::{H2Role, SettingsError, StreamTable};

pub const H2C_HEADER_TABLE_SIZE: u32 = 4_096;
pub const H2C_MAX_FRAME_SIZE: u32 = 16_384;
// NOTE: cap on the advertised peer limit, not the enforced one: the handshake sends
// min(this, live slot capacity), and every shipped table holds 32 slots, so the
// effective ceiling is 32 today (pinned by the managed 33rd-stream refusal test).
// This constant only binds if a larger table ever exists; keep it if the table grows.
pub const H2C_MAX_CONCURRENT_STREAMS: u32 = 100;
pub const H2C_SERVER_SETTINGS_LEN: usize = FRAME_HEADER_LEN + 4 * SETTINGS_ENTRY_LEN;
pub const H2C_GOAWAY_LEN: usize = FRAME_HEADER_LEN + GOAWAY_FIXED_LEN;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum H2cError {
    WrongRole,
    SettingsAlreadySent,
    EmptySettingsAckStorage,
    OutputTooSmall { required: usize },
    Settings(SettingsError),
    Control(FrameWriteError),
    Flow(FlowWindowError),
}

impl From<SettingsError> for H2cError {
    fn from(error: SettingsError) -> Self {
        Self::Settings(error)
    }
}

impl From<FrameWriteError> for H2cError {
    fn from(error: FrameWriteError) -> Self {
        Self::Control(error)
    }
}

impl From<FlowWindowError> for H2cError {
    fn from(error: FlowWindowError) -> Self {
        Self::Flow(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct H2cFeedResult {
    pub consumed: usize,
    pub actions: usize,
    pub headers: usize,
    pub output_used: usize,
    pub status: FeedStatus,
    pub error: Option<ErrorCode>,
}

pub struct H2cServerHandshake<'borrow, 'streams, 'decoder, 'encoder> {
    server: H2ServerDriver<'borrow, 'streams, 'decoder, 'encoder>,
}

impl<'borrow, 'streams, 'decoder, 'encoder>
    H2cServerHandshake<'borrow, 'streams, 'decoder, 'encoder>
{
    pub fn new(
        streams: &'borrow mut StreamTable<'streams>,
        decoder: &'borrow mut HpackTable<'decoder>,
        encoder: &'borrow mut HpackTable<'encoder>,
        settings_ack_limits: &'borrow mut [u32],
    ) -> Result<Self, H2cError> {
        if streams.role() != H2Role::Server {
            return Err(H2cError::WrongRole);
        }
        if settings_ack_limits.is_empty() {
            return Err(H2cError::EmptySettingsAckStorage);
        }
        let server = H2ServerDriver::new(streams, decoder, encoder, settings_ack_limits)
            .map_err(|_| H2cError::WrongRole)?;
        Ok(Self { server })
    }

    pub fn server_settings<'output>(
        &mut self,
        output: &'output mut [u8],
    ) -> Result<ControlFrameWriter<'output>, H2cError> {
        if self.server.driver().streams().first_settings_sent() {
            return Err(H2cError::SettingsAlreadySent);
        }
        if output.len() < H2C_SERVER_SETTINGS_LEN {
            return Err(H2cError::OutputTooSmall {
                required: H2C_SERVER_SETTINGS_LEN,
            });
        }
        let max_frame_size = self
            .server
            .driver()
            .streams()
            .remote_settings()
            .max_frame_size;
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        let capacity = u32::try_from(self.server.driver().streams().slot_capacity());
        let capacity = capacity.unwrap_or(u32::MAX);
        let max_concurrent = H2C_MAX_CONCURRENT_STREAMS.min(capacity);
        // NOTE: advertise the table-sized ceiling, never the configured default alone: the slot
        // slice is the single source (STREAM_SLOTS at the live server), so a peer-legitimate open
        // under RFC 9113 §5.1.2 always fits and excess fails per stream with REFUSED_STREAM.
        // The header-list cap rides the same single source: the assembler only ever holds
        // header_block_limit bytes, so an oversize block is the sender's violation and the
        // server refuses its stream instead of tearing down the connection.
        let local = [
            SettingsParameter {
                id: SETTINGS_HEADER_TABLE_SIZE,
                value: H2C_HEADER_TABLE_SIZE,
            },
            SettingsParameter {
                id: SETTINGS_MAX_FRAME_SIZE,
                value: H2C_MAX_FRAME_SIZE,
            },
            SettingsParameter {
                id: SETTINGS_MAX_CONCURRENT_STREAMS,
                value: max_concurrent,
            },
            SettingsParameter {
                id: SETTINGS_MAX_HEADER_LIST_SIZE,
                value: self.server.driver().streams().header_block_limit(),
            },
        ];
        writer.queue_settings(&local)?;
        self.server
            .driver_mut()
            .register_fully_written_local_settings(&local)?;
        Ok(writer)
    }

    pub fn feed<'input>(
        &mut self,
        input: &'input [u8],
        output: &mut [u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        headers: &mut [HpackHeader],
    ) -> H2cFeedResult {
        let result = self.server.driver_mut().feed(input, actions, headers);
        let mut output_used = 0;
        if result.status == FeedStatus::ConnectionFailed {
            output_used = self.encode_goaway(output, result.error);
        }
        H2cFeedResult {
            consumed: result.consumed,
            actions: result.actions,
            headers: result.headers,
            output_used,
            status: result.status,
            error: result.error,
        }
    }

    pub fn is_complete(&self) -> bool {
        let streams = self.server.driver().streams();
        self.server.driver().failed().is_none()
            && streams.first_settings_sent()
            && streams.first_settings_received()
    }

    pub fn client_preface_complete(&self) -> bool {
        self.server.driver().streams().client_preface_complete()
    }

    pub fn failed(&self) -> Option<ErrorCode> {
        self.server.driver().failed()
    }

    pub fn server(&self) -> &H2ServerDriver<'borrow, 'streams, 'decoder, 'encoder> {
        &self.server
    }

    pub fn server_mut(&mut self) -> &mut H2ServerDriver<'borrow, 'streams, 'decoder, 'encoder> {
        &mut self.server
    }

    pub fn into_server(self) -> H2ServerDriver<'borrow, 'streams, 'decoder, 'encoder> {
        self.server
    }

    fn encode_goaway(&mut self, output: &mut [u8], error: Option<ErrorCode>) -> usize {
        let Some(error_code) = error else { return 0 };
        if output.len() < H2C_GOAWAY_LEN {
            return 0;
        }
        let last_stream_id = self.server.driver().streams().highest_remote();
        let max_frame_size = self
            .server
            .driver()
            .streams()
            .remote_settings()
            .max_frame_size;
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        writer
            .queue_action(
                H2FrameAction::ConnectionError { error_code },
                last_stream_id,
            )
            .unwrap_or(0)
    }
}
