#[cfg(test)]
mod tests;

use super::ControlFrameWriter;
use super::DataFrameWriter;
use super::DataWriteError;
use super::FrameWriteError;
use super::H2FrameAction;
use super::H2FrameDriver;
use super::HeadersFrameWriter;
use super::HeadersWriteError;
use super::QueuedDataFrame;
use super::QueuedHeadersBlock;
use super::RequestFieldSectionError;
use super::encode_request_field_section;
use crate::flow_control::FlowWindowError;
use crate::frame::ErrorCode;
use crate::frame::FRAME_HEADER_LEN;
use crate::frame::FramePayloadError;
use crate::frame::GOAWAY_FIXED_LEN;
use crate::frame::MAX_STREAM_ID;
use crate::frame::RST_STREAM_PAYLOAD_LEN;
use crate::hpack::HpackFieldToEncode;
use crate::hpack::HpackTable;
use crate::stream::H2Role;
use crate::stream::StreamState;
use crate::stream::StreamTable;
use crate::stream::StreamTableError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientError {
    WrongRole,
    SettingsNotSent,
    NewStreamsClosed,
    MaxStreamIdExhausted,
    ConcurrencyExhausted,
    UnknownStream,
    StreamClosed,
    OutputTooSmall { required: usize },
    Request(RequestFieldSectionError),
    Headers(HeadersWriteError),
    Data(DataWriteError),
    Control(FrameWriteError),
    Stream(StreamTableError),
    Flow(FlowWindowError),
}

impl From<RequestFieldSectionError> for ClientError {
    fn from(error: RequestFieldSectionError) -> Self {
        Self::Request(error)
    }
}

impl From<HeadersWriteError> for ClientError {
    fn from(error: HeadersWriteError) -> Self {
        Self::Headers(error)
    }
}

impl From<DataWriteError> for ClientError {
    fn from(error: DataWriteError) -> Self {
        Self::Data(error)
    }
}

impl From<FrameWriteError> for ClientError {
    fn from(error: FrameWriteError) -> Self {
        Self::Control(error)
    }
}

impl From<StreamTableError> for ClientError {
    fn from(error: StreamTableError) -> Self {
        Self::Stream(error)
    }
}

impl From<FlowWindowError> for ClientError {
    fn from(error: FlowWindowError) -> Self {
        Self::Flow(error)
    }
}

pub struct StartedRequest<'output> {
    pub stream_id: u32,
    pub block_length: usize,
    pub frame: QueuedHeadersBlock,
    pub writer: HeadersFrameWriter<'output>,
}

pub struct SentData<'output> {
    pub frame: QueuedDataFrame,
    pub writer: DataFrameWriter<'output>,
}

pub struct H2ClientDriver<'borrow, 'streams, 'decoder, 'encoder> {
    driver: H2FrameDriver<'borrow, 'streams, 'decoder, 'encoder>,
}

impl<'borrow, 'streams, 'decoder, 'encoder> H2ClientDriver<'borrow, 'streams, 'decoder, 'encoder> {
    pub fn new(
        streams: &'borrow mut StreamTable<'streams>,
        hpack_decoder: &'borrow mut HpackTable<'decoder>,
        hpack_encoder: &'borrow mut HpackTable<'encoder>,
        settings_ack_limits: &'borrow mut [u32],
    ) -> Result<Self, ClientError> {
        if streams.role() != H2Role::Client {
            return Err(ClientError::WrongRole);
        }

        let driver =
            H2FrameDriver::new(streams, hpack_decoder, hpack_encoder, settings_ack_limits)?;

        Ok(Self { driver })
    }

    pub fn start_request<'output>(
        &mut self,
        fields: &[HpackFieldToEncode<'_>],
        block: &mut [u8],
        output: &'output mut [u8],
        end_stream: bool,
    ) -> Result<StartedRequest<'output>, ClientError> {
        if self.is_closed_for_new_streams() {
            return Err(ClientError::NewStreamsClosed);
        }
        if !self.driver.streams().first_settings_sent() {
            return Err(ClientError::SettingsNotSent);
        }

        let maximum = self
            .driver
            .streams()
            .remote_settings()
            .max_concurrent_streams;
        let maximum = usize::try_from(maximum).unwrap_or(usize::MAX);
        if self.driver.streams().active_count() >= maximum {
            return Err(ClientError::ConcurrencyExhausted);
        }

        let highest = self.driver.streams().highest_local();
        if highest >= MAX_STREAM_ID - 1 {
            return Err(ClientError::MaxStreamIdExhausted);
        }
        let stream_id = if highest == 0 { 1 } else { highest + 2 };

        let block_length = encode_request_field_section(&mut self.driver, fields, block)?;

        let peer_max_frame_size = self.driver.streams().remote_settings().max_frame_size;
        let peer_max_frame_size =
            usize::try_from(peer_max_frame_size).map_err(|_| HeadersWriteError::LengthOverflow)?;
        if peer_max_frame_size == 0 {
            return Err(ClientError::Headers(HeadersWriteError::LengthOverflow));
        }
        let frame_count = if block_length == 0 {
            1
        } else {
            1 + (block_length - 1) / peer_max_frame_size
        };
        let framing = frame_count
            .checked_mul(FRAME_HEADER_LEN)
            .ok_or(HeadersWriteError::LengthOverflow)?;
        let required = block_length
            .checked_add(framing)
            .ok_or(HeadersWriteError::LengthOverflow)?;
        if output.len() < required {
            return Err(ClientError::Headers(HeadersWriteError::OutputTooSmall {
                required,
            }));
        }

        let mut writer = HeadersFrameWriter::new(output);
        let frame = writer.queue_headers(
            &mut self.driver,
            stream_id,
            &block[..block_length],
            end_stream,
        )?;

        Ok(StartedRequest {
            stream_id,
            block_length,
            frame,
            writer,
        })
    }

    pub fn send_data<'output>(
        &mut self,
        stream_id: u32,
        input: &[u8],
        end_stream: bool,
        output: &'output mut [u8],
    ) -> Result<SentData<'output>, ClientError> {
        let mut writer = DataFrameWriter::new(output);
        let frame = writer.queue_data(&mut self.driver, stream_id, input, end_stream)?;

        Ok(SentData { frame, writer })
    }

    pub fn cancel<'output>(
        &mut self,
        stream_id: u32,
        output: &'output mut [u8],
    ) -> Result<ControlFrameWriter<'output>, ClientError> {
        if stream_id == 0 || stream_id > MAX_STREAM_ID {
            return Err(ClientError::Control(FrameWriteError::InvalidPayload(
                FramePayloadError::InvalidStreamId,
            )));
        }

        let Some(stream) = self.driver.streams().find(stream_id) else {
            return Err(ClientError::UnknownStream);
        };
        if stream.state == StreamState::Closed {
            return Err(ClientError::StreamClosed);
        }

        let required = FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN;
        if output.len() < required {
            return Err(ClientError::OutputTooSmall { required });
        }

        self.driver.streams_mut().close_stream(stream_id);

        let max_frame_size = self.driver.streams().remote_settings().max_frame_size;
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        writer.queue_action(
            H2FrameAction::SendReset {
                stream_id,
                error_code: ErrorCode::Cancel,
            },
            0,
        )?;

        Ok(writer)
    }

    pub fn goaway<'output>(
        &mut self,
        output: &'output mut [u8],
    ) -> Result<ControlFrameWriter<'output>, ClientError> {
        let required = FRAME_HEADER_LEN + GOAWAY_FIXED_LEN;
        if output.len() < required {
            return Err(ClientError::OutputTooSmall { required });
        }

        let last_stream_id = self.driver.streams().highest_remote();
        self.driver.streams_mut().on_goaway_sent(last_stream_id)?;

        let max_frame_size = self.driver.streams().remote_settings().max_frame_size;
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        writer.queue_action(
            H2FrameAction::ConnectionError {
                error_code: ErrorCode::NoError,
            },
            last_stream_id,
        )?;

        Ok(writer)
    }

    pub fn is_closed_for_new_streams(&self) -> bool {
        self.driver.streams().goaway_sent() || self.driver.streams().goaway_received()
    }

    pub fn driver(&self) -> &H2FrameDriver<'borrow, 'streams, 'decoder, 'encoder> {
        &self.driver
    }

    pub fn driver_mut(&mut self) -> &mut H2FrameDriver<'borrow, 'streams, 'decoder, 'encoder> {
        &mut self.driver
    }
}
