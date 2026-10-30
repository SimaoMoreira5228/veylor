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
use super::ServerRequestError;
use super::ValidatedServerRequest;
use super::validate_server_request_fields;
use crate::flow_control::FlowWindowError;
use crate::frame::ErrorCode;
use crate::frame::FRAME_HEADER_LEN;
use crate::frame::FramePayloadError;
use crate::frame::GOAWAY_FIXED_LEN;
use crate::frame::MAX_STREAM_ID;
use crate::frame::RST_STREAM_PAYLOAD_LEN;
use crate::hpack::HpackError;
use crate::hpack::HpackFieldToEncode;
use crate::hpack::HpackHeader;
use crate::hpack::HpackTable;
use crate::hpack::encode_header_block;
use crate::stream::H2Role;
use crate::stream::StreamState;
use crate::stream::StreamTable;
use crate::stream::StreamTableError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerResponseError {
    MissingStatus,
    DuplicateStatus,
    PseudoHeaderAfterRegular,
    UnexpectedPseudoHeader,
    InvalidStatus,
    SwitchingProtocols,
    ProhibitedField,
    InvalidFieldName,
    InvalidFieldValue,
    InvalidContentLength,
    ContentLengthNotAllowed,
    InformationalEndedStream,
    Hpack(HpackError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerError {
    WrongRole,
    SettingsNotSent,
    NewStreamsClosed,
    RefusedStream,
    UnknownStream,
    StreamClosed,
    OutputTooSmall { required: usize },
    Request(ServerRequestError),
    Response(ServerResponseError),
    Headers(HeadersWriteError),
    Data(DataWriteError),
    Control(FrameWriteError),
    Stream(StreamTableError),
    Flow(FlowWindowError),
}

impl From<ServerRequestError> for ServerError {
    fn from(error: ServerRequestError) -> Self {
        Self::Request(error)
    }
}

impl From<ServerResponseError> for ServerError {
    fn from(error: ServerResponseError) -> Self {
        Self::Response(error)
    }
}

impl From<HpackError> for ServerResponseError {
    fn from(error: HpackError) -> Self {
        Self::Hpack(error)
    }
}

impl From<HeadersWriteError> for ServerError {
    fn from(error: HeadersWriteError) -> Self {
        Self::Headers(error)
    }
}

impl From<DataWriteError> for ServerError {
    fn from(error: DataWriteError) -> Self {
        Self::Data(error)
    }
}

impl From<FrameWriteError> for ServerError {
    fn from(error: FrameWriteError) -> Self {
        Self::Control(error)
    }
}

impl From<StreamTableError> for ServerError {
    fn from(error: StreamTableError) -> Self {
        Self::Stream(error)
    }
}

impl From<FlowWindowError> for ServerError {
    fn from(error: FlowWindowError) -> Self {
        Self::Flow(error)
    }
}

pub struct SentResponse<'output> {
    pub block_length: usize,
    pub frame: QueuedHeadersBlock,
    pub writer: HeadersFrameWriter<'output>,
}

pub struct SentServerData<'output> {
    pub frame: QueuedDataFrame,
    pub writer: DataFrameWriter<'output>,
}

pub struct H2ServerDriver<'borrow, 'streams, 'decoder, 'encoder> {
    driver: H2FrameDriver<'borrow, 'streams, 'decoder, 'encoder>,
}

impl<'borrow, 'streams, 'decoder, 'encoder> H2ServerDriver<'borrow, 'streams, 'decoder, 'encoder> {
    pub fn new(
        streams: &'borrow mut StreamTable<'streams>,
        hpack_decoder: &'borrow mut HpackTable<'decoder>,
        hpack_encoder: &'borrow mut HpackTable<'encoder>,
        settings_ack_limits: &'borrow mut [u32],
    ) -> Result<Self, ServerError> {
        if streams.role() != H2Role::Server {
            return Err(ServerError::WrongRole);
        }

        let driver =
            H2FrameDriver::new(streams, hpack_decoder, hpack_encoder, settings_ack_limits)?;

        Ok(Self { driver })
    }

    pub fn accept_request(
        &self,
        stream_id: u32,
        fields: &[HpackHeader],
    ) -> Result<ValidatedServerRequest<'_>, ServerError> {
        if self.driver.streams().goaway_sent()
            && stream_id > self.driver.streams().goaway_send_last()
        {
            return Err(ServerError::NewStreamsClosed);
        }

        let Some(stream) = self.driver.streams().find(stream_id) else {
            return Err(ServerError::UnknownStream);
        };

        if stream.state == StreamState::Closed {
            return Err(ServerError::StreamClosed);
        }

        let maximum = self
            .driver
            .streams()
            .local_settings()
            .max_concurrent_streams;
        let maximum = usize::try_from(maximum).unwrap_or(usize::MAX);

        if self.driver.streams().active_count() > maximum {
            return Err(ServerError::RefusedStream);
        }

        Ok(validate_server_request_fields(&self.driver, fields)?)
    }

    
    
    pub fn send_response<'output>(
        &mut self,
        stream_id: u32,
        fields: &[HpackFieldToEncode<'_>],
        block: &mut [u8],
        output: &'output mut [u8],
        end_stream: bool,
    ) -> Result<SentResponse<'output>, ServerError> {
        let Some(stream) = self.driver.streams().find(stream_id) else {
            return Err(ServerError::UnknownStream);
        };

        if stream.state == StreamState::Closed {
            return Err(ServerError::StreamClosed);
        }

        if !self.driver.streams().first_settings_sent() {
            return Err(ServerError::SettingsNotSent);
        }

        validate_response_fields(fields, end_stream)?;

        let block_length = encode_header_block(fields, self.driver.hpack_encoder_mut(), block)
            .map_err(ServerResponseError::Hpack)?;

        let peer_max_frame_size = self.driver.streams().remote_settings().max_frame_size;
        let peer_max_frame_size =
            usize::try_from(peer_max_frame_size).map_err(|_| HeadersWriteError::LengthOverflow)?;

        if peer_max_frame_size == 0 {
            return Err(ServerError::Headers(HeadersWriteError::LengthOverflow));
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
            return Err(ServerError::Headers(HeadersWriteError::OutputTooSmall {
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

        Ok(SentResponse {
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
    ) -> Result<SentServerData<'output>, ServerError> {
        let mut writer = DataFrameWriter::new(output);
        let frame = writer.queue_data(&mut self.driver, stream_id, input, end_stream)?;

        Ok(SentServerData { frame, writer })
    }

    pub fn refuse<'output>(
        &mut self,
        stream_id: u32,
        output: &'output mut [u8],
    ) -> Result<ControlFrameWriter<'output>, ServerError> {
        self.reset_with(stream_id, ErrorCode::RefusedStream, output)
    }

    pub fn rst_stream<'output>(
        &mut self,
        stream_id: u32,
        error_code: ErrorCode,
        output: &'output mut [u8],
    ) -> Result<ControlFrameWriter<'output>, ServerError> {
        self.reset_with(stream_id, error_code, output)
    }

    pub fn goaway_drain<'output>(
        &mut self,
        last_stream_id: u32,
        output: &'output mut [u8],
    ) -> Result<ControlFrameWriter<'output>, ServerError> {
        let required = FRAME_HEADER_LEN + GOAWAY_FIXED_LEN;

        if output.len() < required {
            return Err(ServerError::OutputTooSmall { required });
        }

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

    pub fn is_closing(&self) -> bool {
        self.driver.streams().goaway_sent()
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

    // NOTE: abuse-detection projection: the managed pump snapshots these per window; the
    // engine itself never trips, so host-driven users keep an unfiltered feed.
    pub const fn flood_counters(&self) -> (u32, u32) {
        self.driver.flood_counters()
    }

    fn reset_with<'output>(
        &mut self,
        stream_id: u32,
        error_code: ErrorCode,
        output: &'output mut [u8],
    ) -> Result<ControlFrameWriter<'output>, ServerError> {
        if stream_id == 0 || stream_id > MAX_STREAM_ID {
            return Err(ServerError::Control(FrameWriteError::InvalidPayload(
                FramePayloadError::InvalidStreamId,
            )));
        }

        let Some(stream) = self.driver.streams().find(stream_id) else {
            return Err(ServerError::UnknownStream);
        };

        if stream.state == StreamState::Closed {
            return Err(ServerError::StreamClosed);
        }

        let required = FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN;

        if output.len() < required {
            return Err(ServerError::OutputTooSmall { required });
        }

        self.driver.streams_mut().close_stream(stream_id);

        let max_frame_size = self.driver.streams().remote_settings().max_frame_size;
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        writer.queue_action(
            H2FrameAction::SendReset {
                stream_id,
                error_code,
            },
            0,
        )?;

        Ok(writer)
    }
}

// NOTE: single source for the H2 content-length shed rule (RFC 9113 section 8.2.2:
// a length-unknown Begin carries no content-length, and 204/304 never carry it while
// every other final status does). The managed H2 routes call this from send_full and
// send_file_head instead of inline status checks, so static, file, and native shapes
// shed together. The driver validator below keeps its wider 1xx bar (status < 200)
// beside this predicate: no 1xx sender flows through those call sites, so the two
// rules agree on every reachable status.
pub fn status_sheds_length(status: u16) -> bool {
    matches!(status, 204 | 304)
}

fn validate_response_fields(
    fields: &[HpackFieldToEncode<'_>],
    end_stream: bool,
) -> Result<(), ServerResponseError> {
    let mut regular_fields_started = false;
    let mut status = None;
    let mut content_length = None;

    for field in fields {
        validate_field_value(field.value)?;

        if field.name.first() == Some(&b':') {
            if regular_fields_started {
                return Err(ServerResponseError::PseudoHeaderAfterRegular);
            }

            if field.name != b":status" {
                return Err(ServerResponseError::UnexpectedPseudoHeader);
            }

            if status.is_some() {
                return Err(ServerResponseError::DuplicateStatus);
            }

            status = Some(parse_status(field.value)?);

            continue;
        }

        regular_fields_started = true;

        if !valid_field_name(field.name) {
            return Err(ServerResponseError::InvalidFieldName);
        }

        if is_connection_specific(field.name) {
            return Err(ServerResponseError::ProhibitedField);
        }

        if field.name == b"content-length" {
            let length = parse_content_length(field.value)
                .ok_or(ServerResponseError::InvalidContentLength)?;

            if content_length.is_some_and(|previous| previous != length) {
                return Err(ServerResponseError::InvalidContentLength);
            }

            content_length = Some(length);
        }
    }

    let status = status.ok_or(ServerResponseError::MissingStatus)?;

    if status == 101 {
        return Err(ServerResponseError::SwitchingProtocols);
    }

    if status < 200 && end_stream {
        return Err(ServerResponseError::InformationalEndedStream);
    }

    if (status < 200 || status == 204) && content_length.is_some() {
        return Err(ServerResponseError::ContentLengthNotAllowed);
    }

    Ok(())
}

fn parse_status(value: &[u8]) -> Result<u16, ServerResponseError> {
    if value.len() != 3 || !value.iter().all(u8::is_ascii_digit) {
        return Err(ServerResponseError::InvalidStatus);
    }

    let status = u16::from(value[0] - b'0') * 100
        + u16::from(value[1] - b'0') * 10
        + u16::from(value[2] - b'0');

    if !(100..=599).contains(&status) {
        return Err(ServerResponseError::InvalidStatus);
    }

    Ok(status)
}

fn parse_content_length(value: &[u8]) -> Option<u64> {
    if value.is_empty() {
        return None;
    }

    let mut length = 0_u64;

    for byte in value {
        if !byte.is_ascii_digit() {
            return None;
        }

        length = length
            .checked_mul(10)?
            .checked_add(u64::from(*byte - b'0'))?;
    }

    Some(length)
}

fn valid_field_name(name: &[u8]) -> bool {
    !name.is_empty() && name.iter().copied().all(is_lowercase_token)
}

fn is_lowercase_token(byte: u8) -> bool {
    byte.is_ascii_lowercase()
        || byte.is_ascii_digit()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn validate_field_value(value: &[u8]) -> Result<(), ServerResponseError> {
    if value.first().is_some_and(is_ows) || value.last().is_some_and(is_ows) {
        return Err(ServerResponseError::InvalidFieldValue);
    }

    if value.iter().any(|byte| {
        *byte == 0
            || *byte == b'\r'
            || *byte == b'\n'
            || *byte == 0x7f
            || (*byte < 0x20 && *byte != b'\t')
    }) {
        return Err(ServerResponseError::InvalidFieldValue);
    }

    Ok(())
}

fn is_ows(byte: &u8) -> bool {
    *byte == b' ' || *byte == b'\t'
}

fn is_connection_specific(name: &[u8]) -> bool {
    matches!(
        name,
        b"connection"
            | b"proxy-connection"
            | b"keep-alive"
            | b"transfer-encoding"
            | b"upgrade"
            | b"te"
    )
}
