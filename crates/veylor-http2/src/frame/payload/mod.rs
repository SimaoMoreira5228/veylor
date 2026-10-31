#[cfg(test)]
mod tests;

use super::{FrameHeader, FrameType, MAX_FRAME_SIZE, MAX_STREAM_ID};

pub const DATA_FLAG_END_STREAM: u8 = 0x01;
pub const DATA_FLAG_PADDED: u8 = 0x08;
pub const HEADERS_FLAG_END_STREAM: u8 = 0x01;
pub const HEADERS_FLAG_END_HEADERS: u8 = 0x04;
pub const HEADERS_FLAG_PADDED: u8 = 0x08;
pub const HEADERS_FLAG_PRIORITY: u8 = 0x20;
pub const PUSH_PROMISE_FLAG_END_HEADERS: u8 = 0x04;
pub const PUSH_PROMISE_FLAG_PADDED: u8 = 0x08;
pub const PING_FLAG_ACK: u8 = 0x01;
pub const SETTINGS_ACK: u8 = 0x01;
pub const CONTINUATION_FLAG_END_HEADERS: u8 = 0x04;

pub const PRIORITY_PAYLOAD_LEN: usize = 5;
pub const RST_STREAM_PAYLOAD_LEN: usize = 4;
pub const SETTINGS_ENTRY_LEN: usize = 6;
pub const PING_PAYLOAD_LEN: usize = 8;
pub const WINDOW_UPDATE_PAYLOAD_LEN: usize = 4;
pub const GOAWAY_FIXED_LEN: usize = 8;
pub const STREAM_IDENTIFIER_LEN: usize = 4;

pub const MAX_WINDOW_INCREMENT: u32 = 0x7fff_ffff;

pub const SETTINGS_HEADER_TABLE_SIZE: u16 = 0x1;
pub const SETTINGS_ENABLE_PUSH: u16 = 0x2;
pub const SETTINGS_MAX_CONCURRENT_STREAMS: u16 = 0x3;
pub const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 0x4;
pub const SETTINGS_MAX_FRAME_SIZE: u16 = 0x5;
pub const SETTINGS_MAX_HEADER_LIST_SIZE: u16 = 0x6;
pub const SETTINGS_ENABLE_CONNECT_PROTOCOL: u16 = 0x8;

pub const SETTINGS_ENABLE_PUSH_MAX: u32 = 1;
pub const SETTINGS_INITIAL_WINDOW_SIZE_MAX: u32 = MAX_WINDOW_INCREMENT;
pub const SETTINGS_MAX_FRAME_SIZE_MIN: u32 = 16_384;
pub const SETTINGS_MAX_FRAME_SIZE_MAX: u32 = MAX_FRAME_SIZE;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    NoError,
    ProtocolError,
    InternalError,
    FlowControlError,
    SettingsTimeout,
    StreamClosed,
    FrameSizeError,
    RefusedStream,
    Cancel,
    CompressionError,
    ConnectError,
    EnhanceYourCalm,
    InadequateSecurity,
    Http11Required,
    Unknown(u32),
}

impl From<u32> for ErrorCode {
    fn from(value: u32) -> Self {
        match value {
            0 => Self::NoError,
            1 => Self::ProtocolError,
            2 => Self::InternalError,
            3 => Self::FlowControlError,
            4 => Self::SettingsTimeout,
            5 => Self::StreamClosed,
            6 => Self::FrameSizeError,
            7 => Self::RefusedStream,
            8 => Self::Cancel,
            9 => Self::CompressionError,
            10 => Self::ConnectError,
            11 => Self::EnhanceYourCalm,
            12 => Self::InadequateSecurity,
            13 => Self::Http11Required,
            other => Self::Unknown(other),
        }
    }
}

impl ErrorCode {
    pub fn value(self) -> u32 {
        match self {
            Self::NoError => 0,
            Self::ProtocolError => 1,
            Self::InternalError => 2,
            Self::FlowControlError => 3,
            Self::SettingsTimeout => 4,
            Self::StreamClosed => 5,
            Self::FrameSizeError => 6,
            Self::RefusedStream => 7,
            Self::Cancel => 8,
            Self::CompressionError => 9,
            Self::ConnectError => 10,
            Self::EnhanceYourCalm => 11,
            Self::InadequateSecurity => 12,
            Self::Http11Required => 13,
            Self::Unknown(value) => value,
        }
    }

    fn wire_value(self) -> Result<u32, FramePayloadError> {
        let value = match self {
            Self::NoError => 0,
            Self::ProtocolError => 1,
            Self::InternalError => 2,
            Self::FlowControlError => 3,
            Self::SettingsTimeout => 4,
            Self::StreamClosed => 5,
            Self::FrameSizeError => 6,
            Self::RefusedStream => 7,
            Self::Cancel => 8,
            Self::CompressionError => 9,
            Self::ConnectError => 10,
            Self::EnhanceYourCalm => 11,
            Self::InadequateSecurity => 12,
            Self::Http11Required => 13,
            Self::Unknown(value) => value,
        };
        if value <= 13 && matches!(self, Self::Unknown(_)) {
            return Err(FramePayloadError::InvalidErrorCode);
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SettingsParameter {
    pub id: u16,
    pub value: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeadersPriority {
    pub exclusive: bool,
    pub dependency: u32,
    pub weight: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataPayload<'a> {
    pub data: &'a [u8],
    pub padding_length: Option<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeadersPayload<'a> {
    pub fragment: &'a [u8],
    pub priority: Option<HeadersPriority>,
    pub padding_length: Option<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PushPromisePayload<'a> {
    pub promised_stream_id: u32,
    pub fragment: &'a [u8],
    pub padding_length: Option<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GoAwayPayload<'a> {
    pub last_stream_id: u32,
    pub error_code: ErrorCode,
    pub debug_data: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowUpdatePayload {
    pub increment: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FramePayloadError {
    WrongFrameType,
    InvalidStreamId,
    PayloadLengthMismatch,
    InvalidPayloadLength,
    InvalidErrorCode,
    InvalidPadding,
    InvalidSetting,
    InvalidWindowIncrement,
    BufferTooSmall { required: usize },
}

pub fn decode_data_payload<'a>(
    header: FrameHeader,
    payload: &'a [u8],
) -> Result<DataPayload<'a>, FramePayloadError> {
    validate_header(header, FrameType::Data, Some(true), payload)?;

    if header.flags & DATA_FLAG_PADDED == 0 {
        return Ok(DataPayload {
            data: payload,
            padding_length: None,
        });
    }

    let Some((&padding_length, rest)) = payload.split_first() else {
        return Err(FramePayloadError::InvalidPadding);
    };
    let padding_length = usize::from(padding_length);
    if padding_length > rest.len() {
        return Err(FramePayloadError::InvalidPadding);
    }

    Ok(DataPayload {
        data: &rest[..rest.len() - padding_length],
        padding_length: Some(padding_length as u8),
    })
}

pub fn encode_data_payload(
    data: &[u8],
    padding_length: Option<u8>,
    output: &mut [u8],
) -> Result<usize, FramePayloadError> {
    let required = data
        .len()
        .checked_add(padding_length.map_or(0, |padding| usize::from(padding) + 1))
        .ok_or(FramePayloadError::InvalidPayloadLength)?;
    ensure_output(output, required)?;

    let mut offset = 0_usize;
    if let Some(padding_length) = padding_length {
        output[0] = padding_length;
        offset = 1;
    }
    output[offset..offset + data.len()].copy_from_slice(data);
    output[offset + data.len()..required].fill(0);
    Ok(required)
}

pub fn encode_priority_fields(
    priority: HeadersPriority,
    output: &mut [u8],
) -> Result<(), FramePayloadError> {
    ensure_output(output, PRIORITY_PAYLOAD_LEN)?;
    validate_stream_id(priority.dependency)?;

    output[0] = (priority.dependency >> 24) as u8;
    output[1] = (priority.dependency >> 16) as u8;
    output[2] = (priority.dependency >> 8) as u8;
    output[3] = priority.dependency as u8;
    if priority.exclusive {
        output[0] |= 0x80;
    }
    output[4] = priority.weight;
    Ok(())
}

pub fn decode_priority_fields(input: &[u8]) -> Result<HeadersPriority, FramePayloadError> {
    if input.len() != PRIORITY_PAYLOAD_LEN {
        return Err(FramePayloadError::InvalidPayloadLength);
    }

    Ok(HeadersPriority {
        exclusive: input[0] & 0x80 != 0,
        dependency: u32::from(input[0] & 0x7f) << 24
            | u32::from(input[1]) << 16
            | u32::from(input[2]) << 8
            | u32::from(input[3]),
        weight: input[4],
    })
}

pub fn decode_priority_payload(
    header: FrameHeader,
    payload: &[u8],
) -> Result<HeadersPriority, FramePayloadError> {
    validate_header(header, FrameType::Priority, Some(true), payload)?;
    decode_priority_fields(payload)
}

pub fn decode_headers_payload<'a>(
    header: FrameHeader,
    payload: &'a [u8],
) -> Result<HeadersPayload<'a>, FramePayloadError> {
    validate_header(header, FrameType::Headers, Some(true), payload)?;

    let mut offset = 0_usize;
    let padding_length = if header.flags & HEADERS_FLAG_PADDED != 0 {
        let Some(&padding_length) = payload.first() else {
            return Err(FramePayloadError::InvalidPadding);
        };
        offset = 1;
        Some(padding_length)
    } else {
        None
    };

    let priority = if header.flags & HEADERS_FLAG_PRIORITY != 0 {
        let end = offset
            .checked_add(PRIORITY_PAYLOAD_LEN)
            .ok_or(FramePayloadError::InvalidPayloadLength)?;
        let priority = payload
            .get(offset..end)
            .ok_or(FramePayloadError::InvalidPayloadLength)?;
        offset = end;
        Some(decode_priority_fields(priority)?)
    } else {
        None
    };

    let padding = usize::from(padding_length.unwrap_or(0));
    if padding > payload.len().saturating_sub(offset) {
        return Err(FramePayloadError::InvalidPadding);
    }
    let end = payload.len() - padding;

    Ok(HeadersPayload {
        fragment: &payload[offset..end],
        priority,
        padding_length,
    })
}

pub fn encode_headers_payload(
    fragment: &[u8],
    priority: Option<HeadersPriority>,
    padding_length: Option<u8>,
    output: &mut [u8],
) -> Result<usize, FramePayloadError> {
    let priority_length = priority.map_or(0, |_| PRIORITY_PAYLOAD_LEN);
    let padding_overhead = padding_length.map_or(0, |padding| usize::from(padding) + 1);
    let required = fragment
        .len()
        .checked_add(priority_length)
        .and_then(|length| length.checked_add(padding_overhead))
        .ok_or(FramePayloadError::InvalidPayloadLength)?;
    if let Some(priority) = priority {
        validate_stream_id(priority.dependency)?;
    }
    ensure_output(output, required)?;

    let mut offset = 0;
    if let Some(padding) = padding_length {
        output[offset] = padding;
        offset += 1;
    }
    if let Some(priority) = priority {
        encode_priority_fields(priority, &mut output[offset..])?;
        offset += PRIORITY_PAYLOAD_LEN;
    }
    output[offset..offset + fragment.len()].copy_from_slice(fragment);
    offset += fragment.len();
    output[offset..required].fill(0);
    Ok(required)
}

pub fn decode_rst_stream_payload(
    header: FrameHeader,
    payload: &[u8],
) -> Result<ErrorCode, FramePayloadError> {
    validate_header(header, FrameType::RstStream, Some(true), payload)?;
    if payload.len() != RST_STREAM_PAYLOAD_LEN {
        return Err(FramePayloadError::InvalidPayloadLength);
    }

    Ok(ErrorCode::from(read_u32(payload)))
}

pub fn encode_rst_stream_payload(
    error_code: ErrorCode,
    output: &mut [u8],
) -> Result<(), FramePayloadError> {
    let value = error_code.wire_value()?;
    ensure_output(output, RST_STREAM_PAYLOAD_LEN)?;
    write_u32(value, output);
    Ok(())
}

pub fn settings_parameter_valid(id: u16, value: u32) -> bool {
    match id {
        SETTINGS_ENABLE_PUSH => value <= SETTINGS_ENABLE_PUSH_MAX,
        SETTINGS_INITIAL_WINDOW_SIZE => value <= SETTINGS_INITIAL_WINDOW_SIZE_MAX,
        SETTINGS_MAX_FRAME_SIZE => {
            (SETTINGS_MAX_FRAME_SIZE_MIN..=SETTINGS_MAX_FRAME_SIZE_MAX).contains(&value)
        }
        SETTINGS_ENABLE_CONNECT_PROTOCOL => value <= 1,
        _ => true,
    }
}

pub fn encode_settings_payload(
    parameters: &[SettingsParameter],
    output: &mut [u8],
) -> Result<usize, FramePayloadError> {
    let required = parameters
        .len()
        .checked_mul(SETTINGS_ENTRY_LEN)
        .ok_or(FramePayloadError::InvalidPayloadLength)?;
    ensure_output(output, required)?;
    if parameters
        .iter()
        .any(|parameter| !settings_parameter_valid(parameter.id, parameter.value))
    {
        return Err(FramePayloadError::InvalidSetting);
    }

    for (parameter, entry) in parameters
        .iter()
        .zip(output.chunks_exact_mut(SETTINGS_ENTRY_LEN))
    {
        entry[0] = (parameter.id >> 8) as u8;
        entry[1] = parameter.id as u8;
        entry[2] = (parameter.value >> 24) as u8;
        entry[3] = (parameter.value >> 16) as u8;
        entry[4] = (parameter.value >> 8) as u8;
        entry[5] = parameter.value as u8;
    }
    Ok(required)
}

pub fn decode_settings_payload(
    header: FrameHeader,
    payload: &[u8],
    parameters_output: &mut [SettingsParameter],
) -> Result<usize, FramePayloadError> {
    validate_header(header, FrameType::Settings, Some(false), payload)?;
    if header.flags & SETTINGS_ACK != 0 {
        if !payload.is_empty() {
            return Err(FramePayloadError::InvalidPayloadLength);
        }
        return Ok(0);
    }
    if payload.len() % SETTINGS_ENTRY_LEN != 0 {
        return Err(FramePayloadError::InvalidPayloadLength);
    }

    let count = payload.len() / SETTINGS_ENTRY_LEN;
    if parameters_output.len() < count {
        return Err(FramePayloadError::BufferTooSmall { required: count });
    }

    for entry in payload.chunks_exact(SETTINGS_ENTRY_LEN) {
        let id = u16::from(entry[0]) << 8 | u16::from(entry[1]);
        let value = read_u32(&entry[2..]);
        if !settings_parameter_valid(id, value) {
            return Err(FramePayloadError::InvalidSetting);
        }
    }
    for (parameter, entry) in parameters_output
        .iter_mut()
        .zip(payload.chunks_exact(SETTINGS_ENTRY_LEN))
    {
        *parameter = SettingsParameter {
            id: u16::from(entry[0]) << 8 | u16::from(entry[1]),
            value: read_u32(&entry[2..]),
        };
    }
    Ok(count)
}

pub fn decode_ping_payload(
    header: FrameHeader,
    payload: &[u8],
) -> Result<[u8; PING_PAYLOAD_LEN], FramePayloadError> {
    validate_header(header, FrameType::Ping, Some(false), payload)?;
    let opaque: [u8; PING_PAYLOAD_LEN] = payload
        .try_into()
        .map_err(|_| FramePayloadError::InvalidPayloadLength)?;
    Ok(opaque)
}

pub fn encode_ping_payload(
    opaque: [u8; PING_PAYLOAD_LEN],
    output: &mut [u8],
) -> Result<(), FramePayloadError> {
    ensure_output(output, PING_PAYLOAD_LEN)?;
    output[..PING_PAYLOAD_LEN].copy_from_slice(&opaque);
    Ok(())
}

pub fn encode_goaway_payload(
    last_stream_id: u32,
    error_code: ErrorCode,
    debug_data: &[u8],
    output: &mut [u8],
) -> Result<usize, FramePayloadError> {
    validate_stream_id(last_stream_id)?;
    let error_code = error_code.wire_value()?;
    let required = GOAWAY_FIXED_LEN
        .checked_add(debug_data.len())
        .ok_or(FramePayloadError::InvalidPayloadLength)?;
    ensure_output(output, required)?;

    write_u32(last_stream_id, output);
    write_u32(error_code, &mut output[4..]);
    output[GOAWAY_FIXED_LEN..required].copy_from_slice(debug_data);
    Ok(required)
}

pub fn decode_goaway_payload<'a>(
    header: FrameHeader,
    payload: &'a [u8],
) -> Result<GoAwayPayload<'a>, FramePayloadError> {
    validate_header(header, FrameType::GoAway, Some(false), payload)?;
    if payload.len() < GOAWAY_FIXED_LEN {
        return Err(FramePayloadError::InvalidPayloadLength);
    }

    Ok(GoAwayPayload {
        last_stream_id: read_u32(payload) & MAX_STREAM_ID,
        error_code: ErrorCode::from(read_u32(&payload[4..])),
        debug_data: &payload[GOAWAY_FIXED_LEN..],
    })
}

pub fn encode_window_update_payload(
    increment: u32,
    output: &mut [u8],
) -> Result<(), FramePayloadError> {
    if increment == 0 || increment > MAX_WINDOW_INCREMENT {
        return Err(FramePayloadError::InvalidWindowIncrement);
    }
    ensure_output(output, WINDOW_UPDATE_PAYLOAD_LEN)?;
    write_u32(increment, output);
    Ok(())
}

pub fn decode_window_update_payload(
    header: FrameHeader,
    payload: &[u8],
) -> Result<WindowUpdatePayload, FramePayloadError> {
    validate_header(header, FrameType::WindowUpdate, None, payload)?;
    if payload.len() != WINDOW_UPDATE_PAYLOAD_LEN {
        return Err(FramePayloadError::InvalidPayloadLength);
    }

    let increment = read_u32(payload) & MAX_WINDOW_INCREMENT;
    if increment == 0 {
        return Err(FramePayloadError::InvalidWindowIncrement);
    }
    Ok(WindowUpdatePayload { increment })
}

pub fn encode_push_promise_payload(
    promised_stream_id: u32,
    fragment: &[u8],
    padding_length: Option<u8>,
    output: &mut [u8],
) -> Result<usize, FramePayloadError> {
    validate_stream_id(promised_stream_id)?;
    if promised_stream_id == 0 {
        return Err(FramePayloadError::InvalidStreamId);
    }
    let padding_overhead = padding_length.map_or(0, |padding| usize::from(padding) + 1);
    let required = STREAM_IDENTIFIER_LEN
        .checked_add(fragment.len())
        .and_then(|length| length.checked_add(padding_overhead))
        .ok_or(FramePayloadError::InvalidPayloadLength)?;
    ensure_output(output, required)?;

    let mut offset = 0;
    if let Some(padding) = padding_length {
        output[0] = padding;
        offset = 1;
    }
    write_u32(promised_stream_id, &mut output[offset..]);
    offset += STREAM_IDENTIFIER_LEN;
    output[offset..offset + fragment.len()].copy_from_slice(fragment);
    offset += fragment.len();
    output[offset..required].fill(0);
    Ok(required)
}

pub fn decode_push_promise_payload<'a>(
    header: FrameHeader,
    payload: &'a [u8],
) -> Result<PushPromisePayload<'a>, FramePayloadError> {
    validate_header(header, FrameType::PushPromise, Some(true), payload)?;

    let mut offset = 0;
    let padding_length = if header.flags & PUSH_PROMISE_FLAG_PADDED != 0 {
        let Some(&padding_length) = payload.first() else {
            return Err(FramePayloadError::InvalidPadding);
        };
        offset = 1;
        Some(padding_length)
    } else {
        None
    };
    let promised_stream_id_bytes = payload
        .get(offset..offset + STREAM_IDENTIFIER_LEN)
        .ok_or(FramePayloadError::InvalidPayloadLength)?;
    let promised_stream_id = read_u32(promised_stream_id_bytes) & MAX_STREAM_ID;
    validate_stream_id(promised_stream_id)?;
    if promised_stream_id == 0 {
        return Err(FramePayloadError::InvalidStreamId);
    }
    offset += STREAM_IDENTIFIER_LEN;

    let padding = usize::from(padding_length.unwrap_or(0));
    if padding > payload.len().saturating_sub(offset) {
        return Err(FramePayloadError::InvalidPadding);
    }
    let end = payload.len() - padding;
    Ok(PushPromisePayload {
        promised_stream_id,
        fragment: &payload[offset..end],
        padding_length,
    })
}

pub fn encode_continuation_payload(
    fragment: &[u8],
    output: &mut [u8],
) -> Result<usize, FramePayloadError> {
    ensure_output(output, fragment.len())?;
    output[..fragment.len()].copy_from_slice(fragment);
    Ok(fragment.len())
}

pub fn decode_continuation_payload(
    header: FrameHeader,
    payload: &[u8],
) -> Result<&[u8], FramePayloadError> {
    validate_header(header, FrameType::Continuation, Some(true), payload)?;
    Ok(payload)
}

fn validate_header(
    header: FrameHeader,
    expected_type: FrameType,
    stream_id_must_be_nonzero: Option<bool>,
    payload: &[u8],
) -> Result<(), FramePayloadError> {
    if header.payload_length > MAX_FRAME_SIZE {
        return Err(FramePayloadError::InvalidPayloadLength);
    }
    validate_stream_id(header.stream_id)?;
    if header.frame_type != expected_type {
        return Err(FramePayloadError::WrongFrameType);
    }
    match stream_id_must_be_nonzero {
        Some(true) if header.stream_id == 0 => return Err(FramePayloadError::InvalidStreamId),
        Some(false) if header.stream_id != 0 => return Err(FramePayloadError::InvalidStreamId),
        None if header.stream_id > MAX_STREAM_ID => {
            return Err(FramePayloadError::InvalidStreamId);
        }
        _ => {}
    }
    if u32::try_from(payload.len()).ok() != Some(header.payload_length) {
        return Err(FramePayloadError::PayloadLengthMismatch);
    }
    Ok(())
}

fn validate_stream_id(stream_id: u32) -> Result<(), FramePayloadError> {
    if stream_id > MAX_STREAM_ID {
        return Err(FramePayloadError::InvalidStreamId);
    }
    Ok(())
}

fn ensure_output(output: &[u8], required: usize) -> Result<(), FramePayloadError> {
    if required > MAX_FRAME_SIZE as usize {
        return Err(FramePayloadError::InvalidPayloadLength);
    }
    if output.len() < required {
        return Err(FramePayloadError::BufferTooSmall { required });
    }
    Ok(())
}

fn read_u32(input: &[u8]) -> u32 {
    u32::from(input[0]) << 24
        | u32::from(input[1]) << 16
        | u32::from(input[2]) << 8
        | u32::from(input[3])
}

fn write_u32(value: u32, output: &mut [u8]) {
    output[0] = (value >> 24) as u8;
    output[1] = (value >> 16) as u8;
    output[2] = (value >> 8) as u8;
    output[3] = value as u8;
}
