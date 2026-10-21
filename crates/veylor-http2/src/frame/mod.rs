#[cfg(test)]
mod tests;

pub const FRAME_HEADER_LEN: usize = 9;
pub const MIN_MAX_FRAME_SIZE: u32 = 16_384;
pub const MAX_FRAME_SIZE: u32 = 0x00ff_ffff;
pub const DEFAULT_MAX_FRAME_SIZE: u32 = MIN_MAX_FRAME_SIZE;
pub const MAX_STREAM_ID: u32 = 0x7fff_ffff;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameType {
    Data,
    Headers,
    Priority,
    RstStream,
    Settings,
    PushPromise,
    Ping,
    GoAway,
    WindowUpdate,
    Continuation,
    Unknown(u8),
}

impl From<u8> for FrameType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Data,
            1 => Self::Headers,
            2 => Self::Priority,
            3 => Self::RstStream,
            4 => Self::Settings,
            5 => Self::PushPromise,
            6 => Self::Ping,
            7 => Self::GoAway,
            8 => Self::WindowUpdate,
            9 => Self::Continuation,
            other => Self::Unknown(other),
        }
    }
}

impl FrameType {
    fn wire_value(self) -> u8 {
        match self {
            Self::Data => 0,
            Self::Headers => 1,
            Self::Priority => 2,
            Self::RstStream => 3,
            Self::Settings => 4,
            Self::PushPromise => 5,
            Self::Ping => 6,
            Self::GoAway => 7,
            Self::WindowUpdate => 8,
            Self::Continuation => 9,
            Self::Unknown(value) => value,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    pub payload_length: u32,
    pub frame_type: FrameType,
    pub flags: u8,
    pub stream_id: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameHeaderError {
    InputTooShort,
    OutputTooShort,
    InvalidMaxFrameSize,
    FrameTooLarge,
    StreamIdOutOfRange,
}

pub fn decode_frame_header(
    input: &[u8],
    max_frame_size: u32,
) -> Result<FrameHeader, FrameHeaderError> {
    validate_max_frame_size(max_frame_size)?;
    if input.len() < FRAME_HEADER_LEN {
        return Err(FrameHeaderError::InputTooShort);
    }

    let payload_length = u32::from(input[0]) << 16 | u32::from(input[1]) << 8 | u32::from(input[2]);
    if payload_length > max_frame_size {
        return Err(FrameHeaderError::FrameTooLarge);
    }

    let stream_id = u32::from(input[5] & 0x7f) << 24
        | u32::from(input[6]) << 16
        | u32::from(input[7]) << 8
        | u32::from(input[8]);

    Ok(FrameHeader {
        payload_length,
        frame_type: FrameType::from(input[3]),
        flags: input[4],
        stream_id,
    })
}

pub fn encode_frame_header(
    header: FrameHeader,
    output: &mut [u8],
    max_frame_size: u32,
) -> Result<(), FrameHeaderError> {
    validate_max_frame_size(max_frame_size)?;
    if header.payload_length > MAX_FRAME_SIZE || header.payload_length > max_frame_size {
        return Err(FrameHeaderError::FrameTooLarge);
    }
    if header.stream_id > MAX_STREAM_ID {
        return Err(FrameHeaderError::StreamIdOutOfRange);
    }
    if output.len() < FRAME_HEADER_LEN {
        return Err(FrameHeaderError::OutputTooShort);
    }

    output[0] = (header.payload_length >> 16) as u8;
    output[1] = (header.payload_length >> 8) as u8;
    output[2] = header.payload_length as u8;
    output[3] = header.frame_type.wire_value();
    output[4] = header.flags;
    output[5] = (header.stream_id >> 24) as u8;
    output[6] = (header.stream_id >> 16) as u8;
    output[7] = (header.stream_id >> 8) as u8;
    output[8] = header.stream_id as u8;
    Ok(())
}

fn validate_max_frame_size(max_frame_size: u32) -> Result<(), FrameHeaderError> {
    if !(MIN_MAX_FRAME_SIZE..=MAX_FRAME_SIZE).contains(&max_frame_size) {
        return Err(FrameHeaderError::InvalidMaxFrameSize);
    }
    Ok(())
}
