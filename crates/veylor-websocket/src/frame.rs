use crate::close::CloseError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpCode {
    Continuation,
    Text,
    Binary,
    Close,
    Ping,
    Pong,
}

impl OpCode {
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x0 => Some(Self::Continuation),
            0x1 => Some(Self::Text),
            0x2 => Some(Self::Binary),
            0x8 => Some(Self::Close),
            0x9 => Some(Self::Ping),
            0xa => Some(Self::Pong),
            _ => None,
        }
    }

    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Continuation => 0x0,
            Self::Text => 0x1,
            Self::Binary => 0x2,
            Self::Close => 0x8,
            Self::Ping => 0x9,
            Self::Pong => 0xa,
        }
    }

    pub const fn is_control(self) -> bool {
        matches!(self, Self::Close | Self::Ping | Self::Pong)
    }

    pub const fn is_data(self) -> bool {
        matches!(self, Self::Continuation | Self::Text | Self::Binary)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    pub fin: bool,
    pub opcode: OpCode,
    pub mask: Option<[u8; 4]>,
    pub payload_len: u64,
    pub header_len: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameError {
    ReservedBits,
    ReservedOpcode(u8),
    ControlTooLong,
    FragmentedControl,
    NonMinimalLength,
    LengthHighBit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncodeError {
    BufferTooSmall { required: usize },
    ControlTooLong,
    FragmentedControl,
    PayloadTooLarge,
    Protocol(&'static str),
    Close(CloseError),
}

pub fn parse_frame_header(input: &[u8]) -> Result<Option<FrameHeader>, FrameError> {
    if input.len() < 2 {
        return Ok(None);
    }
    let first = input[0];
    let second = input[1];
    if first & 0x70 != 0 {
        return Err(FrameError::ReservedBits);
    }
    let opcode = OpCode::from_u8(first & 0x0f).ok_or(FrameError::ReservedOpcode(first & 0x0f))?;
    let fin = first & 0x80 != 0;
    let masked = second & 0x80 != 0;
    let length_code = second & 0x7f;

    let (payload_len, length_bytes) = match length_code {
        126 => {
            if input.len() < 4 {
                return Ok(None);
            }
            let length = u16::from_be_bytes([input[2], input[3]]) as u64;
            if length < 126 {
                return Err(FrameError::NonMinimalLength);
            }
            (length, 2)
        }
        127 => {
            if input.len() < 10 {
                return Ok(None);
            }
            let length = u64::from_be_bytes([
                input[2], input[3], input[4], input[5], input[6], input[7], input[8], input[9],
            ]);
            if length & (1 << 63) != 0 {
                return Err(FrameError::LengthHighBit);
            }
            if length < 65_536 {
                return Err(FrameError::NonMinimalLength);
            }
            (length, 8)
        }
        length => (u64::from(length), 0),
    };

    if opcode.is_control() {
        if payload_len > 125 {
            return Err(FrameError::ControlTooLong);
        }
        if !fin {
            return Err(FrameError::FragmentedControl);
        }
    }

    let mask_start = 2 + length_bytes;
    let header_len = mask_start + if masked { 4 } else { 0 };
    if input.len() < header_len {
        return Ok(None);
    }
    let mask = if masked {
        Some([
            input[mask_start],
            input[mask_start + 1],
            input[mask_start + 2],
            input[mask_start + 3],
        ])
    } else {
        None
    };

    Ok(Some(FrameHeader {
        fin,
        opcode,
        mask,
        payload_len,
        header_len,
    }))
}

pub fn encode_frame_header(
    fin: bool,
    opcode: OpCode,
    mask: Option<[u8; 4]>,
    payload_len: usize,
    out: &mut [u8],
) -> Result<usize, EncodeError> {
    if opcode.is_control() {
        if payload_len > 125 {
            return Err(EncodeError::ControlTooLong);
        }
        if !fin {
            return Err(EncodeError::FragmentedControl);
        }
    }
    let length = u64::try_from(payload_len).map_err(|_| EncodeError::PayloadTooLarge)?;

    let length_bytes = if length <= 125 {
        0
    } else if length <= 65_535 {
        2
    } else {
        8
    };
    let required = 2 + length_bytes + if mask.is_some() { 4 } else { 0 };
    if out.len() < required {
        return Err(EncodeError::BufferTooSmall { required });
    }

    out[0] = (if fin { 0x80 } else { 0 }) | opcode.as_u8();
    out[1] = if mask.is_some() { 0x80 } else { 0 };
    match length_bytes {
        0 => out[1] |= length as u8,
        2 => {
            out[1] |= 126;
            out[2..4].copy_from_slice(&(length as u16).to_be_bytes());
        }
        _ => {
            out[1] |= 127;
            out[2..10].copy_from_slice(&length.to_be_bytes());
        }
    }
    if let Some(mask) = mask {
        out[2 + length_bytes..required].copy_from_slice(&mask);
    }
    Ok(required)
}

pub fn encode_frame(
    fin: bool,
    opcode: OpCode,
    mask: Option<[u8; 4]>,
    payload: &[u8],
    out: &mut [u8],
) -> Result<usize, EncodeError> {
    let header_len = encode_frame_header(fin, opcode, mask, payload.len(), out)?;
    let required = header_len + payload.len();
    if out.len() < required {
        return Err(EncodeError::BufferTooSmall { required });
    }
    out[header_len..required].copy_from_slice(payload);
    if let Some(mask) = mask {
        apply_mask(&mut out[header_len..required], mask, 0);
    }
    Ok(required)
}

pub fn apply_mask(payload: &mut [u8], mask: [u8; 4], offset: usize) {
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte ^= mask[(offset + index) & 3];
    }
}
