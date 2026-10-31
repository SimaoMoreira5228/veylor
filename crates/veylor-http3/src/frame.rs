#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettingsError {
    Truncated,
    Duplicate,
    Invalid,
}

pub const DATA: u64 = 0x0;
pub const HEADERS: u64 = 0x1;
pub const CANCEL_PUSH: u64 = 0x3;
pub const SETTINGS: u64 = 0x4;
pub const PUSH_PROMISE: u64 = 0x5;
pub const GOAWAY: u64 = 0x7;
pub const MAX_PUSH_ID: u64 = 0xd;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    pub kind: u64,
    pub length: u64,
}

pub fn encode_varint(value: u64, out: &mut Vec<u8>) {
    debug_assert!(value < 1 << 62);
    let length = varint_length(value);
    let mut buffer = [0u8; 8];
    for (index, byte) in buffer.iter_mut().take(length).enumerate() {
        *byte = (value >> (8 * (length - index - 1))) as u8;
    }
    buffer[0] |= match length {
        1 => 0,
        2 => 0x40,
        4 => 0x80,
        _ => 0xc0,
    };
    out.extend_from_slice(&buffer[..length]);
}

pub fn decode_varint(input: &[u8]) -> Option<(u64, usize)> {
    let first = *input.first()?;
    let length = 1usize << (first >> 6);
    if input.len() < length {
        return None;
    }
    let mut value = u64::from(first & 0x3f);
    for byte in &input[1..length] {
        value = (value << 8) | u64::from(*byte);
    }
    Some((value, length))
}

pub fn decode_frame_header(input: &[u8]) -> Option<(FrameHeader, usize)> {
    let (kind, used) = decode_varint(input)?;
    let (length, more) = decode_varint(&input[used..])?;
    Some((FrameHeader { kind, length }, used + more))
}

pub fn encode_frame(kind: u64, payload: &[u8], out: &mut Vec<u8>) {
    encode_varint(kind, out);
    encode_varint(payload.len() as u64, out);
    out.extend_from_slice(payload);
}

pub fn decode_settings(payload: &[u8]) -> Result<Vec<(u64, u64)>, SettingsError> {
    let mut settings = Vec::new();
    let mut offset = 0;
    while offset < payload.len() {
        let (id, used) = decode_varint(&payload[offset..]).ok_or(SettingsError::Truncated)?;
        offset += used;
        if matches!(id, 0x2..=0x5) {
            return Err(SettingsError::Invalid);
        }
        let (value, used) = decode_varint(&payload[offset..]).ok_or(SettingsError::Truncated)?;
        offset += used;
        if settings.iter().any(|(seen, _)| *seen == id) {
            return Err(SettingsError::Duplicate);
        }
        settings.push((id, value));
    }
    Ok(settings)
}

pub fn encode_settings(settings: &[(u64, u64)]) -> Vec<u8> {
    let mut payload = Vec::new();
    for (id, value) in settings {
        encode_varint(*id, &mut payload);
        encode_varint(*value, &mut payload);
    }
    payload
}

fn varint_length(value: u64) -> usize {
    if value < 1 << 6 {
        1
    } else if value < 1 << 14 {
        2
    } else if value < 1 << 30 {
        4
    } else {
        8
    }
}

#[cfg(test)]
mod tests {
    use super::{SettingsError, decode_settings, decode_varint, encode_settings, encode_varint};

    #[test]
    fn varints_round_trip() {
        for value in [
            0,
            63,
            64,
            16_383,
            16_384,
            1_073_741_823,
            1_073_741_824,
            (1u64 << 62) - 1,
        ] {
            let mut out = Vec::new();
            encode_varint(value, &mut out);
            assert_eq!(decode_varint(&out), Some((value, out.len())));
        }
    }

    #[test]
    fn settings_reject_duplicates_and_truncation() {
        let duplicate = encode_settings(&[(1, 1), (1, 2)]);
        assert_eq!(decode_settings(&duplicate), Err(SettingsError::Duplicate));
        assert_eq!(decode_settings(&[1]), Err(SettingsError::Truncated));
        assert_eq!(decode_settings(&[2, 0]), Err(SettingsError::Invalid));
    }
}
