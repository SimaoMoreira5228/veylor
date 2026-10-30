#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CloseFrame<'a> {
    pub code: Option<u16>,
    pub reason: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseError {
    InvalidLength,
    InvalidCode(u16),
    InvalidReason,
    TooLong,
    BufferTooSmall { required: usize },
}

pub fn parse_close_payload(payload: &[u8]) -> Result<CloseFrame<'_>, CloseError> {
    if payload.is_empty() {
        return Ok(CloseFrame {
            code: None,
            reason: &[],
        });
    }
    if payload.len() == 1 {
        return Err(CloseError::InvalidLength);
    }
    let code = u16::from_be_bytes([payload[0], payload[1]]);
    if !valid_code(code) {
        return Err(CloseError::InvalidCode(code));
    }
    let reason = &payload[2..];
    if reason.len() > 123 {
        return Err(CloseError::TooLong);
    }
    if std::str::from_utf8(reason).is_err() {
        return Err(CloseError::InvalidReason);
    }
    Ok(CloseFrame {
        code: Some(code),
        reason,
    })
}

pub fn encode_close_payload(
    code: Option<u16>,
    reason: &[u8],
    out: &mut [u8],
) -> Result<usize, CloseError> {
    if reason.len() > 123 {
        return Err(CloseError::TooLong);
    }
    let Some(code) = code else {
        if reason.is_empty() {
            return Ok(0);
        }
        return Err(CloseError::InvalidReason);
    };
    if !valid_code(code) {
        return Err(CloseError::InvalidCode(code));
    }
    if std::str::from_utf8(reason).is_err() {
        return Err(CloseError::InvalidReason);
    }
    let required = 2 + reason.len();
    if out.len() < required {
        return Err(CloseError::BufferTooSmall { required });
    }
    out[..2].copy_from_slice(&code.to_be_bytes());
    out[2..required].copy_from_slice(reason);
    Ok(required)
}

fn valid_code(code: u16) -> bool {
    matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
}
