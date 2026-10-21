#[cfg(test)]
mod tests;

use crate::connection::H2FrameDriver;
use crate::hpack::HpackHeader;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseSectionKind {
    Informational,
    Final,
    Trailers,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientResponsePhase {
    AwaitingFinalHeaders,
    Content,
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseFieldSection<'headers> {
    pub kind: ResponseSectionKind,
    pub status: Option<u16>,
    pub fields: &'headers [HpackHeader],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseSectionError {
    InvalidHpackField,
    InvalidFieldName,
    InvalidFieldValue,
    PseudoHeaderAfterRegular,
    UnexpectedPseudoHeader,
    DuplicateStatus,
    MissingStatus,
    InvalidStatus,
    SwitchingProtocols,
    ProhibitedField,
    ContentLengthNotAllowed,
    ContentNotAllowed,
    InformationalEndedStream,
    TrailerNotFinal,
    UnexpectedHeaders,
    DataBeforeFinalHeaders,
    DataAfterComplete,
}

pub struct ClientResponseHeaders {
    phase: ClientResponsePhase,
    final_status: Option<u16>,
}

struct ValidatedFields {
    status: Option<u16>,
    content_length: bool,
}

impl ClientResponseHeaders {
    pub const fn new() -> Self {
        Self {
            phase: ClientResponsePhase::AwaitingFinalHeaders,
            final_status: None,
        }
    }

    pub const fn phase(&self) -> ClientResponsePhase {
        self.phase
    }

    pub const fn final_status(&self) -> Option<u16> {
        self.final_status
    }

    pub fn classify<'headers>(
        &mut self,
        driver: &H2FrameDriver<'_, '_, '_, '_>,
        fields: &'headers [HpackHeader],
        end_stream: bool,
    ) -> Result<ResponseFieldSection<'headers>, ResponseSectionError> {
        match self.phase {
            ClientResponsePhase::AwaitingFinalHeaders => {
                self.classify_response_headers(driver, fields, end_stream)
            }
            ClientResponsePhase::Content => self.classify_trailers(driver, fields, end_stream),
            ClientResponsePhase::Complete => Err(ResponseSectionError::UnexpectedHeaders),
        }
    }

    pub fn data_received(
        &mut self,
        payload_length: usize,
        end_stream: bool,
    ) -> Result<ClientResponsePhase, ResponseSectionError> {
        match self.phase {
            ClientResponsePhase::AwaitingFinalHeaders => {
                Err(ResponseSectionError::DataBeforeFinalHeaders)
            }
            ClientResponsePhase::Content => {
                if payload_length > 0 && matches!(self.final_status, Some(204 | 304)) {
                    return Err(ResponseSectionError::ContentNotAllowed);
                }
                if end_stream {
                    self.phase = ClientResponsePhase::Complete;
                }
                Ok(self.phase)
            }
            ClientResponsePhase::Complete => Err(ResponseSectionError::DataAfterComplete),
        }
    }

    fn classify_response_headers<'headers>(
        &mut self,
        driver: &H2FrameDriver<'_, '_, '_, '_>,
        fields: &'headers [HpackHeader],
        end_stream: bool,
    ) -> Result<ResponseFieldSection<'headers>, ResponseSectionError> {
        let validated = validate_fields(driver, fields, true)?;
        let status = validated
            .status
            .ok_or(ResponseSectionError::MissingStatus)?;
        if status == 101 {
            return Err(ResponseSectionError::SwitchingProtocols);
        }
        if status < 200 && end_stream {
            return Err(ResponseSectionError::InformationalEndedStream);
        }
        if (status < 200 || status == 204) && validated.content_length {
            return Err(ResponseSectionError::ContentLengthNotAllowed);
        }

        let kind = if status < 200 {
            ResponseSectionKind::Informational
        } else {
            ResponseSectionKind::Final
        };
        if kind == ResponseSectionKind::Final {
            self.final_status = Some(status);
            self.phase = if end_stream {
                ClientResponsePhase::Complete
            } else {
                ClientResponsePhase::Content
            };
        }

        Ok(ResponseFieldSection {
            kind,
            status: Some(status),
            fields,
        })
    }

    fn classify_trailers<'headers>(
        &mut self,
        driver: &H2FrameDriver<'_, '_, '_, '_>,
        fields: &'headers [HpackHeader],
        end_stream: bool,
    ) -> Result<ResponseFieldSection<'headers>, ResponseSectionError> {
        if !end_stream {
            return Err(ResponseSectionError::TrailerNotFinal);
        }
        validate_fields(driver, fields, false)?;

        self.phase = ClientResponsePhase::Complete;
        Ok(ResponseFieldSection {
            kind: ResponseSectionKind::Trailers,
            status: None,
            fields,
        })
    }
}

impl Default for ClientResponseHeaders {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_fields(
    driver: &H2FrameDriver<'_, '_, '_, '_>,
    fields: &[HpackHeader],
    expect_status: bool,
) -> Result<ValidatedFields, ResponseSectionError> {
    let mut regular_fields_started = false;
    let mut status = None;
    let mut content_length = false;

    for field in fields {
        let (name, value) = driver
            .resolve_header(*field)
            .ok_or(ResponseSectionError::InvalidHpackField)?;
        validate_field_value(value)?;

        if name.first() == Some(&b':') {
            if regular_fields_started {
                return Err(ResponseSectionError::PseudoHeaderAfterRegular);
            }
            if !expect_status || name != b":status" {
                return Err(ResponseSectionError::UnexpectedPseudoHeader);
            }
            if status.is_some() {
                return Err(ResponseSectionError::DuplicateStatus);
            }
            status = Some(parse_status(value)?);
            continue;
        }

        regular_fields_started = true;
        if !valid_field_name(name) {
            return Err(ResponseSectionError::InvalidFieldName);
        }
        if is_connection_specific(name) {
            return Err(ResponseSectionError::ProhibitedField);
        }
        if !expect_status && is_prohibited_trailer(name) {
            return Err(ResponseSectionError::ProhibitedField);
        }
        content_length |= name == b"content-length";
    }

    if expect_status && status.is_none() {
        return Err(ResponseSectionError::MissingStatus);
    }
    Ok(ValidatedFields {
        status,
        content_length,
    })
}

fn parse_status(value: &[u8]) -> Result<u16, ResponseSectionError> {
    if value.len() != 3 || !value.iter().all(u8::is_ascii_digit) {
        return Err(ResponseSectionError::InvalidStatus);
    }
    let status = u16::from(value[0] - b'0') * 100
        + u16::from(value[1] - b'0') * 10
        + u16::from(value[2] - b'0');
    if !(100..=599).contains(&status) {
        return Err(ResponseSectionError::InvalidStatus);
    }
    Ok(status)
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

fn validate_field_value(value: &[u8]) -> Result<(), ResponseSectionError> {
    if value.first().is_some_and(is_ows) || value.last().is_some_and(is_ows) {
        return Err(ResponseSectionError::InvalidFieldValue);
    }
    if value.iter().any(|byte| {
        *byte == 0
            || *byte == b'\r'
            || *byte == b'\n'
            || *byte == 0x7f
            || (*byte < 0x20 && *byte != b'\t')
    }) {
        return Err(ResponseSectionError::InvalidFieldValue);
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

fn is_prohibited_trailer(name: &[u8]) -> bool {
    is_connection_specific(name)
        || [
            b"authorization".as_slice(),
            b"cache-control",
            b"content-encoding",
            b"content-length",
            b"content-range",
            b"content-type",
            b"expect",
            b"host",
            b"max-forwards",
            b"proxy-authenticate",
            b"proxy-authorization",
            b"trailer",
            b"www-authenticate",
        ]
        .contains(&name)
}
