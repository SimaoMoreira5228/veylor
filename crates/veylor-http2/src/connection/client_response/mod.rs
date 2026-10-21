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
    InvalidContentLength,
    ContentNotAllowed,
    BodyTooLong,
    ContentLengthMismatch,
    InformationalEndedStream,
    TrailerNotFinal,
    UnexpectedHeaders,
    DataBeforeFinalHeaders,
    DataAfterComplete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestMethod {
    Head,
    Connect,
    Other,
}

pub struct ClientResponseHeaders {
    phase: ClientResponsePhase,
    final_status: Option<u16>,
    method: RequestMethod,
    content_length: Option<u64>,
    remaining: u64,
}

struct ValidatedFields {
    status: Option<u16>,
    content_length: Option<u64>,
}

impl ClientResponseHeaders {
    pub const fn new() -> Self {
        Self {
            phase: ClientResponsePhase::AwaitingFinalHeaders,
            final_status: None,
            method: RequestMethod::Other,
            content_length: None,
            remaining: 0,
        }
    }

    pub const fn new_with_method(method: &[u8]) -> Self {
        Self {
            phase: ClientResponsePhase::AwaitingFinalHeaders,
            final_status: None,
            method: classify_method(method),
            content_length: None,
            remaining: 0,
        }
    }

    pub fn reset_with_method(&mut self, method: &[u8]) {
        self.phase = ClientResponsePhase::AwaitingFinalHeaders;
        self.final_status = None;
        self.method = classify_method(method);
        self.content_length = None;
        self.remaining = 0;
    }

    pub const fn phase(&self) -> ClientResponsePhase {
        self.phase
    }

    pub const fn final_status(&self) -> Option<u16> {
        self.final_status
    }

    pub const fn content_length(&self) -> Option<u64> {
        self.content_length
    }

    pub const fn remaining(&self) -> u64 {
        self.remaining
    }

    pub const fn body_allowed(&self) -> bool {
        match self.phase {
            ClientResponsePhase::AwaitingFinalHeaders => false,
            ClientResponsePhase::Complete => false,
            ClientResponsePhase::Content => !self.body_forbidden(),
        }
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
                if self.body_forbidden() {
                    if payload_length > 0 {
                        return Err(ResponseSectionError::ContentNotAllowed);
                    }

                    if end_stream {
                        self.phase = ClientResponsePhase::Complete;
                    }

                    return Ok(self.phase);
                }

                if self.content_length.is_some() {
                    let Ok(length) = u64::try_from(payload_length) else {
                        return Err(ResponseSectionError::BodyTooLong);
                    };

                    if length > self.remaining {
                        return Err(ResponseSectionError::BodyTooLong);
                    }

                    self.remaining -= length;

                    if end_stream {
                        if self.remaining != 0 {
                            return Err(ResponseSectionError::ContentLengthMismatch);
                        }

                        self.phase = ClientResponsePhase::Complete;
                    }

                    return Ok(self.phase);
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
        if (status < 200 || status == 204) && validated.content_length.is_some() {
            return Err(ResponseSectionError::ContentLengthNotAllowed);
        }

        let method_forbids_body = self.method == RequestMethod::Head
            || (self.method == RequestMethod::Connect && (200..300).contains(&status));

        if self.method == RequestMethod::Connect
            && (200..300).contains(&status)
            && validated.content_length.is_some()
        {
            return Err(ResponseSectionError::ContentLengthNotAllowed);
        }

        let kind = if status < 200 {
            ResponseSectionKind::Informational
        } else {
            ResponseSectionKind::Final
        };

        if kind == ResponseSectionKind::Final {
            let body_forbidden = method_forbids_body || matches!(status, 204 | 304);

            let content_length = if body_forbidden {
                None
            } else {
                validated.content_length
            };

            if end_stream && content_length.is_some_and(|length| length > 0) {
                return Err(ResponseSectionError::ContentLengthMismatch);
            }

            self.final_status = Some(status);
            self.content_length = content_length;
            self.remaining = content_length.unwrap_or(0);
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

        if self.content_length.is_some_and(|_| self.remaining > 0) {
            return Err(ResponseSectionError::ContentLengthMismatch);
        }

        self.phase = ClientResponsePhase::Complete;

        Ok(ResponseFieldSection {
            kind: ResponseSectionKind::Trailers,
            status: None,
            fields,
        })
    }

    const fn body_forbidden(&self) -> bool {
        if matches!(self.method, RequestMethod::Head) {
            return true;
        }

        match self.final_status {
            Some(204 | 304) => true,
            Some(status) => {
                matches!(self.method, RequestMethod::Connect) && status >= 200 && status < 300
            }
            None => false,
        }
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
    let mut content_length = None;

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
        if name == b"content-length" {
            let length =
                parse_content_length(value).ok_or(ResponseSectionError::InvalidContentLength)?;
            if content_length.is_some_and(|previous| previous != length) {
                return Err(ResponseSectionError::InvalidContentLength);
            }
            content_length = Some(length);
        }
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

const fn classify_method(method: &[u8]) -> RequestMethod {
    if method.len() == 4
        && method[0] == b'H'
        && method[1] == b'E'
        && method[2] == b'A'
        && method[3] == b'D'
    {
        return RequestMethod::Head;
    }

    if method.len() == 7
        && method[0] == b'C'
        && method[1] == b'O'
        && method[2] == b'N'
        && method[3] == b'N'
        && method[4] == b'E'
        && method[5] == b'C'
        && method[6] == b'T'
    {
        return RequestMethod::Connect;
    }

    RequestMethod::Other
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
