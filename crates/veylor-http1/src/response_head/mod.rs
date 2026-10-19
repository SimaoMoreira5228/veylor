use crate::fields::{
    ConnectionOptions, is_ows, is_token, parse_connection_options, parse_content_length,
    upgrade_protocols_offered, valid_upgrade_field,
};
use crate::request_head::{Header, Limits, ParseError, Span};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestMethod {
    Head,
    Connect,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseBody {
    None,
    Fixed(u64),
    Chunked,
    CloseDelimited,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseKind {
    Informational,
    Final,
    Upgrade,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseHead {
    pub version: Span,
    pub status: u16,
    pub reason: Span,
    pub header_count: usize,
    pub length: usize,
    pub end_offset: usize,
    pub body: ResponseBody,
    pub kind: ResponseKind,
    pub connection: ConnectionOptions,
    pub reusable: bool,
}

impl ResponseHead {
    pub fn version(self, input: &[u8]) -> Option<&[u8]> {
        input.get(self.version.offset..self.version.offset.checked_add(self.version.length)?)
    }

    pub fn reason(self, input: &[u8]) -> Option<&[u8]> {
        input.get(self.reason.offset..self.reason.offset.checked_add(self.reason.length)?)
    }

    pub fn upgrade_bytes(self, input: &[u8]) -> Option<&[u8]> {
        if self.kind != ResponseKind::Upgrade {
            return None;
        }
        input.get(self.end_offset..)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    StatusLine,
    Headers,
    Ready,
    Error(ParseError),
}

pub struct ResponseParser<'a> {
    request_method: RequestMethod,
    requested_upgrade: &'a [u8],
    state: State,
    scan_offset: usize,
    line_start: usize,
    header_start: usize,
    field_value_start: Option<usize>,
    input_base: *const u8,
    input_length: usize,
    header_base: *mut Header,
    header_count: usize,
    version: Span,
    reason: Span,
    status: u16,
    http_11: bool,
    content_length: Option<u64>,
    transfer_encoding_seen: bool,
    upgrade_seen: bool,
    connection: ConnectionOptions,
    head_start: usize,
    head_end: usize,
}

impl<'a> ResponseParser<'a> {
    pub const fn new(request_method: RequestMethod, requested_upgrade: &'a [u8]) -> Self {
        Self {
            request_method,
            requested_upgrade,
            state: State::StatusLine,
            scan_offset: 0,
            line_start: 0,
            header_start: 0,
            field_value_start: None,
            input_base: core::ptr::null(),
            input_length: 0,
            header_base: core::ptr::null_mut(),
            header_count: 0,
            version: Span {
                offset: 0,
                length: 0,
            },
            reason: Span {
                offset: 0,
                length: 0,
            },
            status: 0,
            http_11: false,
            content_length: None,
            transfer_encoding_seen: false,
            upgrade_seen: false,
            connection: ConnectionOptions {
                close: false,
                keep_alive: false,
                upgrade: false,
            },
            head_start: 0,
            head_end: 0,
        }
    }

    pub fn reset(&mut self) {
        *self = Self::new(self.request_method, self.requested_upgrade);
    }

    pub fn parse(
        &mut self,
        input: &[u8],
        headers: &mut [Header],
        limits: Limits,
    ) -> Result<Option<ResponseHead>, ParseError> {
        if let State::Error(error) = self.state {
            return Err(error);
        }

        let input_base = input.as_ptr();
        if self.input_base.is_null() {
            if !input.is_empty() {
                self.input_base = input_base;
                self.input_length = input.len();
            }
        } else if self.input_base != input_base || input.len() < self.input_length {
            return self.fail(ParseError::BufferChanged);
        } else {
            self.input_length = input.len();
        }

        if headers.len() < self.header_count {
            return Err(ParseError::HeaderCapacity);
        }
        if self.header_count > 0 && self.header_base != headers.as_mut_ptr().cast() {
            return self.fail(ParseError::BufferChanged);
        }
        if self.state == State::Ready {
            return Ok(Some(self.response_head()));
        }

        let mut offset = self.scan_offset;
        while offset < input.len() {
            let byte = input[offset];
            if self.state == State::Headers && self.field_value_start.is_none() && byte == b':' {
                self.field_value_start = Some(offset - self.line_start + 1);
            }
            if byte == b'\r' {
                if input.get(offset + 1).is_some_and(|next| *next != b'\n') {
                    return self.fail(ParseError::Invalid);
                }
                offset += 1;
                continue;
            }
            if byte != b'\n' {
                offset += 1;
                continue;
            }
            if offset == self.line_start || input[offset - 1] != b'\r' {
                return self.fail(ParseError::Invalid);
            }

            let line_end = offset - 1;
            let line_next = offset + 1;
            match self.state {
                State::StatusLine => {
                    if line_end - self.line_start > limits.request_line_bytes {
                        return self.fail(ParseError::StatusLineTooLarge);
                    }
                    let Some((version, status, reason)) =
                        parse_status_line(&input[self.line_start..line_end])
                    else {
                        return self.fail(ParseError::Invalid);
                    };
                    self.version = shift_span(version, self.line_start);
                    self.reason = shift_span(reason, self.line_start);
                    self.status = status;
                    self.http_11 = input[self.line_start + 7] == b'1';
                    self.header_start = line_next;
                    self.state = State::Headers;
                }
                State::Headers => {
                    if line_next - self.header_start > limits.header_bytes {
                        return self.fail(ParseError::HeaderSectionTooLarge);
                    }
                    if line_end == self.line_start {
                        if self.upgrade_seen && !self.connection.upgrade {
                            return self.fail(ParseError::Invalid);
                        }
                        if self.status == 101
                            && (!self.http_11 || !self.upgrade_seen || !self.connection.upgrade)
                        {
                            return self.fail(ParseError::Invalid);
                        }
                        self.head_end = line_next;
                        self.state = State::Ready;
                        return Ok(Some(self.response_head()));
                    }
                    if self.header_count == limits.header_count {
                        return self.fail(ParseError::HeaderSectionTooLarge);
                    }
                    if self.header_count >= headers.len() {
                        return Err(ParseError::HeaderCapacity);
                    }
                    let line = &input[self.line_start..line_end];
                    let Some(colon) = line.iter().position(|byte| *byte == b':') else {
                        return self.fail(ParseError::Invalid);
                    };
                    let name = &line[..colon];
                    if name.is_empty()
                        || name.len() > limits.field_name_bytes
                        || !name.iter().copied().all(is_token)
                    {
                        return self.fail(ParseError::Invalid);
                    }
                    let raw_value = &line[colon + 1..];
                    if raw_value.len() > limits.field_value_bytes {
                        return self.fail(ParseError::HeaderSectionTooLarge);
                    }
                    let value_start = raw_value
                        .iter()
                        .position(|byte| !is_ows(*byte))
                        .unwrap_or(raw_value.len());
                    let value_end = raw_value
                        .iter()
                        .rposition(|byte| !is_ows(*byte))
                        .map_or(value_start, |index| index + 1);
                    let value = &raw_value[value_start..value_end];
                    if value.iter().any(|byte| {
                        *byte == 0
                            || *byte == b'\r'
                            || *byte == b'\n'
                            || (*byte < 0x20 && *byte != b'\t')
                            || *byte == 0x7f
                    }) {
                        return self.fail(ParseError::Invalid);
                    }

                    let field = Header {
                        name: Span {
                            offset: self.line_start,
                            length: colon,
                        },
                        value: Span {
                            offset: self.line_start + colon + 1 + value_start,
                            length: value.len(),
                        },
                    };
                    let parse_framing = !self.no_body_expected();
                    if parse_framing && name.eq_ignore_ascii_case(b"content-length") {
                        let Some(length) = parse_content_length(value) else {
                            return self.fail(ParseError::Invalid);
                        };
                        if self
                            .content_length
                            .is_some_and(|previous| previous != length)
                            || self.transfer_encoding_seen
                        {
                            return self.fail(ParseError::Invalid);
                        }
                        self.content_length = Some(length);
                    }
                    if parse_framing && name.eq_ignore_ascii_case(b"transfer-encoding") {
                        if self.content_length.is_some()
                            || self.transfer_encoding_seen
                            || !self.version_is_11(input)
                            || !value.eq_ignore_ascii_case(b"chunked")
                        {
                            return self.fail(ParseError::Invalid);
                        }
                        self.transfer_encoding_seen = true;
                    }
                    if name.eq_ignore_ascii_case(b"connection") {
                        let Some(options) = parse_connection_options(value) else {
                            return self.fail(ParseError::Invalid);
                        };
                        self.connection.close |= options.close;
                        self.connection.keep_alive |= options.keep_alive;
                        self.connection.upgrade |= options.upgrade;
                    }
                    if name.eq_ignore_ascii_case(b"upgrade") {
                        if !valid_upgrade_field(value)
                            || (self.status == 101
                                && !upgrade_protocols_offered(value, self.requested_upgrade))
                        {
                            return self.fail(ParseError::Invalid);
                        }
                        self.upgrade_seen = true;
                    }

                    headers[self.header_count] = field;
                    if self.header_count == 0 {
                        self.header_base = headers.as_mut_ptr().cast();
                    }
                    self.header_count += 1;
                    self.field_value_start = None;
                }
                State::Ready | State::Error(_) => return Err(ParseError::Invalid),
            }
            self.line_start = line_next;
            offset = line_next;
        }

        self.scan_offset = if input.last() == Some(&b'\r') {
            offset.saturating_sub(1)
        } else {
            offset
        };
        let mut line_length = input.len().saturating_sub(self.line_start);
        if line_length > 0 && input.last() == Some(&b'\r') {
            line_length -= 1;
        }
        if self.state == State::StatusLine && line_length > limits.request_line_bytes {
            return self.fail(ParseError::StatusLineTooLarge);
        }
        if self.state == State::Headers
            && input.len().saturating_sub(self.header_start) > limits.header_bytes
        {
            return self.fail(ParseError::HeaderSectionTooLarge);
        }
        if self.state == State::Headers {
            match self.field_value_start {
                Some(value_start) => {
                    if value_start - 1 > limits.field_name_bytes
                        || line_length.saturating_sub(value_start) > limits.field_value_bytes
                    {
                        return self.fail(ParseError::HeaderSectionTooLarge);
                    }
                }
                None if line_length > limits.field_name_bytes => {
                    return self.fail(ParseError::HeaderSectionTooLarge);
                }
                None => {}
            }
        }
        Ok(None)
    }

    pub fn advance(&mut self, input: &[u8], next_offset: usize) -> Result<(), ParseError> {
        if self.state != State::Ready || next_offset != self.head_end {
            return Err(ParseError::Invalid);
        }
        if input.as_ptr() != self.input_base || input.len() < self.input_length {
            return self.fail(ParseError::BufferChanged);
        }
        self.scan_offset = next_offset;
        self.line_start = next_offset;
        self.header_start = next_offset;
        self.head_start = next_offset;
        self.field_value_start = None;
        self.header_count = 0;
        self.header_base = core::ptr::null_mut();
        self.version = Span::default();
        self.reason = Span::default();
        self.status = 0;
        self.http_11 = false;
        self.content_length = None;
        self.transfer_encoding_seen = false;
        self.upgrade_seen = false;
        self.connection = ConnectionOptions::default();
        self.head_end = 0;
        self.state = State::StatusLine;
        Ok(())
    }

    fn version_is_11(&self, input: &[u8]) -> bool {
        self.version
            .offset
            .checked_add(self.version.length)
            .and_then(|end| input.get(self.version.offset..end))
            == Some(&b"HTTP/1.1"[..])
    }

    fn no_body_expected(&self) -> bool {
        self.request_method == RequestMethod::Head
            || (100..200).contains(&self.status)
            || self.status == 204
            || self.status == 304
            || (self.request_method == RequestMethod::Connect && (200..300).contains(&self.status))
    }

    fn response_head(&self) -> ResponseHead {
        let informational = (100..200).contains(&self.status);
        let upgrade = self.status == 101
            || (self.request_method == RequestMethod::Connect && (200..300).contains(&self.status));
        let no_body = self.no_body_expected() || upgrade;
        let body = if no_body {
            ResponseBody::None
        } else if self.transfer_encoding_seen {
            ResponseBody::Chunked
        } else if let Some(length) = self.content_length {
            ResponseBody::Fixed(length)
        } else {
            ResponseBody::CloseDelimited
        };
        let kind = if upgrade {
            ResponseKind::Upgrade
        } else if informational {
            ResponseKind::Informational
        } else {
            ResponseKind::Final
        };
        let self_delimited = !matches!(body, ResponseBody::CloseDelimited);
        let reusable = kind == ResponseKind::Final
            && self_delimited
            && !self.connection.close
            && (self.http_11 || self.connection.keep_alive);

        ResponseHead {
            version: self.version,
            status: self.status,
            reason: self.reason,
            header_count: self.header_count,
            length: self.head_end - self.head_start,
            end_offset: self.head_end,
            body,
            kind,
            connection: self.connection,
            reusable,
        }
    }

    fn fail<T>(&mut self, error: ParseError) -> Result<T, ParseError> {
        self.state = State::Error(error);
        Err(error)
    }
}

fn parse_status_line(line: &[u8]) -> Option<(Span, u16, Span)> {
    if line.len() < 13 || &line[..7] != b"HTTP/1." {
        return None;
    }
    let version_digit = line[7];
    if !matches!(version_digit, b'0' | b'1') || line[8] != b' ' {
        return None;
    }
    let status_bytes = line.get(9..12)?;
    if !status_bytes.iter().all(u8::is_ascii_digit) || line[12] != b' ' {
        return None;
    }
    let status = u16::from(status_bytes[0] - b'0') * 100
        + u16::from(status_bytes[1] - b'0') * 10
        + u16::from(status_bytes[2] - b'0');
    if !(100..=599).contains(&status) {
        return None;
    }
    let reason = &line[13..];
    if reason
        .iter()
        .any(|byte| (*byte < 0x20 && *byte != b'\t') || *byte == 0x7f)
    {
        return None;
    }
    Some((
        Span {
            offset: 0,
            length: 8,
        },
        status,
        Span {
            offset: 13,
            length: reason.len(),
        },
    ))
}

fn shift_span(span: Span, base: usize) -> Span {
    Span {
        offset: span.offset + base,
        length: span.length,
    }
}

#[cfg(test)]
mod tests;
