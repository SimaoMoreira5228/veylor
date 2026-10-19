use crate::expectation::{ExpectationError, parse as parse_expectation};
use crate::fields::{
    ConnectionOptions, is_ows, is_token, parse_connection_options, parse_content_length,
};
use crate::request_target::{TargetForm, classify, valid_host_field};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Span {
    pub offset: usize,
    pub length: usize,
}

impl Span {
    fn end(self) -> Option<usize> {
        self.offset.checked_add(self.length)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Header {
    pub name: Span,
    pub value: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestHead {
    pub method: Span,
    pub target: Span,
    pub target_form: TargetForm,
    pub authority: Span,
    pub version: Span,
    pub header_count: usize,
    pub length: usize,
    pub content_length: Option<u64>,
    pub is_chunked: bool,
    pub expect_continue: bool,
    pub connection: ConnectionOptions,
}

impl RequestHead {
    pub fn method(self, input: &[u8]) -> Option<&[u8]> {
        slice_at(input, self.method)
    }

    pub fn target(self, input: &[u8]) -> Option<&[u8]> {
        slice_at(input, self.target)
    }

    pub fn version(self, input: &[u8]) -> Option<&[u8]> {
        slice_at(input, self.version)
    }
}

fn slice_at(input: &[u8], span: Span) -> Option<&[u8]> {
    input.get(span.offset..span.end()?)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    pub request_line_bytes: usize,
    pub header_bytes: usize,
    pub header_count: usize,
    pub field_name_bytes: usize,
    pub field_value_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            request_line_bytes: 8 * 1024,
            header_bytes: 32 * 1024,
            header_count: 100,
            field_name_bytes: 256,
            field_value_bytes: 8 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParseError {
    Invalid,
    RequestLineTooLarge,
    StatusLineTooLarge,
    HeaderSectionTooLarge,
    HeaderCapacity,
    BufferChanged,
    ExpectationFailed,
    Incomplete,
    Cancelled,
    BodyTooLarge,
    AdmissionPending,
    BodyRejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    RequestLine,
    Headers,
    Ready,
    Error(ParseError),
}

pub struct Parser {
    state: State,
    scan_offset: usize,
    line_start: usize,
    header_start: usize,
    header_value_start: Option<usize>,
    input_base: *const u8,
    input_length: usize,
    header_base: *mut Header,
    header_count: usize,
    host_seen: bool,
    transfer_encoding_seen: bool,
    content_length: Option<u64>,
    expect_continue: bool,
    connection: ConnectionOptions,
    method: Span,
    target: Span,
    target_form: TargetForm,
    authority: Span,
    version: Span,
    head_length: usize,
    leading_empty_bytes: usize,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    pub const fn new() -> Self {
        Self {
            state: State::RequestLine,
            scan_offset: 0,
            line_start: 0,
            header_start: 0,
            header_value_start: None,
            input_base: core::ptr::null(),
            input_length: 0,
            header_base: core::ptr::null_mut(),
            header_count: 0,
            host_seen: false,
            transfer_encoding_seen: false,
            content_length: None,
            expect_continue: false,
            connection: ConnectionOptions {
                close: false,
                keep_alive: false,
                upgrade: false,
            },
            method: Span {
                offset: 0,
                length: 0,
            },
            target: Span {
                offset: 0,
                length: 0,
            },
            target_form: TargetForm::Origin,
            authority: Span {
                offset: 0,
                length: 0,
            },
            version: Span {
                offset: 0,
                length: 0,
            },
            head_length: 0,
            leading_empty_bytes: 0,
        }
    }

    pub fn reset(&mut self) {
        *self = Self::new();
    }

    pub fn parse(
        &mut self,
        input: &[u8],
        headers: &mut [Header],
        limits: Limits,
    ) -> Result<Option<RequestHead>, ParseError> {
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
            return Ok(Some(self.request_head()));
        }

        let mut offset = self.scan_offset;
        while offset < input.len() {
            let byte = input[offset];
            if self.state == State::Headers && self.header_value_start.is_none() && byte == b':' {
                self.header_value_start = Some(offset - self.line_start + 1);
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
                State::RequestLine => {
                    let line_length = line_end - self.line_start;
                    if line_length > limits.request_line_bytes {
                        return self.fail(ParseError::RequestLineTooLarge);
                    }

                    if line_length == 0 {
                        self.leading_empty_bytes += line_next - self.line_start;
                        if self.leading_empty_bytes > limits.request_line_bytes {
                            return self.fail(ParseError::RequestLineTooLarge);
                        }
                    } else {
                        let Some((method, target, version, target_form, authority)) =
                            parse_request_line(&input[self.line_start..line_end])
                        else {
                            return self.fail(ParseError::Invalid);
                        };
                        self.method = shift_span(method, self.line_start);
                        self.target = shift_span(target, self.line_start);
                        self.version = shift_span(version, self.line_start);
                        self.target_form = target_form;
                        if authority.length > 0 {
                            self.authority = shift_span(authority, self.line_start + target.offset);
                        }
                        self.header_start = line_next;
                        self.header_value_start = None;
                        self.state = State::Headers;
                    }
                }
                State::Headers => {
                    if line_next - self.header_start > limits.header_bytes {
                        return self.fail(ParseError::HeaderSectionTooLarge);
                    }

                    if line_end == self.line_start {
                        let Some(version) = slice_at(input, self.version) else {
                            return self.fail(ParseError::Invalid);
                        };
                        if version == b"HTTP/1.1" && !self.host_seen {
                            return self.fail(ParseError::Invalid);
                        }
                        self.head_length = line_next;
                        self.state = State::Ready;
                        return Ok(Some(self.request_head()));
                    }

                    if self.header_count == limits.header_count {
                        return self.fail(ParseError::HeaderSectionTooLarge);
                    }
                    if self.header_count >= headers.len() {
                        return Err(ParseError::HeaderCapacity);
                    }

                    let line = &input[self.line_start..line_end];
                    let Some(colon) = line.iter().position(|value| *value == b':') else {
                        return self.fail(ParseError::Invalid);
                    };
                    let name = &line[..colon];
                    if name.is_empty() {
                        return self.fail(ParseError::Invalid);
                    }
                    if name.len() > limits.field_name_bytes {
                        return self.fail(ParseError::HeaderSectionTooLarge);
                    }
                    if !name.iter().copied().all(is_token) {
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
                    if value.len() > limits.field_value_bytes
                        || value.iter().any(|byte| {
                            *byte == 0
                                || *byte == b'\r'
                                || *byte == b'\n'
                                || (*byte < 0x20 && *byte != b'\t')
                                || *byte == 0x7f
                        })
                    {
                        return self.fail(ParseError::Invalid);
                    }

                    let entry = Header {
                        name: Span {
                            offset: self.line_start,
                            length: colon,
                        },
                        value: Span {
                            offset: self.line_start + colon + 1 + value_start,
                            length: value.len(),
                        },
                    };
                    if name.eq_ignore_ascii_case(b"host") {
                        if self.host_seen || !valid_host_field(value) {
                            return self.fail(ParseError::Invalid);
                        }
                        self.host_seen = true;
                        if matches!(self.target_form, TargetForm::Origin | TargetForm::Asterisk) {
                            self.authority = Span {
                                offset: self.line_start + colon + 1 + value_start,
                                length: value.len(),
                            };
                        }
                    }

                    if name.eq_ignore_ascii_case(b"content-length") {
                        let Some(content_length) = parse_content_length(value) else {
                            return self.fail(ParseError::Invalid);
                        };
                        if self
                            .content_length
                            .is_some_and(|previous| previous != content_length)
                            || self.transfer_encoding_seen
                        {
                            return self.fail(ParseError::Invalid);
                        }
                        self.content_length = Some(content_length);
                    }

                    if name.eq_ignore_ascii_case(b"transfer-encoding") {
                        let Some(version) = slice_at(input, self.version) else {
                            return self.fail(ParseError::Invalid);
                        };
                        if self.content_length.is_some()
                            || self.transfer_encoding_seen
                            || version != b"HTTP/1.1"
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

                    if name.eq_ignore_ascii_case(b"expect") {
                        let Some(version) = slice_at(input, self.version) else {
                            return self.fail(ParseError::Invalid);
                        };
                        if version == b"HTTP/1.1" {
                            match parse_expectation(value) {
                                Ok(expect_continue) => {
                                    self.expect_continue |= expect_continue;
                                }
                                Err(ExpectationError::Invalid) => {
                                    return self.fail(ParseError::Invalid);
                                }
                                Err(ExpectationError::Unsupported) => {
                                    return self.fail(ParseError::ExpectationFailed);
                                }
                            }
                        }
                    }
                    headers[self.header_count] = entry;
                    if self.header_count == 0 {
                        self.header_base = headers.as_mut_ptr().cast();
                    }
                    self.header_count += 1;
                    self.header_value_start = None;
                }
                State::Ready | State::Error(_) => return Err(ParseError::Invalid),
            }
            self.line_start = line_next;
            offset = line_next;
        }

        self.scan_offset = offset;
        let mut partial_line_length = input.len().saturating_sub(self.line_start);
        if partial_line_length > 0 && input.last() == Some(&b'\r') {
            partial_line_length -= 1;
        }
        if self.state == State::RequestLine && partial_line_length > limits.request_line_bytes {
            return self.fail(ParseError::RequestLineTooLarge);
        }
        if self.state == State::Headers
            && input.len().saturating_sub(self.header_start) > limits.header_bytes
        {
            return self.fail(ParseError::HeaderSectionTooLarge);
        }
        if self.state == State::Headers {
            match self.header_value_start {
                Some(value_start) => {
                    if value_start - 1 > limits.field_name_bytes
                        || partial_line_length.saturating_sub(value_start)
                            > limits.field_value_bytes
                    {
                        return self.fail(ParseError::HeaderSectionTooLarge);
                    }
                }
                None if partial_line_length > limits.field_name_bytes => {
                    return self.fail(ParseError::HeaderSectionTooLarge);
                }
                None => {}
            }
        }
        Ok(None)
    }

    fn request_head(&self) -> RequestHead {
        RequestHead {
            method: self.method,
            target: self.target,
            target_form: self.target_form,
            authority: self.authority,
            version: self.version,
            header_count: self.header_count,
            length: self.head_length,
            content_length: self.content_length,
            is_chunked: self.transfer_encoding_seen,
            expect_continue: self.expect_continue,
            connection: self.connection,
        }
    }

    fn fail<T>(&mut self, error: ParseError) -> Result<T, ParseError> {
        self.state = State::Error(error);
        Err(error)
    }
}

fn shift_span(span: Span, base: usize) -> Span {
    Span {
        offset: span.offset + base,
        length: span.length,
    }
}

fn parse_request_line(line: &[u8]) -> Option<(Span, Span, Span, TargetForm, Span)> {
    let method_end = line.iter().position(|byte| *byte == b' ')?;
    let target_start = method_end + 1;
    let version_rel = line[target_start..].iter().position(|byte| *byte == b' ')?;
    let version_start = target_start + version_rel + 1;
    if method_end == 0 || version_start >= line.len() {
        return None;
    }

    let target_length = version_start - target_start - 1;
    let version = &line[version_start..];
    let method = &line[..method_end];
    let target = &line[target_start..target_start + target_length];
    let (target_form, authority) = classify(method, target)?;

    if !method.iter().copied().all(is_token)
        || line[target_start..target_start + target_length]
            .iter()
            .any(|byte| *byte <= 0x20 || *byte == 0x7f)
        || (version != b"HTTP/1.0" && version != b"HTTP/1.1")
    {
        return None;
    }

    Some((
        Span {
            offset: 0,
            length: method_end,
        },
        Span {
            offset: target_start,
            length: target_length,
        },
        Span {
            offset: version_start,
            length: version.len(),
        },
        target_form,
        authority,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(input: &[u8]) -> Result<(RequestHead, Vec<Header>), ParseError> {
        let mut parser = Parser::new();
        let mut storage = [Header::default(); 16];
        let head = parser
            .parse(input, &mut storage, Limits::default())?
            .ok_or(ParseError::Invalid)?;
        let headers = storage[..head.header_count].to_vec();
        Ok((head, headers))
    }

    #[test]
    fn parses_request_and_preserves_header_spans() {
        let input = b"GET /health HTTP/1.1\r\nHost: example.test\r\nAccept: */*\r\n\r\n";
        let (head, headers) = parse_all(input).unwrap();

        assert_eq!(head.method(input), Some(&b"GET"[..]));
        assert_eq!(head.target(input), Some(&b"/health"[..]));
        assert_eq!(head.version(input), Some(&b"HTTP/1.1"[..]));
        assert_eq!(
            &input[headers[0].name.offset..headers[0].name.offset + headers[0].name.length],
            b"Host"
        );
        assert_eq!(
            &input[headers[0].value.offset..headers[0].value.offset + headers[0].value.length],
            b"example.test"
        );
        assert_eq!(head.length, input.len());
    }

    #[test]
    fn incremental_input_requires_stable_base() {
        let input = *b"GET / HTTP/1.1\r\nHost: test\r\n\r\n";
        let prefix_length = b"GET / HTTP/1.1\r\nHo".len();
        let mut parser = Parser::new();
        let mut storage = [Header::default(); 4];

        assert_eq!(
            parser.parse(&input[..prefix_length], &mut storage, Limits::default()),
            Ok(None)
        );
        assert!(
            parser
                .parse(&input, &mut storage, Limits::default())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn rejects_bad_line_endings_and_field_names() {
        assert_eq!(parse_all(b"GET / HTTP/1.1\n\n"), Err(ParseError::Invalid));
        assert_eq!(
            parse_all(b"GET / HTTP/1.1\r\nBad Name: x\r\n\r\n"),
            Err(ParseError::Invalid)
        );
    }

    #[test]
    fn validates_http_1_1_host_count() {
        assert_eq!(
            parse_all(b"GET / HTTP/1.1\r\n\r\n"),
            Err(ParseError::Invalid)
        );
        assert_eq!(
            parse_all(b"GET / HTTP/1.1\r\nHost: one\r\nhost: two\r\n\r\n"),
            Err(ParseError::Invalid)
        );
        assert!(parse_all(b"GET / HTTP/1.0\r\n\r\n").is_ok());
    }

    #[test]
    fn parses_content_length_and_chunked_framing() {
        let (head, _) =
            parse_all(b"POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5, 005\r\n\r\n")
                .unwrap();
        assert_eq!(head.content_length, Some(5));
        assert!(!head.is_chunked);

        let (head, _) = parse_all(
            b"POST / HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: Chunked\r\n\r\n",
        )
        .unwrap();
        assert_eq!(head.content_length, None);
        assert!(head.is_chunked);

        for request in [
            &b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5, 6\r\n\r\n"[..],
            &b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 18446744073709551616\r\n\r\n"[..],
            &b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
            &b"POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
        ] {
            assert_eq!(parse_all(request), Err(ParseError::Invalid));
        }
    }

    #[test]
    fn parses_connection_tokens_without_allocating() {
        let (head, _) = parse_all(
            b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: keep-alive, CLOSE\r\n\r\n",
        )
        .unwrap();
        assert!(head.connection.close);
        assert!(head.connection.keep_alive);
        assert!(!head.connection.upgrade);

        assert_eq!(
            parse_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close,\r\n\r\n"),
            Err(ParseError::Invalid)
        );
    }

    #[test]
    fn enforces_raw_header_field_limits() {
        let request = b"GET / HTTP/1.1\r\nHost: x\r\nX:       \r\n\r\n";
        let mut parser = Parser::new();
        let mut storage = [Header::default(); 4];
        let limits = Limits {
            field_value_bytes: 4,
            ..Limits::default()
        };
        assert_eq!(
            parser.parse(request, &mut storage, limits),
            Err(ParseError::HeaderSectionTooLarge)
        );

        let partial = b"GET / HTTP/1.1\r\nHost: x\r\nLong-Name";
        parser.reset();
        let limits = Limits {
            field_name_bytes: 4,
            ..Limits::default()
        };
        assert_eq!(
            parser.parse(partial, &mut storage, limits),
            Err(ParseError::HeaderSectionTooLarge)
        );
    }

    #[test]
    fn parses_http_1_1_expectations() {
        let (head, _) =
            parse_all(b"POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\n\r\n").unwrap();
        assert!(head.expect_continue);

        assert_eq!(
            parse_all(b"POST / HTTP/1.1\r\nHost: x\r\nExpect: magic\r\n\r\n"),
            Err(ParseError::ExpectationFailed)
        );
        assert_eq!(
            parse_all(b"POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue,,\r\n\r\n"),
            Err(ParseError::Invalid)
        );
        assert!(parse_all(b"POST / HTTP/1.0\r\nExpect: magic\r\n\r\n").is_ok());
    }
}
