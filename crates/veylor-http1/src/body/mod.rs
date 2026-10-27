use crate::fields::{is_ows, is_token};
use crate::request_head::{Header, Limits, ParseError, Span};
use crate::response_head::ResponseBody;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyEvent {
    Data(Span),
    Trailers { count: usize },
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BodyProgress {
    pub event: Option<BodyEvent>,
    pub consumed: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BodyLimits {
    pub total_body_bytes: u64,
    pub chunk_line_bytes: usize,
    pub extension_bytes: usize,
    pub trailer_bytes: usize,
    pub trailer_count: usize,
}

impl BodyLimits {
    pub const DEFAULT: Self = Self {
        total_body_bytes: 16 * 1024 * 1024,
        chunk_line_bytes: 1024,
        extension_bytes: 512,
        trailer_bytes: 8 * 1024,
        trailer_count: 32,
    };
}

impl Default for BodyLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Fixed { remaining: u64 },
    CloseDelimited,
    ChunkSize,
    ChunkData { remaining: u64 },
    ChunkDataCrlf,
    Trailers,
    Complete,
    Finished,
    Error(ParseError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TrailerPolicy {
    Request,
    Response,
}

pub struct BodyDecoder {
    state: State,
    scan_offset: usize,
    line_start: usize,
    pending_length: usize,
    trailer_base: *mut Header,
    trailer_count: usize,
    required_trailer_count: usize,
    body_limits: BodyLimits,
    trailer_policy: TrailerPolicy,
    decoded_bytes: u64,
}

impl BodyDecoder {
    pub const fn new(framing: ResponseBody) -> Self {
        Self::with_limits(framing, BodyLimits::DEFAULT)
    }

    pub const fn with_limits(framing: ResponseBody, body_limits: BodyLimits) -> Self {
        let state = match framing {
            ResponseBody::None => State::Complete,
            ResponseBody::Fixed(remaining) if remaining > body_limits.total_body_bytes => {
                State::Error(ParseError::BodyTooLarge)
            }
            ResponseBody::Fixed(remaining) => State::Fixed { remaining },
            ResponseBody::Chunked => State::ChunkSize,
            ResponseBody::CloseDelimited => State::CloseDelimited,
        };
        Self {
            state,
            scan_offset: 0,
            line_start: 0,
            pending_length: 0,
            trailer_base: core::ptr::null_mut(),
            trailer_count: 0,
            required_trailer_count: 0,
            body_limits,
            trailer_policy: TrailerPolicy::Response,
            decoded_bytes: 0,
        }
    }

    pub(crate) const fn new_request(
        content_length: Option<u64>,
        is_chunked: bool,
        body_limits: BodyLimits,
    ) -> Self {
        let framing = if is_chunked {
            ResponseBody::Chunked
        } else {
            ResponseBody::Fixed(match content_length {
                Some(length) => length,
                None => 0,
            })
        };
        let mut decoder = Self::with_limits(framing, body_limits);
        decoder.trailer_policy = TrailerPolicy::Request;
        decoder
    }

    pub fn next(
        &mut self,
        input: &[u8],
        eof: bool,
        trailers: &mut [Header],
        limits: Limits,
    ) -> Result<BodyProgress, ParseError> {
        self.required_trailer_count = 0;
        if let State::Error(error) = self.state {
            return Err(error);
        }
        if input.len() < self.pending_length {
            return self.fail(ParseError::BufferChanged);
        }
        if trailers.len() < self.trailer_count {
            self.required_trailer_count = self.trailer_count;
            return Err(ParseError::HeaderCapacity);
        }
        if self.trailer_count > 0 && self.trailer_base != trailers.as_mut_ptr() {
            return self.fail(ParseError::BufferChanged);
        }

        let mut consumed = 0;
        loop {
            match self.state {
                State::Fixed { remaining } => {
                    let length = input.len().min(remaining.min(usize::MAX as u64) as usize);
                    if length == 0 {
                        if eof && remaining > 0 {
                            return self.fail(ParseError::Incomplete);
                        }
                        if remaining == 0 {
                            self.state = State::Complete;
                            continue;
                        }
                        return self.progress(input, 0, None);
                    }
                    self.state = if length as u64 == remaining {
                        State::Complete
                    } else {
                        State::Fixed {
                            remaining: remaining - length as u64,
                        }
                    };
                    return self.progress(
                        input,
                        length,
                        Some(BodyEvent::Data(Span { offset: 0, length })),
                    );
                }
                State::CloseDelimited => {
                    if !input.is_empty() {
                        let remaining = self.body_limits.total_body_bytes - self.decoded_bytes;
                        if remaining == 0 {
                            return self.fail(ParseError::BodyTooLarge);
                        }
                        let length = input
                            .len()
                            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
                        self.decoded_bytes += length as u64;
                        return self.progress(
                            input,
                            length,
                            Some(BodyEvent::Data(Span { offset: 0, length })),
                        );
                    }
                    if eof {
                        self.state = State::Finished;
                        return self.progress(input, 0, Some(BodyEvent::Complete));
                    }
                    return self.progress(input, 0, None);
                }
                State::ChunkSize => {
                    let line_limit = limits.header_bytes.min(self.body_limits.chunk_line_bytes);
                    let Some((line, next)) = self.read_line(input, line_limit)? else {
                        return self.need_more(input, eof, consumed);
                    };
                    let size = match parse_chunk_line(line, self.body_limits.extension_bytes) {
                        Ok(size) => size,
                        Err(error) => return self.fail(error),
                    };
                    let remaining = self.body_limits.total_body_bytes - self.decoded_bytes;
                    if size > remaining {
                        return self.fail(ParseError::BodyTooLarge);
                    }
                    consumed = next;
                    self.scan_offset = next;
                    self.line_start = next;
                    if size == 0 {
                        self.state = State::Trailers;
                        self.scan_offset = 0;
                        self.line_start = 0;
                        return self.progress(input, consumed, None);
                    }
                    self.state = State::ChunkData { remaining: size };
                }
                State::ChunkData { remaining } => {
                    let available = input.len().saturating_sub(consumed);
                    let length = available.min(remaining.min(usize::MAX as u64) as usize);
                    if length == 0 {
                        if eof {
                            return self.fail(ParseError::Incomplete);
                        }
                        return self.progress(input, consumed, None);
                    }
                    let Some(length_u64) = u64::try_from(length).ok() else {
                        return self.fail(ParseError::BodyTooLarge);
                    };
                    let Some(decoded_bytes) = self.decoded_bytes.checked_add(length_u64) else {
                        return self.fail(ParseError::BodyTooLarge);
                    };
                    if decoded_bytes > self.body_limits.total_body_bytes {
                        return self.fail(ParseError::BodyTooLarge);
                    }
                    self.decoded_bytes = decoded_bytes;
                    self.state = if length as u64 == remaining {
                        State::ChunkDataCrlf
                    } else {
                        State::ChunkData {
                            remaining: remaining - length as u64,
                        }
                    };
                    return self.progress(
                        input,
                        consumed + length,
                        Some(BodyEvent::Data(Span {
                            offset: consumed,
                            length,
                        })),
                    );
                }
                State::ChunkDataCrlf => {
                    let available = input.len().saturating_sub(consumed);
                    if available < 2 {
                        if eof {
                            return self.fail(ParseError::Incomplete);
                        }
                        return self.progress(input, consumed, None);
                    }
                    if input.get(consumed..consumed + 2) != Some(&b"\r\n"[..]) {
                        return self.fail(ParseError::Invalid);
                    }
                    consumed += 2;
                    self.state = State::ChunkSize;
                    self.scan_offset = consumed;
                    self.line_start = consumed;
                }
                State::Trailers => {
                    let trailer_limit = limits.header_bytes.min(self.body_limits.trailer_bytes);
                    let Some((line, next)) = self.read_line(input, trailer_limit)? else {
                        self.check_trailer_limit(input, trailer_limit)?;
                        return self.need_more(input, eof, consumed);
                    };
                    self.check_trailer_end(next, trailer_limit)?;
                    if line.is_empty() {
                        self.state = State::Complete;
                        return self.progress(
                            input,
                            next,
                            Some(BodyEvent::Trailers {
                                count: self.trailer_count,
                            }),
                        );
                    }
                    if self.trailer_count >= limits.header_count
                        || self.trailer_count >= self.body_limits.trailer_count
                    {
                        return self.fail(ParseError::HeaderSectionTooLarge);
                    }
                    if self.trailer_count >= trailers.len() {
                        self.required_trailer_count = self.trailer_count.saturating_add(1);
                        return Err(ParseError::HeaderCapacity);
                    }
                    let Some(colon) = line.iter().position(|byte| *byte == b':') else {
                        return self.fail(ParseError::Invalid);
                    };
                    let name = &line[..colon];
                    if name.is_empty()
                        || name.len() > limits.field_name_bytes
                        || !name.iter().copied().all(is_token)
                        || self.prohibited_trailer(name)
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

                    let Some(line_offset) = self.line_start.checked_sub(consumed) else {
                        return self.fail(ParseError::Invalid);
                    };
                    let Some(value_offset) = line_offset
                        .checked_add(colon)
                        .and_then(|offset| offset.checked_add(1))
                        .and_then(|offset| offset.checked_add(value_start))
                    else {
                        return self.fail(ParseError::Invalid);
                    };
                    trailers[self.trailer_count] = Header {
                        name: Span {
                            offset: line_offset,
                            length: colon,
                        },
                        value: Span {
                            offset: value_offset,
                            length: value.len(),
                        },
                    };
                    if self.trailer_count == 0 {
                        self.trailer_base = trailers.as_mut_ptr();
                    }
                    self.trailer_count += 1;
                    self.scan_offset = next;
                    self.line_start = next;
                    return self.progress(input, consumed, None);
                }
                State::Complete => {
                    self.state = State::Finished;
                    return self.progress(input, consumed, Some(BodyEvent::Complete));
                }
                State::Finished => return self.progress(input, consumed, None),
                State::Error(error) => return Err(error),
            }
        }
    }

    pub(crate) fn required_trailer_capacity(&self) -> usize {
        self.required_trailer_count
    }

    fn read_line<'a>(
        &mut self,
        input: &'a [u8],
        limit: usize,
    ) -> Result<Option<(&'a [u8], usize)>, ParseError> {
        let mut offset = self.scan_offset;
        while offset < input.len() {
            match input[offset] {
                b'\r' => {
                    if input.get(offset + 1).is_some_and(|next| *next != b'\n') {
                        return self.fail(ParseError::Invalid);
                    }
                    offset += 1;
                }
                b'\n' => {
                    if offset == self.line_start || input[offset - 1] != b'\r' {
                        return self.fail(ParseError::Invalid);
                    }
                    let end = offset - 1;
                    if end.saturating_sub(self.line_start) > limit {
                        return self.fail(ParseError::HeaderSectionTooLarge);
                    }
                    return Ok(Some((&input[self.line_start..end], offset + 1)));
                }
                _ => offset += 1,
            }
        }
        self.scan_offset = if input.last() == Some(&b'\r') {
            offset.saturating_sub(1)
        } else {
            offset
        };
        let mut pending = input.len().saturating_sub(self.line_start);
        if pending > 0 && input.last() == Some(&b'\r') {
            pending -= 1;
        }
        if pending > limit {
            return self.fail(ParseError::HeaderSectionTooLarge);
        }
        Ok(None)
    }

    fn check_trailer_limit(&mut self, input: &[u8], limit: usize) -> Result<(), ParseError> {
        if input.len() > limit {
            return self.fail(ParseError::HeaderSectionTooLarge);
        }
        Ok(())
    }

    fn check_trailer_end(&mut self, end: usize, limit: usize) -> Result<(), ParseError> {
        if end > limit {
            return self.fail(ParseError::HeaderSectionTooLarge);
        }
        Ok(())
    }

    fn need_more(
        &mut self,
        input: &[u8],
        eof: bool,
        consumed: usize,
    ) -> Result<BodyProgress, ParseError> {
        if eof {
            return self.fail(ParseError::Incomplete);
        }
        self.progress(input, consumed, None)
    }

    fn progress(
        &mut self,
        input: &[u8],
        consumed: usize,
        event: Option<BodyEvent>,
    ) -> Result<BodyProgress, ParseError> {
        let retains_input = event.is_none()
            && matches!(
                self.state,
                State::ChunkSize | State::ChunkDataCrlf | State::Trailers
            );
        self.pending_length = if retains_input {
            input.len().saturating_sub(consumed)
        } else {
            0
        };
        if consumed > 0 && retains_input {
            self.scan_offset = self.scan_offset.saturating_sub(consumed);
            self.line_start = self.line_start.saturating_sub(consumed);
        } else if !retains_input {
            self.scan_offset = 0;
            self.line_start = 0;
        }
        Ok(BodyProgress { event, consumed })
    }

    fn fail<T>(&mut self, error: ParseError) -> Result<T, ParseError> {
        self.state = State::Error(error);
        Err(error)
    }

    fn prohibited_trailer(&self, name: &[u8]) -> bool {
        match self.trailer_policy {
            TrailerPolicy::Request => prohibited_request_trailer(name),
            TrailerPolicy::Response => prohibited_response_trailer(name),
        }
    }
}

fn parse_chunk_line(line: &[u8], extension_limit: usize) -> Result<u64, ParseError> {
    let mut offset = 0;
    let mut size = 0_u64;
    while let Some(byte) = line.get(offset).copied() {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => break,
        };
        size = size
            .checked_mul(16)
            .and_then(|size| size.checked_add(u64::from(digit)))
            .ok_or(ParseError::Invalid)?;
        offset += 1;
    }
    if offset == 0 {
        return Err(ParseError::Invalid);
    }
    if line.len() - offset > extension_limit {
        return Err(ParseError::HeaderSectionTooLarge);
    }

    while offset < line.len() {
        while line.get(offset).is_some_and(|byte| is_ows(*byte)) {
            offset += 1;
        }
        if line.get(offset) != Some(&b';') {
            return Err(ParseError::Invalid);
        }
        offset += 1;
        while line.get(offset).is_some_and(|byte| is_ows(*byte)) {
            offset += 1;
        }
        let name_start = offset;
        while line.get(offset).is_some_and(|byte| is_token(*byte)) {
            offset += 1;
        }
        if name_start == offset {
            return Err(ParseError::Invalid);
        }
        while line.get(offset).is_some_and(|byte| is_ows(*byte)) {
            offset += 1;
        }
        if line.get(offset) == Some(&b'=') {
            offset += 1;
            while line.get(offset).is_some_and(|byte| is_ows(*byte)) {
                offset += 1;
            }
            if line.get(offset) == Some(&b'"') {
                if !parse_quoted(line, &mut offset) {
                    return Err(ParseError::Invalid);
                }
            } else {
                let value_start = offset;
                while line.get(offset).is_some_and(|byte| is_token(*byte)) {
                    offset += 1;
                }
                if value_start == offset {
                    return Err(ParseError::Invalid);
                }
            }
        }
    }
    Ok(size)
}

fn parse_quoted(input: &[u8], offset: &mut usize) -> bool {
    *offset += 1;
    while let Some(byte) = input.get(*offset).copied() {
        match byte {
            b'"' => {
                *offset += 1;
                return true;
            }
            b'\\' => {
                *offset += 1;
                let Some(escaped) = input.get(*offset).copied() else {
                    return false;
                };
                if escaped != b'\t' && !(0x20..=0x7e).contains(&escaped) && escaped < 0x80 {
                    return false;
                }
                *offset += 1;
            }
            b'\t' | b' ' | 0x21 | 0x23..=0x5b | 0x5d..=0x7e | 0x80..=0xff => {
                *offset += 1;
            }
            _ => return false,
        }
    }
    false
}

fn prohibited_response_trailer(name: &[u8]) -> bool {
    [
        b"authorization".as_slice(),
        b"cache-control",
        b"connection",
        b"content-encoding",
        b"content-length",
        b"content-range",
        b"content-type",
        b"expect",
        b"host",
        b"max-forwards",
        b"proxy-authenticate",
        b"proxy-authorization",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
        b"www-authenticate",
    ]
    .iter()
    .any(|prohibited| name.eq_ignore_ascii_case(prohibited))
}

// NOTE: single source for the request-trailer prohibited set: the managed H1
// response-terminal stage and the H2 request-trailer gate both import this predicate
// instead of keeping local copies, so membership drift is impossible by construction.
pub fn prohibited_request_trailer(name: &[u8]) -> bool {
    [
        b"authorization".as_slice(),
        b"connection",
        b"content-encoding",
        b"content-length",
        b"content-range",
        b"content-type",
        b"expect",
        b"host",
        b"proxy-authorization",
        b"proxy-connection",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
        b"keep-alive",
    ]
    .iter()
    .any(|prohibited| name.eq_ignore_ascii_case(prohibited))
}

#[cfg(test)]
mod tests;
