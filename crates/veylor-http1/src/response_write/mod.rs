use crate::fields::{is_token, upgrade_protocols_offered, valid_upgrade_field};
use crate::request_head::RequestHead;
use crate::request_write::Field;

pub fn encode_response_chunk(
    bytes: &[u8],
    output: &mut [u8],
) -> Result<usize, ResponseEncodeError> {
    let mut required_length = 0;
    encode_response_chunk_with_required_length(bytes, output, &mut required_length)
}

pub fn encode_response_chunk_with_required_length(
    bytes: &[u8],
    output: &mut [u8],
    required_length: &mut usize,
) -> Result<usize, ResponseEncodeError> {
    *required_length = 0;
    if bytes.is_empty() {
        return Err(ResponseEncodeError::Invalid);
    }
    let required = chunk_hex_length(bytes.len())
        .checked_add(4)
        .and_then(|length| length.checked_add(bytes.len()))
        .ok_or(ResponseEncodeError::BufferTooSmall)?;
    *required_length = required;
    if required > output.len() {
        return Err(ResponseEncodeError::BufferTooSmall);
    }
    crate::request_write::encode_chunk(bytes, output).map_err(map_request_encoding_error)
}

pub fn encode_response_final_chunk(
    trailers: &[Field<'_>],
    output: &mut [u8],
) -> Result<usize, ResponseEncodeError> {
    let mut required_length = 0;
    encode_response_final_chunk_with_required_length(trailers, output, &mut required_length)
}

pub fn encode_response_final_chunk_with_required_length(
    trailers: &[Field<'_>],
    output: &mut [u8],
    required_length: &mut usize,
) -> Result<usize, ResponseEncodeError> {
    *required_length = 0;
    let mut required = 5_usize;
    for field in trailers {
        if field.name.is_empty()
            || !field.name.iter().copied().all(is_token)
            || !valid_field_value(field.value)
        {
            return Err(ResponseEncodeError::InvalidField);
        }
        if prohibited_response_trailer(field.name) {
            return Err(ResponseEncodeError::InvalidFramingField);
        }
        required = required
            .checked_add(field.name.len())
            .and_then(|length| length.checked_add(2))
            .and_then(|length| length.checked_add(field.value.len()))
            .and_then(|length| length.checked_add(2))
            .ok_or(ResponseEncodeError::BufferTooSmall)?;
    }
    *required_length = required;
    if required > output.len() {
        return Err(ResponseEncodeError::BufferTooSmall);
    }
    crate::request_write::finish_chunks(trailers, output).map_err(map_request_encoding_error)
}

fn map_request_encoding_error(
    error: crate::request_write::RequestEncodeError,
) -> ResponseEncodeError {
    match error {
        crate::request_write::RequestEncodeError::BufferTooSmall => {
            ResponseEncodeError::BufferTooSmall
        }
        crate::request_write::RequestEncodeError::Invalid => ResponseEncodeError::Invalid,
        crate::request_write::RequestEncodeError::InvalidField => ResponseEncodeError::InvalidField,
        crate::request_write::RequestEncodeError::InvalidFramingField
        | crate::request_write::RequestEncodeError::MissingHost
        | crate::request_write::RequestEncodeError::DuplicateHost => {
            ResponseEncodeError::InvalidFramingField
        }
        crate::request_write::RequestEncodeError::OutputPending
        | crate::request_write::RequestEncodeError::BodyTooLong
        | crate::request_write::RequestEncodeError::WriteOverrun
        | crate::request_write::RequestEncodeError::InvalidState => {
            ResponseEncodeError::InvalidState
        }
    }
}

fn prohibited_response_trailer(name: &[u8]) -> bool {
    [
        b"authorization".as_slice(),
        b"cache-control",
        b"connection",
        b"content-encoding",
        b"content-range",
        b"content-type",
        b"date",
        b"expect",
        b"host",
        b"keep-alive",
        b"location",
        b"max-forwards",
        b"proxy-authenticate",
        b"proxy-authorization",
        b"proxy-connection",
        b"retry-after",
        b"set-cookie",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
        b"vary",
        b"www-authenticate",
    ]
    .iter()
    .any(|prohibited| name.eq_ignore_ascii_case(prohibited))
}

fn chunk_hex_length(mut value: usize) -> usize {
    let mut length = 1;
    while value >= 16 {
        value >>= 4;
        length += 1;
    }
    length
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseFraming {
    FixedLength,
    Chunked,
    CloseDelimited,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionHeader {
    Omit,
    Close,
    KeepAlive,
    Upgrade,
}

pub fn connection_header_for_response(
    request: RequestHead,
    request_input: &[u8],
    status: u16,
    framing: ResponseFraming,
    request_body_complete: bool,
    response_close: bool,
) -> ConnectionHeader {
    let connect_tunnel =
        request.method(request_input) == Some(&b"CONNECT"[..]) && (200..300).contains(&status);
    if status < 200 || connect_tunnel {
        return ConnectionHeader::Omit;
    }
    if !request_body_complete
        || response_close
        || request.connection.close
        || framing == ResponseFraming::CloseDelimited
    {
        return ConnectionHeader::Close;
    }

    match request.version(request_input) {
        Some(b"HTTP/1.1") => ConnectionHeader::Omit,
        Some(b"HTTP/1.0") if request.connection.keep_alive => ConnectionHeader::KeepAlive,
        _ => ConnectionHeader::Close,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseToWrite<'a> {
    pub request_method: &'a [u8],
    pub requested_upgrade: &'a [u8],
    pub status: u16,
    pub reason: &'a [u8],
    pub fields: &'a [Field<'a>],
    pub framing: ResponseFraming,
    pub content_length: u64,
    pub connection: ConnectionHeader,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodedResponseHead {
    pub length: usize,
    pub body_allowed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseEncodeError {
    Invalid,
    InvalidField,
    InvalidFramingField,
    BufferTooSmall,
    BodyOverrun,
    ShortBody,
    WriteOverrun,
    InvalidState,
}

pub struct ResponseWriteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
    invalid: bool,
}

impl<'a> ResponseWriteCursor<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            offset: 0,
            invalid: false,
        }
    }

    pub fn pending(&self) -> &[u8] {
        if self.invalid {
            return &[];
        }
        &self.bytes[self.offset..]
    }

    pub fn is_complete(&self) -> bool {
        !self.invalid && self.offset == self.bytes.len()
    }

    pub fn advance_write(&mut self, written: usize) -> Result<bool, ResponseEncodeError> {
        if self.invalid {
            return Err(ResponseEncodeError::InvalidState);
        }
        if written > self.bytes.len() - self.offset {
            self.invalid = true;
            return Err(ResponseEncodeError::WriteOverrun);
        }
        self.offset += written;
        Ok(self.is_complete())
    }
}

pub fn encode_response_head(
    response: &ResponseToWrite<'_>,
    output: &mut [u8],
) -> Result<EncodedResponseHead, ResponseEncodeError> {
    let mut required_length = 0;
    encode_response_head_with_required_length(response, output, &mut required_length)
}

pub fn encode_response_head_with_required_length(
    response: &ResponseToWrite<'_>,
    output: &mut [u8],
    required_length: &mut usize,
) -> Result<EncodedResponseHead, ResponseEncodeError> {
    *required_length = 0;
    validate_response(response)?;

    let required = response_head_length(response)?;
    *required_length = required;
    if required > output.len() {
        return Err(ResponseEncodeError::BufferTooSmall);
    }

    let mut writer = Writer::new(output);
    writer.write(b"HTTP/1.1 ")?;
    writer.write_decimal_status(response.status)?;
    writer.write(b" ")?;
    writer.write(response.reason)?;
    writer.write(b"\r\n")?;
    for field in response.fields {
        writer.write(field.name)?;
        writer.write(b": ")?;
        writer.write(field.value)?;
        writer.write(b"\r\n")?;
    }
    match response.connection {
        ConnectionHeader::Omit => {}
        ConnectionHeader::Close => writer.write(b"Connection: close\r\n")?,
        ConnectionHeader::KeepAlive => writer.write(b"Connection: keep-alive\r\n")?,
        ConnectionHeader::Upgrade => writer.write(b"Connection: upgrade\r\n")?,
    }
    match response.framing {
        ResponseFraming::FixedLength if content_length_allowed(response) => {
            writer.write(b"Content-Length: ")?;
            writer.write_decimal(response.content_length)?;
            writer.write(b"\r\n")?;
        }
        ResponseFraming::Chunked => writer.write(b"Transfer-Encoding: chunked\r\n")?,
        ResponseFraming::FixedLength | ResponseFraming::CloseDelimited => {}
    }
    writer.write(b"\r\n")?;
    Ok(EncodedResponseHead {
        length: writer.length,
        body_allowed: response_body_allowed(response),
    })
}

fn validate_response(response: &ResponseToWrite<'_>) -> Result<(), ResponseEncodeError> {
    if !(100..=599).contains(&response.status)
        || response.request_method.is_empty()
        || !response.request_method.iter().copied().all(is_token)
        || !valid_field_value(response.reason)
    {
        return Err(ResponseEncodeError::Invalid);
    }
    for field in response.fields {
        if field.name.is_empty()
            || !field.name.iter().copied().all(is_token)
            || !valid_field_value(field.value)
        {
            return Err(ResponseEncodeError::InvalidField);
        }
        if field.name.eq_ignore_ascii_case(b"content-length")
            || field.name.eq_ignore_ascii_case(b"transfer-encoding")
            || field.name.eq_ignore_ascii_case(b"connection")
        {
            return Err(ResponseEncodeError::InvalidFramingField);
        }
    }

    let connect_tunnel =
        response.request_method == b"CONNECT" && (200..300).contains(&response.status);
    let has_upgrade = response
        .fields
        .iter()
        .any(|field| field.name.eq_ignore_ascii_case(b"upgrade"));
    let upgrade_is_valid = response.fields.iter().all(|field| {
        !field.name.eq_ignore_ascii_case(b"upgrade") || valid_upgrade_field(field.value)
    });
    let upgrade_was_offered = response.fields.iter().all(|field| {
        !field.name.eq_ignore_ascii_case(b"upgrade")
            || upgrade_protocols_offered(field.value, response.requested_upgrade)
    });
    if response.status == 101 {
        if response.connection != ConnectionHeader::Upgrade
            || !has_upgrade
            || !upgrade_is_valid
            || !upgrade_was_offered
        {
            return Err(ResponseEncodeError::Invalid);
        }
    } else if response.connection == ConnectionHeader::Upgrade || !upgrade_is_valid {
        return Err(ResponseEncodeError::Invalid);
    }
    if ((response.status < 200 && response.status != 101) || connect_tunnel)
        && response.connection != ConnectionHeader::Omit
    {
        return Err(ResponseEncodeError::Invalid);
    }
    if has_upgrade && response.connection != ConnectionHeader::Upgrade {
        return Err(ResponseEncodeError::Invalid);
    }
    if response.framing == ResponseFraming::Chunked
        && (!chunked_allowed(response) || response.content_length != 0)
    {
        return Err(ResponseEncodeError::Invalid);
    }
    if response.framing == ResponseFraming::CloseDelimited
        && (!response_body_allowed(response)
            || response.connection != ConnectionHeader::Close
            || response.content_length != 0)
    {
        return Err(ResponseEncodeError::Invalid);
    }
    if !content_length_allowed(response) && response.content_length != 0 {
        return Err(ResponseEncodeError::Invalid);
    }
    if response.status == 205 && response.content_length != 0 {
        return Err(ResponseEncodeError::Invalid);
    }
    Ok(())
}

fn response_body_allowed(response: &ResponseToWrite<'_>) -> bool {
    response.status >= 200
        && response.request_method != b"HEAD"
        && response.status != 204
        && response.status != 205
        && response.status != 304
        && !(response.request_method == b"CONNECT" && (200..300).contains(&response.status))
}

fn content_length_allowed(response: &ResponseToWrite<'_>) -> bool {
    response.status >= 200
        && response.status != 204
        && !(response.request_method == b"CONNECT" && (200..300).contains(&response.status))
}

fn chunked_allowed(response: &ResponseToWrite<'_>) -> bool {
    response.status >= 200
        && response.status != 204
        && response.status != 205
        && !(response.request_method == b"CONNECT" && (200..300).contains(&response.status))
        && (response_body_allowed(response)
            || response.request_method == b"HEAD"
            || response.status == 304)
}

fn valid_field_value(value: &[u8]) -> bool {
    !value.iter().any(|byte| {
        *byte == b'\r' || *byte == b'\n' || (*byte < 0x20 && *byte != b'\t') || *byte == 0x7f
    })
}

fn response_head_length(response: &ResponseToWrite<'_>) -> Result<usize, ResponseEncodeError> {
    let mut length = 0;
    add_length(&mut length, b"HTTP/1.1 ".len())?;
    add_length(&mut length, 3)?;
    add_length(&mut length, 1)?;
    add_length(&mut length, response.reason.len())?;
    add_length(&mut length, 2)?;
    for field in response.fields {
        add_length(&mut length, field.name.len())?;
        add_length(&mut length, 2)?;
        add_length(&mut length, field.value.len())?;
        add_length(&mut length, 2)?;
    }
    match response.connection {
        ConnectionHeader::Omit => {}
        ConnectionHeader::Close => add_length(&mut length, b"Connection: close\r\n".len())?,
        ConnectionHeader::KeepAlive => {
            add_length(&mut length, b"Connection: keep-alive\r\n".len())?;
        }
        ConnectionHeader::Upgrade => add_length(&mut length, b"Connection: upgrade\r\n".len())?,
    }
    match response.framing {
        ResponseFraming::FixedLength if content_length_allowed(response) => {
            add_length(&mut length, b"Content-Length: ".len())?;
            add_length(&mut length, decimal_length(response.content_length))?;
            add_length(&mut length, 2)?;
        }
        ResponseFraming::Chunked => {
            add_length(&mut length, b"Transfer-Encoding: chunked\r\n".len())?;
        }
        ResponseFraming::FixedLength | ResponseFraming::CloseDelimited => {}
    }
    add_length(&mut length, 2)?;
    Ok(length)
}

fn add_length(length: &mut usize, amount: usize) -> Result<(), ResponseEncodeError> {
    *length = length
        .checked_add(amount)
        .ok_or(ResponseEncodeError::BufferTooSmall)?;
    Ok(())
}

fn decimal_length(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 10 {
        value /= 10;
        length += 1;
    }
    length
}

pub struct FixedResponseBodyWriter<'a> {
    content_length: u64,
    queued_bytes: u64,
    written_bytes: u64,
    current: &'a [u8],
    offset: usize,
    body_allowed: bool,
    finish_called: bool,
    complete: bool,
    invalid: bool,
}

impl<'a> FixedResponseBodyWriter<'a> {
    pub const fn new(content_length: u64, body_allowed: bool) -> Self {
        Self {
            content_length,
            queued_bytes: 0,
            written_bytes: 0,
            current: &[],
            offset: 0,
            body_allowed,
            finish_called: false,
            complete: !body_allowed || content_length == 0,
            invalid: false,
        }
    }

    pub fn pending(&self) -> &'a [u8] {
        &self.current[self.offset..]
    }

    pub fn queue(&mut self, bytes: &'a [u8]) -> Result<(), ResponseEncodeError> {
        if self.invalid || self.complete || self.finish_called || !self.body_allowed {
            return Err(ResponseEncodeError::InvalidState);
        }
        if !self.current.is_empty() {
            return Err(ResponseEncodeError::InvalidState);
        }
        if bytes.is_empty() {
            return Ok(());
        }
        let length = u64::try_from(bytes.len()).map_err(|_| ResponseEncodeError::BodyOverrun)?;
        if length > self.content_length - self.queued_bytes {
            self.invalid = true;
            return Err(ResponseEncodeError::BodyOverrun);
        }
        self.current = bytes;
        self.offset = 0;
        self.queued_bytes += length;
        Ok(())
    }

    pub fn advance_write(&mut self, written: usize) -> Result<bool, ResponseEncodeError> {
        if self.invalid || self.complete || self.current.is_empty() {
            return Err(ResponseEncodeError::InvalidState);
        }
        if written > self.current.len() - self.offset {
            self.invalid = true;
            return Err(ResponseEncodeError::WriteOverrun);
        }
        self.offset += written;
        if self.offset == self.current.len() {
            self.written_bytes += self.current.len() as u64;
            self.current = &[];
            self.offset = 0;
            if self.written_bytes == self.content_length {
                self.complete = true;
            }
        }
        if self.finish_called && self.current.is_empty() {
            self.finish()?;
        }
        Ok(self.complete)
    }

    pub fn finish(&mut self) -> Result<bool, ResponseEncodeError> {
        if self.invalid {
            return Err(ResponseEncodeError::InvalidState);
        }
        if self.complete {
            return Ok(true);
        }
        self.finish_called = true;
        if !self.current.is_empty() {
            return Ok(false);
        }
        if self.written_bytes != self.content_length {
            self.invalid = true;
            return Err(ResponseEncodeError::ShortBody);
        }
        self.complete = true;
        Ok(true)
    }
}

struct Writer<'a> {
    output: &'a mut [u8],
    length: usize,
}

impl<'a> Writer<'a> {
    fn new(output: &'a mut [u8]) -> Self {
        Self { output, length: 0 }
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), ResponseEncodeError> {
        let end = self
            .length
            .checked_add(bytes.len())
            .ok_or(ResponseEncodeError::BufferTooSmall)?;
        let destination = self
            .output
            .get_mut(self.length..end)
            .ok_or(ResponseEncodeError::BufferTooSmall)?;
        destination.copy_from_slice(bytes);
        self.length = end;
        Ok(())
    }

    fn write_decimal(&mut self, mut value: u64) -> Result<(), ResponseEncodeError> {
        let mut digits = [0; 20];
        let mut start = digits.len();
        loop {
            start -= 1;
            digits[start] = b'0' + (value % 10) as u8;
            value /= 10;
            if value == 0 {
                break;
            }
        }
        self.write(&digits[start..])
    }

    fn write_decimal_status(&mut self, status: u16) -> Result<(), ResponseEncodeError> {
        let status = status as u32;
        self.write(&[
            b'0' + (status / 100) as u8,
            b'0' + ((status / 10) % 10) as u8,
            b'0' + (status % 10) as u8,
        ])
    }
}

#[cfg(test)]
mod tests;
