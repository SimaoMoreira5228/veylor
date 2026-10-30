use crate::fields::is_token;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Field<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyFraming {
    None,
    Fixed(u64),
    Chunked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Request<'a> {
    pub method: &'a [u8],
    pub target: &'a [u8],
    pub fields: &'a [Field<'a>],
    pub body: BodyFraming,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestEncodeError {
    Invalid,
    InvalidField,
    InvalidFramingField,
    MissingHost,
    DuplicateHost,
    BufferTooSmall,
    OutputPending,
    BodyTooLong,
    WriteOverrun,
    InvalidState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BodyState {
    Fixed(u64),
    Chunked,
    Finished,
    Cancelled,
}

pub struct ClientRequestWriter<'a> {
    output: &'a mut [u8],
    pending_offset: usize,
    pending_length: usize,
    head_pending: bool,
    body: BodyState,
    wrote_any: bool,
}

impl<'a> ClientRequestWriter<'a> {
    pub fn new(request: &Request<'_>, output: &'a mut [u8]) -> Result<Self, RequestEncodeError> {
        let head_length = encode_head(request, output)?;
        let body = match request.body {
            BodyFraming::None | BodyFraming::Fixed(0) => BodyState::Finished,
            BodyFraming::Fixed(length) => BodyState::Fixed(length),
            BodyFraming::Chunked => BodyState::Chunked,
        };
        Ok(Self {
            output,
            pending_offset: 0,
            pending_length: head_length,
            head_pending: true,
            body,
            wrote_any: false,
        })
    }

    pub fn pending(&self) -> &[u8] {
        &self.output[self.pending_offset..self.pending_length]
    }

    pub fn advance_write(&mut self, written: usize) -> Result<(), RequestEncodeError> {
        if written > self.pending().len() {
            return Err(RequestEncodeError::WriteOverrun);
        }
        if written == 0 {
            return Ok(());
        }

        self.wrote_any = true;
        self.pending_offset += written;
        if self.pending_offset == self.pending_length {
            self.pending_offset = 0;
            self.pending_length = 0;
            self.head_pending = false;
        }
        Ok(())
    }

    pub fn queue_fixed(&mut self, bytes: &[u8]) -> Result<usize, RequestEncodeError> {
        self.prepare_body_write()?;
        let BodyState::Fixed(remaining) = self.body else {
            return Err(RequestEncodeError::InvalidState);
        };
        let body_length =
            u64::try_from(bytes.len()).map_err(|_| RequestEncodeError::BodyTooLong)?;
        if body_length > remaining {
            return Err(RequestEncodeError::BodyTooLong);
        }
        if bytes.is_empty() {
            return Ok(0);
        }

        let length = bytes.len().min(self.output.len());
        if length == 0 {
            return Err(RequestEncodeError::BufferTooSmall);
        }
        let accepted = u64::try_from(length).map_err(|_| RequestEncodeError::BodyTooLong)?;
        self.output[..length].copy_from_slice(&bytes[..length]);
        self.pending_length = length;
        self.body = BodyState::Fixed(remaining - accepted);
        Ok(length)
    }

    pub fn queue_chunk(&mut self, bytes: &[u8]) -> Result<(), RequestEncodeError> {
        self.prepare_body_write()?;
        if self.body != BodyState::Chunked {
            return Err(RequestEncodeError::InvalidState);
        }
        self.pending_length = encode_chunk(bytes, self.output)?;
        Ok(())
    }

    pub fn finish_chunked(&mut self, trailers: &[Field<'_>]) -> Result<(), RequestEncodeError> {
        self.prepare_body_write()?;
        if self.body != BodyState::Chunked {
            return Err(RequestEncodeError::InvalidState);
        }
        self.pending_length = finish_chunks(trailers, self.output)?;
        self.body = BodyState::Finished;
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        !self.head_pending
            && self.pending_length == 0
            && matches!(self.body, BodyState::Fixed(0) | BodyState::Finished)
    }

    pub fn is_cancelled(&self) -> bool {
        self.body == BodyState::Cancelled
    }

    pub fn cancel(&mut self) -> bool {
        if self.body == BodyState::Cancelled {
            return self.wrote_any;
        }
        self.body = BodyState::Cancelled;
        self.pending_offset = 0;
        self.pending_length = 0;
        self.head_pending = false;
        self.wrote_any
    }

    fn prepare_body_write(&self) -> Result<(), RequestEncodeError> {
        if self.body == BodyState::Cancelled || self.body == BodyState::Finished {
            return Err(RequestEncodeError::InvalidState);
        }
        if self.head_pending {
            return Err(RequestEncodeError::OutputPending);
        }
        if self.pending_length > 0 {
            return Err(RequestEncodeError::OutputPending);
        }
        Ok(())
    }
}

pub fn encode_head(request: &Request<'_>, output: &mut [u8]) -> Result<usize, RequestEncodeError> {
    let mut required_length = 0;
    encode_head_with_required_length(request, output, &mut required_length)
}

pub fn encode_head_with_required_length(
    request: &Request<'_>,
    output: &mut [u8],
    required_length: &mut usize,
) -> Result<usize, RequestEncodeError> {
    *required_length = 0;
    if request.method.is_empty() || !request.method.iter().copied().all(is_token) {
        return Err(RequestEncodeError::Invalid);
    }

    if request.target.is_empty()
        || request
            .target
            .iter()
            .any(|byte| *byte <= 0x20 || *byte == 0x7f)
        || crate::request_target::classify(request.method, request.target).is_none()
    {
        return Err(RequestEncodeError::Invalid);
    }

    let mut host_count = 0;
    let mut head_length = request
        .method
        .len()
        .saturating_add(1)
        .saturating_add(request.target.len())
        .saturating_add(b" HTTP/1.1\r\n".len())
        .saturating_add(2);
    for field in request.fields {
        if field.name.is_empty() || !field.name.iter().copied().all(is_token) {
            return Err(RequestEncodeError::InvalidField);
        }

        if field.value.iter().any(|byte| {
            *byte == b'\r' || *byte == b'\n' || (*byte < 0x20 && *byte != b'\t') || *byte == 0x7f
        }) {
            return Err(RequestEncodeError::InvalidField);
        }

        if field.name.eq_ignore_ascii_case(b"host") {
            if field.value.is_empty() || !crate::request_target::valid_host_field(field.value) {
                return Err(RequestEncodeError::InvalidField);
            }
            host_count += 1;
        }

        head_length = head_length
            .saturating_add(field.name.len())
            .saturating_add(2)
            .saturating_add(field.value.len())
            .saturating_add(2);

        if field.name.eq_ignore_ascii_case(b"content-length")
            || field.name.eq_ignore_ascii_case(b"transfer-encoding")
        {
            return Err(RequestEncodeError::InvalidFramingField);
        }
    }

    match host_count {
        0 => return Err(RequestEncodeError::MissingHost),
        1 => {}
        _ => return Err(RequestEncodeError::DuplicateHost),
    }

    match request.body {
        BodyFraming::None => {}
        BodyFraming::Fixed(length) => {
            head_length = head_length
                .saturating_add(b"Content-Length: ".len())
                .saturating_add(decimal_length(length))
                .saturating_add(2);
        }
        BodyFraming::Chunked => {
            head_length = head_length.saturating_add(b"Transfer-Encoding: chunked\r\n".len());
        }
    }

    *required_length = head_length;

    if output.len() < head_length {
        return Err(RequestEncodeError::BufferTooSmall);
    }

    let mut writer = Writer::new(output);
    writer.write(request.method)?;
    writer.write(b" ")?;
    writer.write(request.target)?;
    writer.write(b" HTTP/1.1\r\n")?;
    for field in request.fields {
        writer.write(field.name)?;
        writer.write(b": ")?;
        writer.write(field.value)?;
        writer.write(b"\r\n")?;
    }

    match request.body {
        BodyFraming::None => {}
        BodyFraming::Fixed(length) => {
            writer.write(b"Content-Length: ")?;
            writer.write_decimal(length)?;
            writer.write(b"\r\n")?;
        }
        BodyFraming::Chunked => writer.write(b"Transfer-Encoding: chunked\r\n")?,
    }

    writer.write(b"\r\n")?;
    Ok(writer.length)
}

fn decimal_length(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 10 {
        value /= 10;
        length += 1;
    }
    length
}

pub fn encode_chunk(chunk: &[u8], output: &mut [u8]) -> Result<usize, RequestEncodeError> {
    let mut required_length = 0;
    encode_chunk_with_required_length(chunk, output, &mut required_length)
}

pub fn encode_chunk_with_required_length(
    chunk: &[u8],
    output: &mut [u8],
    required_length: &mut usize,
) -> Result<usize, RequestEncodeError> {
    *required_length = 0;
    if chunk.is_empty() {
        return Err(RequestEncodeError::Invalid);
    }
    let required = chunk_hex_length(chunk.len())
        .checked_add(4)
        .and_then(|length| length.checked_add(chunk.len()))
        .ok_or(RequestEncodeError::BufferTooSmall)?;
    *required_length = required;
    if required > output.len() {
        return Err(RequestEncodeError::BufferTooSmall);
    }
    let mut writer = Writer::new(output);
    writer.write_hex(chunk.len())?;
    writer.write(b"\r\n")?;
    writer.write(chunk)?;
    writer.write(b"\r\n")?;
    Ok(writer.length)
}

pub(crate) fn chunk_hex_length(mut value: usize) -> usize {
    let mut length = 1;
    while value >= 16 {
        value >>= 4;
        length += 1;
    }
    length
}

pub fn finish_chunks(fields: &[Field<'_>], output: &mut [u8]) -> Result<usize, RequestEncodeError> {
    let mut required_length = 0;
    finish_chunks_with_required_length(fields, output, &mut required_length)
}

pub fn finish_chunks_with_required_length(
    fields: &[Field<'_>],
    output: &mut [u8],
    required_length: &mut usize,
) -> Result<usize, RequestEncodeError> {
    *required_length = 0;
    let mut required = b"0\r\n\r\n".len();
    for field in fields {
        validate_trailer(field)?;
        required = required
            .saturating_add(field.name.len())
            .saturating_add(2)
            .saturating_add(field.value.len())
            .saturating_add(2);
    }
    *required_length = required;
    if output.len() < required {
        return Err(RequestEncodeError::BufferTooSmall);
    }

    let mut writer = Writer::new(output);
    writer.write(b"0\r\n")?;
    for field in fields {
        writer.write(field.name)?;
        writer.write(b": ")?;
        writer.write(field.value)?;
        writer.write(b"\r\n")?;
    }
    writer.write(b"\r\n")?;
    Ok(writer.length)
}

fn validate_trailer(field: &Field<'_>) -> Result<(), RequestEncodeError> {
    if field.name.is_empty() || !field.name.iter().copied().all(is_token) {
        return Err(RequestEncodeError::InvalidField);
    }
    if field.value.iter().any(|byte| {
        *byte == b'\r' || *byte == b'\n' || (*byte < 0x20 && *byte != b'\t') || *byte == 0x7f
    }) {
        return Err(RequestEncodeError::InvalidField);
    }
    if field.name.eq_ignore_ascii_case(b"content-length")
        || field.name.eq_ignore_ascii_case(b"transfer-encoding")
        || field.name.eq_ignore_ascii_case(b"host")
        || field.name.eq_ignore_ascii_case(b"trailer")
    {
        return Err(RequestEncodeError::InvalidFramingField);
    }
    Ok(())
}

struct Writer<'a> {
    output: &'a mut [u8],
    length: usize,
}

impl<'a> Writer<'a> {
    fn new(output: &'a mut [u8]) -> Self {
        Self { output, length: 0 }
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), RequestEncodeError> {
        let Some(end) = self.length.checked_add(bytes.len()) else {
            return Err(RequestEncodeError::BufferTooSmall);
        };
        let Some(destination) = self.output.get_mut(self.length..end) else {
            return Err(RequestEncodeError::BufferTooSmall);
        };
        destination.copy_from_slice(bytes);
        self.length = end;
        Ok(())
    }

    fn write_decimal(&mut self, mut value: u64) -> Result<(), RequestEncodeError> {
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

    fn write_hex(&mut self, mut value: usize) -> Result<(), RequestEncodeError> {
        let mut digits = [0; 2 * core::mem::size_of::<usize>()];
        let mut start = digits.len();
        loop {
            start -= 1;
            let digit = (value & 0xf) as u8;
            digits[start] = if digit < 10 {
                b'0' + digit
            } else {
                b'a' + digit - 10
            };
            value >>= 4;
            if value == 0 {
                break;
            }
        }
        self.write(&digits[start..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_bounded_request_head_and_body_frames() {
        let fields = [Field {
            name: b"Host",
            value: b"example.test",
        }];
        let request = Request {
            method: b"POST",
            target: b"/items",
            fields: &fields,
            body: BodyFraming::Fixed(123),
        };
        let mut output = [0; 128];
        let length = encode_head(&request, &mut output).unwrap();
        assert_eq!(
            &output[..length],
            b"POST /items HTTP/1.1\r\nHost: example.test\r\nContent-Length: 123\r\n\r\n"
        );

        let chunked = Request {
            body: BodyFraming::Chunked,
            ..request
        };
        let length = encode_head(&chunked, &mut output).unwrap();
        assert_eq!(
            &output[..length],
            b"POST /items HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\n\r\n"
        );

        let length = encode_chunk(b"abc", &mut output).unwrap();
        assert_eq!(&output[..length], b"3\r\nabc\r\n");
        let trailers = [Field {
            name: b"Digest",
            value: b"sha-256=abc",
        }];
        let length = finish_chunks(&trailers, &mut output).unwrap();
        assert_eq!(&output[..length], b"0\r\nDigest: sha-256=abc\r\n\r\n");
    }

    #[test]
    fn reports_required_head_capacity_without_partial_output() {
        let fields = [Field {
            name: b"Host",
            value: b"example.test",
        }];
        let request = Request {
            method: b"POST",
            target: b"/items",
            fields: &fields,
            body: BodyFraming::Fixed(12345),
        };
        let mut output = [0xaa; 4];
        let mut required_length = 0;

        assert_eq!(
            encode_head_with_required_length(&request, &mut output, &mut required_length),
            Err(RequestEncodeError::BufferTooSmall)
        );
        assert_eq!(required_length, 67);
        assert_eq!(output, [0xaa; 4]);
    }

    #[test]
    fn reports_required_trailer_capacity_without_partial_output() {
        let trailers = [Field {
            name: b"Digest",
            value: b"sha-256=abc",
        }];
        let mut output = [0xaa; 4];
        let mut required_length = 0;

        assert_eq!(
            finish_chunks_with_required_length(&trailers, &mut output, &mut required_length),
            Err(RequestEncodeError::BufferTooSmall)
        );
        assert_eq!(required_length, b"0\r\nDigest: sha-256=abc\r\n\r\n".len());
        assert_eq!(output, [0xaa; 4]);
    }

    #[test]
    fn reports_required_chunk_capacity_without_partial_output() {
        let mut output = [0; 8];
        let mut required_length = 0;

        assert_eq!(
            encode_chunk_with_required_length(b"abc", &mut output, &mut required_length),
            Ok(8)
        );
        assert_eq!(required_length, 8);
        assert_eq!(&output, b"3\r\nabc\r\n");

        let mut output = [0xaa; 7];
        let mut required_length = 0;

        assert_eq!(
            encode_chunk_with_required_length(b"abc", &mut output, &mut required_length),
            Err(RequestEncodeError::BufferTooSmall)
        );
        assert_eq!(required_length, 8);
        assert_eq!(output, [0xaa; 7]);

        let mut required_length = 0;

        assert_eq!(
            encode_chunk_with_required_length(b"", &mut [0; 8], &mut required_length),
            Err(RequestEncodeError::Invalid)
        );
        assert_eq!(required_length, 0);
    }

    #[test]
    fn rejects_ambiguous_fields_and_invalid_output_inputs() {
        let fields = [
            Field {
                name: b"Host",
                value: b"example.test",
            },
            Field {
                name: b"content-length",
                value: b"4",
            },
        ];
        let request = Request {
            method: b"GET",
            target: b"/",
            fields: &fields,
            body: BodyFraming::None,
        };
        assert_eq!(
            encode_head(&request, &mut [0; 128]),
            Err(RequestEncodeError::InvalidFramingField)
        );

        let fields = [Field {
            name: b"Host",
            value: b"evil\r\nX: y",
        }];
        let request = Request {
            fields: &fields,
            ..request
        };
        assert_eq!(
            encode_head(&request, &mut [0; 128]),
            Err(RequestEncodeError::InvalidField)
        );
        assert_eq!(
            encode_chunk(b"abc", &mut [0; 2]),
            Err(RequestEncodeError::BufferTooSmall)
        );
    }

    #[test]
    fn resumes_partial_request_writes_and_streams_fixed_content() {
        let fields = [Field {
            name: b"Host",
            value: b"example.test",
        }];
        let request = Request {
            method: b"POST",
            target: b"/upload",
            fields: &fields,
            body: BodyFraming::Fixed(3),
        };
        let mut output = [0; 128];
        let mut writer = ClientRequestWriter::new(&request, &mut output).unwrap();

        assert_eq!(writer.advance_write(1), Ok(()));
        assert_eq!(writer.pending()[0], b'O');
        let remaining_head = writer.pending().len();
        writer.advance_write(remaining_head).unwrap();
        assert_eq!(
            writer.queue_fixed(b"abcd"),
            Err(RequestEncodeError::BodyTooLong)
        );
        assert_eq!(writer.queue_fixed(b"abc"), Ok(3));
        assert_eq!(writer.pending(), b"abc");
        assert_eq!(
            writer.queue_fixed(b""),
            Err(RequestEncodeError::OutputPending)
        );
        writer.advance_write(1).unwrap();
        assert_eq!(writer.pending(), b"bc");
        writer.advance_write(2).unwrap();
        assert!(writer.is_complete());
        assert_eq!(
            writer.advance_write(1),
            Err(RequestEncodeError::WriteOverrun)
        );

        let request = Request {
            body: BodyFraming::Fixed(200),
            ..request
        };
        let mut head = [0; 128];
        let head_length = encode_head(&request, &mut head).unwrap();
        let mut small_output = [0; 128];
        let mut writer =
            ClientRequestWriter::new(&request, &mut small_output[..head_length + 1]).unwrap();
        writer.advance_write(head_length).unwrap();
        let body = [b'x'; 200];
        let mut offset = 0;
        while offset < body.len() {
            let accepted = writer.queue_fixed(&body[offset..]).unwrap();
            assert!(accepted > 0);
            assert_eq!(writer.pending(), &body[offset..offset + accepted]);
            writer.advance_write(accepted).unwrap();
            offset += accepted;
        }
        assert!(writer.is_complete());
    }

    #[test]
    fn streams_chunked_requests_and_cancels_after_partial_output() {
        let fields = [Field {
            name: b"Host",
            value: b"example.test",
        }];
        let request = Request {
            method: b"POST",
            target: b"/upload",
            fields: &fields,
            body: BodyFraming::Chunked,
        };
        let mut output = [0; 128];
        let mut writer = ClientRequestWriter::new(&request, &mut output).unwrap();
        let head_length = writer.pending().len();
        writer.advance_write(head_length).unwrap();
        writer.queue_chunk(b"abc").unwrap();
        assert_eq!(writer.pending(), b"3\r\nabc\r\n");
        let chunk_length = writer.pending().len();
        writer.advance_write(chunk_length).unwrap();
        let trailers = [Field {
            name: b"Digest",
            value: b"sha-256=abc",
        }];
        writer.finish_chunked(&trailers).unwrap();
        assert_eq!(writer.pending(), b"0\r\nDigest: sha-256=abc\r\n\r\n");
        let final_length = writer.pending().len();
        writer.advance_write(final_length).unwrap();
        assert!(writer.is_complete());

        let mut cancelled = ClientRequestWriter::new(&request, &mut output).unwrap();
        cancelled.advance_write(1).unwrap();
        assert!(cancelled.cancel());
        assert!(cancelled.pending().is_empty());
        assert!(!cancelled.is_complete());
        assert_eq!(
            cancelled.queue_chunk(b"abc"),
            Err(RequestEncodeError::InvalidState)
        );
    }
}
