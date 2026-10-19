use crate::body::{BodyDecoder, BodyEvent, BodyLimits, BodyProgress};
use crate::fields::{is_ows, is_token};
use crate::request_head::{Header, Limits, ParseError, RequestHead, Span};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestBodyAdmission {
    ReadyToRead,
    SendContinue,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionState {
    Pending { expect_continue: bool },
    AwaitingContinueWrite,
    Ready,
    Rejected,
}

pub struct RequestBodyDecoder<'a> {
    decoder: BodyDecoder,
    request_input: &'a [u8],
    request_headers: &'a [Header],
    permitted_trailers: &'a [&'a [u8]],
    admission: AdmissionState,
    failure: Option<ParseError>,
}

impl<'a> RequestBodyDecoder<'a> {
    pub fn new(
        request: RequestHead,
        request_input: &'a [u8],
        request_headers: &'a [Header],
        permitted_trailers: &'a [&'a [u8]],
        limits: BodyLimits,
    ) -> Result<Self, ParseError> {
        if request.content_length.is_some() && request.is_chunked {
            return Err(ParseError::Invalid);
        }
        if request.header_count > request_headers.len() {
            return Err(ParseError::HeaderCapacity);
        }
        for header in &request_headers[..request.header_count] {
            field(request_input, header.name).ok_or(ParseError::Invalid)?;
            field(request_input, header.value).ok_or(ParseError::Invalid)?;
        }
        for name in permitted_trailers {
            if name.is_empty() || !name.iter().copied().all(is_token) {
                return Err(ParseError::Invalid);
            }
        }
        if request
            .content_length
            .is_some_and(|length| length > limits.total_body_bytes)
        {
            return Err(ParseError::BodyTooLarge);
        }

        Ok(Self {
            decoder: BodyDecoder::new_request(request.content_length, request.is_chunked, limits),
            request_input,
            request_headers: &request_headers[..request.header_count],
            permitted_trailers,
            admission: AdmissionState::Pending {
                expect_continue: request.expect_continue
                    && (request.is_chunked || request.content_length.unwrap_or(0) > 0),
            },
            failure: None,
        })
    }

    pub fn admit_body(&mut self, allowed: bool) -> Result<RequestBodyAdmission, ParseError> {
        let AdmissionState::Pending { expect_continue } = self.admission else {
            return Err(ParseError::Invalid);
        };
        if !allowed {
            self.admission = AdmissionState::Rejected;
            return Ok(RequestBodyAdmission::Rejected);
        }
        if expect_continue {
            self.admission = AdmissionState::AwaitingContinueWrite;
            return Ok(RequestBodyAdmission::SendContinue);
        }
        self.admission = AdmissionState::Ready;
        Ok(RequestBodyAdmission::ReadyToRead)
    }

    pub fn continue_sent(&mut self) -> Result<(), ParseError> {
        if self.admission != AdmissionState::AwaitingContinueWrite {
            return Err(ParseError::Invalid);
        }
        self.admission = AdmissionState::Ready;
        Ok(())
    }

    pub fn next(
        &mut self,
        input: &[u8],
        eof: bool,
        trailers: &mut [Header],
        limits: Limits,
    ) -> Result<BodyProgress, ParseError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        match self.admission {
            AdmissionState::Pending { .. } | AdmissionState::AwaitingContinueWrite => {
                return Err(ParseError::AdmissionPending);
            }
            AdmissionState::Rejected => return Err(ParseError::BodyRejected),
            AdmissionState::Ready => {}
        }
        let progress = match self.decoder.next(input, eof, trailers, limits) {
            Ok(progress) => progress,
            Err(error) => return self.fail(error),
        };
        if let Some(BodyEvent::Trailers { count }) = progress.event {
            if count > trailers.len() {
                return self.fail(ParseError::HeaderCapacity);
            }
            for trailer in &trailers[..count] {
                let Some(name) = field(input, trailer.name) else {
                    return self.fail(ParseError::Invalid);
                };
                if !self
                    .permitted_trailers
                    .iter()
                    .any(|allowed| name.eq_ignore_ascii_case(allowed))
                {
                    return self.fail(ParseError::Invalid);
                }
                if let Err(error) = self.reject_connection_nominated(name) {
                    return self.fail(error);
                }
            }
        }
        Ok(progress)
    }

    fn reject_connection_nominated(&self, trailer_name: &[u8]) -> Result<(), ParseError> {
        for header in self.request_headers {
            let name = field(self.request_input, header.name).ok_or(ParseError::Invalid)?;
            if !name.eq_ignore_ascii_case(b"connection") {
                continue;
            }
            let value = field(self.request_input, header.value).ok_or(ParseError::Invalid)?;
            if connection_option_present(value, trailer_name)? {
                return Err(ParseError::Invalid);
            }
        }
        Ok(())
    }

    fn fail<T>(&mut self, error: ParseError) -> Result<T, ParseError> {
        self.failure = Some(error);
        Err(error)
    }
}

fn field(input: &[u8], span: Span) -> Option<&[u8]> {
    input.get(span.offset..span.offset.checked_add(span.length)?)
}

fn connection_option_present(value: &[u8], expected: &[u8]) -> Result<bool, ParseError> {
    let mut offset = 0;
    let mut found = false;
    loop {
        while value.get(offset).is_some_and(|byte| is_ows(*byte)) {
            offset += 1;
        }
        let start = offset;
        while value.get(offset).is_some_and(|byte| is_token(*byte)) {
            offset += 1;
        }
        if start == offset {
            return Err(ParseError::Invalid);
        }
        found |= value[start..offset].eq_ignore_ascii_case(expected);
        while value.get(offset).is_some_and(|byte| is_ows(*byte)) {
            offset += 1;
        }
        if offset == value.len() {
            return Ok(found);
        }
        if value[offset] != b',' {
            return Err(ParseError::Invalid);
        }
        offset += 1;
        if offset == value.len() {
            return Err(ParseError::Invalid);
        }
    }
}

#[cfg(test)]
mod tests;
