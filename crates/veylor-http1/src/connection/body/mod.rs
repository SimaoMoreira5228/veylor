use crate::body::{BodyEvent as DecodedBodyEvent, BodyLimits};
use crate::request_body::{RequestBodyAdmission, RequestBodyDecoder};
use crate::request_head::{Header, Limits, ParseError, RequestHead, Span};

use super::{IoCompletion, IoOperation, IoResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyState {
    AdmissionPending,
    AwaitingContinue,
    Reading,
    Complete,
    Rejected,
    Cancelled,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyResult {
    Data,
    Trailers,
    NeedMore,
    WouldBlock,
    Interrupted,
    Complete,
    PeerClosed,
    Cancelled,
    TransportError,
    AdmissionPending,
    Rejected,
    Invalid,
    TooLarge,
    HeaderCapacity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BodyEvent {
    pub span: Option<Span>,
    pub consumed: usize,
    pub trailer_count: usize,
    pub result: BodyResult,
    pub body_complete: bool,
    pub error_code: i32,
}

pub struct BodyDriver<'a> {
    connection_id: u64,
    operation_id: Option<u64>,
    pending_length: usize,
    request_body: RequestBodyDecoder<'a>,
    chunked: bool,
    content_length: u64,
    received_body_bytes: u64,
    state: BodyState,
    result: BodyResult,
    error_code: i32,
}

impl<'a> BodyDriver<'a> {
    pub fn new(
        connection_id: u64,
        request: RequestHead,
        request_input: &'a [u8],
        request_headers: &'a [Header],
        permitted_trailers: &'a [&'a [u8]],
        body_limits: BodyLimits,
    ) -> Result<Self, ParseError> {
        if request.length > request_input.len() {
            return Err(ParseError::Invalid);
        }
        let request_body = RequestBodyDecoder::new(
            request,
            request_input,
            request_headers,
            permitted_trailers,
            body_limits,
        )?;
        Ok(Self {
            connection_id,
            operation_id: None,
            pending_length: request_input.len() - request.length,
            request_body,
            chunked: request.is_chunked,
            content_length: request.content_length.unwrap_or(0),
            received_body_bytes: 0,
            state: BodyState::AdmissionPending,
            result: BodyResult::AdmissionPending,
            error_code: 0,
        })
    }

    pub fn admit_body(&mut self, allowed: bool) -> Result<RequestBodyAdmission, ParseError> {
        let admission = self.request_body.admit_body(allowed)?;
        match admission {
            RequestBodyAdmission::ReadyToRead => {
                self.state = BodyState::Reading;
                self.result = BodyResult::NeedMore;
            }
            RequestBodyAdmission::SendContinue => {
                self.state = BodyState::AwaitingContinue;
                self.result = BodyResult::AdmissionPending;
            }
            RequestBodyAdmission::Rejected => {
                self.state = BodyState::Rejected;
                self.result = BodyResult::Rejected;
            }
        }
        Ok(admission)
    }

    pub fn continue_sent(&mut self) -> Result<(), ParseError> {
        if self.state != BodyState::AwaitingContinue {
            return Err(ParseError::Invalid);
        }
        self.request_body.continue_sent()?;
        self.state = BodyState::Reading;
        self.result = BodyResult::NeedMore;
        Ok(())
    }

    pub fn consume(
        &mut self,
        completion: IoCompletion,
        input: &[u8],
        trailers: &mut [Header],
        limits: Limits,
    ) -> BodyEvent {
        if self.state != BodyState::Reading {
            return self.event(0, 0, None);
        }
        if !completion.valid_for(self.connection_id, self.operation_id, IoOperation::Read) {
            return self.fail(BodyResult::Invalid);
        }
        self.operation_id = Some(completion.operation_id);

        if !matches!(completion.result, IoResult::Progress { .. })
            && input.len() != self.pending_length
        {
            return self.fail(BodyResult::Invalid);
        }

        match completion.result {
            IoResult::WouldBlock => {
                self.result = BodyResult::WouldBlock;
                self.event(0, 0, None)
            }
            IoResult::Interrupted => {
                self.result = BodyResult::Interrupted;
                self.event(0, 0, None)
            }
            IoResult::EndOfStream => self.fail(BodyResult::PeerClosed),
            IoResult::Cancelled => {
                self.state = BodyState::Cancelled;
                self.result = BodyResult::Cancelled;
                self.event(0, 0, None)
            }
            IoResult::Closed => self.fail(BodyResult::Invalid),
            IoResult::Error { code } => {
                self.state = BodyState::Failed;
                self.result = BodyResult::TransportError;
                self.error_code = code;
                self.event(0, 0, None)
            }
            IoResult::Progress { transferred_bytes } => {
                let Some(added) = input.len().checked_sub(self.pending_length) else {
                    return self.fail(BodyResult::Invalid);
                };
                if added != transferred_bytes {
                    return self.fail(BodyResult::Invalid);
                }
                self.decode(input, trailers, limits)
            }
        }
    }

    pub fn consume_buffered(
        &mut self,
        input: &[u8],
        trailers: &mut [Header],
        limits: Limits,
    ) -> BodyEvent {
        if self.state != BodyState::Reading {
            return self.event(0, 0, None);
        }
        if input.len() != self.pending_length {
            return self.fail(BodyResult::Invalid);
        }
        self.decode(input, trailers, limits)
    }

    pub fn state(&self) -> BodyState {
        self.state
    }

    fn decode(&mut self, input: &[u8], trailers: &mut [Header], limits: Limits) -> BodyEvent {
        let progress = match self.request_body.next(input, false, trailers, limits) {
            Ok(progress) => progress,
            Err(error) => return self.fail(parse_error(error)),
        };
        let Some(pending_length) = input.len().checked_sub(progress.consumed) else {
            return self.fail(BodyResult::Invalid);
        };
        self.pending_length = pending_length;

        match progress.event {
            Some(DecodedBodyEvent::Data(span)) => {
                let Ok(span_length) = u64::try_from(span.length) else {
                    return self.fail(BodyResult::TooLarge);
                };
                let Some(received_body_bytes) = self.received_body_bytes.checked_add(span_length)
                else {
                    return self.fail(BodyResult::TooLarge);
                };
                self.received_body_bytes = received_body_bytes;
                let body_complete =
                    !self.chunked && self.received_body_bytes == self.content_length;
                if body_complete {
                    self.state = BodyState::Complete;
                    self.result = BodyResult::Complete;
                } else {
                    self.result = BodyResult::Data;
                }
                self.event(progress.consumed, 0, Some(span))
            }
            Some(DecodedBodyEvent::Trailers { count }) => {
                self.result = BodyResult::Trailers;
                self.event(progress.consumed, count, None)
            }
            Some(DecodedBodyEvent::Complete) => {
                self.state = BodyState::Complete;
                self.result = BodyResult::Complete;
                self.event(progress.consumed, 0, None)
            }
            None => {
                self.result = BodyResult::NeedMore;
                self.event(progress.consumed, 0, None)
            }
        }
    }

    fn event(&self, consumed: usize, trailer_count: usize, span: Option<Span>) -> BodyEvent {
        BodyEvent {
            span,
            consumed,
            trailer_count,
            result: self.result,
            body_complete: self.state == BodyState::Complete,
            error_code: self.error_code,
        }
    }

    fn fail(&mut self, result: BodyResult) -> BodyEvent {
        self.state = BodyState::Failed;
        self.result = result;
        self.event(0, 0, None)
    }
}

fn parse_error(error: ParseError) -> BodyResult {
    match error {
        ParseError::BodyTooLarge => BodyResult::TooLarge,
        ParseError::HeaderCapacity => BodyResult::HeaderCapacity,
        ParseError::AdmissionPending => BodyResult::AdmissionPending,
        ParseError::BodyRejected => BodyResult::Rejected,
        ParseError::Invalid
        | ParseError::RequestLineTooLarge
        | ParseError::StatusLineTooLarge
        | ParseError::HeaderSectionTooLarge
        | ParseError::BufferChanged
        | ParseError::ExpectationFailed
        | ParseError::Incomplete
        | ParseError::Cancelled => BodyResult::Invalid,
    }
}

#[cfg(test)]
mod tests;
