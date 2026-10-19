use crate::body::{BodyEvent, BodyLimits};
use crate::client_response::{ClientResponse, ClientResponseEvent};
use crate::request_head::{Header, Limits, ParseError};
use crate::response_head::RequestMethod;

use super::{IoCompletion, IoOperation, IoResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientReadState {
    Reading,
    Complete,
    Upgraded,
    Cancelled,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientReadResult {
    Informational,
    Head,
    Body,
    Trailers,
    NeedMore,
    WouldBlock,
    Interrupted,
    Complete,
    Upgraded,
    Cancelled,
    TransportError,
    Invalid,
    Incomplete,
    TooLarge,
    HeaderCapacity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientReadEvent {
    pub response: Option<ClientResponseEvent>,
    pub consumed: usize,
    pub result: ClientReadResult,
    pub error_code: i32,
    pub parse_error: Option<ParseError>,
}

pub struct ClientResponseDriver<'a> {
    connection_id: u64,
    operation_id: Option<u64>,
    pending_length: usize,
    eof: bool,
    response: ClientResponse<'a>,
    state: ClientReadState,
    result: ClientReadResult,
    error_code: i32,
    parse_error: Option<ParseError>,
}

impl<'a> ClientResponseDriver<'a> {
    pub const fn new(
        connection_id: u64,
        request_method: RequestMethod,
        requested_upgrade: &'a [u8],
        body_limits: BodyLimits,
    ) -> Self {
        Self {
            connection_id,
            operation_id: None,
            pending_length: 0,
            eof: false,
            response: ClientResponse::with_body_limits(
                request_method,
                requested_upgrade,
                body_limits,
            ),
            state: ClientReadState::Reading,
            result: ClientReadResult::NeedMore,
            error_code: 0,
            parse_error: None,
        }
    }

    pub fn consume(
        &mut self,
        completion: IoCompletion,
        input: &[u8],
        headers: &mut [Header],
        trailers: &mut [Header],
        limits: Limits,
    ) -> ClientReadEvent {
        if self.state != ClientReadState::Reading {
            return self.event(0, None);
        }
        if !completion.valid_for(self.connection_id, self.operation_id, IoOperation::Read)
            || self.eof
        {
            return self.fail_parse(ParseError::Invalid);
        }
        self.operation_id = Some(completion.operation_id);

        if !matches!(completion.result, IoResult::Progress { .. })
            && input.len() != self.pending_length
        {
            return self.fail_parse(ParseError::Invalid);
        }

        match completion.result {
            IoResult::WouldBlock => {
                self.result = ClientReadResult::WouldBlock;
                self.event(0, None)
            }
            IoResult::Interrupted => {
                self.result = ClientReadResult::Interrupted;
                self.event(0, None)
            }
            IoResult::EndOfStream => {
                self.eof = true;
                self.decode(input, headers, trailers, limits)
            }
            IoResult::Cancelled => {
                self.state = ClientReadState::Cancelled;
                self.response.cancel();
                self.result = ClientReadResult::Cancelled;
                self.event(0, None)
            }
            IoResult::Closed => self.fail_parse(ParseError::Invalid),
            IoResult::Error { code } => {
                self.state = ClientReadState::Failed;
                self.result = ClientReadResult::TransportError;
                self.error_code = code;
                self.event(0, None)
            }
            IoResult::Progress { transferred_bytes } => {
                let Some(added) = input.len().checked_sub(self.pending_length) else {
                    return self.fail_parse(ParseError::Invalid);
                };
                if added != transferred_bytes {
                    return self.fail_parse(ParseError::Invalid);
                }
                self.decode(input, headers, trailers, limits)
            }
        }
    }

    pub fn consume_buffered(
        &mut self,
        input: &[u8],
        headers: &mut [Header],
        trailers: &mut [Header],
        limits: Limits,
    ) -> ClientReadEvent {
        if self.state != ClientReadState::Reading {
            return self.event(0, None);
        }
        if input.len() != self.pending_length {
            return self.fail_parse(ParseError::Invalid);
        }
        self.decode(input, headers, trailers, limits)
    }

    pub fn can_reuse(&self) -> bool {
        self.state == ClientReadState::Complete && self.response.can_reuse()
    }

    pub fn state(&self) -> ClientReadState {
        self.state
    }

    pub fn cancel(&mut self) {
        self.response.cancel();
        self.state = ClientReadState::Cancelled;
        self.result = ClientReadResult::Cancelled;
        self.parse_error = Some(ParseError::Cancelled);
    }

    fn decode(
        &mut self,
        input: &[u8],
        headers: &mut [Header],
        trailers: &mut [Header],
        limits: Limits,
    ) -> ClientReadEvent {
        let progress = match self
            .response
            .next(input, self.eof, headers, trailers, limits)
        {
            Ok(progress) => progress,
            Err(error) => return self.fail_parse(error),
        };
        let Some(pending_length) = input.len().checked_sub(progress.consumed) else {
            return self.fail_parse(ParseError::Invalid);
        };
        self.pending_length = pending_length;

        let Some(response) = progress.event else {
            self.result = ClientReadResult::NeedMore;
            return self.event(progress.consumed, None);
        };

        if self.response.is_upgraded() {
            self.state = ClientReadState::Upgraded;
            self.result = ClientReadResult::Upgraded;
        } else if response == ClientResponseEvent::Body(BodyEvent::Complete) {
            self.state = ClientReadState::Complete;
            self.result = ClientReadResult::Complete;
        } else {
            self.result = match response {
                ClientResponseEvent::Informational(_) => ClientReadResult::Informational,
                ClientResponseEvent::Head(_) => ClientReadResult::Head,
                ClientResponseEvent::Body(BodyEvent::Data(_)) => ClientReadResult::Body,
                ClientResponseEvent::Body(BodyEvent::Trailers { .. }) => ClientReadResult::Trailers,
                ClientResponseEvent::Body(BodyEvent::Complete) => ClientReadResult::Complete,
            };
        }
        self.event(progress.consumed, Some(response))
    }

    fn event(&self, consumed: usize, response: Option<ClientResponseEvent>) -> ClientReadEvent {
        ClientReadEvent {
            response,
            consumed,
            result: self.result,
            error_code: self.error_code,
            parse_error: self.parse_error,
        }
    }

    fn fail_parse(&mut self, error: ParseError) -> ClientReadEvent {
        self.state = ClientReadState::Failed;
        self.result = parse_result(error);
        self.parse_error = Some(error);
        self.event(0, None)
    }
}

fn parse_result(error: ParseError) -> ClientReadResult {
    match error {
        ParseError::Incomplete => ClientReadResult::Incomplete,
        ParseError::BodyTooLarge => ClientReadResult::TooLarge,
        ParseError::HeaderCapacity => ClientReadResult::HeaderCapacity,
        ParseError::Invalid
        | ParseError::RequestLineTooLarge
        | ParseError::StatusLineTooLarge
        | ParseError::HeaderSectionTooLarge
        | ParseError::BufferChanged
        | ParseError::ExpectationFailed
        | ParseError::Cancelled
        | ParseError::AdmissionPending
        | ParseError::BodyRejected => ClientReadResult::Invalid,
    }
}
