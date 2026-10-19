use crate::request_head::{Header, Limits, ParseError, Parser, RequestHead};

use super::{IoCompletion, IoOperation, IoResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeadState {
    Reading,
    Ready,
    Closed,
    Cancelled,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeadResult {
    NeedMore,
    Ready,
    WouldBlock,
    Interrupted,
    PeerClosed,
    Cancelled,
    TransportError,
    Invalid,
    RequestLineTooLarge,
    HeaderSectionTooLarge,
    HeaderCapacity,
    ExpectationFailed,
    BufferChanged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeadEvent {
    pub head: Option<RequestHead>,
    pub result: HeadResult,
    pub error_code: i32,
}

pub struct HeadDriver {
    connection_id: u64,
    operation_id: Option<u64>,
    input_length: usize,
    parser: Parser,
    state: HeadState,
    result: HeadResult,
    error_code: i32,
}

impl HeadDriver {
    pub const fn new(connection_id: u64) -> Self {
        Self {
            connection_id,
            operation_id: None,
            input_length: 0,
            parser: Parser::new(),
            state: HeadState::Reading,
            result: HeadResult::NeedMore,
            error_code: 0,
        }
    }

    pub fn consume(
        &mut self,
        completion: IoCompletion,
        input: &[u8],
        headers: &mut [Header],
        limits: Limits,
    ) -> HeadEvent {
        if self.state == HeadState::Ready {
            return self.ready(input, headers, limits);
        }
        if self.state != HeadState::Reading {
            return self.event();
        }
        if !completion.valid_for(self.connection_id, self.operation_id, IoOperation::Read) {
            return self.fail(HeadResult::Invalid);
        }
        self.operation_id = Some(completion.operation_id);

        if !matches!(completion.result, IoResult::Progress { .. })
            && input.len() != self.input_length
        {
            return self.fail(HeadResult::Invalid);
        }

        match completion.result {
            IoResult::WouldBlock => {
                self.result = HeadResult::WouldBlock;
                self.event()
            }
            IoResult::Interrupted => {
                self.result = HeadResult::Interrupted;
                self.event()
            }
            IoResult::EndOfStream => {
                if self.input_length != 0 {
                    return self.fail(HeadResult::Invalid);
                }
                self.state = HeadState::Closed;
                self.result = HeadResult::PeerClosed;
                self.event()
            }
            IoResult::Cancelled => {
                self.state = HeadState::Cancelled;
                self.result = HeadResult::Cancelled;
                self.event()
            }
            IoResult::Closed => self.fail(HeadResult::Invalid),
            IoResult::Error { code } => {
                self.state = HeadState::Failed;
                self.result = HeadResult::TransportError;
                self.error_code = code;
                self.event()
            }
            IoResult::Progress { transferred_bytes } => {
                let Some(added) = input.len().checked_sub(self.input_length) else {
                    return self.fail(HeadResult::Invalid);
                };
                if added != transferred_bytes {
                    return self.fail(HeadResult::Invalid);
                }
                self.parse(input, headers, limits)
            }
        }
    }

    pub fn state(&self) -> HeadState {
        self.state
    }

    fn parse(&mut self, input: &[u8], headers: &mut [Header], limits: Limits) -> HeadEvent {
        let parsed = self.parser.parse(input, headers, limits);
        self.input_length = input.len();

        match parsed {
            Ok(Some(head)) => {
                self.state = HeadState::Ready;
                self.result = HeadResult::Ready;
                HeadEvent {
                    head: Some(head),
                    result: HeadResult::Ready,
                    error_code: self.error_code,
                }
            }
            Ok(None) => {
                self.result = HeadResult::NeedMore;
                self.event()
            }
            Err(error) => self.fail(parse_error(error)),
        }
    }

    fn event(&self) -> HeadEvent {
        HeadEvent {
            head: None,
            result: self.result,
            error_code: self.error_code,
        }
    }

    fn ready(&mut self, input: &[u8], headers: &mut [Header], limits: Limits) -> HeadEvent {
        match self.parser.parse(input, headers, limits) {
            Ok(Some(head)) => HeadEvent {
                head: Some(head),
                result: HeadResult::Ready,
                error_code: self.error_code,
            },
            Ok(None) => self.fail(HeadResult::Invalid),
            Err(error) => self.fail(parse_error(error)),
        }
    }

    fn fail(&mut self, result: HeadResult) -> HeadEvent {
        self.state = HeadState::Failed;
        self.result = result;
        self.event()
    }
}

fn parse_error(error: ParseError) -> HeadResult {
    match error {
        ParseError::Invalid => HeadResult::Invalid,
        ParseError::RequestLineTooLarge => HeadResult::RequestLineTooLarge,
        ParseError::HeaderSectionTooLarge => HeadResult::HeaderSectionTooLarge,
        ParseError::HeaderCapacity => HeadResult::HeaderCapacity,
        ParseError::BufferChanged => HeadResult::BufferChanged,
        ParseError::ExpectationFailed => HeadResult::ExpectationFailed,
        ParseError::StatusLineTooLarge
        | ParseError::Incomplete
        | ParseError::Cancelled
        | ParseError::BodyTooLarge
        | ParseError::AdmissionPending
        | ParseError::BodyRejected => HeadResult::Invalid,
    }
}
