use crate::body::{BodyDecoder, BodyEvent, BodyLimits};
use crate::request_head::{Header, Limits, ParseError};
use crate::response_head::{RequestMethod, ResponseHead, ResponseKind, ResponseParser};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientResponseEvent {
    Informational(ResponseHead),
    Head(ResponseHead),
    Body(BodyEvent),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientResponseProgress {
    pub event: Option<ClientResponseEvent>,
    pub consumed: usize,
}

enum State {
    Head,
    Body {
        decoder: BodyDecoder,
        reusable: bool,
    },
    Complete,
    Upgrade,
    Failed(ParseError),
}

pub struct ClientResponse<'a> {
    parser: ResponseParser<'a>,
    state: State,
    reusable: bool,
    body_limits: BodyLimits,
}

impl<'a> ClientResponse<'a> {
    pub const fn new(request_method: RequestMethod, requested_upgrade: &'a [u8]) -> Self {
        Self::with_body_limits(request_method, requested_upgrade, BodyLimits::DEFAULT)
    }

    pub const fn with_body_limits(
        request_method: RequestMethod,
        requested_upgrade: &'a [u8],
        body_limits: BodyLimits,
    ) -> Self {
        Self {
            parser: ResponseParser::new(request_method, requested_upgrade),
            state: State::Head,
            reusable: false,
            body_limits,
        }
    }

    pub fn next(
        &mut self,
        input: &[u8],
        eof: bool,
        headers: &mut [Header],
        trailers: &mut [Header],
        limits: Limits,
    ) -> Result<ClientResponseProgress, ParseError> {
        if let State::Failed(error) = self.state {
            return Err(error);
        }

        match &mut self.state {
            State::Head => {
                let head = match self.parser.parse(input, headers, limits) {
                    Ok(Some(head)) => head,
                    Ok(None) => {
                        if eof {
                            return self.fail(ParseError::Incomplete);
                        }
                        return Ok(ClientResponseProgress {
                            event: None,
                            consumed: 0,
                        });
                    }
                    Err(ParseError::HeaderCapacity) => return Err(ParseError::HeaderCapacity),
                    Err(error) => return self.fail(error),
                };
                match head.kind {
                    ResponseKind::Informational => {
                        self.parser.reset();
                        Ok(ClientResponseProgress {
                            event: Some(ClientResponseEvent::Informational(head)),
                            consumed: head.end_offset,
                        })
                    }
                    ResponseKind::Upgrade => {
                        self.state = State::Upgrade;
                        Ok(ClientResponseProgress {
                            event: Some(ClientResponseEvent::Head(head)),
                            consumed: head.end_offset,
                        })
                    }
                    ResponseKind::Final => {
                        self.state = State::Body {
                            decoder: BodyDecoder::with_limits(head.body, self.body_limits),
                            reusable: head.reusable,
                        };
                        Ok(ClientResponseProgress {
                            event: Some(ClientResponseEvent::Head(head)),
                            consumed: head.end_offset,
                        })
                    }
                }
            }
            State::Body { decoder, reusable } => {
                let progress = match decoder.next(input, eof, trailers, limits) {
                    Ok(progress) => progress,
                    Err(ParseError::HeaderCapacity) => return Err(ParseError::HeaderCapacity),
                    Err(error) => return self.fail(error),
                };
                if progress.event == Some(BodyEvent::Complete) {
                    if progress.consumed < input.len() {
                        return self.fail(ParseError::Invalid);
                    }
                    self.reusable = *reusable && !eof;
                    self.state = State::Complete;
                }
                Ok(ClientResponseProgress {
                    event: progress.event.map(ClientResponseEvent::Body),
                    consumed: progress.consumed,
                })
            }
            State::Complete => {
                if !input.is_empty() {
                    return self.fail(ParseError::Invalid);
                }
                if eof {
                    self.reusable = false;
                }
                Ok(ClientResponseProgress {
                    event: None,
                    consumed: 0,
                })
            }
            State::Upgrade => Ok(ClientResponseProgress {
                event: None,
                consumed: 0,
            }),
            State::Failed(error) => Err(*error),
        }
    }

    pub fn can_reuse(&self) -> bool {
        self.reusable && matches!(self.state, State::Complete)
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.state, State::Complete)
    }

    pub fn is_upgraded(&self) -> bool {
        matches!(self.state, State::Upgrade)
    }

    pub fn required_header_capacity(&self) -> usize {
        if matches!(self.state, State::Head) {
            return self.parser.required_header_capacity();
        }
        0
    }

    pub fn required_trailer_capacity(&self) -> usize {
        if let State::Body { decoder, .. } = &self.state {
            return decoder.required_trailer_capacity();
        }
        0
    }

    pub fn cancel(&mut self) {
        self.state = State::Failed(ParseError::Cancelled);
        self.reusable = false;
    }

    fn fail<T>(&mut self, error: ParseError) -> Result<T, ParseError> {
        self.state = State::Failed(error);
        self.reusable = false;
        Err(error)
    }
}

#[cfg(test)]
mod tests;
