#[cfg(test)]
mod tests;

use veylor_http1::{
    BodyEvent, BodyLimits, Header, Limits, ParseError, RequestBodyAdmission, RequestBodyDecoder,
    RequestHead, Span,
};

use super::{MAX_REQUEST_FIELDS, ServerConnection, ServerError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    Ready,
    SendContinue,
    Rejected,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BodyKind {
    #[default]
    NeedMore,
    Data,
    Trailers,
    Complete,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BodyOut {
    pub kind: BodyKind,
    pub consumed: usize,
    pub data: Span,
    pub trailer_count: usize,
}

impl<'a> ServerConnection<'a> {
    pub fn parse_head(
        &mut self,
        input: &'a [u8],
        headers: &mut [Header],
    ) -> Result<RequestHead, ServerError> {
        if self.cancelled {
            return Err(ServerError::Cancelled);
        }
        if self.response_started || self.body.is_some() {
            return Err(ServerError::BadState);
        }
        match self.parser.parse(input, headers, Limits::default()) {
            Ok(Some(head)) => {
                if headers.len() < head.header_count {
                    return Err(ServerError::HeaderCapacity {
                        headers_required: head.header_count,
                        trailers_required: 0,
                    });
                }
                self.request = Some(head);
                self.request_input = Some(input);
                Ok(head)
            }
            Ok(None) => Err(ServerError::NeedMore),
            Err(ParseError::HeaderCapacity) => Err(ServerError::HeaderCapacity {
                headers_required: self.parser.required_header_capacity(),
                trailers_required: 0,
            }),
            Err(_) => Err(ServerError::ProtocolError),
        }
    }

    pub fn admit_body(
        &mut self,
        input: &[u8],
        request_headers: &'a [Header],
        trailer_name_data: &'a [u8],
        permitted_trailers: &'a [Span],
        allowed: bool,
    ) -> Result<Admission, ServerError> {
        if self.cancelled {
            return Err(ServerError::Cancelled);
        }
        let Some(request) = self.request else {
            return Err(ServerError::BadState);
        };
        if self.body.is_some() || self.response_started {
            return Err(ServerError::BadState);
        }
        let Some(stored) = self.request_input else {
            return Err(ServerError::BadState);
        };
        if !std::ptr::eq(input.as_ptr(), stored.as_ptr()) || input.len() < request.length {
            return Err(ServerError::BadState);
        }
        if request_headers.len() < request.header_count {
            return Err(ServerError::BufferTooSmall {
                required: request.header_count,
            });
        }
        if permitted_trailers.len() > MAX_REQUEST_FIELDS {
            return Err(ServerError::Capacity);
        }
        let mut decoder = RequestBodyDecoder::new_with_trailer_spans(
            request,
            stored,
            &request_headers[..request.header_count],
            trailer_name_data,
            permitted_trailers,
            BodyLimits::default(),
        )
        .map_err(|error| map_parse_error(error, request.header_count))?;
        let admission = decoder
            .admit_body(allowed)
            .map_err(|error| map_parse_error(error, request.header_count))?;
        let admission = match admission {
            RequestBodyAdmission::ReadyToRead => Admission::Ready,
            RequestBodyAdmission::SendContinue => Admission::SendContinue,
            RequestBodyAdmission::Rejected => Admission::Rejected,
        };
        self.body = Some(decoder);
        Ok(admission)
    }

    pub fn mark_continue_sent(&mut self) -> Result<(), ServerError> {
        if self.cancelled {
            return Err(ServerError::Cancelled);
        }
        if self.response_started {
            return Err(ServerError::BadState);
        }
        if !self.continue_ready {
            return Err(ServerError::BadState);
        }
        let Some(body) = self.body.as_mut() else {
            return Err(ServerError::BadState);
        };
        body.continue_sent()
            .map_err(|error| map_parse_error(error, 0))?;
        if !self.continue_flushed() {
            return Err(ServerError::BadState);
        }
        Ok(())
    }

    pub fn read_body(
        &mut self,
        input: &[u8],
        eof: bool,
        trailers: &mut [Header],
    ) -> Result<BodyOut, ServerError> {
        if self.cancelled {
            return Err(ServerError::Cancelled);
        }
        if self.response_started {
            return Err(ServerError::BadState);
        }
        let Some(body) = self.body.as_mut() else {
            return Err(ServerError::BadState);
        };
        let progress = match body.next(input, eof, trailers, Limits::default()) {
            Ok(progress) => progress,
            Err(ParseError::HeaderCapacity) => {
                return Err(ServerError::HeaderCapacity {
                    headers_required: 0,
                    trailers_required: body.required_trailer_capacity(),
                });
            }
            Err(error) => {
                return Err(map_parse_error(error, body.required_trailer_capacity()));
            }
        };
        let mut event = BodyOut {
            consumed: progress.consumed,
            ..BodyOut::default()
        };
        match progress.event {
            Some(BodyEvent::Data(span)) => {
                event.kind = BodyKind::Data;
                event.data = span;
            }
            Some(BodyEvent::Trailers { count }) => {
                event.kind = BodyKind::Trailers;
                event.trailer_count = count;
            }
            Some(BodyEvent::Complete) => {
                event.kind = BodyKind::Complete;
                self.body_complete = true;
            }
            None => {}
        }
        Ok(event)
    }

    pub(crate) fn continue_flushed(&mut self) -> bool {
        let was_flushed = self.continue_ready;
        self.continue_ready = false;
        was_flushed
    }
}

fn map_parse_error(error: ParseError, required: usize) -> ServerError {
    match error {
        ParseError::BodyTooLarge => ServerError::BodyTooLong,
        ParseError::AdmissionPending | ParseError::BodyRejected | ParseError::Cancelled => {
            ServerError::BadState
        }
        ParseError::HeaderCapacity => ServerError::BufferTooSmall { required },
        _ => ServerError::ProtocolError,
    }
}
