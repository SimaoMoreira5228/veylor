use veylor_http1::{
    ConnectionHeader, Field, Header, ResponseEncodeError, ResponseFraming, ResponseToWrite, Span,
    can_reuse_after_response, connection_header_for_response,
    encode_response_chunk_with_required_length, encode_response_final_chunk_with_required_length,
    encode_response_head_with_required_length,
};

use super::{MAX_REQUEST_FIELDS, ServerConnection, ServerError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseBodyState {
    Fixed { remaining: u64 },
    Chunked,
    CloseDelimited,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseWriteState {
    Idle,
    HeadPending {
        remaining: usize,
        status: u16,
        body: Option<ResponseBodyState>,
    },
    Ready(ResponseBodyState),
    BodyPending {
        remaining: usize,
        body: ResponseBodyState,
        finish: bool,
    },
    Complete,
    Failed(ServerError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LastResponse {
    pub(crate) connection: ConnectionHeader,
    pub(crate) upgraded: bool,
    pub(crate) request_body_complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodeOut {
    pub written: usize,
    pub required: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseHeadOut {
    pub written: usize,
    pub body_allowed: bool,
    pub required: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BodyWriteOut {
    pub consumed: usize,
    pub written: usize,
    pub required: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseHeadParams<'f> {
    pub status: u16,
    pub reason: &'f [u8],
    pub fields: &'f [Field<'f>],
    pub framing: ResponseFraming,
    pub content_length: u64,
    pub request_body_complete: bool,
    pub response_close: bool,
}

impl ResponseWriteState {
    pub(crate) const fn new() -> Self {
        Self::Idle
    }

    pub(crate) const fn cancelled() -> Self {
        Self::Failed(ServerError::Cancelled)
    }
}

impl<'a> ServerConnection<'a> {
    pub fn check_encode_head(
        &self,
        input: &[u8],
        request_headers: &[Header],
    ) -> Result<(), ServerError> {
        if self.cancelled {
            return Err(ServerError::Cancelled);
        }
        let Some(request) = self.request else {
            return Err(ServerError::BadState);
        };
        if self.response_started || self.response != ResponseWriteState::Idle {
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
        Ok(())
    }

    pub fn check_write_body(&self) -> Result<(), ServerError> {
        match self.response {
            ResponseWriteState::Ready(_) => Ok(()),
            ResponseWriteState::HeadPending { .. } | ResponseWriteState::BodyPending { .. } => {
                Err(ServerError::WouldBlock)
            }
            ResponseWriteState::Failed(error) => Err(error),
            ResponseWriteState::Idle | ResponseWriteState::Complete => Err(ServerError::BadState),
        }
    }

    pub fn encode_response_head(
        &mut self,
        input: &[u8],
        request_headers: &[Header],
        params: ResponseHeadParams<'_>,
        output: &mut [u8],
    ) -> Result<ResponseHeadOut, ServerError> {
        if params.fields.len() > MAX_REQUEST_FIELDS {
            return Err(ServerError::Capacity);
        }
        if self.cancelled {
            return Err(ServerError::Cancelled);
        }
        let Some(request) = self.request else {
            return Err(ServerError::BadState);
        };
        if self.response_started || self.response != ResponseWriteState::Idle {
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
        let request_headers = &request_headers[..request.header_count];
        let mut requested_upgrade: &[u8] = &[];
        let mut upgrade_seen = false;
        for header in request_headers {
            let (Some(name), Some(value)) = (
                field_bytes(input, header.name),
                field_bytes(input, header.value),
            ) else {
                return Err(ServerError::ProtocolError);
            };
            if name.eq_ignore_ascii_case(b"upgrade") {
                if upgrade_seen {
                    return Err(ServerError::InvalidArgument);
                }
                requested_upgrade = value;
                upgrade_seen = true;
            }
        }
        if params.status == 101 && (!request.connection.upgrade || !upgrade_seen) {
            return Err(ServerError::InvalidArgument);
        }
        let Some(method) = request.method(input) else {
            return Err(ServerError::ProtocolError);
        };
        let connection = if params.status == 101 {
            ConnectionHeader::Upgrade
        } else {
            connection_header_for_response(
                request,
                input,
                params.status,
                params.framing,
                params.request_body_complete,
                params.response_close,
            )
        };
        let response = ResponseToWrite {
            request_method: method,
            request_version: request
                .protocol_version(input)
                .ok_or(ServerError::ProtocolError)?,
            requested_upgrade,
            status: params.status,
            reason: params.reason,
            fields: params.fields,
            framing: params.framing,
            content_length: params.content_length,
            connection,
        };
        let mut required = 0;
        match encode_response_head_with_required_length(&response, output, &mut required) {
            Ok(encoded) => {
                let body = encoded.body_allowed.then_some(match params.framing {
                    ResponseFraming::FixedLength => ResponseBodyState::Fixed {
                        remaining: params.content_length,
                    },
                    ResponseFraming::Chunked => ResponseBodyState::Chunked,
                    ResponseFraming::CloseDelimited => ResponseBodyState::CloseDelimited,
                });
                if params.status == 100 {
                    // NOTE: the 100 interim head shape is shared (veylor_http1::continue_interim);
                    // this flag only tracks that this engine flushed it, so mark_continue_sent can
                    // release body reads. Flush timing stays transport-local in both engines.
                    self.continue_ready = false;
                }
                self.response = ResponseWriteState::HeadPending {
                    remaining: encoded.length,
                    status: params.status,
                    body,
                };
                let upgraded = params.status == 101
                    || (method == b"CONNECT" && (200..300).contains(&params.status));
                self.last_response = Some(LastResponse {
                    connection,
                    upgraded,
                    request_body_complete: params.request_body_complete,
                });
                if params.status >= 200 || params.status == 101 {
                    self.response_started = true;
                }
                Ok(ResponseHeadOut {
                    written: encoded.length,
                    body_allowed: encoded.body_allowed,
                    required,
                })
            }
            Err(ResponseEncodeError::BufferTooSmall) => {
                Err(ServerError::BufferTooSmall { required })
            }
            Err(_) => Err(ServerError::InvalidArgument),
        }
    }

    pub fn advance_response_write(&mut self, transferred: usize) -> Result<usize, ServerError> {
        let (remaining, next) = match self.response {
            ResponseWriteState::HeadPending {
                remaining,
                status,
                body,
            } => {
                let Ok(remaining) = pending_after(remaining, transferred) else {
                    self.response = ResponseWriteState::Failed(ServerError::WriteOverrun);
                    return Err(ServerError::WriteOverrun);
                };
                if remaining != 0 {
                    (
                        remaining,
                        ResponseWriteState::HeadPending {
                            remaining,
                            status,
                            body,
                        },
                    )
                } else if status < 200 && status != 101 {
                    if status == 100 {
                        // NOTE: see the encode-side NOTE: only the shared 100 interim arms the
                        // continue gate; other 1xx return to Idle without starting the final
                        // response, so the exchange stays reusable in both engines.
                        self.continue_ready = true;
                    }
                    (0, ResponseWriteState::Idle)
                } else {
                    match body {
                        Some(body) => (0, ResponseWriteState::Ready(body)),
                        None => (0, ResponseWriteState::Complete),
                    }
                }
            }
            ResponseWriteState::BodyPending {
                remaining,
                body,
                finish,
            } => {
                let Ok(remaining) = pending_after(remaining, transferred) else {
                    self.response = ResponseWriteState::Failed(ServerError::WriteOverrun);
                    return Err(ServerError::WriteOverrun);
                };
                if remaining != 0 {
                    (
                        remaining,
                        ResponseWriteState::BodyPending {
                            remaining,
                            body,
                            finish,
                        },
                    )
                } else if finish {
                    (0, ResponseWriteState::Complete)
                } else {
                    (0, ResponseWriteState::Ready(body))
                }
            }
            ResponseWriteState::Idle
            | ResponseWriteState::Ready(_)
            | ResponseWriteState::Complete => {
                return Err(ServerError::BadState);
            }
            ResponseWriteState::Failed(error) => return Err(error),
        };
        self.response = next;
        Ok(remaining)
    }

    pub fn encode_response_body(
        &mut self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<BodyWriteOut, ServerError> {
        let ResponseWriteState::Ready(body) = self.response else {
            return match self.response {
                ResponseWriteState::HeadPending { .. } | ResponseWriteState::BodyPending { .. } => {
                    Err(ServerError::WouldBlock)
                }
                ResponseWriteState::Failed(error) => Err(error),
                _ => Err(ServerError::BadState),
            };
        };
        let (consumed, written, next_body) = match body {
            ResponseBodyState::Fixed { remaining } => {
                let Ok(input_length) = u64::try_from(input.len()) else {
                    return Err(ServerError::BodyTooLong);
                };
                if input_length > remaining {
                    return Err(ServerError::BodyTooLong);
                }
                if input.is_empty() {
                    return Ok(BodyWriteOut {
                        consumed: 0,
                        written: 0,
                        required: 0,
                    });
                }
                if output.is_empty() {
                    return Err(ServerError::BufferTooSmall {
                        required: input.len(),
                    });
                }
                let written = input.len().min(output.len());
                output[..written].copy_from_slice(&input[..written]);
                let Ok(written_u64) = u64::try_from(written) else {
                    return Err(ServerError::BodyTooLong);
                };
                (
                    written,
                    written,
                    ResponseBodyState::Fixed {
                        remaining: remaining - written_u64,
                    },
                )
            }
            ResponseBodyState::Chunked => {
                if input.is_empty() {
                    return Err(ServerError::InvalidArgument);
                }
                let mut required = 0;
                match encode_response_chunk_with_required_length(input, output, &mut required) {
                    Ok(written) => (input.len(), written, ResponseBodyState::Chunked),
                    Err(ResponseEncodeError::BufferTooSmall) => {
                        return Err(ServerError::BufferTooSmall { required });
                    }
                    Err(_) => return Err(ServerError::InvalidArgument),
                }
            }
            ResponseBodyState::CloseDelimited => {
                if input.is_empty() {
                    return Ok(BodyWriteOut {
                        consumed: 0,
                        written: 0,
                        required: 0,
                    });
                }
                if output.is_empty() {
                    return Err(ServerError::BufferTooSmall {
                        required: input.len(),
                    });
                }
                let written = input.len().min(output.len());
                output[..written].copy_from_slice(&input[..written]);
                (written, written, ResponseBodyState::CloseDelimited)
            }
        };
        self.response = ResponseWriteState::BodyPending {
            remaining: written,
            body: next_body,
            finish: false,
        };
        Ok(BodyWriteOut {
            consumed,
            written,
            required: written,
        })
    }

    pub fn finish_response_body(
        &mut self,
        trailers: &[Field<'_>],
        output: &mut [u8],
    ) -> Result<EncodeOut, ServerError> {
        let ResponseWriteState::Ready(body) = self.response else {
            return match self.response {
                ResponseWriteState::HeadPending { .. } | ResponseWriteState::BodyPending { .. } => {
                    Err(ServerError::WouldBlock)
                }
                ResponseWriteState::Failed(error) => Err(error),
                _ => Err(ServerError::BadState),
            };
        };
        if trailers.len() > MAX_REQUEST_FIELDS {
            return Err(ServerError::Capacity);
        }
        match body {
            ResponseBodyState::Fixed { remaining: 0 } | ResponseBodyState::CloseDelimited => {
                if !trailers.is_empty() {
                    return Err(ServerError::InvalidArgument);
                }
                self.response = ResponseWriteState::Complete;
                Ok(EncodeOut {
                    written: 0,
                    required: 0,
                })
            }
            ResponseBodyState::Fixed { .. } => {
                self.response = ResponseWriteState::Failed(ServerError::ProtocolError);
                Err(ServerError::ProtocolError)
            }
            ResponseBodyState::Chunked => {
                let mut required = 0;
                match encode_response_final_chunk_with_required_length(
                    trailers,
                    output,
                    &mut required,
                ) {
                    Ok(written) => {
                        self.response = ResponseWriteState::BodyPending {
                            remaining: written,
                            body: ResponseBodyState::Chunked,
                            finish: true,
                        };
                        Ok(EncodeOut { written, required })
                    }
                    Err(ResponseEncodeError::BufferTooSmall) => {
                        Err(ServerError::BufferTooSmall { required })
                    }
                    Err(_) => Err(ServerError::InvalidArgument),
                }
            }
        }
    }

    pub fn can_reuse(&self) -> bool {
        if self.cancelled {
            return false;
        }
        if self.response != ResponseWriteState::Complete {
            return false;
        }
        let Some(last) = self.last_response else {
            return false;
        };
        // NOTE: the exchange verdict lives in veylor_http1::can_reuse_after_response (shared
        // with the managed pump's keep_open verdict): upgrade/tunnel and Close pin the conn
        // shut, and at least one request-body drain signal must hold. Cancelled, incomplete,
        // and no-response gating stays local: it is this engine's state machine, not policy.
        can_reuse_after_response(
            last.upgraded,
            last.connection,
            self.body_complete,
            last.request_body_complete,
        )
    }
}

fn pending_after(pending: usize, transferred: usize) -> Result<usize, ServerError> {
    if transferred > pending {
        return Err(ServerError::WriteOverrun);
    }
    Ok(pending - transferred)
}

fn field_bytes(input: &[u8], span: Span) -> Option<&[u8]> {
    input.get(span.offset..span.offset.checked_add(span.length)?)
}

#[cfg(test)]
mod tests;
