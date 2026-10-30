use veylor_http1::{
    BodyFraming, Field, Request, RequestEncodeError, encode_chunk_with_required_length,
    encode_head_with_required_length, finish_chunks_with_required_length,
};

use super::{ClientConnection, ClientError, MAX_REQUEST_FIELDS, RequestMethod};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestBodyState {
    Finished,
    Fixed(u64),
    Chunked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestWriteState {
    Idle,
    HeadPending {
        pending: usize,
        body: RequestBodyState,
    },
    Ready(RequestBodyState),
    Pending {
        pending: usize,
        body: RequestBodyState,
    },
    Complete,
    Cancelled,
}

impl RequestWriteState {
    pub(crate) const fn new() -> Self {
        Self::Idle
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodeOut {
    pub written: usize,
    pub required: usize,
}

impl<'a> ClientConnection<'a> {
    pub fn encode_request_head(
        &mut self,
        method: &[u8],
        target: &[u8],
        fields: &[Field<'_>],
        body: BodyFraming,
        output: &mut [u8],
    ) -> Result<EncodeOut, ClientError> {
        if fields.len() > MAX_REQUEST_FIELDS {
            return Err(ClientError::Capacity);
        }
        if self.writer == RequestWriteState::Cancelled {
            return Err(ClientError::Cancelled);
        }
        if self.writer != RequestWriteState::Idle {
            return Err(ClientError::BadState);
        }
        if !matches_request_method(self.method, method) {
            return Err(ClientError::InvalidArgument);
        }
        let request = Request {
            method,
            target,
            fields,
            body,
        };
        let mut required = 0;
        match encode_head_with_required_length(&request, output, &mut required) {
            Ok(written) => {
                self.writer = RequestWriteState::HeadPending {
                    pending: written,
                    body: body_state(body),
                };
                Ok(EncodeOut { written, required })
            }
            Err(RequestEncodeError::BufferTooSmall) => {
                Err(ClientError::BufferTooSmall { required })
            }
            Err(_) => Err(ClientError::InvalidArgument),
        }
    }

    pub fn advance_request_write(&mut self, transferred: usize) -> Result<usize, ClientError> {
        let (pending, body, is_head) = match self.writer {
            RequestWriteState::HeadPending { pending, body } => (pending, body, true),
            RequestWriteState::Pending { pending, body } => (pending, body, false),
            RequestWriteState::Idle | RequestWriteState::Ready(_) | RequestWriteState::Complete => {
                return Err(ClientError::BadState);
            }
            RequestWriteState::Cancelled => return Err(ClientError::Cancelled),
        };
        if transferred > pending {
            return Err(ClientError::WriteOverrun);
        }
        let pending = pending - transferred;
        self.writer = if pending != 0 {
            if is_head {
                RequestWriteState::HeadPending { pending, body }
            } else {
                RequestWriteState::Pending { pending, body }
            }
        } else if body == RequestBodyState::Finished {
            RequestWriteState::Complete
        } else {
            RequestWriteState::Ready(body)
        };
        Ok(pending)
    }

    pub fn encode_request_body(
        &mut self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<EncodeOut, ClientError> {
        let body = match self.writer {
            RequestWriteState::Ready(body) => body,
            RequestWriteState::HeadPending { .. } | RequestWriteState::Pending { .. } => {
                return Err(ClientError::WouldBlock);
            }
            RequestWriteState::Cancelled => return Err(ClientError::Cancelled),
            RequestWriteState::Idle | RequestWriteState::Complete => {
                return Err(ClientError::BadState);
            }
        };
        let (written, next_body) = match body {
            RequestBodyState::Fixed(remaining) => {
                let input_length =
                    u64::try_from(input.len()).map_err(|_| ClientError::BodyTooLong)?;
                if input_length > remaining {
                    return Err(ClientError::BodyTooLong);
                }
                if input.is_empty() {
                    return Ok(EncodeOut {
                        written: 0,
                        required: 0,
                    });
                }
                if output.is_empty() {
                    return Err(ClientError::BufferTooSmall {
                        required: input.len(),
                    });
                }
                let written = input.len().min(output.len());
                output[..written].copy_from_slice(&input[..written]);
                let written_u64 = u64::try_from(written).map_err(|_| ClientError::BodyTooLong)?;
                (written, RequestBodyState::Fixed(remaining - written_u64))
            }
            RequestBodyState::Chunked => {
                if input.is_empty() {
                    return Err(ClientError::InvalidArgument);
                }
                let mut required = 0;
                match encode_chunk_with_required_length(input, output, &mut required) {
                    Ok(written) => (written, RequestBodyState::Chunked),
                    Err(RequestEncodeError::BufferTooSmall) => {
                        return Err(ClientError::BufferTooSmall { required });
                    }
                    Err(_) => return Err(ClientError::InvalidArgument),
                }
            }
            RequestBodyState::Finished => return Err(ClientError::BadState),
        };
        self.writer = RequestWriteState::Pending {
            pending: written,
            body: next_body,
        };
        Ok(EncodeOut {
            written,
            required: written,
        })
    }

    pub fn finish_request_body(
        &mut self,
        trailers: &[Field<'_>],
        output: &mut [u8],
    ) -> Result<EncodeOut, ClientError> {
        if trailers.len() > MAX_REQUEST_FIELDS {
            return Err(ClientError::Capacity);
        }
        let body = match self.writer {
            RequestWriteState::Ready(body) => body,
            RequestWriteState::HeadPending { .. } | RequestWriteState::Pending { .. } => {
                return Err(ClientError::WouldBlock);
            }
            RequestWriteState::Cancelled => return Err(ClientError::Cancelled),
            RequestWriteState::Idle | RequestWriteState::Complete => {
                return Err(ClientError::BadState);
            }
        };
        match body {
            RequestBodyState::Fixed(0) => {
                self.writer = RequestWriteState::Complete;
                Ok(EncodeOut {
                    written: 0,
                    required: 0,
                })
            }
            RequestBodyState::Fixed(_) | RequestBodyState::Finished => Err(ClientError::BadState),
            RequestBodyState::Chunked => {
                let mut required = 0;
                let written =
                    match finish_chunks_with_required_length(trailers, output, &mut required) {
                        Ok(written) => written,
                        Err(RequestEncodeError::BufferTooSmall) => {
                            return Err(ClientError::BufferTooSmall { required });
                        }
                        Err(_) => return Err(ClientError::InvalidArgument),
                    };
                self.writer = RequestWriteState::Pending {
                    pending: written,
                    body: RequestBodyState::Finished,
                };
                Ok(EncodeOut { written, required })
            }
        }
    }
}

fn body_state(framing: BodyFraming) -> RequestBodyState {
    match framing {
        BodyFraming::None | BodyFraming::Fixed(0) => RequestBodyState::Finished,
        BodyFraming::Fixed(length) => RequestBodyState::Fixed(length),
        BodyFraming::Chunked => RequestBodyState::Chunked,
    }
}

fn matches_request_method(request_method: RequestMethod, method: &[u8]) -> bool {
    match request_method {
        RequestMethod::Head => method == b"HEAD",
        RequestMethod::Connect => method == b"CONNECT",
        RequestMethod::Other => method != b"HEAD" && method != b"CONNECT",
    }
}

#[cfg(test)]
mod tests;
