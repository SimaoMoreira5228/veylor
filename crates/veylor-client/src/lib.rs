mod request_write;
mod response_read;

pub mod h2;

pub use request_write::EncodeOut;
pub use response_read::{ClientReadEvent, ClientReadKind};
pub use veylor_http1::ResponseRequestMethod as RequestMethod;
pub use veylor_http1::{
    BodyEvent, BodyFraming, Field, Header, Limits, Request, ResponseBody, ResponseHead,
    ResponseKind, Span,
};

pub const MAX_REQUEST_FIELDS: usize = Limits::DEFAULT_HEADER_COUNT;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientError {
    InvalidArgument,
    ProtocolError,
    BufferTooSmall {
        required: usize,
    },
    HeaderCapacity {
        headers_required: usize,
        trailers_required: usize,
    },
    Capacity,
    WouldBlock,
    BodyTooLong,
    BadState,
    WriteOverrun,
    Cancelled,
}

pub struct ClientConnection<'a> {
    method: RequestMethod,
    requested_upgrade: &'a [u8],
    response: veylor_http1::ClientResponse<'a>,
    response_started: bool,
    writer: request_write::RequestWriteState,
}

impl<'a> ClientConnection<'a> {
    pub fn new(method: RequestMethod, requested_upgrade: &'a [u8]) -> Self {
        Self {
            method,
            requested_upgrade,
            response: veylor_http1::ClientResponse::new(method, requested_upgrade),
            response_started: false,
            writer: request_write::RequestWriteState::new(),
        }
    }

    pub fn reset(&mut self) -> Result<(), ClientError> {
        if self.writer == request_write::RequestWriteState::Cancelled {
            return Err(ClientError::Cancelled);
        }
        if self.response.is_upgraded()
            || (self.response_started && !self.response.is_complete())
            || (self.writer == request_write::RequestWriteState::Complete && !self.response_started)
        {
            return Err(ClientError::BadState);
        }
        if !matches!(
            self.writer,
            request_write::RequestWriteState::Idle | request_write::RequestWriteState::Complete
        ) {
            return Err(ClientError::BadState);
        }
        self.response = veylor_http1::ClientResponse::new(self.method, self.requested_upgrade);
        self.response_started = false;
        self.writer = request_write::RequestWriteState::new();
        Ok(())
    }

    pub fn cancel(&mut self) {
        self.response.cancel();
        self.writer = request_write::RequestWriteState::Cancelled;
    }
}
