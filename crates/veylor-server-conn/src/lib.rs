mod request_read;
mod response_write;

pub mod h2;

pub use request_read::{Admission, BodyKind, BodyOut};
pub use response_write::{BodyWriteOut, EncodeOut, ResponseHeadOut, ResponseHeadParams};
pub use veylor_http1::{
    ConnectionHeader, Field, Header, Limits, RequestHead, ResponseFraming, Span,
};

pub const MAX_REQUEST_FIELDS: usize = Limits::DEFAULT_HEADER_COUNT;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerError {
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
    NeedMore,
}






























































pub struct ServerConnection<'a> {
    parser: veylor_http1::Parser,
    request: Option<RequestHead>,
    request_input: Option<&'a [u8]>,
    body: Option<veylor_http1::RequestBodyDecoder<'a>>,
    body_complete: bool,
    response: response_write::ResponseWriteState,
    continue_ready: bool,
    response_started: bool,
    last_response: Option<response_write::LastResponse>,
    cancelled: bool,
}

impl<'a> ServerConnection<'a> {
    pub fn new() -> Self {
        Self {
            parser: veylor_http1::Parser::new(),
            request: None,
            request_input: None,
            body: None,
            body_complete: false,
            response: response_write::ResponseWriteState::new(),
            continue_ready: false,
            response_started: false,
            last_response: None,
            cancelled: false,
        }
    }

    pub fn reset(&mut self) -> Result<(), ServerError> {
        if self.cancelled {
            return Err(ServerError::Cancelled);
        }
        self.parser.reset();
        self.request = None;
        self.request_input = None;
        self.body = None;
        self.body_complete = false;
        self.response = response_write::ResponseWriteState::new();
        self.continue_ready = false;
        self.response_started = false;
        self.last_response = None;
        Ok(())
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
        self.parser.reset();
        self.request = None;
        self.request_input = None;
        self.body = None;
        self.response = response_write::ResponseWriteState::cancelled();
        self.continue_ready = false;
        self.response_started = true;
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    pub fn request_head(&self) -> Option<RequestHead> {
        self.request
    }
}

impl Default for ServerConnection<'_> {
    fn default() -> Self {
        Self::new()
    }
}
