use std::fmt;
use std::io;

use veylor_client::h2::ErrorCode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeoutKind {
    Handshake,
    Stream,
    Close,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum H2Error {
    QueueFull,
    Closed,
    Cancelled,
    InvalidRequest,
    GoAway { error_code: ErrorCode },
    Reset(ErrorCode),
    Connection(ErrorCode),
    Io(io::ErrorKind),
    Timeout(TimeoutKind),
    BodyOverflow,
}

impl H2Error {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::QueueFull | Self::Closed | Self::GoAway { .. } => true,
            Self::Reset(code) => *code == ErrorCode::RefusedStream,
            _ => false,
        }
    }
}

impl fmt::Display for H2Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull => formatter.write_str("pending request queue is full"),
            Self::Closed => formatter.write_str("connection is closed"),
            Self::Cancelled => formatter.write_str("request was cancelled"),
            Self::InvalidRequest => formatter.write_str("request cannot be encoded"),
            Self::GoAway { error_code } => write!(formatter, "peer sent GOAWAY ({error_code:?})"),
            Self::Reset(code) => write!(formatter, "stream reset ({code:?})"),
            Self::Connection(code) => write!(formatter, "connection error ({code:?})"),
            Self::Io(kind) => write!(formatter, "i/o error ({kind})"),
            Self::Timeout(kind) => write!(formatter, "deadline exceeded ({kind:?})"),
            Self::BodyOverflow => formatter.write_str("response body exceeds the stream buffer"),
        }
    }
}

impl std::error::Error for H2Error {}

impl From<H2Error> for io::Error {
    fn from(error: H2Error) -> Self {
        let kind = match error {
            H2Error::Io(kind) => kind,
            H2Error::Timeout(_) => io::ErrorKind::TimedOut,
            H2Error::Closed | H2Error::Cancelled => io::ErrorKind::ConnectionAborted,
            H2Error::InvalidRequest => io::ErrorKind::InvalidInput,
            _ => io::ErrorKind::Other,
        };
        io::Error::new(kind, error)
    }
}
