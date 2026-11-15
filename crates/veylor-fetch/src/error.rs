use std::fmt;
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Builder(String),
    Connect(io::Error),
    Request(io::Error),
    Body(io::Error),
    Timeout,
    Redirect(String),
    Decode(String),
    Status { status: u16, url: String },
}

impl Error {
    pub fn is_builder(&self) -> bool {
        matches!(self, Self::Builder(_))
    }

    pub fn is_connect(&self) -> bool {
        matches!(self, Self::Connect(_))
    }

    pub fn is_request(&self) -> bool {
        matches!(self, Self::Request(_))
    }

    pub fn is_body(&self) -> bool {
        matches!(self, Self::Body(_))
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout)
    }

    pub fn is_redirect(&self) -> bool {
        matches!(self, Self::Redirect(_))
    }

    pub fn is_decode(&self) -> bool {
        matches!(self, Self::Decode(_))
    }

    pub fn is_status(&self) -> bool {
        matches!(self, Self::Status { .. })
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status { status, .. } => Some(*status),
            _ => None,
        }
    }

    pub fn url(&self) -> Option<&str> {
        match self {
            Self::Status { url, .. } => Some(url),
            _ => None,
        }
    }

    pub(crate) fn builder(message: impl Into<String>) -> Self {
        Self::Builder(message.into())
    }

    pub(crate) fn from_send(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::TimedOut => Self::Timeout,
            io::ErrorKind::InvalidInput => Self::Builder(error.to_string()),
            io::ErrorKind::ConnectionRefused
            | io::ErrorKind::NotFound
            | io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NotConnected => Self::Connect(error),
            _ => Self::Request(error),
        }
    }

    pub(crate) fn from_body(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::TimedOut => Self::Timeout,
            _ => Self::Body(error),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Builder(message) => write!(f, "invalid request: {message}"),
            Self::Connect(error) => write!(f, "connection failed: {error}"),
            Self::Request(error) => write!(f, "request failed: {error}"),
            Self::Body(error) => write!(f, "reading the body failed: {error}"),
            Self::Timeout => f.write_str("operation timed out"),
            Self::Redirect(message) => write!(f, "redirect failed: {message}"),
            Self::Decode(message) => write!(f, "decoding failed: {message}"),
            Self::Status { status, url } => write!(f, "status {status} for {url}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(error) | Self::Request(error) | Self::Body(error) => Some(error),
            _ => None,
        }
    }
}
