use std::fmt;
use std::io;

use crate::response::Response;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeoutKind {
    Connect,
    Read,
    Write,
    Overall,
}

impl fmt::Display for TimeoutKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Connect => "connect",
            Self::Read => "read",
            Self::Write => "write",
            Self::Overall => "overall",
        };

        f.write_str(name)
    }
}

#[derive(Debug)]
pub enum Error {
    InvalidUrl(String),
    InvalidRequest(String),
    Connect(io::Error),
    Tls(String),
    Timeout(TimeoutKind),
    Io(io::Error),
    Protocol(String),
    TooManyRedirects,
    BodyTooLarge { limit: usize },
    Status(u16, Box<Response>),
    Json(String),
}

impl Error {
    pub fn is_timeout(&self) -> bool {
        match self {
            Self::Timeout(_) => true,
            Self::Connect(error) | Self::Io(error) => error.kind() == io::ErrorKind::TimedOut,
            _ => false,
        }
    }

    pub fn is_connect(&self) -> bool {
        matches!(self, Self::Connect(_) | Self::Tls(_))
    }

    pub fn is_builder(&self) -> bool {
        matches!(self, Self::InvalidUrl(_) | Self::InvalidRequest(_))
    }

    pub fn is_request(&self) -> bool {
        matches!(self, Self::Io(_) | Self::Protocol(_))
    }

    pub fn is_body(&self) -> bool {
        matches!(self, Self::BodyTooLarge { .. })
    }

    pub fn is_redirect(&self) -> bool {
        matches!(self, Self::TooManyRedirects)
    }

    pub fn is_decode(&self) -> bool {
        matches!(self, Self::Json(_))
    }

    pub fn is_status(&self) -> bool {
        matches!(self, Self::Status(..))
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status(status, _) => Some(*status),
            _ => None,
        }
    }

    pub fn url(&self) -> Option<&str> {
        match self {
            Self::Status(_, response) => Some(response.url()),
            _ => None,
        }
    }

    pub fn into_response(self) -> Option<Response> {
        match self {
            Self::Status(_, response) => Some(*response),
            _ => None,
        }
    }

    pub fn response(&self) -> Option<&Response> {
        match self {
            Self::Status(_, response) => Some(response),
            _ => None,
        }
    }

    pub(crate) fn from_io(error: io::Error, phase: TimeoutKind, overall_expired: bool) -> Self {
        if matches!(
            error.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ) {
            if overall_expired {
                return Self::Timeout(TimeoutKind::Overall);
            }

            return Self::Timeout(phase);
        }

        if phase == TimeoutKind::Connect {
            return Self::Connect(error);
        }

        if error.kind() == io::ErrorKind::InvalidData {
            return Self::Protocol(error.to_string());
        }

        Self::Io(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl(message) => write!(f, "invalid url: {message}"),
            Self::InvalidRequest(message) => write!(f, "invalid request: {message}"),
            Self::Connect(error) => write!(f, "connect failed: {error}"),
            Self::Tls(message) => write!(f, "tls failed: {message}"),
            Self::Timeout(kind) => write!(f, "{kind} timeout"),
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
            Self::TooManyRedirects => f.write_str("too many redirects"),
            Self::BodyTooLarge { limit } => write!(f, "body exceeds the {limit} byte limit"),
            Self::Status(status, _) => write!(f, "http status {status}"),
            Self::Json(message) => write!(f, "json error: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(error) | Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::from_io(error, TimeoutKind::Read, false)
    }
}
