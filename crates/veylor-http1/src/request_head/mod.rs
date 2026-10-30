use crate::fields::ConnectionOptions;
use crate::request_target::TargetForm;

mod parser;
#[cfg(test)]
mod tests;

pub use parser::Parser;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Span {
    pub offset: usize,
    pub length: usize,
}

impl Span {
    fn end(self) -> Option<usize> {
        self.offset.checked_add(self.length)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Header {
    pub name: Span,
    pub value: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestHead {
    pub method: Span,
    pub target: Span,
    pub target_form: TargetForm,
    pub authority: Span,
    pub version: Span,
    pub header_count: usize,
    pub length: usize,
    pub content_length: Option<u64>,
    pub is_chunked: bool,
    pub expect_continue: bool,
    pub connection: ConnectionOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestVersion {
    Http10,
    Http11,
}

impl RequestVersion {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            b"HTTP/1.0" => Some(Self::Http10),
            b"HTTP/1.1" => Some(Self::Http11),
            _ => None,
        }
    }
}

impl RequestHead {
    pub fn method(self, input: &[u8]) -> Option<&[u8]> {
        slice_at(input, self.method)
    }

    pub fn target(self, input: &[u8]) -> Option<&[u8]> {
        slice_at(input, self.target)
    }

    pub fn version(self, input: &[u8]) -> Option<&[u8]> {
        slice_at(input, self.version)
    }

    pub fn protocol_version(self, input: &[u8]) -> Option<RequestVersion> {
        RequestVersion::from_bytes(self.version(input)?)
    }
}

pub(super) fn slice_at(input: &[u8], span: Span) -> Option<&[u8]> {
    input.get(span.offset..span.end()?)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    pub request_line_bytes: usize,
    pub header_bytes: usize,
    pub header_count: usize,
    pub field_name_bytes: usize,
    pub field_value_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            request_line_bytes: 8 * 1024,
            header_bytes: 32 * 1024,
            header_count: Self::DEFAULT_HEADER_COUNT,
            field_name_bytes: 256,
            field_value_bytes: 8 * 1024,
        }
    }
}

impl Limits {
    pub const DEFAULT_HEADER_COUNT: usize = 100;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParseError {
    Invalid,
    RequestLineTooLarge,
    StatusLineTooLarge,
    HeaderSectionTooLarge,
    HeaderCapacity,
    BufferChanged,
    ExpectationFailed,
    Incomplete,
    Cancelled,
    BodyTooLarge,
    AdmissionPending,
    BodyRejected,
}
