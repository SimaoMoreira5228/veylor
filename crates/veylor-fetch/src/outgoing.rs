use std::io::Read;

use crate::method::Method;

pub(crate) enum Payload<'a> {
    Buffered(&'a [u8]),
    Stream {
        reader: &'a mut dyn Read,
        declared: Option<u64>,
    },
}

impl Payload<'_> {
    pub(crate) fn reader(&mut self) -> Option<&mut dyn Read> {
        match self {
            Self::Buffered(_) => None,
            Self::Stream { reader, .. } => Some(&mut **reader),
        }
    }

    pub(crate) fn declared(&self) -> Option<u64> {
        match self {
            Self::Buffered(_) => None,
            Self::Stream { declared, .. } => *declared,
        }
    }

    pub(crate) fn is_replayable(&self) -> bool {
        matches!(self, Self::Buffered(_))
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Self::Buffered(bytes) => bytes,
            Self::Stream { .. } => &[],
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Buffered(bytes) => bytes.is_empty(),
            Self::Stream { .. } => false,
        }
    }
}

pub(crate) struct Outgoing<'a, 'r> {
    pub(crate) method: &'a Method,
    pub(crate) host: &'a str,
    pub(crate) target: &'a str,
    pub(crate) headers: &'a [(String, String)],
    pub(crate) payload: &'a mut Payload<'r>,
}

impl Outgoing<'_, '_> {
    pub(crate) fn expects_body(&self) -> bool {
        !self.payload.is_empty()
            || matches!(self.method, Method::Post | Method::Put | Method::Patch)
            || !self.payload.is_replayable()
    }
}
