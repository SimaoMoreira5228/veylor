use veylor_http1::{Header, Span};
use veylor_runtime::ConnectionToken;

pub use veylor_http1::{Header as RequestHeader, Span as RequestSpan};

use super::{HOST_MAX_PATH_BYTES, HostHandle, RespondOutcome};

pub const MAX_REQUEST_HEADERS: usize = 128;
pub const MAX_REQUEST_HEADER_BYTES: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestHeadReport {
    pub target: Span,
    pub query: Option<Span>,
    pub header_count: usize,
    pub bytes_used: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestHeadError {
    Unavailable(RespondOutcome),
    Capacity {
        headers_required: usize,
        bytes_required: usize,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct RequestSnapshot {
    bytes: Vec<u8>,
    headers: Vec<Header>,
    target_len: usize,
}

impl RequestSnapshot {
    pub(crate) fn new<'a>(
        target: &[u8],
        fields: impl Iterator<Item = Option<(&'a [u8], &'a [u8])>> + Clone,
    ) -> Option<Self> {
        if target.len() > HOST_MAX_PATH_BYTES {
            return None;
        }

        let mut count = 0;
        let mut header_bytes = 0usize;
        for field in fields.clone() {
            let (name, value) = field?;
            count += 1;
            header_bytes = header_bytes
                .checked_add(name.len())?
                .checked_add(value.len())?;
            if count > MAX_REQUEST_HEADERS || header_bytes > MAX_REQUEST_HEADER_BYTES {
                return None;
            }
        }

        let total = target.len().checked_add(header_bytes)?;
        let mut bytes = Vec::with_capacity(total);
        let mut headers = Vec::with_capacity(count);
        bytes.extend_from_slice(target);
        for field in fields {
            let (name, value) = field?;
            let name_span = Span {
                offset: bytes.len(),
                length: name.len(),
            };
            bytes.extend_from_slice(name);
            let value_span = Span {
                offset: bytes.len(),
                length: value.len(),
            };
            bytes.extend_from_slice(value);
            headers.push(Header {
                name: name_span,
                value: value_span,
            });
        }

        Some(Self {
            bytes,
            headers,
            target_len: target.len(),
        })
    }

    pub(crate) fn fields(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.headers.iter().filter_map(|header| {
            let name_end = header.name.offset.checked_add(header.name.length)?;
            let value_end = header.value.offset.checked_add(header.value.length)?;
            let name = self.bytes.get(header.name.offset..name_end)?;
            let value = self.bytes.get(header.value.offset..value_end)?;
            Some((name, value))
        })
    }

    pub(crate) fn copy(
        &self,
        headers: &mut [Header],
        bytes: &mut [u8],
    ) -> Result<RequestHeadReport, RequestHeadError> {
        if headers.len() < self.headers.len() || bytes.len() < self.bytes.len() {
            return Err(RequestHeadError::Capacity {
                headers_required: self.headers.len(),
                bytes_required: self.bytes.len(),
            });
        }

        headers[..self.headers.len()].copy_from_slice(&self.headers);
        bytes[..self.bytes.len()].copy_from_slice(&self.bytes);
        let query = self.bytes[..self.target_len]
            .iter()
            .position(|byte| *byte == b'?')
            .map(|offset| Span {
                offset: offset + 1,
                length: self.target_len - offset - 1,
            });

        Ok(RequestHeadReport {
            target: Span {
                offset: 0,
                length: self.target_len,
            },
            query,
            header_count: self.headers.len(),
            bytes_used: self.bytes.len(),
        })
    }
}

impl HostHandle {
    pub fn request_head(
        &self,
        token: ConnectionToken,
        headers: &mut [Header],
        bytes: &mut [u8],
    ) -> Result<RequestHeadReport, RequestHeadError> {
        self.shared
            .with_inner(|inner| {
                let snapshot = inner
                    .parked
                    .get(token.worker_id as usize)
                    .and_then(|worker| worker.get(token.slot as usize))
                    .and_then(|entry| entry.as_ref())
                    .filter(|entry| entry.token == token)
                    .and_then(|entry| entry.request.as_ref());
                let result = snapshot.map_or(
                    Err(RequestHeadError::Unavailable(RespondOutcome::Stale)),
                    |snapshot| snapshot.copy(headers, bytes),
                );
                (result, None)
            })
            .map(|(result, _)| result)
            .unwrap_or(Err(RequestHeadError::Unavailable(RespondOutcome::Stale)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_checks_limits_and_capacity_before_copying() {
        let fields = [(b"x-name".as_slice(), b"value".as_slice())];
        let snapshot = RequestSnapshot::new(b"/path?x=1", fields.into_iter().map(Some)).unwrap();
        assert_eq!(snapshot.bytes.capacity(), 20);
        assert_eq!(snapshot.headers.capacity(), 1);

        let mut bytes = [0xA5; 32];
        let result = snapshot.copy(&mut [], &mut bytes);
        assert_eq!(
            result,
            Err(RequestHeadError::Capacity {
                headers_required: 1,
                bytes_required: 20,
            })
        );
        assert_eq!(bytes, [0xA5; 32]);

        let too_many = [(b"x".as_slice(), b"y".as_slice()); MAX_REQUEST_HEADERS + 1];
        assert!(RequestSnapshot::new(b"/", too_many.into_iter().map(Some)).is_none());

        let large = [b'x'; MAX_REQUEST_HEADER_BYTES];
        let oversized = [(b"x".as_slice(), large.as_slice())];
        assert!(RequestSnapshot::new(b"/", oversized.into_iter().map(Some)).is_none());
    }
}
