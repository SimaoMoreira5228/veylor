use std::array;
use std::borrow::Cow;

use veylor_http2::{DataWriteError, ErrorCode, H2ServerDriver, ServerError, StreamState};

use super::STREAM_SLOTS;

pub(super) struct PendingResponses {
    entries: [Option<PendingBody>; STREAM_SLOTS],
    next: usize,
    now_ns: u64,
    timeout_ns: u64,
}

struct PendingBody {
    stream: u32,
    bytes: Vec<u8>,
    offset: usize,
    last_progress_ns: u64,
}

impl PendingResponses {
    pub(super) fn new() -> Self {
        Self {
            entries: array::from_fn(|_| None),
            next: 0,
            now_ns: 0,
            timeout_ns: 0,
        }
    }

    pub(super) fn set_time(&mut self, now_ns: u64, timeout_ns: u64) {
        self.now_ns = now_ns;
        self.timeout_ns = timeout_ns;
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.iter().all(Option::is_none)
    }

    pub(super) fn send(
        &mut self,
        server: &mut H2ServerDriver<'_, '_, '_, '_>,
        stream: u32,
        body: Cow<'_, [u8]>,
        output: &mut [u8],
        used: &mut usize,
    ) -> Result<bool, ErrorCode> {
        if body.is_empty() {
            return Ok(true);
        }

        let (offset, complete) = write_available(server, stream, body.as_ref(), output, used)?;
        if complete {
            return Ok(true);
        }

        let (bytes, offset) = match body {
            Cow::Borrowed(bytes) => (bytes[offset..].to_vec(), 0),
            Cow::Owned(bytes) => (bytes, offset),
        };

        let Some(entry) = self.entries.iter_mut().find(|entry| entry.is_none()) else {
            return Err(ErrorCode::InternalError);
        };

        *entry = Some(PendingBody {
            stream,
            bytes,
            offset,
            last_progress_ns: self.now_ns,
        });

        Ok(false)
    }

    pub(super) fn pump(
        &mut self,
        server: &mut H2ServerDriver<'_, '_, '_, '_>,
        output: &mut [u8],
        used: &mut usize,
    ) -> Result<(u16, bool), ErrorCode> {
        let first = self.next;
        self.next = (self.next + 1) % STREAM_SLOTS;
        let mut completed = 0;

        for relative in 0..STREAM_SLOTS {
            let index = (first + relative) % STREAM_SLOTS;
            let Some(entry) = self.entries[index].as_mut() else {
                continue;
            };

            let live = server.driver().streams().find(entry.stream);
            if live.is_none_or(|stream| stream.state == StreamState::Closed) {
                self.entries[index] = None;
                continue;
            }

            let (sent, complete) = write_available(
                server,
                entry.stream,
                &entry.bytes[entry.offset..],
                output,
                used,
            )?;
            entry.offset += sent;

            if sent > 0 {
                entry.last_progress_ns = self.now_ns;
            }

            if complete {
                self.entries[index] = None;
                completed += 1;
            }
        }

        let active = self.has_credit(server, output.len().saturating_sub(*used));
        Ok((completed, active))
    }

    pub(super) fn expire(
        &mut self,
        server: &mut H2ServerDriver<'_, '_, '_, '_>,
        output: &mut [u8],
    ) -> (usize, u16) {
        if self.timeout_ns == 0 {
            return (0, 0);
        }

        let mut written = 0;
        let mut refused = 0;

        for entry in &mut self.entries {
            let Some(pending) = entry.as_ref() else {
                continue;
            };

            if !response_expired(self.now_ns, pending.last_progress_ns, self.timeout_ns) {
                continue;
            }
            if output.len().saturating_sub(written) < 13 {
                break;
            }

            match server.rst_stream(pending.stream, ErrorCode::Cancel, &mut output[written..]) {
                Ok(frame) => {
                    written += frame.pending_bytes().len();
                    refused += 1;
                    *entry = None;
                }
                Err(ServerError::StreamClosed | ServerError::UnknownStream) => {
                    *entry = None;
                }
                Err(_) => break,
            }
        }

        (written, refused)
    }

    fn has_credit(&self, server: &H2ServerDriver<'_, '_, '_, '_>, room: usize) -> bool {
        if room < 10 || server.driver().connection_send_window().value() <= 0 {
            return false;
        }

        self.entries.iter().flatten().any(|entry| {
            server
                .driver()
                .streams()
                .find(entry.stream)
                .is_some_and(|stream| {
                    stream.state != StreamState::Closed && stream.send_window.value() > 0
                })
        })
    }
}

fn response_expired(now_ns: u64, last_progress_ns: u64, timeout_ns: u64) -> bool {
    timeout_ns != 0 && now_ns.saturating_sub(last_progress_ns) >= timeout_ns
}

fn write_available(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    bytes: &[u8],
    output: &mut [u8],
    used: &mut usize,
) -> Result<(usize, bool), ErrorCode> {
    if bytes.is_empty() {
        return Ok((0, true));
    }

    if output.len().saturating_sub(*used) < 10 {
        return Ok((0, false));
    }

    match server.send_data(stream, bytes, true, &mut output[*used..]) {
        Ok(sent) => {
            *used += sent.writer.pending_bytes().len();
            let complete = sent.frame.payload_length == bytes.len();
            Ok((sent.frame.payload_length, complete))
        }
        Err(ServerError::Data(DataWriteError::NoSendCredit)) => Ok((0, false)),
        Err(_) => Err(ErrorCode::InternalError),
    }
}

#[cfg(test)]
mod tests {
    use super::response_expired;

    #[test]
    fn response_timeout_zero_disables_expiry() {
        assert!(!response_expired(u64::MAX, 0, 0));
    }

    #[test]
    fn response_timeout_expires_only_after_full_inactivity_window() {
        assert!(!response_expired(109, 10, 100));
        assert!(response_expired(110, 10, 100));
        assert!(!response_expired(1, u64::MAX, 100));
    }
}
