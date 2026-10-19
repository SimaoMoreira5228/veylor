#[cfg(test)]
mod tests;

use crate::request_write::{ClientRequestWriter, Field, Request, RequestEncodeError};

use super::{IoCompletion, IoOperation, IoResult, WriteEvent, WriteResult, WriteState};

pub struct ClientRequestDriver<'a> {
    connection_id: u64,
    operation_id: Option<u64>,
    writer: ClientRequestWriter<'a>,
    state: WriteState,
    result: WriteResult,
    error_code: i32,
}

impl<'a> ClientRequestDriver<'a> {
    pub fn new(
        connection_id: u64,
        request: &Request<'_>,
        output: &'a mut [u8],
    ) -> Result<Self, RequestEncodeError> {
        let writer = ClientRequestWriter::new(request, output)?;
        Ok(Self {
            connection_id,
            operation_id: None,
            writer,
            state: WriteState::Pending,
            result: WriteResult::Progress,
            error_code: 0,
        })
    }

    pub fn pending(&self) -> &[u8] {
        if self.state != WriteState::Pending {
            return &[];
        }
        self.writer.pending()
    }

    pub fn queue_fixed(&mut self, bytes: &[u8]) -> Result<usize, RequestEncodeError> {
        self.ensure_pending()?;
        self.writer.queue_fixed(bytes)
    }

    pub fn queue_chunk(&mut self, bytes: &[u8]) -> Result<(), RequestEncodeError> {
        self.ensure_pending()?;
        self.writer.queue_chunk(bytes)
    }

    pub fn finish_chunked(&mut self, trailers: &[Field<'_>]) -> Result<(), RequestEncodeError> {
        self.ensure_pending()?;
        self.writer.finish_chunked(trailers)
    }

    pub fn consume(&mut self, completion: IoCompletion) -> WriteEvent {
        if self.state != WriteState::Pending {
            return self.event(0);
        }
        if !completion.valid_for(self.connection_id, self.operation_id, IoOperation::Write) {
            return self.fail(WriteResult::Invalid);
        }
        self.operation_id = Some(completion.operation_id);

        match completion.result {
            IoResult::WouldBlock => {
                self.result = WriteResult::WouldBlock;
                self.event(0)
            }
            IoResult::Interrupted => {
                self.result = WriteResult::Interrupted;
                self.event(0)
            }
            IoResult::EndOfStream => self.fail(WriteResult::Invalid),
            IoResult::Closed => {
                self.state = WriteState::Closed;
                self.result = WriteResult::PeerClosed;
                self.event(0)
            }
            IoResult::Cancelled => {
                self.writer.cancel();
                self.state = WriteState::Cancelled;
                self.result = WriteResult::Cancelled;
                self.event(0)
            }
            IoResult::Error { code } => {
                self.state = WriteState::Failed;
                self.result = WriteResult::TransportError;
                self.error_code = code;
                self.event(0)
            }
            IoResult::Progress { transferred_bytes } => {
                if transferred_bytes > self.writer.pending().len() {
                    return self.fail(WriteResult::Invalid);
                }
                if self.writer.advance_write(transferred_bytes).is_err() {
                    return self.fail(WriteResult::Invalid);
                }
                if self.writer.is_complete() {
                    self.state = WriteState::Complete;
                    self.result = WriteResult::Complete;
                } else {
                    self.result = WriteResult::Progress;
                }
                self.event(transferred_bytes)
            }
        }
    }

    pub fn cancel(&mut self) -> bool {
        if self.state != WriteState::Pending {
            return false;
        }
        let wrote_any = self.writer.cancel();
        self.state = WriteState::Cancelled;
        self.result = WriteResult::Cancelled;
        wrote_any
    }

    pub fn state(&self) -> WriteState {
        self.state
    }

    fn ensure_pending(&self) -> Result<(), RequestEncodeError> {
        if self.state == WriteState::Pending {
            return Ok(());
        }
        Err(RequestEncodeError::InvalidState)
    }

    fn event(&self, accepted_bytes: usize) -> WriteEvent {
        WriteEvent {
            accepted_bytes,
            result: self.result,
            error_code: self.error_code,
        }
    }

    fn fail(&mut self, result: WriteResult) -> WriteEvent {
        self.state = WriteState::Failed;
        self.result = result;
        self.event(0)
    }
}
