use crate::response_write::{ResponseEncodeError, ResponseWriteCursor};

use super::{IoCompletion, IoOperation, IoResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteState {
    Pending,
    Complete,
    Closed,
    Cancelled,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteResult {
    Progress,
    WouldBlock,
    Interrupted,
    Complete,
    PeerClosed,
    Cancelled,
    TransportError,
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteEvent {
    pub accepted_bytes: usize,
    pub result: WriteResult,
    pub error_code: i32,
}

pub struct WriteDriver<'a> {
    connection_id: u64,
    operation_id: Option<u64>,
    cursor: ResponseWriteCursor<'a>,
    state: WriteState,
    result: WriteResult,
    error_code: i32,
}

impl<'a> WriteDriver<'a> {
    pub fn new(connection_id: u64, bytes: &'a [u8]) -> Self {
        let cursor = ResponseWriteCursor::new(bytes);
        let state = if cursor.is_complete() {
            WriteState::Complete
        } else {
            WriteState::Pending
        };
        let result = if state == WriteState::Complete {
            WriteResult::Complete
        } else {
            WriteResult::Progress
        };
        Self {
            connection_id,
            operation_id: None,
            cursor,
            state,
            result,
            error_code: 0,
        }
    }

    pub fn pending(&self) -> &[u8] {
        if self.state != WriteState::Pending {
            return &[];
        }
        self.cursor.pending()
    }

    pub fn state(&self) -> WriteState {
        self.state
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
                if transferred_bytes > self.cursor.pending().len() {
                    return self.fail(WriteResult::Invalid);
                }
                match self.cursor.advance_write(transferred_bytes) {
                    Ok(complete) => {
                        self.result = WriteResult::Progress;
                        if complete {
                            self.state = WriteState::Complete;
                            self.result = WriteResult::Complete;
                        }
                        self.event(transferred_bytes)
                    }
                    Err(ResponseEncodeError::WriteOverrun | ResponseEncodeError::InvalidState) => {
                        self.fail(WriteResult::Invalid)
                    }
                    Err(_) => self.fail(WriteResult::Invalid),
                }
            }
        }
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
