#[cfg(test)]
mod tests;

use crate::request_write::Field;
use crate::response_write::{
    EncodedResponseHead, FixedResponseBodyWriter, ResponseEncodeError, ResponseFraming,
    ResponseToWrite, encode_response_chunk, encode_response_final_chunk, encode_response_head,
};

use super::{IoCompletion, IoOperation, IoResult, WriteEvent, WriteResult, WriteState};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingKind {
    None,
    Output,
    FixedBody,
    CloseDelimitedBody,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputKind {
    Head,
    Chunk,
    FinalChunk,
}

enum BodyWriter<'a> {
    Fixed(FixedResponseBodyWriter<'a>),
    Chunked {
        finished: bool,
    },
    CloseDelimited {
        current: &'a [u8],
        offset: usize,
        finished: bool,
    },
    Inactive,
}

pub struct ResponseWriteDriver<'output, 'body> {
    connection_id: u64,
    operation_id: Option<u64>,
    output: &'output mut [u8],
    output_offset: usize,
    output_length: usize,
    output_kind: OutputKind,
    pending: PendingKind,
    body: BodyWriter<'body>,
    body_allowed: bool,
    head_finishes: bool,
    state: WriteState,
    result: WriteResult,
    error_code: i32,
}

impl<'output, 'body> ResponseWriteDriver<'output, 'body> {
    pub fn new(
        connection_id: u64,
        response: ResponseToWrite<'body>,
        output: &'output mut [u8],
    ) -> Result<Self, ResponseEncodeError> {
        let EncodedResponseHead {
            length,
            body_allowed,
        } = encode_response_head(&response, output)?;
        let body = match response.framing {
            ResponseFraming::FixedLength => BodyWriter::Fixed(FixedResponseBodyWriter::new(
                response.content_length,
                body_allowed,
            )),
            ResponseFraming::Chunked => BodyWriter::Chunked { finished: false },
            ResponseFraming::CloseDelimited => BodyWriter::CloseDelimited {
                current: &[],
                offset: 0,
                finished: false,
            },
        };
        let head_finishes = !body_allowed
            || (response.framing == ResponseFraming::FixedLength && response.content_length == 0);

        Ok(Self {
            connection_id,
            operation_id: None,
            output,
            output_offset: 0,
            output_length: length,
            output_kind: OutputKind::Head,
            pending: PendingKind::Output,
            body,
            body_allowed,
            head_finishes,
            state: WriteState::Pending,
            result: WriteResult::Progress,
            error_code: 0,
        })
    }

    pub fn pending(&self) -> &[u8] {
        if self.state != WriteState::Pending {
            return &[];
        }
        match self.pending {
            PendingKind::None => &[],
            PendingKind::Output => &self.output[self.output_offset..self.output_length],
            PendingKind::FixedBody => match &self.body {
                BodyWriter::Fixed(writer) => writer.pending(),
                BodyWriter::Chunked { .. }
                | BodyWriter::CloseDelimited { .. }
                | BodyWriter::Inactive => &[],
            },
            PendingKind::CloseDelimitedBody => match &self.body {
                BodyWriter::CloseDelimited {
                    current, offset, ..
                } => &current[*offset..],
                BodyWriter::Fixed(_) | BodyWriter::Chunked { .. } | BodyWriter::Inactive => &[],
            },
        }
    }

    pub fn queue_fixed(&mut self, bytes: &'body [u8]) -> Result<(), ResponseEncodeError> {
        self.ensure_body_ready()?;
        let (result, has_pending) = match &mut self.body {
            BodyWriter::Fixed(writer) => {
                let result = writer.queue(bytes);
                (result, !writer.pending().is_empty())
            }
            BodyWriter::Chunked { .. }
            | BodyWriter::CloseDelimited { .. }
            | BodyWriter::Inactive => {
                return Err(ResponseEncodeError::InvalidState);
            }
        };
        if let Err(error) = result {
            if error == ResponseEncodeError::BodyOverrun {
                self.fail(WriteResult::Invalid);
            }
            return Err(error);
        }
        if has_pending {
            self.pending = PendingKind::FixedBody;
        }
        Ok(())
    }

    pub fn finish_fixed(&mut self) -> Result<bool, ResponseEncodeError> {
        self.ensure_streaming()?;
        if !matches!(self.pending, PendingKind::None | PendingKind::FixedBody) {
            return Err(ResponseEncodeError::InvalidState);
        }
        let result = match &mut self.body {
            BodyWriter::Fixed(writer) => writer.finish(),
            BodyWriter::Chunked { .. }
            | BodyWriter::CloseDelimited { .. }
            | BodyWriter::Inactive => {
                return Err(ResponseEncodeError::InvalidState);
            }
        };
        match result {
            Ok(true) => {
                self.complete();
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(error) => {
                self.fail(WriteResult::Invalid);
                Err(error)
            }
        }
    }

    pub fn queue_chunk(&mut self, bytes: &[u8]) -> Result<(), ResponseEncodeError> {
        self.ensure_body_ready()?;
        if !matches!(&self.body, BodyWriter::Chunked { finished: false }) {
            return Err(ResponseEncodeError::InvalidState);
        }
        let length = encode_response_chunk(bytes, self.output)?;
        self.queue_output(length, OutputKind::Chunk);
        Ok(())
    }

    pub fn finish_chunked(&mut self, trailers: &[Field<'_>]) -> Result<(), ResponseEncodeError> {
        self.ensure_body_ready()?;
        let BodyWriter::Chunked { finished } = &mut self.body else {
            return Err(ResponseEncodeError::InvalidState);
        };
        if *finished {
            return Err(ResponseEncodeError::InvalidState);
        }
        let length = encode_response_final_chunk(trailers, self.output)?;
        *finished = true;
        self.queue_output(length, OutputKind::FinalChunk);
        Ok(())
    }

    pub fn queue_close_delimited(&mut self, bytes: &'body [u8]) -> Result<(), ResponseEncodeError> {
        self.ensure_body_ready()?;
        let BodyWriter::CloseDelimited {
            current,
            offset,
            finished,
        } = &mut self.body
        else {
            return Err(ResponseEncodeError::InvalidState);
        };
        if *finished || !current.is_empty() {
            return Err(ResponseEncodeError::InvalidState);
        }
        *current = bytes;
        *offset = 0;
        if !bytes.is_empty() {
            self.pending = PendingKind::CloseDelimitedBody;
        }
        Ok(())
    }

    pub fn finish_close_delimited(&mut self) -> Result<bool, ResponseEncodeError> {
        self.ensure_streaming()?;
        if !matches!(
            self.pending,
            PendingKind::None | PendingKind::CloseDelimitedBody
        ) {
            return Err(ResponseEncodeError::InvalidState);
        }
        let is_empty = match &mut self.body {
            BodyWriter::CloseDelimited {
                current, finished, ..
            } => {
                *finished = true;
                current.is_empty()
            }
            BodyWriter::Fixed(_) | BodyWriter::Chunked { .. } | BodyWriter::Inactive => {
                return Err(ResponseEncodeError::InvalidState);
            }
        };
        if is_empty {
            self.complete();
            return Ok(true);
        }
        Ok(false)
    }

    pub fn consume(&mut self, completion: IoCompletion) -> WriteEvent {
        if self.state != WriteState::Pending {
            return self.event(0);
        }
        if self.pending().is_empty()
            || !completion.valid_for(self.connection_id, self.operation_id, IoOperation::Write)
        {
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
                self.discard_body();
                self.event(0)
            }
            IoResult::Cancelled => {
                self.cancel();
                self.event(0)
            }
            IoResult::Error { code } => {
                self.state = WriteState::Failed;
                self.result = WriteResult::TransportError;
                self.error_code = code;
                self.discard_body();
                self.event(0)
            }
            IoResult::Progress { transferred_bytes } => {
                if transferred_bytes > self.pending().len() {
                    return self.fail(WriteResult::Invalid);
                }
                self.advance_write(transferred_bytes)
            }
        }
    }

    pub fn cancel(&mut self) {
        if self.state != WriteState::Pending {
            return;
        }
        self.state = WriteState::Cancelled;
        self.result = WriteResult::Cancelled;
        self.discard_body();
    }

    pub fn state(&self) -> WriteState {
        self.state
    }

    fn ensure_body_ready(&self) -> Result<(), ResponseEncodeError> {
        if self.pending != PendingKind::None {
            return Err(ResponseEncodeError::InvalidState);
        }
        self.ensure_streaming()
    }

    fn ensure_streaming(&self) -> Result<(), ResponseEncodeError> {
        if self.state != WriteState::Pending || !self.body_allowed {
            return Err(ResponseEncodeError::InvalidState);
        }
        Ok(())
    }

    fn queue_output(&mut self, length: usize, kind: OutputKind) {
        self.output_offset = 0;
        self.output_length = length;
        self.output_kind = kind;
        self.pending = PendingKind::Output;
    }

    fn advance_write(&mut self, written: usize) -> WriteEvent {
        match self.pending {
            PendingKind::Output => {
                self.output_offset += written;
                if self.output_offset == self.output_length {
                    let kind = self.output_kind;
                    self.pending = PendingKind::None;
                    match kind {
                        OutputKind::Head if self.head_finishes => self.complete(),
                        OutputKind::FinalChunk => self.complete(),
                        OutputKind::Head | OutputKind::Chunk => {
                            self.result = WriteResult::Progress;
                        }
                    }
                }
            }
            PendingKind::FixedBody => {
                let result = match &mut self.body {
                    BodyWriter::Fixed(writer) => writer.advance_write(written),
                    BodyWriter::Chunked { .. }
                    | BodyWriter::CloseDelimited { .. }
                    | BodyWriter::Inactive => {
                        return self.fail(WriteResult::Invalid);
                    }
                };
                match result {
                    Ok(true) => self.complete(),
                    Ok(false) if matches!(&self.body, BodyWriter::Fixed(writer) if writer.pending().is_empty()) =>
                    {
                        self.pending = PendingKind::None;
                        self.result = WriteResult::Progress;
                    }
                    Ok(false) => self.result = WriteResult::Progress,
                    Err(_) => return self.fail(WriteResult::Invalid),
                }
            }
            PendingKind::CloseDelimitedBody => {
                let (finished, body_complete) = match &mut self.body {
                    BodyWriter::CloseDelimited {
                        current,
                        offset,
                        finished,
                    } => {
                        *offset += written;
                        let body_complete = *offset == current.len();
                        if body_complete {
                            *current = &[];
                            *offset = 0;
                        }
                        (*finished, body_complete)
                    }
                    BodyWriter::Fixed(_) | BodyWriter::Chunked { .. } | BodyWriter::Inactive => {
                        return self.fail(WriteResult::Invalid);
                    }
                };
                if body_complete {
                    self.pending = PendingKind::None;
                    if finished {
                        self.complete();
                    } else {
                        self.result = WriteResult::Progress;
                    }
                } else {
                    self.result = WriteResult::Progress;
                }
            }
            PendingKind::None => return self.fail(WriteResult::Invalid),
        }
        self.event(written)
    }

    fn complete(&mut self) {
        self.state = WriteState::Complete;
        self.result = WriteResult::Complete;
        self.pending = PendingKind::None;
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
        self.discard_body();
        self.event(0)
    }

    fn discard_body(&mut self) {
        self.body = BodyWriter::Inactive;
        self.pending = PendingKind::None;
    }
}
