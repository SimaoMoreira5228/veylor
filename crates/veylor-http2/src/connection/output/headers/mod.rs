#[cfg(test)]
mod tests;

use super::OutputProgress;
use crate::connection::H2FrameDriver;
use crate::frame::{
    CONTINUATION_FLAG_END_HEADERS, FRAME_HEADER_LEN, FrameHeader, FrameHeaderError, FrameType,
    HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, MAX_STREAM_ID, encode_frame_header,
};
use crate::stream::{FrameDirection, StreamAction, StreamActionKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeadersWriteError {
    PendingFrame,
    OutputTooSmall { required: usize },
    InvalidStreamId,
    SettingsNotSent,
    LengthOverflow,
    InvalidHeader(FrameHeaderError),
    StreamRejected(StreamAction),
    OverAcknowledged,
    Terminal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueuedHeadersBlock {
    pub frame_count: usize,
    pub block_length: usize,
    pub output_length: usize,
    pub end_stream: bool,
}

pub struct HeadersFrameWriter<'buffer> {
    buffer: &'buffer mut [u8],
    output_length: usize,
    acknowledged: usize,
    terminal: bool,
}

impl<'buffer> HeadersFrameWriter<'buffer> {
    pub fn new(buffer: &'buffer mut [u8]) -> Self {
        Self {
            buffer,
            output_length: 0,
            acknowledged: 0,
            terminal: false,
        }
    }

    pub fn queue_headers(
        &mut self,
        driver: &mut H2FrameDriver<'_, '_, '_, '_>,
        stream_id: u32,
        block: &[u8],
        end_stream: bool,
    ) -> Result<QueuedHeadersBlock, HeadersWriteError> {
        self.ensure_writable()?;
        if stream_id == 0 || stream_id > MAX_STREAM_ID {
            return Err(HeadersWriteError::InvalidStreamId);
        }
        if !driver.streams().first_settings_sent() {
            return Err(HeadersWriteError::SettingsNotSent);
        }

        let peer_max_frame_size = driver.streams().remote_settings().max_frame_size;
        let peer_max_frame_size =
            usize::try_from(peer_max_frame_size).map_err(|_| HeadersWriteError::LengthOverflow)?;
        if peer_max_frame_size == 0 {
            return Err(HeadersWriteError::LengthOverflow);
        }

        let frame_count = if block.is_empty() {
            1
        } else {
            1 + (block.len() - 1) / peer_max_frame_size
        };
        let frame_header_length = frame_count
            .checked_mul(FRAME_HEADER_LEN)
            .ok_or(HeadersWriteError::LengthOverflow)?;
        let output_length = block
            .len()
            .checked_add(frame_header_length)
            .ok_or(HeadersWriteError::LengthOverflow)?;
        self.ensure_capacity(output_length)?;

        let first_fragment_length = block.len().min(peer_max_frame_size);
        let first_flags = if end_stream {
            HEADERS_FLAG_END_STREAM
        } else {
            0
        } | if frame_count == 1 {
            HEADERS_FLAG_END_HEADERS
        } else {
            0
        };
        let first_header = FrameHeader {
            payload_length: u32::try_from(first_fragment_length)
                .map_err(|_| HeadersWriteError::LengthOverflow)?,
            frame_type: FrameType::Headers,
            flags: first_flags,
            stream_id,
        };
        let mut encoded_header = [0; FRAME_HEADER_LEN];
        encode_frame_header(
            first_header,
            &mut encoded_header,
            peer_max_frame_size as u32,
        )
        .map_err(HeadersWriteError::InvalidHeader)?;

        if frame_count > 1 {
            let last_fragment_length = block.len() - peer_max_frame_size * (frame_count - 1);
            let last_header = FrameHeader {
                payload_length: u32::try_from(last_fragment_length)
                    .map_err(|_| HeadersWriteError::LengthOverflow)?,
                frame_type: FrameType::Continuation,
                flags: CONTINUATION_FLAG_END_HEADERS,
                stream_id,
            };
            encode_frame_header(last_header, &mut encoded_header, peer_max_frame_size as u32)
                .map_err(HeadersWriteError::InvalidHeader)?;
        }

        let action = driver.streams_mut().on_frame(
            first_header,
            FrameDirection::Sent,
            &block[..first_fragment_length],
        );
        if action.kind != StreamActionKind::Accept {
            return Err(HeadersWriteError::StreamRejected(action));
        }

        let mut block_offset = 0;
        let mut output_offset = 0;
        for frame_index in 0..frame_count {
            let fragment_length = (block.len() - block_offset).min(peer_max_frame_size);
            let last_frame = frame_index + 1 == frame_count;
            let (frame_type, flags) = if frame_index == 0 {
                (FrameType::Headers, first_flags)
            } else if last_frame {
                (FrameType::Continuation, CONTINUATION_FLAG_END_HEADERS)
            } else {
                (FrameType::Continuation, 0)
            };
            let header = FrameHeader {
                payload_length: fragment_length as u32,
                frame_type,
                flags,
                stream_id,
            };
            let header_end = output_offset + FRAME_HEADER_LEN;
            encode_frame_header(
                header,
                &mut self.buffer[output_offset..header_end],
                peer_max_frame_size as u32,
            )
            .map_err(HeadersWriteError::InvalidHeader)?;

            let payload_end = header_end + fragment_length;
            self.buffer[header_end..payload_end]
                .copy_from_slice(&block[block_offset..block_offset + fragment_length]);
            output_offset = payload_end;
            block_offset += fragment_length;
        }

        self.output_length = output_length;
        self.acknowledged = 0;

        Ok(QueuedHeadersBlock {
            frame_count,
            block_length: block.len(),
            output_length,
            end_stream,
        })
    }

    pub fn pending_bytes(&self) -> &[u8] {
        &self.buffer[self.acknowledged..self.output_length]
    }

    pub const fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn acknowledge(&mut self, byte_count: usize) -> Result<OutputProgress, HeadersWriteError> {
        if self.terminal {
            return Err(HeadersWriteError::Terminal);
        }
        let pending = self.output_length - self.acknowledged;
        if byte_count > pending {
            self.terminal = true;
            self.output_length = 0;
            self.acknowledged = 0;
            return Err(HeadersWriteError::OverAcknowledged);
        }
        if pending == 0 {
            return Ok(OutputProgress::Idle);
        }

        self.acknowledged += byte_count;
        let remaining = self.output_length - self.acknowledged;
        if remaining == 0 {
            self.output_length = 0;
            self.acknowledged = 0;
            return Ok(OutputProgress::Complete);
        }
        Ok(OutputProgress::Pending { remaining })
    }

    fn ensure_writable(&self) -> Result<(), HeadersWriteError> {
        if self.terminal {
            return Err(HeadersWriteError::Terminal);
        }
        if self.output_length != self.acknowledged {
            return Err(HeadersWriteError::PendingFrame);
        }
        Ok(())
    }

    fn ensure_capacity(&self, required: usize) -> Result<(), HeadersWriteError> {
        if self.buffer.len() < required {
            return Err(HeadersWriteError::OutputTooSmall { required });
        }
        Ok(())
    }
}
