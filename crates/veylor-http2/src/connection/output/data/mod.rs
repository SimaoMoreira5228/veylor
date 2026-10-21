#[cfg(test)]
mod tests;

use super::OutputProgress;
use crate::connection::H2FrameDriver;
use crate::flow_control::choose_data_frame_size;
use crate::frame::{
    DATA_FLAG_END_STREAM, FRAME_HEADER_LEN, FrameHeader, FrameHeaderError, FrameType,
    MAX_STREAM_ID, encode_frame_header,
};
use crate::stream::{FrameDirection, StreamActionKind, stream_state_on_frame};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataWriteError {
    PendingFrame,
    OutputTooSmall { required: usize },
    InvalidStreamId,
    UnknownStream,
    StreamClosed,
    SettingsNotSent,
    NoSendCredit,
    InvalidHeader(FrameHeaderError),
    OverAcknowledged,
    Terminal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueuedDataFrame {
    pub frame_length: usize,
    pub payload_length: usize,
    pub end_stream: bool,
}

pub struct DataFrameWriter<'buffer> {
    buffer: &'buffer mut [u8],
    frame_length: usize,
    acknowledged: usize,
    terminal: bool,
}

impl<'buffer> DataFrameWriter<'buffer> {
    pub fn new(buffer: &'buffer mut [u8]) -> Self {
        Self {
            buffer,
            frame_length: 0,
            acknowledged: 0,
            terminal: false,
        }
    }

    pub fn queue_data(
        &mut self,
        driver: &mut H2FrameDriver<'_, '_, '_, '_>,
        stream_id: u32,
        input: &[u8],
        end_stream: bool,
    ) -> Result<QueuedDataFrame, DataWriteError> {
        self.ensure_writable()?;
        if stream_id == 0 || stream_id > MAX_STREAM_ID {
            return Err(DataWriteError::InvalidStreamId);
        }
        if !driver.streams().first_settings_sent() {
            return Err(DataWriteError::SettingsNotSent);
        }

        let Some(stream) = driver.streams().find(stream_id) else {
            return Err(DataWriteError::UnknownStream);
        };
        let stream_state = stream.state;
        let stream_credit = stream.send_window.value();
        let peer_max_frame_size = driver.streams().remote_settings().max_frame_size;
        let (payload_length, frame_length) = if input.is_empty() {
            let required = FRAME_HEADER_LEN;
            self.ensure_capacity(required)?;
            (0, required)
        } else {
            if self.buffer.len() <= FRAME_HEADER_LEN {
                return Err(DataWriteError::OutputTooSmall {
                    required: FRAME_HEADER_LEN + 1,
                });
            }
            let connection_credit = driver.connection_send_window().value();
            let remaining = input.len().min(self.buffer.len() - FRAME_HEADER_LEN);
            let payload_length = choose_data_frame_size(
                connection_credit,
                stream_credit,
                remaining as u64,
                peer_max_frame_size,
            ) as usize;
            if payload_length == 0 {
                return Err(DataWriteError::NoSendCredit);
            }
            (payload_length, FRAME_HEADER_LEN + payload_length)
        };
        let frame_end_stream = end_stream && payload_length == input.len();
        let header = FrameHeader {
            payload_length: payload_length as u32,
            frame_type: FrameType::Data,
            flags: if frame_end_stream {
                DATA_FLAG_END_STREAM
            } else {
                0
            },
            stream_id,
        };
        let mut encoded_header = [0; FRAME_HEADER_LEN];
        encode_frame_header(header, &mut encoded_header, peer_max_frame_size)
            .map_err(DataWriteError::InvalidHeader)?;

        let transition = stream_state_on_frame(
            stream_state,
            FrameType::Data,
            header.flags,
            FrameDirection::Sent,
        );
        if transition.action.kind != StreamActionKind::Accept {
            return Err(DataWriteError::StreamClosed);
        }
        let action = driver
            .streams_mut()
            .on_frame(header, FrameDirection::Sent, &[]);
        if action.kind != StreamActionKind::Accept {
            return Err(DataWriteError::StreamClosed);
        }
        if payload_length > 0 {
            driver
                .take_send_credit(stream_id, payload_length as i64)
                .map_err(|_| DataWriteError::NoSendCredit)?;
        }

        self.buffer[..FRAME_HEADER_LEN].copy_from_slice(&encoded_header);
        self.buffer[FRAME_HEADER_LEN..frame_length].copy_from_slice(&input[..payload_length]);
        self.frame_length = frame_length;
        self.acknowledged = 0;

        Ok(QueuedDataFrame {
            frame_length,
            payload_length,
            end_stream: frame_end_stream,
        })
    }

    pub fn pending_bytes(&self) -> &[u8] {
        &self.buffer[self.acknowledged..self.frame_length]
    }

    pub const fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn acknowledge(&mut self, byte_count: usize) -> Result<OutputProgress, DataWriteError> {
        if self.terminal {
            return Err(DataWriteError::Terminal);
        }
        let pending = self.frame_length - self.acknowledged;
        if byte_count > pending {
            self.terminal = true;
            self.frame_length = 0;
            self.acknowledged = 0;
            return Err(DataWriteError::OverAcknowledged);
        }
        if pending == 0 {
            return Ok(OutputProgress::Idle);
        }

        self.acknowledged += byte_count;
        let remaining = self.frame_length - self.acknowledged;
        if remaining == 0 {
            self.frame_length = 0;
            self.acknowledged = 0;
            return Ok(OutputProgress::Complete);
        }
        Ok(OutputProgress::Pending { remaining })
    }

    fn ensure_writable(&self) -> Result<(), DataWriteError> {
        if self.terminal {
            return Err(DataWriteError::Terminal);
        }
        if self.frame_length != self.acknowledged {
            return Err(DataWriteError::PendingFrame);
        }
        Ok(())
    }

    fn ensure_capacity(&self, required: usize) -> Result<(), DataWriteError> {
        if self.buffer.len() < required {
            return Err(DataWriteError::OutputTooSmall { required });
        }
        Ok(())
    }
}
