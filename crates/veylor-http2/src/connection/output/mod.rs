mod data;
mod headers;
#[cfg(test)]
mod tests;

pub use data::{DataFrameWriter, DataWriteError, QueuedDataFrame};
pub use headers::{HeadersFrameWriter, HeadersWriteError, QueuedHeadersBlock};

use super::H2FrameAction;
use crate::frame::{
    FRAME_HEADER_LEN, FrameHeader, FrameHeaderError, FrameType, GOAWAY_FIXED_LEN, MAX_FRAME_SIZE,
    MAX_STREAM_ID, PING_PAYLOAD_LEN, RST_STREAM_PAYLOAD_LEN, SETTINGS_ACK, SETTINGS_ENTRY_LEN,
    SettingsParameter, WINDOW_UPDATE_PAYLOAD_LEN, encode_frame_header, encode_goaway_payload,
    encode_ping_payload, encode_rst_stream_payload, encode_settings_payload,
    encode_window_update_payload,
};

const MAX_CONTROL_FRAME_LENGTH: usize = FRAME_HEADER_LEN + GOAWAY_FIXED_LEN;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameWriteError {
    PendingFrame,
    OutputTooSmall { required: usize },
    UnsupportedAction,
    InvalidHeader(FrameHeaderError),
    InvalidPayload(crate::frame::FramePayloadError),
    OverAcknowledged,
    Terminal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputProgress {
    Idle,
    Pending { remaining: usize },
    Complete,
}

pub struct ControlFrameWriter<'buffer> {
    buffer: &'buffer mut [u8],
    max_frame_size: u32,
    frame_length: usize,
    acknowledged: usize,
    terminal: bool,
}

impl<'buffer> ControlFrameWriter<'buffer> {
    pub fn new(buffer: &'buffer mut [u8], max_frame_size: u32) -> Self {
        Self {
            buffer,
            max_frame_size,
            frame_length: 0,
            acknowledged: 0,
            terminal: false,
        }
    }

    pub fn queue_action(
        &mut self,
        action: H2FrameAction<'_>,
        goaway_last_stream_id: u32,
    ) -> Result<usize, FrameWriteError> {
        self.ensure_writable()?;
        let (header, required) = match action {
            H2FrameAction::SettingsAckRequired => (
                FrameHeader {
                    payload_length: 0,
                    frame_type: FrameType::Settings,
                    flags: SETTINGS_ACK,
                    stream_id: 0,
                },
                FRAME_HEADER_LEN,
            ),
            H2FrameAction::PingAck { .. } => (
                FrameHeader {
                    payload_length: PING_PAYLOAD_LEN as u32,
                    frame_type: FrameType::Ping,
                    flags: crate::frame::PING_FLAG_ACK,
                    stream_id: 0,
                },
                FRAME_HEADER_LEN + PING_PAYLOAD_LEN,
            ),
            H2FrameAction::SendReset { stream_id, .. } => {
                if stream_id == 0 || stream_id > MAX_STREAM_ID {
                    return Err(FrameWriteError::InvalidPayload(
                        crate::frame::FramePayloadError::InvalidStreamId,
                    ));
                }
                (
                    FrameHeader {
                        payload_length: RST_STREAM_PAYLOAD_LEN as u32,
                        frame_type: FrameType::RstStream,
                        flags: 0,
                        stream_id,
                    },
                    FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN,
                )
            }
            H2FrameAction::ConnectionError { .. } => (
                FrameHeader {
                    payload_length: GOAWAY_FIXED_LEN as u32,
                    frame_type: FrameType::GoAway,
                    flags: 0,
                    stream_id: 0,
                },
                FRAME_HEADER_LEN + GOAWAY_FIXED_LEN,
            ),
            _ => return Err(FrameWriteError::UnsupportedAction),
        };
        self.ensure_capacity(required)?;

        let mut encoded = [0; MAX_CONTROL_FRAME_LENGTH];
        encode_frame_header(
            header,
            &mut encoded[..FRAME_HEADER_LEN],
            self.max_frame_size,
        )
        .map_err(FrameWriteError::InvalidHeader)?;
        match action {
            H2FrameAction::SettingsAckRequired => {}
            H2FrameAction::PingAck { opaque_data } => {
                encode_ping_payload(opaque_data, &mut encoded[FRAME_HEADER_LEN..required])
                    .map_err(FrameWriteError::InvalidPayload)?;
            }
            H2FrameAction::SendReset {
                error_code,
                stream_id: _,
            } => {
                encode_rst_stream_payload(error_code, &mut encoded[FRAME_HEADER_LEN..required])
                    .map_err(FrameWriteError::InvalidPayload)?;
            }
            H2FrameAction::ConnectionError { error_code } => {
                encode_goaway_payload(
                    goaway_last_stream_id,
                    error_code,
                    &[],
                    &mut encoded[FRAME_HEADER_LEN..required],
                )
                .map_err(FrameWriteError::InvalidPayload)?;
            }
            _ => return Err(FrameWriteError::UnsupportedAction),
        }

        self.store(&encoded[..required]);
        Ok(required)
    }

    pub fn queue_window_update(
        &mut self,
        stream_id: u32,
        increment: u32,
    ) -> Result<usize, FrameWriteError> {
        self.ensure_writable()?;
        if stream_id > MAX_STREAM_ID {
            return Err(FrameWriteError::InvalidHeader(
                FrameHeaderError::StreamIdOutOfRange,
            ));
        }
        let required = FRAME_HEADER_LEN + WINDOW_UPDATE_PAYLOAD_LEN;
        self.ensure_capacity(required)?;
        let header = FrameHeader {
            payload_length: WINDOW_UPDATE_PAYLOAD_LEN as u32,
            frame_type: FrameType::WindowUpdate,
            flags: 0,
            stream_id,
        };
        let mut encoded = [0; FRAME_HEADER_LEN + WINDOW_UPDATE_PAYLOAD_LEN];
        encode_frame_header(
            header,
            &mut encoded[..FRAME_HEADER_LEN],
            self.max_frame_size,
        )
        .map_err(FrameWriteError::InvalidHeader)?;
        encode_window_update_payload(increment, &mut encoded[FRAME_HEADER_LEN..])
            .map_err(FrameWriteError::InvalidPayload)?;
        self.store(&encoded);
        Ok(required)
    }

    pub fn queue_settings(
        &mut self,
        parameters: &[SettingsParameter],
    ) -> Result<usize, FrameWriteError> {
        self.ensure_writable()?;
        let payload_length = parameters.len().checked_mul(SETTINGS_ENTRY_LEN).ok_or(
            FrameWriteError::InvalidPayload(crate::frame::FramePayloadError::InvalidPayloadLength),
        )?;
        let payload_length = u32::try_from(payload_length).map_err(|_| {
            FrameWriteError::InvalidPayload(crate::frame::FramePayloadError::InvalidPayloadLength)
        })?;
        let required = FRAME_HEADER_LEN
            .checked_add(usize::try_from(payload_length).map_err(|_| {
                FrameWriteError::InvalidPayload(
                    crate::frame::FramePayloadError::InvalidPayloadLength,
                )
            })?)
            .ok_or(FrameWriteError::InvalidPayload(
                crate::frame::FramePayloadError::InvalidPayloadLength,
            ))?;
        self.ensure_capacity(required)?;
        if payload_length > MAX_FRAME_SIZE || payload_length > self.max_frame_size {
            return Err(FrameWriteError::InvalidHeader(
                FrameHeaderError::FrameTooLarge,
            ));
        }
        let header = FrameHeader {
            payload_length,
            frame_type: FrameType::Settings,
            flags: 0,
            stream_id: 0,
        };
        let mut encoded_header = [0; FRAME_HEADER_LEN];
        encode_frame_header(header, &mut encoded_header, self.max_frame_size)
            .map_err(FrameWriteError::InvalidHeader)?;
        let payload = &mut self.buffer[FRAME_HEADER_LEN..required];
        let written = encode_settings_payload(parameters, payload)
            .map_err(FrameWriteError::InvalidPayload)?;
        if written != payload.len() {
            return Err(FrameWriteError::InvalidPayload(
                crate::frame::FramePayloadError::InvalidPayloadLength,
            ));
        }
        self.buffer[..FRAME_HEADER_LEN].copy_from_slice(&encoded_header);
        self.mark_queued(required);
        Ok(required)
    }

    pub fn pending_bytes(&self) -> &[u8] {
        &self.buffer[self.acknowledged..self.frame_length]
    }

    pub const fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn acknowledge(&mut self, byte_count: usize) -> Result<OutputProgress, FrameWriteError> {
        if self.terminal {
            return Err(FrameWriteError::Terminal);
        }
        let pending = self.frame_length - self.acknowledged;
        if byte_count > pending {
            self.terminal = true;
            self.frame_length = 0;
            self.acknowledged = 0;
            return Err(FrameWriteError::OverAcknowledged);
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

    fn ensure_writable(&self) -> Result<(), FrameWriteError> {
        if self.terminal {
            return Err(FrameWriteError::Terminal);
        }
        if self.frame_length != self.acknowledged {
            return Err(FrameWriteError::PendingFrame);
        }
        Ok(())
    }

    fn ensure_capacity(&self, required: usize) -> Result<(), FrameWriteError> {
        if self.buffer.len() < required {
            return Err(FrameWriteError::OutputTooSmall { required });
        }
        Ok(())
    }

    fn store(&mut self, frame: &[u8]) {
        self.buffer[..frame.len()].copy_from_slice(frame);
        self.mark_queued(frame.len());
    }

    fn mark_queued(&mut self, frame_length: usize) {
        self.acknowledged = 0;
        self.frame_length = frame_length;
    }
}
