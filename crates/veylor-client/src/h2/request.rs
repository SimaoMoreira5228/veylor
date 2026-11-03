use veylor_http2::{
    ClientError, ControlFrameWriter, DataWriteError, ErrorCode, FRAME_HEADER_LEN, FrameWriteError,
    HeadersWriteError, HpackFieldToEncode, RST_STREAM_PAYLOAD_LEN, StreamState,
};

use super::{H2ClientConnection, H2ClientError, PendingWrite, PendingWriteKind, WriteOut};
use crate::h2::{MAX_FIELDS, MAX_RESPONSES};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestOut {
    pub stream_id: u32,
    pub written: usize,
    pub required: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataOut {
    pub consumed: usize,
    pub written: usize,
    pub end_stream: bool,
    pub required: usize,
}

fn map_client_error(error: ClientError, required: &mut usize) -> H2ClientError {
    match error {
        ClientError::OutputTooSmall { required: need } => {
            *required = need;
            H2ClientError::BufferTooSmall { required: need }
        }
        ClientError::Headers(error) => map_headers_error(error, required),
        ClientError::Data(error) => map_data_error(error, required),
        ClientError::Control(error) => match error {
            FrameWriteError::OutputTooSmall { required: need } => {
                *required = need;
                H2ClientError::BufferTooSmall { required: need }
            }
            FrameWriteError::InvalidHeader(_) | FrameWriteError::InvalidPayload(_) => {
                H2ClientError::InvalidArgument
            }
            FrameWriteError::PendingFrame => H2ClientError::WouldBlock,
            FrameWriteError::OverAcknowledged => H2ClientError::WriteOverrun,
            FrameWriteError::Terminal => H2ClientError::BadState,
            FrameWriteError::UnsupportedAction => H2ClientError::InternalError,
        },
        ClientError::Request(_) => H2ClientError::InvalidArgument,
        ClientError::SettingsNotSent | ClientError::NewStreamsClosed => H2ClientError::BadState,
        ClientError::ConcurrencyExhausted | ClientError::MaxStreamIdExhausted => {
            H2ClientError::Capacity
        }
        ClientError::UnknownStream => H2ClientError::InvalidArgument,
        ClientError::StreamClosed => H2ClientError::BadState,
        ClientError::WrongRole | ClientError::Stream(_) | ClientError::Flow(_) => {
            H2ClientError::InternalError
        }
    }
}

fn map_headers_error(error: HeadersWriteError, required: &mut usize) -> H2ClientError {
    match error {
        HeadersWriteError::OutputTooSmall { required: need } => {
            *required = need;
            H2ClientError::BufferTooSmall { required: need }
        }
        HeadersWriteError::SettingsNotSent => H2ClientError::BadState,
        HeadersWriteError::PendingFrame => H2ClientError::WouldBlock,
        HeadersWriteError::Terminal | HeadersWriteError::OverAcknowledged => {
            H2ClientError::WriteOverrun
        }
        HeadersWriteError::StreamRejected(_) => H2ClientError::ProtocolError,
        HeadersWriteError::InvalidStreamId
        | HeadersWriteError::LengthOverflow
        | HeadersWriteError::InvalidHeader(_) => H2ClientError::InternalError,
    }
}

fn map_data_error(error: DataWriteError, required: &mut usize) -> H2ClientError {
    match error {
        DataWriteError::OutputTooSmall { required: need } => {
            *required = need;
            H2ClientError::BufferTooSmall { required: need }
        }
        DataWriteError::InvalidStreamId | DataWriteError::UnknownStream => {
            H2ClientError::InvalidArgument
        }
        DataWriteError::StreamClosed => H2ClientError::BadState,
        DataWriteError::SettingsNotSent => H2ClientError::BadState,
        DataWriteError::NoSendCredit | DataWriteError::PendingFrame => H2ClientError::WouldBlock,
        DataWriteError::OverAcknowledged | DataWriteError::Terminal => H2ClientError::WriteOverrun,
        DataWriteError::InvalidHeader(_) => H2ClientError::InternalError,
    }
}

impl<'c, 'b> H2ClientConnection<'c, 'b> {
    pub fn check_start_request(&self) -> Result<(), H2ClientError> {
        self.ensure_writable()?;
        if self.response_len >= MAX_RESPONSES {
            return Err(H2ClientError::Capacity);
        }
        Ok(())
    }

    pub fn start_request(
        &mut self,
        fields: &[HpackFieldToEncode<'_>],
        output: &mut [u8],
        end_stream: bool,
    ) -> Result<RequestOut, H2ClientError> {
        if fields.len() > MAX_FIELDS {
            return Err(H2ClientError::Capacity);
        }
        self.ensure_writable()?;
        if self.response_len >= MAX_RESPONSES {
            return Err(H2ClientError::Capacity);
        }
        let mut method: &[u8] = &[];
        for field in fields {
            if field.name == b":method" {
                method = field.value;
            }
        }
        let mut required = 0;
        let started = self
            .driver
            .start_request(fields, &mut self.block, output, end_stream)
            .map_err(|error| map_client_error(error, &mut required))?;
        self.pending = PendingWrite {
            kind: PendingWriteKind::Headers,
            remaining: started.frame.output_length,
            ..PendingWrite::default()
        };
        self.push_response(
            started.stream_id,
            veylor_http2::ClientResponseHeaders::new_with_method(method),
        )?;
        Ok(RequestOut {
            stream_id: started.stream_id,
            written: started.frame.output_length,
            required: started.frame.output_length,
        })
    }

    pub fn send_data(
        &mut self,
        stream_id: u32,
        input: &[u8],
        end_stream: bool,
        output: &mut [u8],
    ) -> Result<DataOut, H2ClientError> {
        self.ensure_writable()?;
        let mut required = 0;
        let sent = self
            .driver
            .send_data(stream_id, input, end_stream, output)
            .map_err(|error| map_client_error(error, &mut required))?;
        self.pending = PendingWrite {
            kind: PendingWriteKind::Data,
            remaining: sent.frame.frame_length,
            ..PendingWrite::default()
        };
        Ok(DataOut {
            consumed: sent.frame.payload_length,
            written: sent.frame.frame_length,
            end_stream: sent.frame.end_stream,
            required: sent.frame.frame_length,
        })
    }

    pub fn cancel_stream(
        &mut self,
        stream_id: u32,
        output: &mut [u8],
    ) -> Result<WriteOut, H2ClientError> {
        self.send_reset(stream_id, ErrorCode::Cancel, output)
    }

    pub fn send_reset(
        &mut self,
        stream_id: u32,
        error_code: ErrorCode,
        output: &mut [u8],
    ) -> Result<WriteOut, H2ClientError> {
        if let ErrorCode::Unknown(value) = error_code
            && value <= 13
        {
            return Err(H2ClientError::InvalidArgument);
        }
        self.ensure_writable()?;
        if stream_id == 0 || stream_id > veylor_http2::MAX_STREAM_ID {
            return Err(H2ClientError::InvalidArgument);
        }
        match self.driver.driver().streams().find(stream_id) {
            Some(stream) if stream.state != StreamState::Closed => {}
            Some(_) => return Err(H2ClientError::BadState),
            None => return Err(H2ClientError::InvalidArgument),
        }
        if output.len() < FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN {
            return Err(H2ClientError::BufferTooSmall {
                required: FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN,
            });
        }
        let max_frame_size = self
            .driver
            .driver()
            .streams()
            .remote_settings()
            .max_frame_size;
        self.driver
            .driver_mut()
            .streams_mut()
            .close_stream(stream_id);
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        let written = writer
            .queue_action(
                veylor_http2::H2FrameAction::SendReset {
                    stream_id,
                    error_code,
                },
                0,
            )
            .map_err(|error| match error {
                FrameWriteError::OutputTooSmall { required: need } => {
                    H2ClientError::BufferTooSmall { required: need }
                }
                FrameWriteError::InvalidHeader(_) | FrameWriteError::InvalidPayload(_) => {
                    H2ClientError::InvalidArgument
                }
                FrameWriteError::PendingFrame => H2ClientError::WouldBlock,
                FrameWriteError::OverAcknowledged => H2ClientError::WriteOverrun,
                FrameWriteError::Terminal => H2ClientError::BadState,
                FrameWriteError::UnsupportedAction => H2ClientError::InternalError,
            })?;
        self.pending = PendingWrite {
            kind: PendingWriteKind::Control,
            remaining: written,
            ..PendingWrite::default()
        };
        Ok(WriteOut {
            written,
            required: written,
        })
    }

    pub fn goaway(&mut self, output: &mut [u8]) -> Result<WriteOut, H2ClientError> {
        self.ensure_writable()?;
        let mut required = 0;
        let writer = self
            .driver
            .goaway(output)
            .map_err(|error| map_client_error(error, &mut required))?;
        let written = writer.pending_bytes().len();
        self.pending = PendingWrite {
            kind: PendingWriteKind::Control,
            remaining: written,
            ..PendingWrite::default()
        };
        Ok(WriteOut {
            written,
            required: written,
        })
    }
}
