use veylor_http2::{ErrorCode, HpackFieldToEncode};

use super::response::map_server_error;
use super::{H2ServerConnection, H2ServerError, MAX_FIELDS, PendingWrite, PendingWriteKind};
use crate::h2::WriteOut;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedRequest {
    pub method: Option<usize>,
    pub scheme: Option<usize>,
    pub authority: Option<usize>,
    pub path: Option<usize>,
    pub content_length: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataOut {
    pub consumed: usize,
    pub written: usize,
    pub end_stream: bool,
    pub required: usize,
}

impl<'c, 'b> H2ServerConnection<'c, 'b> {
    pub fn accept_request(
        &self,
        stream_id: u32,
        headers: &[veylor_http2::HpackHeader],
    ) -> Result<ValidatedRequest, H2ServerError> {
        self.ensure_live()?;
        let validated = self
            .driver()
            .accept_request(stream_id, headers)
            .map_err(|error| {
                let mut required = 0;
                map_server_error(error, &mut required)
            })?;
        let mut out = ValidatedRequest {
            method: None,
            scheme: None,
            authority: None,
            path: None,
            content_length: validated.content_length,
        };
        for (index, header) in headers.iter().enumerate() {
            let Some((name, value)) = self.resolve_header(*header) else {
                return Err(H2ServerError::InternalError);
            };
            if name == b":method" && value == validated.method {
                out.method = Some(index);
            } else if name == b":scheme" && Some(value) == validated.scheme {
                out.scheme = Some(index);
            } else if name == b":authority" && Some(value) == validated.authority {
                out.authority = Some(index);
            } else if name == b":path" && Some(value) == validated.path {
                out.path = Some(index);
            }
        }
        if out.method.is_none() {
            return Err(H2ServerError::InternalError);
        }
        Ok(out)
    }

    pub fn send_response(
        &mut self,
        stream_id: u32,
        fields: &[HpackFieldToEncode<'_>],
        end_stream: bool,
        output: &mut [u8],
    ) -> Result<WriteOut, H2ServerError> {
        if fields.len() > MAX_FIELDS {
            return Err(H2ServerError::Capacity);
        }
        self.ensure_writable()?;
        let mut required = 0;
        let sent = self
            .driver
            .send_response(stream_id, fields, &mut self.block, output, end_stream)
            .map_err(|error| map_server_error(error, &mut required))?;
        self.pending = PendingWrite {
            kind: PendingWriteKind::Headers,
            remaining: sent.frame.output_length,
            ..PendingWrite::default()
        };
        Ok(WriteOut {
            written: sent.frame.output_length,
            required: sent.frame.output_length,
        })
    }

    pub fn send_data(
        &mut self,
        stream_id: u32,
        input: &[u8],
        end_stream: bool,
        output: &mut [u8],
    ) -> Result<DataOut, H2ServerError> {
        self.ensure_writable()?;
        let mut required = 0;
        let sent = self
            .driver_mut()
            .send_data(stream_id, input, end_stream, output)
            .map_err(|error| map_server_error(error, &mut required))?;
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

    pub fn refuse(&mut self, stream_id: u32, output: &mut [u8]) -> Result<WriteOut, H2ServerError> {
        self.ensure_writable()?;
        let mut required = 0;
        let writer = self
            .driver_mut()
            .refuse(stream_id, output)
            .map_err(|error| map_server_error(error, &mut required))?;
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

    pub fn send_reset(
        &mut self,
        stream_id: u32,
        error_code: ErrorCode,
        output: &mut [u8],
    ) -> Result<WriteOut, H2ServerError> {
        if let ErrorCode::Unknown(value) = error_code
            && value <= 13
        {
            return Err(H2ServerError::InvalidArgument);
        }
        self.ensure_writable()?;
        let mut required = 0;
        let writer = self
            .driver_mut()
            .rst_stream(stream_id, error_code, output)
            .map_err(|error| map_server_error(error, &mut required))?;
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

    pub fn goaway_drain(
        &mut self,
        last_stream_id: u32,
        output: &mut [u8],
    ) -> Result<WriteOut, H2ServerError> {
        self.ensure_writable()?;
        let mut required = 0;
        let writer = self
            .driver_mut()
            .goaway_drain(last_stream_id, output)
            .map_err(|error| map_server_error(error, &mut required))?;
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
