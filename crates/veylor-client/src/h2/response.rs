use veylor_http2::{ClientResponsePhase, HpackHeader, ResponseSectionKind};

use super::{H2ClientConnection, H2ClientError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseClass {
    pub kind: ResponseSectionKind,
    pub status: u16,
    pub phase: ClientResponsePhase,
    pub content_length: Option<u64>,
    pub end_stream: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccountPhase {
    pub phase: ClientResponsePhase,
}

impl<'c, 'b> H2ClientConnection<'c, 'b> {
    pub fn classify_headers(
        &mut self,
        stream_id: u32,
        headers: &[HpackHeader],
        end_stream: bool,
    ) -> Result<ResponseClass, H2ClientError> {
        self.ensure_live()?;
        let response_len = self.response_len;
        let position = self.responses[..response_len]
            .iter()
            .position(|slot| matches!(slot, Some((id, _)) if *id == stream_id));
        let Some(position) = position else {
            return Err(H2ClientError::InvalidArgument);
        };
        let driver = self.driver.driver();
        let Some((_, response)) = self.responses[position].as_mut() else {
            return Err(H2ClientError::InvalidArgument);
        };
        let section = response
            .classify(driver, headers, end_stream)
            .map_err(|_| H2ClientError::ProtocolError)?;
        Ok(ResponseClass {
            kind: section.kind,
            status: section.status.unwrap_or(0),
            phase: response.phase(),
            content_length: response.content_length(),
            end_stream,
        })
    }

    pub fn account_data(
        &mut self,
        stream_id: u32,
        data_length: usize,
        end_stream: bool,
    ) -> Result<AccountPhase, H2ClientError> {
        self.ensure_live()?;
        let response = match self.find_response(stream_id) {
            Some(response) => response,
            None => return Err(H2ClientError::InvalidArgument),
        };
        let phase = response
            .data_received(data_length, end_stream)
            .map_err(|_| H2ClientError::ProtocolError)?;
        Ok(AccountPhase { phase })
    }
}
