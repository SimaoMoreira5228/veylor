mod api;
mod feed;
mod request;
mod response;
#[cfg(test)]
mod tests;

pub use api::WriteOut;
pub use feed::{BatchStatus, FeedBatch, FeedScratch, error_code_value};
pub use request::{DataOut, RequestOut};
pub use response::{AccountPhase, ResponseClass};
pub use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, ClientResponsePhase, ErrorCode, H2FrameAction, HpackEntrySlot,
    HpackFieldToEncode, HpackHeader, HpackRepresentation, HpackTable, ResponseSectionKind,
    SettingsParameter, StreamSlot, StreamTable,
};

pub const STREAM_SLOTS: usize = 32;
pub const HEADER_BLOCK: usize = 4_096;
pub const HPACK_ENTRIES: usize = 32;
pub const HPACK_SCRATCH: usize = 4_096;
pub const SETTINGS_ACKS: usize = 8;
pub const HEADER_BLOCK_SCRATCH: usize = 4_096;
pub const MAX_RESPONSES: usize = 32;
pub const MAX_ACTIONS: usize = 16;
pub const MAX_FIELDS: usize = 64;
pub const MAX_SETTINGS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum H2ClientError {
    InvalidArgument,
    ProtocolError,
    BufferTooSmall { required: usize },
    InternalError,
    Capacity,
    WouldBlock,
    BadState,
    WriteOverrun,
    Cancelled,
}









































































































pub struct H2ClientTables<'a> {
    streams: StreamTable<'a>,
    decoder: HpackTable<'a>,
    encoder: HpackTable<'a>,
    ack_limits: &'a mut [u32],
}

impl<'a> H2ClientTables<'a> {
    pub fn new(
        slots: &'a mut [StreamSlot],
        header_block: &'a mut [u8],
        decoder_entries: &'a mut [HpackEntrySlot],
        decoder_scratch: &'a mut [u8],
        encoder_entries: &'a mut [HpackEntrySlot],
        encoder_scratch: &'a mut [u8],
        ack_limits: &'a mut [u32],
    ) -> Self {
        Self {
            streams: StreamTable::new(
                slots,
                veylor_http2::H2Role::Client,
                header_block,
                HEADER_BLOCK as u32,
            ),
            decoder: HpackTable::new(decoder_entries, decoder_scratch, 4_096),
            encoder: HpackTable::new(encoder_entries, encoder_scratch, 4_096),
            ack_limits,
        }
    }

    pub fn connect<'c>(&'c mut self) -> Result<H2ClientConnection<'c, 'a>, H2ClientError>
    where
        'a: 'c,
    {
        let driver = veylor_http2::H2ClientDriver::new(
            &mut self.streams,
            &mut self.decoder,
            &mut self.encoder,
            &mut *self.ack_limits,
        )
        .map_err(|_| H2ClientError::InternalError)?;
        Ok(H2ClientConnection::with_driver(driver))
    }
}

pub struct H2ClientConnection<'c, 'b> {
    driver: veylor_http2::H2ClientDriver<'c, 'b, 'b, 'b>,
    pending: PendingWrite,
    responses: [Option<(u32, veylor_http2::ClientResponseHeaders)>; MAX_RESPONSES],
    response_len: usize,
    block: [u8; HEADER_BLOCK_SCRATCH],
    cancelled: bool,
    feed_action_count: usize,
    feed_header_count: usize,
    feed_consumed: usize,
    feed_error: Option<ErrorCode>,
    feed_failed: bool,
    feed_pending: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum PendingWriteKind {
    #[default]
    None,
    Settings,
    Headers,
    Data,
    Control,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingWrite {
    pub(crate) kind: PendingWriteKind,
    pub(crate) remaining: usize,
    pub(crate) setting_count: usize,
    pub(crate) settings: [(u16, u32); MAX_SETTINGS],
}

impl Default for PendingWrite {
    fn default() -> Self {
        Self {
            kind: PendingWriteKind::None,
            remaining: 0,
            setting_count: 0,
            settings: [(0, 0); MAX_SETTINGS],
        }
    }
}

impl<'c, 'b> H2ClientConnection<'c, 'b> {
    fn with_driver(driver: veylor_http2::H2ClientDriver<'c, 'b, 'b, 'b>) -> Self {
        Self {
            driver,
            pending: PendingWrite::default(),
            responses: std::array::from_fn(|_| None),
            response_len: 0,
            block: [0; HEADER_BLOCK_SCRATCH],
            cancelled: false,
            feed_action_count: 0,
            feed_header_count: 0,
            feed_consumed: 0,
            feed_error: None,
            feed_failed: false,
            feed_pending: false,
        }
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
        self.pending = PendingWrite::default();
        self.responses.fill_with(|| None);
        self.response_len = 0;
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    pub(crate) fn ensure_live(&self) -> Result<(), H2ClientError> {
        if self.cancelled {
            return Err(H2ClientError::Cancelled);
        }
        Ok(())
    }

    pub(crate) fn ensure_writable(&self) -> Result<(), H2ClientError> {
        self.ensure_live()?;
        if self.pending.kind != PendingWriteKind::None {
            return Err(H2ClientError::WouldBlock);
        }
        if self.driver.driver().failed().is_some() {
            return Err(H2ClientError::BadState);
        }
        Ok(())
    }

    pub fn resolve_header(&self, header: HpackHeader) -> Option<(&[u8], &[u8])> {
        self.driver.driver().resolve_header(header)
    }

    pub(crate) fn push_response(
        &mut self,
        stream_id: u32,
        response: veylor_http2::ClientResponseHeaders,
    ) -> Result<(), H2ClientError> {
        if self.response_len >= MAX_RESPONSES {
            return Err(H2ClientError::Capacity);
        }
        self.responses[self.response_len] = Some((stream_id, response));
        self.response_len += 1;
        Ok(())
    }

    pub(crate) fn find_response(
        &mut self,
        stream_id: u32,
    ) -> Option<&mut veylor_http2::ClientResponseHeaders> {
        self.responses[..self.response_len]
            .iter_mut()
            .find_map(|slot| match slot {
                Some((id, response)) if *id == stream_id => Some(response),
                _ => None,
            })
    }
}
