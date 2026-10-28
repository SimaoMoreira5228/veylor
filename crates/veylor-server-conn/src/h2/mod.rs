mod api;
mod feed;
mod request;
mod response;
#[cfg(test)]
mod tests;

// NOTE: framing mechanism, not response shaping: this module exposes stream primitives
// (send_response/send_data/refuse/reset/goaway_drain) while Date/length/trailer/file
// assembly stays the caller's job — the managed server shapes those in its own routes
// layer and host-driven callers assemble them from the same primitives. Closing that
// gap here would duplicate the shaping layer instead of sharing it.

pub use api::WriteOut;
pub use feed::{BatchStatus, FeedBatch, FeedScratch, error_code_value};
pub use request::{DataOut, ValidatedRequest};
pub use veylor_http2::{
    ErrorCode, H2FrameAction, HpackEntrySlot, HpackFieldToEncode, HpackHeader, HpackRepresentation,
    HpackTable, SettingsParameter, StreamSlot, StreamTable, ValidatedServerRequest,
};

pub const STREAM_SLOTS: usize = 32;
pub const HEADER_BLOCK: usize = 4_096;
pub const HPACK_ENTRIES: usize = 32;
pub const HPACK_SCRATCH: usize = 4_096;
pub const SETTINGS_ACKS: usize = 8;
pub const HEADER_BLOCK_SCRATCH: usize = 4_096;
pub const MAX_ACTIONS: usize = 16;
pub const MAX_FIELDS: usize = 64;
pub const MAX_SETTINGS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum H2ServerError {
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























































































pub struct H2ServerTables<'a> {
    streams: StreamTable<'a>,
    decoder: HpackTable<'a>,
    encoder: HpackTable<'a>,
    ack_limits: &'a mut [u32],
}

impl<'a> H2ServerTables<'a> {
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
                veylor_http2::H2Role::Server,
                header_block,
                HEADER_BLOCK as u32,
            ),
            decoder: HpackTable::new(decoder_entries, decoder_scratch, 4_096),
            encoder: HpackTable::new(encoder_entries, encoder_scratch, 4_096),
            ack_limits,
        }
    }

    pub fn connect<'c>(&'c mut self) -> Result<H2ServerConnection<'c, 'a>, H2ServerError>
    where
        'a: 'c,
    {
        let driver = veylor_http2::H2ServerDriver::new(
            &mut self.streams,
            &mut self.decoder,
            &mut self.encoder,
            &mut *self.ack_limits,
        )
        .map_err(|_| H2ServerError::InternalError)?;
        Ok(H2ServerConnection::with_driver(driver))
    }
}

pub struct H2ServerConnection<'c, 'b> {
    driver: veylor_http2::H2ServerDriver<'c, 'b, 'b, 'b>,
    pending: PendingWrite,
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

impl<'c, 'b> H2ServerConnection<'c, 'b> {
    fn with_driver(driver: veylor_http2::H2ServerDriver<'c, 'b, 'b, 'b>) -> Self {
        Self {
            driver,
            pending: PendingWrite::default(),
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
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    pub fn has_pending_write(&self) -> bool {
        self.pending.kind != PendingWriteKind::None
    }

    pub fn check_writable(&self) -> Result<(), H2ServerError> {
        self.ensure_writable()
    }

    pub(crate) fn ensure_live(&self) -> Result<(), H2ServerError> {
        if self.cancelled {
            return Err(H2ServerError::Cancelled);
        }
        Ok(())
    }

    pub(crate) fn ensure_writable(&self) -> Result<(), H2ServerError> {
        self.ensure_live()?;
        if self.pending.kind != PendingWriteKind::None {
            return Err(H2ServerError::WouldBlock);
        }
        Ok(())
    }

    pub fn resolve_header(&self, header: HpackHeader) -> Option<(&[u8], &[u8])> {
        self.driver.driver().resolve_header(header)
    }

    pub(crate) fn driver(&self) -> &veylor_http2::H2ServerDriver<'c, 'b, 'b, 'b> {
        &self.driver
    }

    pub(crate) fn driver_mut(&mut self) -> &mut veylor_http2::H2ServerDriver<'c, 'b, 'b, 'b> {
        &mut self.driver
    }
}
