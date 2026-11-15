mod api;
mod feed;
mod request;
mod response;
mod state;
#[cfg(test)]
mod tests;

pub use api::WriteOut;
pub use feed::{BatchStatus, FeedBatch, FeedScratch};
pub use request::{DataOut, RequestOut};
pub use response::{AccountPhase, ResponseClass};
pub use state::GoAwayInfo;
pub use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, ClientResponsePhase, ErrorCode, H2FrameAction, HpackEntrySlot,
    HpackFieldToEncode, HpackHeader, HpackRepresentation, HpackTable, ResponseSectionKind,
    SETTINGS_INITIAL_WINDOW_SIZE, SettingsParameter, StreamSlot, StreamTable,
};

pub const MIN_STREAM_SLOTS: usize = 3;
pub const MAX_STREAM_SLOTS: usize = 1_024;
pub const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = 16 * 1024;
pub const DEFAULT_MAX_FIELDS: usize = 128;
pub const DEFAULT_REQUEST_BLOCK: usize = 16 * 1024;
pub const HPACK_ENTRIES: usize = 128;
pub const SETTINGS_ACKS: usize = 8;
pub const MAX_ACTIONS: usize = 16;
pub const MAX_SETTINGS: usize = 8;

const HPACK_TABLE_SIZE: usize = 4_096;

// NOTE: sizing for the caller-owned tables, derived from one header-list limit so the pieces
// cannot disagree: the wire block is capped at the list limit (HPACK literals never expand past
// it), and the decoder scratch holds the block's decoded strings plus the live dynamic table
// with one more table of headroom for entries inserted before older ones are reclaimed.
pub const fn header_block_len(max_header_list_size: u32) -> usize {
    max_header_list_size as usize
}

pub const fn decoder_scratch_len(max_header_list_size: u32) -> usize {
    (max_header_list_size as usize) + 2 * HPACK_TABLE_SIZE
}

pub const fn encoder_scratch_len(request_block: usize) -> usize {
    request_block / 2 + HPACK_TABLE_SIZE
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct H2ClientLimits {
    pub max_header_list_size: u32,
    pub max_fields: usize,
    pub request_block: usize,
}

impl Default for H2ClientLimits {
    fn default() -> Self {
        Self {
            max_header_list_size: DEFAULT_MAX_HEADER_LIST_SIZE,
            max_fields: DEFAULT_MAX_FIELDS,
            request_block: DEFAULT_REQUEST_BLOCK,
        }
    }
}

pub(crate) const MAX_PENDING_SETTINGS: usize = MAX_SETTINGS + 2;

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
    limits: H2ClientLimits,
    response_slots: usize,
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
        let response_slots = slots.len().clamp(MIN_STREAM_SLOTS, MAX_STREAM_SLOTS);
        let block_cap = u32::try_from(header_block.len()).unwrap_or(u32::MAX);
        Self {
            streams: StreamTable::new(slots, veylor_http2::H2Role::Client, header_block, block_cap),
            decoder: HpackTable::new(decoder_entries, decoder_scratch, HPACK_TABLE_SIZE as u32),
            encoder: HpackTable::new(encoder_entries, encoder_scratch, HPACK_TABLE_SIZE as u32),
            ack_limits,
            limits: H2ClientLimits::default(),
            response_slots,
        }
    }

    pub fn with_limits(mut self, limits: H2ClientLimits) -> Self {
        self.limits = limits;
        self
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
        Ok(H2ClientConnection::with_driver(
            driver,
            self.limits,
            self.response_slots,
        ))
    }
}

type ResponseSlot = Option<(u32, veylor_http2::ClientResponseHeaders)>;

pub struct H2ClientConnection<'c, 'b> {
    driver: veylor_http2::H2ClientDriver<'c, 'b, 'b, 'b>,
    pending: PendingWrite,
    limits: H2ClientLimits,
    responses: Box<[ResponseSlot]>,
    response_len: usize,
    block: Box<[u8]>,
    cancelled: bool,
    feed_action_count: usize,
    feed_header_count: usize,
    feed_consumed: usize,
    feed_error: Option<ErrorCode>,
    feed_failed: bool,
    feed_pending: bool,
    feed_resets: [u32; MAX_ACTIONS],
    feed_reset_count: usize,
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
    pub(crate) settings: [(u16, u32); MAX_PENDING_SETTINGS],
}

impl Default for PendingWrite {
    fn default() -> Self {
        Self {
            kind: PendingWriteKind::None,
            remaining: 0,
            setting_count: 0,
            settings: [(0, 0); MAX_PENDING_SETTINGS],
        }
    }
}

impl<'c, 'b> H2ClientConnection<'c, 'b> {
    fn with_driver(
        driver: veylor_http2::H2ClientDriver<'c, 'b, 'b, 'b>,
        limits: H2ClientLimits,
        response_slots: usize,
    ) -> Self {
        Self {
            driver,
            pending: PendingWrite::default(),
            limits,
            responses: std::iter::repeat_with(|| None)
                .take(response_slots)
                .collect(),
            response_len: 0,
            block: vec![0; limits.request_block].into_boxed_slice(),
            cancelled: false,
            feed_action_count: 0,
            feed_header_count: 0,
            feed_consumed: 0,
            feed_error: None,
            feed_failed: false,
            feed_pending: false,
            feed_resets: [0; MAX_ACTIONS],
            feed_reset_count: 0,
        }
    }

    pub const fn limits(&self) -> H2ClientLimits {
        self.limits
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
        if self.response_len >= self.responses.len() {
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
