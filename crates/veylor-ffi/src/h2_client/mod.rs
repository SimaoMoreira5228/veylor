#[allow(unsafe_code)]
mod api;
#[allow(unsafe_code)]
mod feed;
#[allow(unsafe_code)]
mod request;
#[allow(unsafe_code)]
mod response;
#[cfg(test)]
#[allow(unsafe_code)]
mod roundtrip;
#[cfg(test)]
#[allow(unsafe_code)]
mod tests;

use veylor_http2::{ClientResponseHeaders, H2ClientDriver, H2FrameAction, HpackHeader};

pub(crate) const STREAM_SLOTS: usize = 32;
pub(crate) const HEADER_BLOCK: usize = 4_096;
pub(crate) const HPACK_ENTRIES: usize = 32;
pub(crate) const HPACK_SCRATCH: usize = 4_096;
pub(crate) const SETTINGS_ACKS: usize = 8;
pub(crate) const HEADER_BLOCK_SCRATCH: usize = 4_096;
pub(crate) const MAX_RESPONSES: usize = 32;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum PendingKind {
    #[default]
    None,
    Settings,
    Headers,
    Data,
    Control,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PendingWrite {
    pub(crate) kind: PendingKind,
    pub(crate) remaining: usize,
    pub(crate) setting_count: usize,
    pub(crate) settings: [(u16, u32); crate::abi::VYL_H2_MAX_SETTINGS],
}

pub struct VylH2Client {
    pub(crate) slots: [veylor_http2::StreamSlot; STREAM_SLOTS],
    pub(crate) header_block: [u8; HEADER_BLOCK],
    pub(crate) decoder_entries: [veylor_http2::HpackEntrySlot; HPACK_ENTRIES],
    pub(crate) decoder_scratch: [u8; HPACK_SCRATCH],
    pub(crate) encoder_entries: [veylor_http2::HpackEntrySlot; HPACK_ENTRIES],
    pub(crate) encoder_scratch: [u8; HPACK_SCRATCH],
    pub(crate) settings_ack_limits: [u32; SETTINGS_ACKS],
    pub(crate) streams: Option<veylor_http2::StreamTable<'static>>,
    pub(crate) decoder: Option<veylor_http2::HpackTable<'static>>,
    pub(crate) encoder: Option<veylor_http2::HpackTable<'static>>,
    pub(crate) driver: Option<H2ClientDriver<'static, 'static, 'static, 'static>>,
    pub(crate) feed_actions: [Option<H2FrameAction<'static>>; crate::abi::VYL_H2_MAX_ACTIONS],
    pub(crate) feed_headers: [HpackHeader; crate::abi::VYL_H2_MAX_FIELDS],
    pub(crate) feed_action_count: usize,
    pub(crate) feed_header_count: usize,
    pub(crate) feed_pending: bool,
    pub(crate) feed_consumed: usize,
    pub(crate) feed_error: Option<u32>,
    pub(crate) feed_failed: bool,
    pub(crate) block: [u8; HEADER_BLOCK_SCRATCH],
    pub(crate) pending: PendingWrite,
    pub(crate) responses: Vec<(u32, ClientResponseHeaders)>,
    pub(crate) cancelled: bool,
}

pub use api::{
    vyl_h2_client_acknowledge, vyl_h2_client_cancel, vyl_h2_client_connection_preface,
    vyl_h2_client_destroy, vyl_h2_client_new, vyl_h2_client_send_ping_ack,
    vyl_h2_client_send_settings, vyl_h2_client_send_settings_ack, vyl_h2_client_send_window_update,
};
pub use feed::vyl_h2_client_poll_feed;
pub use request::{
    vyl_h2_client_cancel_stream, vyl_h2_client_goaway, vyl_h2_client_send_data,
    vyl_h2_client_send_reset, vyl_h2_client_start_request,
};
pub use response::{vyl_h2_client_account_data, vyl_h2_client_classify_headers};
