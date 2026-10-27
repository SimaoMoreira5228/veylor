#[allow(unsafe_code)]
mod api;
#[allow(unsafe_code)]
mod feed;
#[allow(unsafe_code)]
mod request;
#[cfg(test)]
#[allow(unsafe_code)]
mod tests;

use veylor_server_conn::h2::{
    FeedScratch, H2ServerConnection, H2ServerError, H2ServerTables, HEADER_BLOCK, HPACK_ENTRIES,
    HPACK_SCRATCH, HpackEntrySlot, SETTINGS_ACKS, STREAM_SLOTS, StreamSlot,
};

const _: () = assert!(veylor_server_conn::h2::MAX_ACTIONS == crate::abi::VYL_H2_MAX_ACTIONS);
const _: () = assert!(veylor_server_conn::h2::MAX_FIELDS == crate::abi::VYL_H2_MAX_FIELDS);
const _: () = assert!(veylor_server_conn::h2::MAX_SETTINGS == crate::abi::VYL_H2_MAX_SETTINGS);

pub struct VylH2Server {
    pub(crate) slots: [StreamSlot; STREAM_SLOTS],
    pub(crate) header_block: [u8; HEADER_BLOCK],
    pub(crate) decoder_entries: [HpackEntrySlot; HPACK_ENTRIES],
    pub(crate) decoder_scratch: [u8; HPACK_SCRATCH],
    pub(crate) encoder_entries: [HpackEntrySlot; HPACK_ENTRIES],
    pub(crate) encoder_scratch: [u8; HPACK_SCRATCH],
    pub(crate) settings_ack_limits: [u32; SETTINGS_ACKS],
    pub(crate) tables: Option<H2ServerTables<'static>>,
    pub(crate) connection: Option<H2ServerConnection<'static, 'static>>,
    pub(crate) feed_scratch: FeedScratch<'static>,
}

pub(crate) fn split_error(error: H2ServerError) -> (crate::abi::VylResult, usize) {
    match error {
        H2ServerError::InvalidArgument => (crate::abi::VylResult::InvalidArgument, 0),
        H2ServerError::ProtocolError => (crate::abi::VylResult::ProtocolError, 0),
        H2ServerError::BufferTooSmall { required } => {
            (crate::abi::VylResult::BufferTooSmall, required)
        }
        H2ServerError::InternalError => (crate::abi::VylResult::InternalError, 0),
        H2ServerError::Capacity => (crate::abi::VylResult::Capacity, 0),
        H2ServerError::WouldBlock => (crate::abi::VylResult::WouldBlock, 0),
        H2ServerError::BadState => (crate::abi::VylResult::BadState, 0),
        H2ServerError::WriteOverrun => (crate::abi::VylResult::WriteOverrun, 0),
        H2ServerError::Cancelled => (crate::abi::VylResult::Cancelled, 0),
    }
}

pub use api::{
    vyl_h2_server_acknowledge, vyl_h2_server_cancel, vyl_h2_server_destroy, vyl_h2_server_new,
    vyl_h2_server_send_ping_ack, vyl_h2_server_send_settings, vyl_h2_server_send_settings_ack,
    vyl_h2_server_send_window_update,
};
pub use feed::vyl_h2_server_poll_feed;
pub use request::{
    vyl_h2_server_accept_request, vyl_h2_server_goaway_drain, vyl_h2_server_refuse,
    vyl_h2_server_send_data, vyl_h2_server_send_reset, vyl_h2_server_send_response,
};
