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

use veylor_client::h2::{
    FeedScratch, H2ClientConnection, H2ClientError, H2ClientTables, HEADER_BLOCK, HPACK_ENTRIES,
    HPACK_SCRATCH, HpackEntrySlot, SETTINGS_ACKS, STREAM_SLOTS, StreamSlot,
};

const _: () = assert!(veylor_client::h2::MAX_ACTIONS == crate::abi::VYL_H2_MAX_ACTIONS);
const _: () = assert!(veylor_client::h2::MAX_FIELDS == crate::abi::VYL_H2_MAX_FIELDS);
const _: () = assert!(veylor_client::h2::MAX_SETTINGS == crate::abi::VYL_H2_MAX_SETTINGS);

pub struct VylH2Client {
    pub(crate) slots: [StreamSlot; STREAM_SLOTS],
    pub(crate) header_block: [u8; HEADER_BLOCK],
    pub(crate) decoder_entries: [HpackEntrySlot; HPACK_ENTRIES],
    pub(crate) decoder_scratch: [u8; HPACK_SCRATCH],
    pub(crate) encoder_entries: [HpackEntrySlot; HPACK_ENTRIES],
    pub(crate) encoder_scratch: [u8; HPACK_SCRATCH],
    pub(crate) settings_ack_limits: [u32; SETTINGS_ACKS],
    pub(crate) tables: Option<H2ClientTables<'static>>,
    pub(crate) connection: Option<H2ClientConnection<'static, 'static>>,
    pub(crate) feed_scratch: FeedScratch<'static>,
}

pub(crate) fn split_error(error: H2ClientError) -> (crate::abi::VylResult, usize) {
    match error {
        H2ClientError::InvalidArgument => (crate::abi::VylResult::InvalidArgument, 0),
        H2ClientError::ProtocolError => (crate::abi::VylResult::ProtocolError, 0),
        H2ClientError::BufferTooSmall { required } => {
            (crate::abi::VylResult::BufferTooSmall, required)
        }
        H2ClientError::InternalError => (crate::abi::VylResult::InternalError, 0),
        H2ClientError::Capacity => (crate::abi::VylResult::Capacity, 0),
        H2ClientError::WouldBlock => (crate::abi::VylResult::WouldBlock, 0),
        H2ClientError::BadState => (crate::abi::VylResult::BadState, 0),
        H2ClientError::WriteOverrun => (crate::abi::VylResult::WriteOverrun, 0),
        H2ClientError::Cancelled => (crate::abi::VylResult::Cancelled, 0),
    }
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
