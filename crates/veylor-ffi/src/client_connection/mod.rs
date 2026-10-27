#[allow(unsafe_code)]
mod api;
#[allow(unsafe_code)]
mod request_write;
#[allow(unsafe_code)]
mod response_read;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylRequestMethod {
    Head = 0,
    Connect = 1,
    #[default]
    Other = 2,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylRequestBodyKind {
    #[default]
    None = 0,
    Fixed = 1,
    Chunked = 2,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylResponseBodyKind {
    #[default]
    None = 0,
    Fixed = 1,
    Chunked = 2,
    CloseDelimited = 3,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylResponseKind {
    Informational = 0,
    #[default]
    Final = 1,
    Upgrade = 2,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylResponseHead {
    pub version: crate::VylSpan,
    pub reason: crate::VylSpan,
    pub status: u16,
    pub body_kind: VylResponseBodyKind,
    pub kind: VylResponseKind,
    pub header_count: usize,
    pub length: usize,
    pub end_offset: usize,
    pub body_length: u64,
    pub flags: u32,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylClientReadKind {
    #[default]
    NeedMore = 0,
    Informational = 1,
    Head = 2,
    BodyData = 3,
    Trailers = 4,
    Complete = 5,
    Upgrade = 6,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylClientReadEvent {
    pub kind: VylClientReadKind,
    pub consumed: usize,
    pub response: VylResponseHead,
    pub body_data: crate::VylSpan,
    pub trailer_count: usize,
    pub can_reuse: u32,
}

pub struct VylClientConnection {
    inner: veylor_client::ClientConnection<'static>,
}

pub use api::{
    VYL_HTTP1_MAX_REQUEST_FIELDS, VYL_HTTP1_RESPONSE_CONNECTION_CLOSE,
    VYL_HTTP1_RESPONSE_CONNECTION_KEEP_ALIVE, VYL_HTTP1_RESPONSE_CONNECTION_UPGRADE,
    VYL_HTTP1_RESPONSE_REUSABLE, vyl_http1_client_connection_destroy,
    vyl_http1_client_connection_new, vyl_http1_client_connection_reset,
};

pub use response_read::vyl_http1_client_connection_read_response;

pub use request_write::{
    vyl_http1_client_connection_advance_request_write, vyl_http1_client_connection_cancel,
    vyl_http1_client_connection_encode_request_body,
    vyl_http1_client_connection_encode_request_head,
    vyl_http1_client_connection_finish_request_body,
};
