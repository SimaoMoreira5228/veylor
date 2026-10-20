mod abi;
mod client_connection;
mod server_connection;

pub use abi::{VylHeader, VylResult, VylSpan};
pub use client_connection::{
    VYL_HTTP1_MAX_REQUEST_FIELDS, VYL_HTTP1_RESPONSE_CONNECTION_CLOSE,
    VYL_HTTP1_RESPONSE_CONNECTION_KEEP_ALIVE, VYL_HTTP1_RESPONSE_CONNECTION_UPGRADE,
    VYL_HTTP1_RESPONSE_REUSABLE, VylBytes, VylClientConnection, VylClientReadEvent,
    VylClientReadKind, VylField, VylRequestBodyKind, VylRequestMethod, VylResponseBodyKind,
    VylResponseHead, VylResponseKind, vyl_http1_client_connection_advance_request_write,
    vyl_http1_client_connection_destroy, vyl_http1_client_connection_encode_request_body,
    vyl_http1_client_connection_encode_request_head,
    vyl_http1_client_connection_finish_request_body, vyl_http1_client_connection_new,
    vyl_http1_client_connection_read_response, vyl_http1_client_connection_reset,
};
pub use server_connection::{
    VYL_HTTP1_CHUNKED, VYL_HTTP1_CONNECTION_CLOSE, VYL_HTTP1_CONNECTION_KEEP_ALIVE,
    VYL_HTTP1_CONNECTION_UPGRADE, VYL_HTTP1_EXPECT_CONTINUE, VYL_HTTP1_HAS_CONTENT_LENGTH,
    VylRequestHead, VylServerConnection, VylTargetForm, vyl_http1_server_connection_destroy,
    vyl_http1_server_connection_new, vyl_http1_server_connection_parse_head,
    vyl_http1_server_connection_reset,
};
