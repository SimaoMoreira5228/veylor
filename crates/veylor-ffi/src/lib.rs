mod abi;
mod client_connection;
mod server_connection;

pub use abi::{VylBytes, VylField, VylHeader, VylResult, VylSpan};
pub use client_connection::{
    VYL_HTTP1_MAX_REQUEST_FIELDS, VYL_HTTP1_RESPONSE_CONNECTION_CLOSE,
    VYL_HTTP1_RESPONSE_CONNECTION_KEEP_ALIVE, VYL_HTTP1_RESPONSE_CONNECTION_UPGRADE,
    VYL_HTTP1_RESPONSE_REUSABLE, VylClientConnection, VylClientReadEvent, VylClientReadKind,
    VylRequestBodyKind, VylRequestMethod, VylResponseBodyKind, VylResponseHead, VylResponseKind,
    vyl_http1_client_connection_advance_request_write, vyl_http1_client_connection_cancel,
    vyl_http1_client_connection_destroy, vyl_http1_client_connection_encode_request_body,
    vyl_http1_client_connection_encode_request_head,
    vyl_http1_client_connection_finish_request_body, vyl_http1_client_connection_new,
    vyl_http1_client_connection_read_response, vyl_http1_client_connection_reset,
};
pub use server_connection::{
    VYL_HTTP1_ADMISSION_READY, VYL_HTTP1_ADMISSION_REJECTED, VYL_HTTP1_ADMISSION_SEND_CONTINUE,
    VYL_HTTP1_BODY_COMPLETE, VYL_HTTP1_BODY_DATA, VYL_HTTP1_BODY_NEED_MORE,
    VYL_HTTP1_BODY_TRAILERS, VYL_HTTP1_CHUNKED, VYL_HTTP1_CONNECTION_CLOSE,
    VYL_HTTP1_CONNECTION_KEEP_ALIVE, VYL_HTTP1_CONNECTION_UPGRADE, VYL_HTTP1_EXPECT_CONTINUE,
    VYL_HTTP1_HAS_CONTENT_LENGTH, VylRequestHead, VylServerBodyEvent, VylServerConnection,
    VylTargetForm, vyl_http1_server_connection_admit_body,
    vyl_http1_server_connection_advance_response_write, vyl_http1_server_connection_continue_sent,
    vyl_http1_server_connection_destroy, vyl_http1_server_connection_encode_response_body,
    vyl_http1_server_connection_finish_response_body, vyl_http1_server_connection_new,
    vyl_http1_server_connection_parse_head, vyl_http1_server_connection_read_body,
    vyl_http1_server_connection_reset,
};
