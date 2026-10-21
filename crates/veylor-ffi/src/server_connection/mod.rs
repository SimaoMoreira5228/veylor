#[allow(unsafe_code)]
mod api;
#[allow(unsafe_code)]
mod body;
#[allow(unsafe_code)]
mod response_write;

#[cfg(test)]
#[allow(unsafe_code)]
mod tests;

pub use api::{
    VYL_HTTP1_CHUNKED, VYL_HTTP1_CONNECTION_CLOSE, VYL_HTTP1_CONNECTION_KEEP_ALIVE,
    VYL_HTTP1_CONNECTION_UPGRADE, VYL_HTTP1_EXPECT_CONTINUE, VYL_HTTP1_HAS_CONTENT_LENGTH,
    VylRequestHead, VylServerConnection, VylTargetForm, vyl_http1_server_connection_destroy,
    vyl_http1_server_connection_new, vyl_http1_server_connection_parse_head,
    vyl_http1_server_connection_reset,
};
pub use body::{
    VYL_HTTP1_ADMISSION_READY, VYL_HTTP1_ADMISSION_REJECTED, VYL_HTTP1_ADMISSION_SEND_CONTINUE,
    VYL_HTTP1_BODY_COMPLETE, VYL_HTTP1_BODY_DATA, VYL_HTTP1_BODY_NEED_MORE,
    VYL_HTTP1_BODY_TRAILERS, VylServerBodyEvent, vyl_http1_server_connection_admit_body,
    vyl_http1_server_connection_continue_sent, vyl_http1_server_connection_read_body,
};
pub use response_write::{
    vyl_http1_server_connection_advance_response_write,
    vyl_http1_server_connection_encode_response_body,
    vyl_http1_server_connection_finish_response_body,
};
