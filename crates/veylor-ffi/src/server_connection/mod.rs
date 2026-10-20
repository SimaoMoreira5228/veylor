#[allow(unsafe_code)]
mod api;

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
