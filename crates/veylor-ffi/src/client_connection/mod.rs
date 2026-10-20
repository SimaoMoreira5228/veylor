#[allow(unsafe_code)]
mod api;

pub use api::{
    VYL_HTTP1_RESPONSE_CONNECTION_CLOSE, VYL_HTTP1_RESPONSE_CONNECTION_KEEP_ALIVE,
    VYL_HTTP1_RESPONSE_CONNECTION_UPGRADE, VYL_HTTP1_RESPONSE_REUSABLE, VylClientConnection,
    VylRequestMethod, VylResponseBodyKind, VylResponseHead, VylResponseKind,
    vyl_http1_client_connection_advance, vyl_http1_client_connection_destroy,
    vyl_http1_client_connection_new, vyl_http1_client_connection_parse_head,
    vyl_http1_client_connection_reset,
};
