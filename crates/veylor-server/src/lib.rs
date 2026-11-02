mod connection;
pub mod h2;
pub mod host;
mod multi_worker;
mod static_files;
mod static_routes;
mod static_server;

pub use connection::{ConnClose, PumpOutcome, PumpProgress};
#[cfg(target_os = "linux")]
pub use connection::{FD_CACHE_ENTRIES, FdCache};
pub use connection::{HEADER_SLOTS, INPUT_CAP, OUTPUT_CAP};
pub use connection::{TrailerOutcome, TrailerReport};
pub use h2::{
    ACK_SLOTS, DECIMAL_CAP, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES, ENCODE_ENTRIES,
    FrozenRoute, FrozenRoutesView, H2_BODY_CAP, H2C_MAX_HEADER_LIST_SIZE, H2Conn, H2HostCtx,
    HEADER_BLOCK_CAP, HPACK_BLOCK_CAP, INPUT_STAGING_CAP, MAX_H2_CONNS, MAX_STREAMS_PER_STEP,
    PushError, STREAM_SLOTS, StepOutcome,
};
pub use host::{
    BodyPull, CancelOutcome, EventSlot, HOST_MAX_METHOD_BYTES, HOST_MAX_PATH_BYTES,
    HOST_MAX_REASON_BYTES, HOST_QUEUE_DEPTH, HostCounters, HostEventKind, HostHandle, HostShared,
    MAX_REQUEST_HEADER_BYTES, MAX_REQUEST_HEADERS, MAX_STAGED_RESPONSE_BYTES,
    MAX_STAGED_RESPONSE_HEADERS, PollReport, RequestHeadError, RequestHeadReport, RequestHeader,
    RequestSpan, RespondOutcome,
};
pub use static_files::{
    FileMounts, FilePlan, FileReject, MountError, RangeApply, content_type_for_name,
    parse_http_date, read_chunk, write_http_date,
};
pub use static_files::{MAX_FILE_SIZE, MAX_MOUNTS};
pub use static_routes::{
    MAX_NATIVE_HEADERS, MAX_RESPONSE_BODY, MAX_ROUTES, MAX_STATIC_BODY_BYTES, NativeHandler,
    NativeHeader, NativeRequestView, NativeResponseOut, RouteBuildError, in_native_handler,
};
pub use static_server::{
    CloseCounts, MAX_WORKERS, RunningServer, Server, ServerConfig, ServerError, TlsConfig,
    TlsConfigError,
};
pub use static_server::{LISTENER_TOKEN, MAX_CONNECTIONS, SHUTDOWN_TIMER_ID};
pub use veylor_runtime::PollBackend;
pub use veylor_tls::{CertificateDer, PrivateKeyDer};
