mod connection;
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
pub use host::{
    BodyPull, CancelOutcome, EventSlot, HOST_MAX_METHOD_BYTES, HOST_MAX_PATH_BYTES,
    HOST_MAX_REASON_BYTES, HOST_QUEUE_DEPTH, HostCounters, HostEventKind, HostHandle, PollReport,
    RespondOutcome,
};
pub use static_files::{
    FileMounts, FilePlan, FileReject, MountError, RangeApply, content_type_for_name, read_chunk,
};
pub use static_files::{MAX_FILE_SIZE, MAX_MOUNTS};
pub use static_routes::{MAX_RESPONSE_BODY, MAX_ROUTES, MAX_STATIC_BODY_BYTES, RouteBuildError};
pub use static_server::{
    CloseCounts, MAX_WORKERS, RunningServer, Server, ServerConfig, ServerError,
};
pub use static_server::{LISTENER_TOKEN, MAX_CONNECTIONS, SHUTDOWN_TIMER_ID};
