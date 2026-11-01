mod connections;
#[cfg(not(all(target_family = "wasm", not(target_os = "wasi"))))]
mod io;
#[cfg(not(all(target_family = "wasm", not(target_os = "wasi"))))]
mod polling;
mod shutdown;
mod timers;
#[cfg(not(all(target_family = "wasm", not(target_os = "wasi"))))]
mod worker;

pub use connections::{
    ConnectionDeadline, ConnectionDeadlineKind, ConnectionPhase, ConnectionSlot, ConnectionTable,
    ConnectionTimeouts, ConnectionToken, DeadlineError, ReleaseOutcome, TableError,
    cancel_phase_deadline, deadline_timer_id, refresh_phase_deadline, resume_keep_alive_request,
    schedule_phase_deadline, start_keep_alive_idle_deadline,
};
#[cfg(target_os = "linux")]
pub use io::{BatchRing, ReadOp, SendOp};
#[cfg(not(all(target_family = "wasm", not(target_os = "wasi"))))]
pub use io::{
    CloseReason, IoBatch, IoOutcome, ReadStatus, WriteStatus, dispatch, note_completion,
    note_read_progress, note_write_progress,
};
#[cfg(all(
    not(all(target_family = "wasm", not(target_os = "wasi"))),
    not(any(target_os = "wasi", target_os = "horizon"))
))]
pub use polling::WakeHandle;
#[cfg(not(all(target_family = "wasm", not(target_os = "wasi"))))]
pub use polling::{
    AcceptBatch, AcceptBatchOutcome, AcceptStop, AcceptedConnection, WAKE_TOKEN, accept_ready,
};
#[cfg(not(all(target_family = "wasm", not(target_os = "wasi"))))]
pub use polling::{
    PollBackend, PollInterest, PollSource, Poller, Readiness, TcpListener, TcpStream,
    bind_reuse_port,
};
pub use shutdown::{
    ShutdownPhase, ShutdownResult, ShutdownState, close_connection, destroy, force_shutdown,
    process_deadline, register_connection, register_connections, request_shutdown,
};
pub use timers::{
    MAX_TIMER_CAPACITY, TIMER_INDEX_STORAGE_FACTOR, TimerExpiration, TimerQueue, TimerQueueError,
};
#[cfg(not(all(target_family = "wasm", not(target_os = "wasi"))))]
pub use worker::{
    AdmittedConnection, TurnConfig, TurnInput, TurnOutcome, Worker, WorkerError, WorkerStorage,
};
