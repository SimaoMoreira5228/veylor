mod connections;
#[cfg(not(all(target_family = "wasm", not(target_os = "wasi"))))]
mod polling;
mod shutdown;
mod timers;

pub use connections::{
    ConnectionDeadline, ConnectionDeadlineKind, ConnectionPhase, ConnectionSlot, ConnectionTable,
    ConnectionTimeouts, ConnectionToken, DeadlineError, ReleaseOutcome, TableError,
    cancel_phase_deadline, deadline_timer_id, resume_keep_alive_request, schedule_phase_deadline,
    start_keep_alive_idle_deadline,
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
pub use polling::{PollInterest, Poller, Readiness, TcpListener, TcpStream};
pub use shutdown::{
    ShutdownPhase, ShutdownResult, ShutdownState, close_connection, destroy, force_shutdown,
    process_deadline, register_connection, register_connections, request_shutdown,
};
pub use timers::{
    MAX_TIMER_CAPACITY, TIMER_INDEX_STORAGE_FACTOR, TimerExpiration, TimerQueue, TimerQueueError,
};
