mod deadlines;
mod table;

pub use deadlines::{
    ConnectionTimeouts, DeadlineError, cancel_phase_deadline, resume_keep_alive_request,
    schedule_phase_deadline, start_keep_alive_idle_deadline,
};
pub use table::{
    ConnectionDeadline, ConnectionDeadlineKind, ConnectionPhase, ConnectionSlot, ConnectionTable,
    ConnectionToken, ReleaseOutcome, TableError, deadline_timer_id,
};

#[cfg(test)]
mod tests;
