use super::{
    ConnectionDeadlineKind, ConnectionPhase, ConnectionTable, ConnectionToken, TableError,
};
use crate::timers::{TimerExpiration, TimerQueue, TimerQueueError};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectionTimeouts {
    pub request_head_ns: u64,
    pub body_progress_ns: u64,
    pub response_write_ns: u64,
    pub keep_alive_idle_ns: u64,
}

// NOTE: production timeout defaults, named so deployments can cite and override them
// individually: 5 s head/body/response budgets, 30 s keep-alive idle. Zero disables
// the corresponding deadline (see schedule_phase_deadline); u64::MAX never appears here.
impl ConnectionTimeouts {
    pub const REQUEST_HEAD_NS: u64 = 5_000_000_000;
    pub const BODY_PROGRESS_NS: u64 = 5_000_000_000;
    pub const RESPONSE_WRITE_NS: u64 = 5_000_000_000;
    pub const KEEP_ALIVE_IDLE_NS: u64 = 30_000_000_000;

    pub const fn production() -> Self {
        Self {
            request_head_ns: Self::REQUEST_HEAD_NS,
            body_progress_ns: Self::BODY_PROGRESS_NS,
            response_write_ns: Self::RESPONSE_WRITE_NS,
            keep_alive_idle_ns: Self::KEEP_ALIVE_IDLE_NS,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeadlineError {
    Invalid,
    Full,
    DuplicateTimer,
    ConnectionAlreadyTimed,
    PhaseMismatch,
    StaleConnection,
    InvalidTimeout,
    DeadlineOverflow,
    TimerMissing,
    NotFound,
}

pub fn schedule_phase_deadline(
    table: &mut ConnectionTable<'_, '_>,
    timers: &mut TimerQueue<'_, '_>,
    token: ConnectionToken,
    now_ns: u64,
    timeouts: ConnectionTimeouts,
) -> Result<TimerExpiration, DeadlineError> {
    let phase = table.phase(token).map_err(map_table_error)?;
    if table.deadline(token).map_err(map_table_error)?.is_some() {
        return Err(DeadlineError::ConnectionAlreadyTimed);
    }
    let kind = phase.deadline_kind().ok_or(DeadlineError::PhaseMismatch)?;
    let timeout_ns = timeout_for(timeouts, kind);
    if timeout_ns == 0 {
        return Err(DeadlineError::InvalidTimeout);
    }
    let deadline_ns = now_ns
        .checked_add(timeout_ns)
        .ok_or(DeadlineError::DeadlineOverflow)?;
    let timer = TimerExpiration {
        timer_id: super::super::deadline_timer_id(token),
        deadline_ns,
    };
    timers.schedule(timer).map_err(map_timer_error)?;

    if let Err(error) = table.bind_deadline(token, kind) {
        let cancel_result = timers.cancel(timer.timer_id);
        if cancel_result.is_err() {
            return Err(DeadlineError::Invalid);
        }
        return Err(map_table_error(error));
    }
    Ok(timer)
}

pub fn cancel_phase_deadline(
    table: &mut ConnectionTable<'_, '_>,
    timers: &mut TimerQueue<'_, '_>,
    token: ConnectionToken,
) -> Result<(), DeadlineError> {
    let deadline = table
        .deadline(token)
        .map_err(map_table_error)?
        .ok_or(DeadlineError::NotFound)?;
    timers
        .cancel(deadline.timer_id)
        .map_err(|error| match error {
            TimerQueueError::NotFound => DeadlineError::TimerMissing,
            TimerQueueError::Invalid => DeadlineError::Invalid,
            TimerQueueError::Full | TimerQueueError::Duplicate => DeadlineError::Invalid,
        })?;
    table.clear_deadline(token).map_err(map_table_error)
}

pub fn start_keep_alive_idle_deadline(
    table: &mut ConnectionTable<'_, '_>,
    timers: &mut TimerQueue<'_, '_>,
    token: ConnectionToken,
    now_ns: u64,
    timeouts: ConnectionTimeouts,
) -> Result<TimerExpiration, DeadlineError> {
    table
        .transition(token, ConnectionPhase::KeepAliveIdle)
        .map_err(map_table_error)?;
    match schedule_phase_deadline(table, timers, token, now_ns, timeouts) {
        Ok(timer) => Ok(timer),
        Err(error) => {
            let _ = table.transition(token, ConnectionPhase::Closing);
            Err(error)
        }
    }
}

pub fn resume_keep_alive_request(
    table: &mut ConnectionTable<'_, '_>,
    timers: &mut TimerQueue<'_, '_>,
    token: ConnectionToken,
) -> Result<(), DeadlineError> {
    if table.phase(token).map_err(map_table_error)? != ConnectionPhase::KeepAliveIdle {
        return Err(DeadlineError::PhaseMismatch);
    }
    cancel_phase_deadline(table, timers, token).map_err(|error| match error {
        DeadlineError::NotFound => DeadlineError::TimerMissing,
        error => error,
    })?;
    table
        .transition(token, ConnectionPhase::ReadingRequestHead)
        .map_err(map_table_error)
}

fn timeout_for(timeouts: ConnectionTimeouts, kind: ConnectionDeadlineKind) -> u64 {
    match kind {
        ConnectionDeadlineKind::RequestHead => timeouts.request_head_ns,
        ConnectionDeadlineKind::BodyProgress | ConnectionDeadlineKind::AwaitingHandler => {
            timeouts.body_progress_ns
        }
        ConnectionDeadlineKind::ResponseWrite => timeouts.response_write_ns,
        ConnectionDeadlineKind::KeepAliveIdle => timeouts.keep_alive_idle_ns,
    }
}

fn map_table_error(error: TableError) -> DeadlineError {
    match error {
        TableError::Stale => DeadlineError::StaleConnection,
        TableError::InvalidTransition => DeadlineError::PhaseMismatch,
        TableError::DuplicateDeadline => DeadlineError::DuplicateTimer,
        TableError::NotFound => DeadlineError::NotFound,
        TableError::Invalid | TableError::Full => DeadlineError::Invalid,
    }
}

fn map_timer_error(error: TimerQueueError) -> DeadlineError {
    match error {
        TimerQueueError::Full => DeadlineError::Full,
        TimerQueueError::Duplicate => DeadlineError::DuplicateTimer,
        TimerQueueError::NotFound | TimerQueueError::Invalid => DeadlineError::Invalid,
    }
}
