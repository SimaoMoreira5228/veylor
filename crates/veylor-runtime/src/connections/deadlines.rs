use super::{
    ConnectionDeadlineKind, ConnectionPhase, ConnectionTable, ConnectionToken, TableError,
};
use crate::timers::TimerExpiration;

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
    table.bind_deadline(token, kind).map_err(map_table_error)?;
    table
        .set_deadline_ns(token, deadline_ns)
        .map_err(map_table_error)?;
    Ok(TimerExpiration {
        timer_id: super::super::deadline_timer_id(token),
        deadline_ns,
    })
}

pub fn refresh_phase_deadline(
    table: &mut ConnectionTable<'_, '_>,
    token: ConnectionToken,
    now_ns: u64,
    timeouts: ConnectionTimeouts,
) -> Result<TimerExpiration, DeadlineError> {
    let phase = table.phase(token).map_err(map_table_error)?;
    let kind = phase.deadline_kind().ok_or(DeadlineError::PhaseMismatch)?;
    let timeout_ns = timeout_for(timeouts, kind);
    if timeout_ns == 0 {
        return Err(DeadlineError::InvalidTimeout);
    }
    if table.deadline(token).map_err(map_table_error)?.is_none() {
        return Err(DeadlineError::NotFound);
    }
    let deadline_ns = now_ns
        .checked_add(timeout_ns)
        .ok_or(DeadlineError::DeadlineOverflow)?;
    table
        .set_deadline_ns(token, deadline_ns)
        .map_err(map_table_error)?;
    Ok(TimerExpiration {
        timer_id: super::super::deadline_timer_id(token),
        deadline_ns,
    })
}

pub fn cancel_phase_deadline(
    table: &mut ConnectionTable<'_, '_>,
    token: ConnectionToken,
) -> Result<(), DeadlineError> {
    table
        .deadline(token)
        .map_err(map_table_error)?
        .ok_or(DeadlineError::NotFound)?;
    table.clear_deadline(token).map_err(map_table_error)
}

pub fn start_keep_alive_idle_deadline(
    table: &mut ConnectionTable<'_, '_>,
    token: ConnectionToken,
    now_ns: u64,
    timeouts: ConnectionTimeouts,
) -> Result<TimerExpiration, DeadlineError> {
    table
        .transition(token, ConnectionPhase::KeepAliveIdle)
        .map_err(map_table_error)?;
    match schedule_phase_deadline(table, token, now_ns, timeouts) {
        Ok(timer) => Ok(timer),
        Err(error) => {
            let _ = table.transition(token, ConnectionPhase::Closing);
            Err(error)
        }
    }
}

pub fn resume_keep_alive_request(
    table: &mut ConnectionTable<'_, '_>,
    token: ConnectionToken,
) -> Result<(), DeadlineError> {
    if table.phase(token).map_err(map_table_error)? != ConnectionPhase::KeepAliveIdle {
        return Err(DeadlineError::PhaseMismatch);
    }
    cancel_phase_deadline(table, token).map_err(|error| match error {
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
