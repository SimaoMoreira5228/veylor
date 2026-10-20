use crate::timers::{TimerExpiration, TimerQueue, TimerQueueError};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ShutdownPhase {
    #[default]
    Running,
    Draining,
    Forcing,
    Complete,
    Destroyed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownResult {
    Accepted,
    Draining,
    ForceConnections,
    ConnectionClosed,
    Completed,
    StaleExpiration,
    NotAccepting,
    TimerFull,
    DuplicateTimer,
    DeadlineOverflow,
    Busy,
    Destroyed,
    Invalid,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShutdownState {
    active_connections: u64,
    timer_id: u64,
    deadline_ns: u64,
    phase: ShutdownPhase,
    has_deadline: bool,
}

impl ShutdownState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn phase(&self) -> ShutdownPhase {
        self.phase
    }

    pub fn active_connections(&self) -> u64 {
        self.active_connections
    }

    pub fn has_deadline(&self) -> bool {
        self.has_deadline
    }
}

pub fn request_shutdown(
    state: &mut ShutdownState,
    timers: Option<&mut TimerQueue<'_, '_>>,
    timer_id: u64,
    now_ns: u64,
    grace_ns: u64,
) -> ShutdownResult {
    if state.phase != ShutdownPhase::Running {
        return ShutdownResult::Invalid;
    }
    if state.active_connections == 0 {
        state.phase = ShutdownPhase::Complete;
        return ShutdownResult::Completed;
    }
    if grace_ns == 0 {
        return force_shutdown(state, timers);
    }

    let Some(timers) = timers else {
        return ShutdownResult::Invalid;
    };
    let Some(deadline_ns) = now_ns.checked_add(grace_ns) else {
        return ShutdownResult::DeadlineOverflow;
    };
    let timer = TimerExpiration {
        timer_id,
        deadline_ns,
    };
    match timers.schedule(timer) {
        Ok(()) => {
            state.timer_id = timer_id;
            state.deadline_ns = deadline_ns;
            state.has_deadline = true;
            state.phase = ShutdownPhase::Draining;
            ShutdownResult::Draining
        }
        Err(TimerQueueError::Full) => ShutdownResult::TimerFull,
        Err(TimerQueueError::Duplicate) => ShutdownResult::DuplicateTimer,
        Err(TimerQueueError::NotFound | TimerQueueError::Invalid) => ShutdownResult::Invalid,
    }
}

pub fn force_shutdown(
    state: &mut ShutdownState,
    timers: Option<&mut TimerQueue<'_, '_>>,
) -> ShutdownResult {
    if state.phase == ShutdownPhase::Destroyed {
        return ShutdownResult::Invalid;
    }
    if state.active_connections == 0 {
        if let Err(result) = cancel_deadline(state, timers) {
            return result;
        }
        state.phase = ShutdownPhase::Complete;
        return ShutdownResult::Completed;
    }
    if let Err(result) = cancel_deadline(state, timers) {
        return result;
    }
    state.phase = ShutdownPhase::Forcing;
    ShutdownResult::ForceConnections
}

pub fn register_connection(state: &mut ShutdownState) -> ShutdownResult {
    register_connections(state, 1)
}

pub fn register_connections(state: &mut ShutdownState, count: u64) -> ShutdownResult {
    if state.phase != ShutdownPhase::Running {
        return ShutdownResult::NotAccepting;
    }
    let Some(active_connections) = state.active_connections.checked_add(count) else {
        return ShutdownResult::Invalid;
    };
    state.active_connections = active_connections;
    ShutdownResult::Accepted
}

pub fn close_connection(
    state: &mut ShutdownState,
    timers: Option<&mut TimerQueue<'_, '_>>,
) -> ShutdownResult {
    if state.active_connections == 0 {
        return ShutdownResult::Invalid;
    }
    if state.has_deadline && timers.is_none() {
        return ShutdownResult::Invalid;
    }

    state.active_connections -= 1;
    if state.active_connections != 0 {
        return ShutdownResult::ConnectionClosed;
    }
    if state.phase != ShutdownPhase::Draining && state.phase != ShutdownPhase::Forcing {
        return ShutdownResult::ConnectionClosed;
    }
    if let Err(result) = cancel_deadline(state, timers) {
        return result;
    }
    state.phase = ShutdownPhase::Complete;
    ShutdownResult::Completed
}

pub fn process_deadline(state: &mut ShutdownState, timer: TimerExpiration) -> ShutdownResult {
    if state.phase != ShutdownPhase::Draining
        || !state.has_deadline
        || timer.timer_id != state.timer_id
        || timer.deadline_ns != state.deadline_ns
    {
        return ShutdownResult::StaleExpiration;
    }
    state.has_deadline = false;
    state.phase = ShutdownPhase::Forcing;
    ShutdownResult::ForceConnections
}

pub fn destroy(state: &mut ShutdownState) -> ShutdownResult {
    if state.phase == ShutdownPhase::Destroyed {
        return ShutdownResult::Destroyed;
    }
    if state.phase != ShutdownPhase::Complete || state.active_connections != 0 || state.has_deadline
    {
        return ShutdownResult::Busy;
    }
    state.phase = ShutdownPhase::Destroyed;
    ShutdownResult::Destroyed
}

fn cancel_deadline(
    state: &mut ShutdownState,
    timers: Option<&mut TimerQueue<'_, '_>>,
) -> Result<(), ShutdownResult> {
    if !state.has_deadline {
        return Ok(());
    }
    let Some(timers) = timers else {
        return Err(ShutdownResult::Invalid);
    };
    match timers.cancel(state.timer_id) {
        Ok(()) | Err(TimerQueueError::NotFound) => {
            state.has_deadline = false;
            Ok(())
        }
        Err(TimerQueueError::Full | TimerQueueError::Duplicate | TimerQueueError::Invalid) => {
            Err(ShutdownResult::Invalid)
        }
    }
}
