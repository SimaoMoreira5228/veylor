use std::io;
use std::net::SocketAddr;

use crate::{
    AcceptBatchOutcome, AcceptStop, AcceptedConnection, ConnectionDeadline, ConnectionPhase,
    ConnectionSlot, ConnectionTable, ConnectionTimeouts, ConnectionToken, DeadlineError, Poller,
    Readiness, ShutdownPhase, ShutdownResult, ShutdownState, TableError, TcpListener, TcpStream,
    TimerExpiration, TimerQueue, TimerQueueError, accept_ready, cancel_phase_deadline,
    close_connection, deadline_timer_id, destroy, force_shutdown, process_deadline,
    register_connection, request_shutdown, resume_keep_alive_request, schedule_phase_deadline,
    start_keep_alive_idle_deadline,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TurnConfig {
    pub max_accepts_per_turn: usize,
    pub max_events_per_turn: usize,
    pub byte_budget_per_turn: u64,
}

impl TurnConfig {
    pub const DEFAULT: Self = Self {
        max_accepts_per_turn: 8,
        max_events_per_turn: 256,
        byte_budget_per_turn: 65_536,
    };

    pub fn validate(self) -> Result<Self, WorkerError> {
        if self.max_events_per_turn == 0 {
            return Err(WorkerError::ZeroEventBudget);
        }

        Ok(self)
    }
}

// NOTE: bytes_moved stays zero until connection IO dispatch lands; the budget rides
// along now so the turn shape does not change under the managed server.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TurnOutcome {
    pub accepted: usize,
    pub events_drained: usize,
    pub bytes_moved: u64,
    pub expired_deadlines: usize,
    pub shutdown_complete: bool,
}

#[derive(Debug)]
pub enum WorkerError {
    ZeroEventBudget,
    SlotCapacity,
    TimerCapacity,
    StaleToken,
    Accept(io::Error),
    Poll(io::Error),
    Invalid,
}

impl From<TableError> for WorkerError {
    fn from(error: TableError) -> Self {
        match error {
            TableError::Full => Self::SlotCapacity,
            TableError::Stale => Self::StaleToken,
            TableError::Invalid
            | TableError::NotFound
            | TableError::DuplicateDeadline
            | TableError::InvalidTransition => Self::Invalid,
        }
    }
}

impl From<TimerQueueError> for WorkerError {
    fn from(error: TimerQueueError) -> Self {
        match error {
            TimerQueueError::Full => Self::TimerCapacity,
            TimerQueueError::Invalid | TimerQueueError::Duplicate | TimerQueueError::NotFound => {
                Self::Invalid
            }
        }
    }
}

impl From<DeadlineError> for WorkerError {
    fn from(error: DeadlineError) -> Self {
        match error {
            DeadlineError::Full => Self::TimerCapacity,
            DeadlineError::StaleConnection => Self::StaleToken,
            _ => Self::Invalid,
        }
    }
}

#[derive(Debug)]
pub struct AdmittedConnection {
    pub token: ConnectionToken,
    pub stream: TcpStream,
    pub peer: SocketAddr,
}

pub struct WorkerStorage<'a> {
    pub slots: &'a mut [ConnectionSlot],
    pub free_stack: &'a mut [u32],
    pub timer_storage: &'a mut [TimerExpiration],
    pub timer_index_storage: &'a mut [u32],
    pub staging: &'a mut [Option<AcceptedConnection>],
    pub live: &'a mut [Option<ConnectionToken>],
}

pub struct TurnInput<'a> {
    pub listener_token: usize,
    pub now_ns: u64,
    pub max_wait_ms: i32,
    pub events: &'a mut [Readiness],
    pub expired: &'a mut [ConnectionDeadline],
    pub admitted: &'a mut [Option<AdmittedConnection>],
}

pub struct Worker<'a> {
    table: ConnectionTable<'a, 'a>,
    timers: TimerQueue<'a, 'a>,
    shutdown: &'a mut ShutdownState,
    poller: &'a mut Poller,
    staging: &'a mut [Option<AcceptedConnection>],
    live: &'a mut [Option<ConnectionToken>],
    staged: usize,
    live_count: usize,
    capacity: usize,
    config: TurnConfig,
    timeouts: ConnectionTimeouts,
    next_resource_id: u64,
    shutdown_timer_id: u64,
    shutdown_armed: bool,
    accept_pending: bool,
}

impl<'a> Worker<'a> {
    pub fn new(
        worker_id: u32,
        worker_generation: u64,
        config: TurnConfig,
        timeouts: ConnectionTimeouts,
        storage: WorkerStorage<'a>,
        shutdown: &'a mut ShutdownState,
        poller: &'a mut Poller,
    ) -> Result<Self, WorkerError> {
        let config = config.validate()?;
        let capacity = storage.slots.len();

        if capacity == 0 {
            return Err(WorkerError::Invalid);
        }

        if storage.staging.is_empty() {
            return Err(WorkerError::Invalid);
        }

        if storage.live.len() < capacity {
            return Err(WorkerError::Invalid);
        }

        let table = ConnectionTable::new(
            worker_id,
            worker_generation,
            storage.slots,
            storage.free_stack,
        )
        .map_err(WorkerError::from)?;
        let timers = TimerQueue::new(storage.timer_storage, storage.timer_index_storage)
            .map_err(WorkerError::from)?;
        storage.live.fill(None);

        Ok(Self {
            table,
            timers,
            shutdown,
            poller,
            staging: storage.staging,
            live: storage.live,
            staged: 0,
            live_count: 0,
            capacity,
            config,
            timeouts,
            next_resource_id: 1,
            shutdown_timer_id: 0,
            shutdown_armed: false,
            accept_pending: false,
        })
    }

    pub fn turn(
        &mut self,
        listener: &mut TcpListener,
        input: TurnInput<'_>,
    ) -> Result<TurnOutcome, WorkerError> {
        if self.shutdown.phase() == ShutdownPhase::Destroyed {
            return Err(WorkerError::Invalid);
        }

        let TurnInput {
            listener_token,
            now_ns,
            max_wait_ms,
            events,
            expired,
            admitted,
        } = input;
        let mut outcome = TurnOutcome::default();

        let waited = self
            .poller
            .poll_with_timers(&self.timers, now_ns, max_wait_ms)
            .map_err(WorkerError::Poll)?;
        let mut drained = 0;

        for readiness in waited.take(self.config.max_events_per_turn) {
            if drained == events.len() {
                break;
            }

            events[drained] = readiness;
            drained += 1;
        }

        outcome.events_drained = drained;

        let mut listener_ready = false;

        for event in events.iter().take(drained) {
            if event.token == listener_token && event.readable && !event.error_hint {
                listener_ready = true;
                break;
            }
        }

        let mut expired_count = 0;

        while expired_count < expired.len() {
            let Some(timer) = self.timers.pop_expired(now_ns)? else {
                break;
            };

            if self.shutdown_armed && timer.timer_id == self.shutdown_timer_id {
                self.shutdown_armed = false;
                let result = process_deadline(self.shutdown, timer);

                if result == ShutdownResult::ForceConnections {
                    self.force_close_live();
                }

                continue;
            }

            if let Ok(deadline) = self.table.expire_deadline(timer.timer_id) {
                expired[expired_count] = deadline;
                expired_count += 1;
            }
        }

        for index in 0..self.live.len() {
            if expired_count == expired.len() {
                break;
            }
            let Some(token) = self.live[index] else {
                continue;
            };
            let Ok(Some(deadline_ns)) = self.table.deadline_ns(token) else {
                continue;
            };
            if deadline_ns == 0 || deadline_ns > now_ns {
                continue;
            }
            if let Ok(deadline) = self.table.expire_deadline(deadline_timer_id(token)) {
                expired[expired_count] = deadline;
                expired_count += 1;
            }
        }
        outcome.expired_deadlines = expired_count;

        if self.shutdown.phase() == ShutdownPhase::Running {
            if listener_ready || self.accept_pending {
                self.accept_into_staging(listener)?;
            }

            outcome.accepted = self.admit_staged(admitted)?;
        } else {
            self.drop_staged();
        }

        if self.shutdown.phase() == ShutdownPhase::Forcing && self.live_count > 0 {
            self.force_close_live();
        }

        outcome.shutdown_complete = self.shutdown.phase() == ShutdownPhase::Complete;

        if self.staged > 0 {
            self.accept_pending = true;
            return Err(WorkerError::SlotCapacity);
        }

        Ok(outcome)
    }

    pub fn close(&mut self, token: ConnectionToken) -> Result<(), WorkerError> {
        let armed = self
            .table
            .deadline(token)
            .map_err(WorkerError::from)?
            .is_some();

        if armed {
            self.table
                .clear_deadline(token)
                .map_err(WorkerError::from)?;
        }

        self.table.release(token).map_err(WorkerError::from)?;
        let live_slot = self
            .live
            .get_mut(token.slot as usize)
            .ok_or(WorkerError::Invalid)?;
        *live_slot = None;

        if self.live_count > 0 {
            self.live_count -= 1;
        }

        close_connection(&mut *self.shutdown, Some(&mut self.timers));

        Ok(())
    }

    // NOTE: refresh happens only on byte progress; a WouldBlock turn reports
    // zero bytes and leaves the armed deadline alone so slow peers still time out.
    pub fn note_progress(
        &mut self,
        token: ConnectionToken,
        bytes: u64,
        now_ns: u64,
    ) -> Result<bool, WorkerError> {
        if bytes == 0 {
            return Ok(false);
        }
        if self.table.phase(token)? == ConnectionPhase::Tunnel {
            return Ok(true);
        }
        match crate::refresh_phase_deadline(&mut self.table, token, now_ns, self.timeouts) {
            Ok(_) => Ok(true),
            Err(DeadlineError::NotFound | DeadlineError::TimerMissing) => {
                schedule_phase_deadline(&mut self.table, token, now_ns, self.timeouts)?;
                Ok(true)
            }
            Err(error) => Err(error.into()),
        }
    }

    // NOTE: the armed deadline is cancelled before the move because the table
    // rejects kind changes while armed; any failure parks the slot in Closing
    // so the server must close it instead of running it deadline-less.
    pub fn transition_phase(
        &mut self,
        token: ConnectionToken,
        next: ConnectionPhase,
        now_ns: u64,
    ) -> Result<(), WorkerError> {
        match cancel_phase_deadline(&mut self.table, token) {
            Ok(()) | Err(DeadlineError::NotFound | DeadlineError::TimerMissing) => {}
            Err(error) => return Err(error.into()),
        }
        if let Err(error) = self.table.transition(token, next) {
            let _ = self.table.transition(token, ConnectionPhase::Closing);
            return Err(error.into());
        }
        if next.deadline_kind().is_none() {
            return Ok(());
        }
        if let Err(error) = schedule_phase_deadline(&mut self.table, token, now_ns, self.timeouts) {
            let _ = self.table.transition(token, ConnectionPhase::Closing);
            return Err(error.into());
        }
        Ok(())
    }

    pub fn ensure_head_deadline(
        &mut self,
        token: ConnectionToken,
        now_ns: u64,
    ) -> Result<(), WorkerError> {
        if self.table.phase(token)? != ConnectionPhase::ReadingRequestHead {
            return Ok(());
        }
        if self
            .table
            .deadline(token)
            .map_err(WorkerError::from)?
            .is_some()
        {
            return Ok(());
        }
        schedule_phase_deadline(&mut self.table, token, now_ns, self.timeouts)?;
        Ok(())
    }

    // NOTE: fully flushed response fast path: the request and its response
    // completed in one turn, so only the keep-alive idle deadline is needed.
    // The normal transition chain would arm and cancel a response deadline
    // around a write that never blocked.
    pub fn complete_response(
        &mut self,
        token: ConnectionToken,
        now_ns: u64,
    ) -> Result<(), WorkerError> {
        match cancel_phase_deadline(&mut self.table, token) {
            Ok(()) | Err(DeadlineError::NotFound | DeadlineError::TimerMissing) => {}
            Err(error) => return Err(error.into()),
        }
        let phase = self.table.phase(token)?;
        if matches!(
            phase,
            ConnectionPhase::ReadingRequestHead
                | ConnectionPhase::ReadingRequestBody
                | ConnectionPhase::AwaitingHandler
        ) {
            self.table
                .transition(token, ConnectionPhase::WritingResponse)
                .map_err(WorkerError::from)?;
        }
        if phase != ConnectionPhase::KeepAliveIdle {
            self.table
                .transition(token, ConnectionPhase::KeepAliveIdle)
                .map_err(WorkerError::from)?;
        }
        schedule_phase_deadline(&mut self.table, token, now_ns, self.timeouts)?;
        Ok(())
    }

    pub fn schedule_idle(
        &mut self,
        token: ConnectionToken,
        now_ns: u64,
    ) -> Result<(), WorkerError> {
        start_keep_alive_idle_deadline(&mut self.table, token, now_ns, self.timeouts)?;
        Ok(())
    }

    // NOTE: single round-trip replacing the cancel_idle+transition_phase pair at
    // resume sites: drops the idle timer, re-enters ReadingRequestHead, and arms
    // the head deadline. A re-arm failure parks the slot in Closing so the
    // server closes it instead of running it deadline-less.
    pub fn resume_idle(&mut self, token: ConnectionToken, now_ns: u64) -> Result<(), WorkerError> {
        if let Err(error) = resume_keep_alive_request(&mut self.table, token) {
            return Err(error.into());
        }
        if let Err(error) = schedule_phase_deadline(&mut self.table, token, now_ns, self.timeouts) {
            let _ = self.table.transition(token, ConnectionPhase::Closing);
            return Err(error.into());
        }
        Ok(())
    }

    pub fn phase(&self, token: ConnectionToken) -> Result<ConnectionPhase, WorkerError> {
        self.table.phase(token).map_err(WorkerError::from)
    }

    // NOTE: prefer resume_idle for keep-alive resume: it drops the idle timer,
    // re-enters the head phase, and arms the head deadline in one round-trip.
    // This only drops the idle timer, so a resumed request still needs its
    // head deadline armed separately to avoid idle expiry.
    pub fn cancel_idle(&mut self, token: ConnectionToken) -> Result<(), WorkerError> {
        if self.table.phase(token)? != ConnectionPhase::KeepAliveIdle {
            return Err(WorkerError::Invalid);
        }
        match cancel_phase_deadline(&mut self.table, token) {
            Ok(()) | Err(DeadlineError::NotFound | DeadlineError::TimerMissing) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn cancel_deadline(&mut self, token: ConnectionToken) -> Result<(), WorkerError> {
        match cancel_phase_deadline(&mut self.table, token) {
            Ok(()) | Err(DeadlineError::NotFound | DeadlineError::TimerMissing) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn begin_shutdown(&mut self, now_ns: u64, grace_ns: u64, timer_id: u64) -> ShutdownResult {
        // NOTE: connection timer IDs always carry a nonzero slot generation in the
        // upper 32 bits, so the shutdown timer must live below that range; anything
        // else could be mistaken for a connection deadline on expiry.
        if timer_id == 0 || timer_id >= (1u64 << 32) {
            return ShutdownResult::Invalid;
        }

        let result = request_shutdown(
            &mut *self.shutdown,
            Some(&mut self.timers),
            timer_id,
            now_ns,
            grace_ns,
        );

        if result == ShutdownResult::Draining {
            self.shutdown_timer_id = timer_id;
            self.shutdown_armed = true;
        }

        result
    }

    pub fn force_shutdown(&mut self) -> ShutdownResult {
        self.shutdown_armed = false;

        force_shutdown(&mut *self.shutdown, Some(&mut self.timers))
    }

    pub fn destroy(&mut self) -> ShutdownResult {
        destroy(&mut *self.shutdown)
    }

    pub fn shutdown_phase(&self) -> ShutdownPhase {
        self.shutdown.phase()
    }

    pub fn pending_accepts(&self) -> usize {
        self.staged
    }

    pub fn live_count(&self) -> usize {
        self.live_count
    }

    pub fn slot_horizon(&self) -> usize {
        self.table.slot_horizon()
    }

    fn accept_into_staging(&mut self, listener: &mut TcpListener) -> Result<(), WorkerError> {
        let free = self.staging.len() - self.staged;

        if free == 0 {
            return Ok(());
        }

        let window = free.min(self.config.max_accepts_per_turn);

        if window == 0 {
            return Ok(());
        }

        let batch = accept_ready(
            listener,
            &mut self.staging[self.staged..self.staged + window],
            window,
        )
        .map_err(WorkerError::Accept)?;
        self.staged += batch.accepted;

        // NOTE: the poller reports edge-triggered readiness, so a batch that stops
        // short of drained leaves no fresh event behind; the flag re-arms accept on
        // the next turn until a batch proves the backlog empty.
        match batch.outcome {
            AcceptBatchOutcome::Stopped(AcceptStop::Drained) => {
                self.accept_pending = false;
                Ok(())
            }
            AcceptBatchOutcome::Stopped(_) => {
                self.accept_pending = true;
                Ok(())
            }
            AcceptBatchOutcome::InvalidStorage => Err(WorkerError::Invalid),
            AcceptBatchOutcome::Error(error) => {
                self.accept_pending = true;
                Err(WorkerError::Accept(error))
            }
        }
    }

    fn admit_staged(
        &mut self,
        admitted: &mut [Option<AdmittedConnection>],
    ) -> Result<usize, WorkerError> {
        let mut done = 0;
        let mut reported = 0;

        while done < self.staged {
            if reported == self.config.max_accepts_per_turn {
                break;
            }

            if reported == admitted.len() {
                break;
            }

            if self.live_count == self.capacity {
                break;
            }

            if admitted[reported].is_some() {
                self.compact_staging(done);
                return Err(WorkerError::Invalid);
            }

            if self.next_resource_id == u64::MAX {
                self.compact_staging(done);
                return Err(WorkerError::Invalid);
            }

            let accepted = self.staging[done].take().ok_or(WorkerError::Invalid)?;
            done += 1;
            // NOTE: the slot resource ID is a worker-issued sequence, not the socket
            // file descriptor: fd extraction is not portable across the
            // managed-server matrix, and the host already correlates each token with
            // its stream through the admitted out-buffer.
            let resource_id = self.next_resource_id;
            self.next_resource_id += 1;
            let token = match self.table.allocate(resource_id) {
                Ok(token) => token,
                Err(error) => {
                    drop(accepted);
                    self.compact_staging(done);
                    return Err(error.into());
                }
            };

            if register_connection(self.shutdown) != ShutdownResult::Accepted {
                let release = self.table.release(token);
                drop(accepted);
                self.compact_staging(done);
                release.map_err(WorkerError::from)?;
                return Err(WorkerError::Invalid);
            }

            let live_slot = self
                .live
                .get_mut(token.slot as usize)
                .ok_or(WorkerError::Invalid)?;
            *live_slot = Some(token);
            self.live_count += 1;
            admitted[reported] = Some(AdmittedConnection {
                token,
                stream: accepted.stream,
                peer: accepted.peer,
            });
            reported += 1;
        }

        self.compact_staging(done);

        Ok(reported)
    }

    fn compact_staging(&mut self, consumed: usize) {
        let remaining = self.staged - consumed;

        for index in 0..remaining {
            self.staging[index] = self.staging[consumed + index].take();
        }

        self.staged = remaining;
    }

    fn drop_staged(&mut self) {
        let staged = self.staged;

        for entry in self.staging.iter_mut().take(staged) {
            *entry = None;
        }

        self.staged = 0;
        self.accept_pending = false;
    }

    fn force_close_live(&mut self) {
        for entry in self.live.iter_mut() {
            let Some(token) = entry.take() else {
                continue;
            };
            let _ = self.table.clear_deadline(token);

            if self.table.release(token).is_ok() {
                self.live_count -= 1;
                close_connection(&mut *self.shutdown, Some(&mut self.timers));
            } else {
                *entry = Some(token);
            }
        }
    }
}

#[cfg(test)]
mod tests;
