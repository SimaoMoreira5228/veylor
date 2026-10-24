use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use veylor_runtime::{ConnectionToken, WakeHandle};

use crate::connection::parked::BodyPushOutcome;
use crate::connection::{pull_staged_body, push_staged_body};
use crate::static_routes::MAX_RESPONSE_BODY;

mod signals;

// NOTE: advisory signal staging (BodyAvailable/Writable stage, admission rollback, signal
// retire) lives in signals.rs; re-exported here so existing crate::host:: paths resolve.
pub(crate) use signals::{
    retire_signals, retract_body_append, stage_body_available, stage_writable_locked,
};

// NOTE: park/poll/respond/cancel mirrors the Odin host queue: bounded per-worker event
// queues, whole-event dequeue with required-capacity reporting, level-triggered wakeups,
// one Shutdown event per worker. Fixed-length bodies stage per parked record up to the
// shared static cap: the worker pushes wire bytes, the host pulls, completion is observed.

// NOTE: depths mirror the Odin HOST_QUEUE_DEPTH for comparable overload behavior.
pub const HOST_QUEUE_DEPTH: usize = 64;
// NOTE: H2 stream-keyed park range appended to the parked table: 8 conns x 32 subslots,
// indexed MAX_CONNECTIONS + conn * 32 + subslot; sized from the H2 turn constants.
pub(crate) const H2_PARK_SLOTS: usize = crate::h2::turn::MAX_H2_CONNS * crate::h2::STREAM_SLOTS;
pub const HOST_MAX_METHOD_BYTES: usize = 16;
pub const HOST_MAX_PATH_BYTES: usize = 2048;
pub const HOST_MAX_REASON_BYTES: usize = 64;
const HOST_TOMB_DEPTH: usize = 64;
const SHUTDOWN_PUSH_RETRIES: usize = 50_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostEventKind {
    Request,
    Shutdown,
    // NOTE: advisory body signal staged at byte admission: token names the parked exchange,
    // the counts ride the reused poll length fields (ready bytes, end flag), route is zero.
    // One pending per token, latest-wins; a full queue with no coalesce target refuses the
    // byte admission itself, so the stranded-signal case cannot happen. Terminal framing with
    // no new bytes (chunked 0-chunk) stages no signal: completion is observed via pull.
    BodyAvailable,
    // NOTE: unenforced until the stream re-arm seam lands: stage one coalesced event per token
    // via HostShared::stage_writable where the arm releases (take_stream_cmd re-arm in
    // connection/streams.rs plus the H2 re-arm loop in h2/serve.rs). Each event admits exactly
    // one write_stream continuation; the is_writable query stays authoritative on a full queue.
    Writable,
}

#[derive(Clone, Debug)]
pub(crate) enum StagedAction {
    Respond {
        status: u16,
        reason: Vec<u8>,
        body: Vec<u8>,
    },
    Cancel,
}

#[derive(Clone, Debug)]
pub(crate) struct ParkedEntry {
    pub(crate) token: ConnectionToken,
    pub(crate) method_len: u8,
    pub(crate) method: [u8; HOST_MAX_METHOD_BYTES],
    pub(crate) staged: Option<StagedAction>,
    pub(crate) stream_cmd: Option<crate::connection::parked::StreamCmd>,
    pub(crate) stream: Option<crate::connection::parked::StreamState>,
    pub(crate) body_total: u64,
    pub(crate) body: Vec<u8>,
    pub(crate) body_pulled: u64,
    pub(crate) trailers: Option<Vec<crate::connection::StagedTrailer>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TombCause {
    Responded,
    Cancelled,
    Gone,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Tomb {
    token: ConnectionToken,
    cause: TombCause,
}

pub(crate) struct HostEvent {
    kind: HostEventKind,
    token: ConnectionToken,
    route_id: u64,
    method_len: u8,
    method: [u8; HOST_MAX_METHOD_BYTES],
    path_len: u16,
    path: [u8; HOST_MAX_PATH_BYTES],
    body_ready: usize,
    body_complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventSlot {
    pub kind: HostEventKind,
    pub token: ConnectionToken,
    pub route_id: u64,
    pub method_len: usize,
    pub path_len: usize,
    pub method_offset: usize,
    pub path_offset: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PollReport {
    pub copied: usize,
    pub events_required: usize,
    pub method_required: usize,
    pub path_required: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParkError {
    Full,
    SnapshotTooLarge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RespondOutcome {
    Ok,
    Stale,
    Duplicate,
    Gone,
    Invalid,
    BodyTooLarge,
    NotWritable,
    WouldBlock,
}

// NOTE: pull triple (outcome, bytes copied, end-of-body); Duplicate means already answered.
pub type BodyPull = (RespondOutcome, usize, bool);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelOutcome {
    Ok,
    Stale,
    Invalid,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HostCounters {
    pub parks: u64,
    pub queue_full: u64,
    pub responds_ok: u64,
    pub responds_duplicate: u64,
    pub responds_stale: u64,
    pub responds_gone: u64,
    pub cancels_ok: u64,
    pub cancels_stale: u64,
    pub shutdowns: u64,
}

pub struct StagedWork {
    pub token: ConnectionToken,
    pub slot: usize,
    pub status: u16,
    pub reason: Vec<u8>,
    pub body: Vec<u8>,
    pub cancel: bool,
    pub grant_owed: u32,
}

pub(crate) struct HostInner {
    pub(crate) events: Vec<VecDeque<HostEvent>>,
    pub(crate) parked: Vec<Vec<Option<ParkedEntry>>>,
    pub(crate) tombs: Vec<VecDeque<Tomb>>,
    seqs: Vec<Vec<u32>>,
    pub(crate) wakers: Vec<Option<WakeHandle>>,
    next_poll: usize,
    pub(crate) counters: HostCounters,
}

pub struct HostShared {
    inner: Mutex<HostInner>,
    wake: Condvar,
}

impl HostShared {
    pub fn new(worker_count: usize) -> Self {
        let mut events = Vec::with_capacity(worker_count);
        let mut parked = Vec::with_capacity(worker_count);
        let mut tombs = Vec::with_capacity(worker_count);
        let mut seqs = Vec::with_capacity(worker_count);
        let mut wakers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            events.push(VecDeque::with_capacity(HOST_QUEUE_DEPTH));
            let park_cap = super::MAX_CONNECTIONS + H2_PARK_SLOTS;
            parked.push((0..park_cap).map(|_| None).collect());
            tombs.push(VecDeque::with_capacity(HOST_TOMB_DEPTH));
            seqs.push(vec![0u32; super::MAX_CONNECTIONS + H2_PARK_SLOTS]);
            wakers.push(None);
        }
        Self {
            inner: Mutex::new(HostInner {
                events,
                parked,
                tombs,
                seqs,
                wakers,
                next_poll: 0,
                counters: HostCounters::default(),
            }),
            wake: Condvar::new(),
        }
    }

    pub fn worker_count(&self) -> usize {
        self.inner
            .lock()
            .map(|inner| inner.events.len())
            .unwrap_or(0)
    }

    pub fn register_waker(&self, worker_id: u32, waker: WakeHandle) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        if let Some(slot) = inner.wakers.get_mut(worker_id as usize) {
            *slot = Some(waker);
        }
    }

    // NOTE: the parked deadline reuses the AwaitingHandler (body-progress) timeout so an
    // abandoned park times out like an abandoned body. Only the event copy and the staged
    // response cross this lock, never parser pointers or IO.
    pub fn try_park(
        &self,
        worker_id: u32,
        token: ConnectionToken,
        route_id: u64,
        method: &[u8],
        path: &[u8],
        body_total: u64,
    ) -> Result<ConnectionToken, ParkError> {
        if method.is_empty()
            || method.len() > HOST_MAX_METHOD_BYTES
            || path.is_empty()
            || path.len() > HOST_MAX_PATH_BYTES
        {
            return Err(ParkError::SnapshotTooLarge);
        }
        let outcome = self.with_inner(|inner| {
            let worker = worker_id as usize;
            let slot = token.slot as usize;
            let vacant = inner
                .parked
                .get(worker)
                .and_then(|table| table.get(slot))
                .is_some_and(|entry| entry.is_none());
            let room = inner
                .events
                .get(worker)
                .is_some_and(|queue| queue.len() < HOST_QUEUE_DEPTH);
            if !vacant || !room {
                inner.counters.queue_full += 1;
                return (Err(ParkError::Full), None);
            }
            let seq = inner.seqs[worker][slot].wrapping_add(1);
            let seq = if seq == 0 { 1 } else { seq };
            inner.seqs[worker][slot] = seq;
            let event_token = ConnectionToken {
                request_seq: seq,
                ..token
            };
            let mut event = HostEvent {
                kind: HostEventKind::Request,
                token: event_token,
                route_id,
                method_len: method.len() as u8,
                method: [0; HOST_MAX_METHOD_BYTES],
                path_len: path.len() as u16,
                path: [0; HOST_MAX_PATH_BYTES],
                body_ready: 0,
                body_complete: false,
            };
            event.method[..method.len()].copy_from_slice(method);
            event.path[..path.len()].copy_from_slice(path);
            inner.events[worker].push_back(event);
            inner.parked[worker][slot] = Some(ParkedEntry::new(event_token, body_total, method));
            inner.counters.parks += 1;
            let waker = inner.wakers.get(worker).and_then(|slot| slot.clone());
            (Ok(event_token), waker)
        });
        let Some((result, waker)) = outcome else {
            return Err(ParkError::Full);
        };
        if result.is_ok() {
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
            self.wake.notify_one();
        }
        result
    }

    pub fn unpark_closed(&self, worker_id: u32, slot: u32) -> bool {
        let done = self.with_inner(|inner| {
            let worker = worker_id as usize;
            let entry = inner
                .parked
                .get_mut(worker)
                .and_then(|table| table.get_mut(slot as usize))
                .and_then(|entry| entry.take());
            // NOTE: the tomb records the sequenced token: only the exact parked exchange
            // observes Gone; anything else is Stale.
            match entry {
                Some(entry) => {
                    retire_signals(inner, worker, entry.token);
                    push_tomb(inner, worker, entry.token, TombCause::Gone);
                    (true, None)
                }
                None => (false, None),
            }
        });
        done.is_some_and(|(closed, _)| closed)
    }

    pub fn push_body(&self, token: ConnectionToken, bytes: &[u8]) -> bool {
        let done = self.with_inner(|inner| {
            if !push_staged_body(inner, token, bytes) {
                return (false, None);
            }
            if bytes.is_empty() {
                return (true, None);
            }
            if stage_body_available(inner, token.worker_id as usize, token) {
                return (true, None);
            }
            // NOTE: atomic admission: the signal has no coalesce target and no room, so the
            // appended bytes roll back and the caller stalls or fails closed like any refusal.
            retract_body_append(inner, token, bytes.len());
            (false, None)
        });
        let staged = done.is_some_and(|(ok, _)| ok);
        if staged && !bytes.is_empty() {
            self.wake.notify_one();
        }
        staged
    }

    pub(crate) fn push_body_or_answered(
        &self,
        token: ConnectionToken,
        bytes: &[u8],
    ) -> BodyPushOutcome {
        let done = self.with_inner(|inner| {
            let outcome = crate::connection::streams::push_body_or_answered(inner, token, bytes);
            if outcome != BodyPushOutcome::Pushed || bytes.is_empty() {
                return (outcome, None);
            }
            if stage_body_available(inner, token.worker_id as usize, token) {
                return (outcome, None);
            }
            retract_body_append(inner, token, bytes.len());
            (BodyPushOutcome::Refused, None)
        });
        let outcome = done
            .map(|(outcome, _)| outcome)
            .unwrap_or(BodyPushOutcome::Refused);
        if outcome == BodyPushOutcome::Pushed && !bytes.is_empty() {
            self.wake.notify_one();
        }
        outcome
    }

    // NOTE: H2 DATA admission stages under with_inner and reports an outcome instead of a
    // bool, so the poll wake rides this helper to mirror the push_body notify shape.
    pub(crate) fn notify_pollers(&self) {
        self.wake.notify_one();
    }

    pub fn stage_respond(
        &self,
        token: ConnectionToken,
        status: u16,
        reason: &[u8],
        body: &[u8],
    ) -> RespondOutcome {
        if !(200..=599).contains(&status)
            || reason.is_empty()
            || reason.len() > HOST_MAX_REASON_BYTES
            || !valid_reason(reason)
        {
            return RespondOutcome::Invalid;
        }
        if body.len() > MAX_RESPONSE_BODY {
            return RespondOutcome::BodyTooLarge;
        }
        let outcome = self.with_inner(|inner| {
            let worker = token.worker_id as usize;
            let slot = token.slot as usize;
            if inner.events.len() <= worker || token.generation == 0 {
                return (RespondOutcome::Invalid, None);
            }
            if let Some(entry) = inner
                .parked
                .get_mut(worker)
                .and_then(|table| table.get_mut(slot))
                .and_then(|entry| entry.as_mut())
            {
                // NOTE: a token mismatch falls through to the tomb search: the slot may hold
                // a newer exchange while the tomb records this exact token.
                if entry.token == token {
                    // NOTE: one-shot respond loses to an open or staged stream: the entry stays
                    // parked for pump-pull, so a late fixed respond reports Duplicate.
                    if entry.staged.is_some() || entry.stream.is_some() {
                        inner.counters.responds_duplicate += 1;
                        return (RespondOutcome::Duplicate, None);
                    }
                    entry.staged = Some(StagedAction::Respond {
                        status,
                        reason: reason.to_vec(),
                        body: body.to_vec(),
                    });
                    retire_signals(inner, worker, token);
                    inner.counters.responds_ok += 1;
                    let waker = inner.wakers.get(worker).and_then(|slot| slot.clone());
                    return (RespondOutcome::Ok, waker);
                }
            }
            let found = match find_tomb(inner, worker, token) {
                Some(TombCause::Responded) => RespondOutcome::Duplicate,
                Some(TombCause::Gone) => RespondOutcome::Gone,
                Some(TombCause::Cancelled) | None => RespondOutcome::Stale,
            };
            match found {
                RespondOutcome::Duplicate => inner.counters.responds_duplicate += 1,
                RespondOutcome::Gone => inner.counters.responds_gone += 1,
                _ => inner.counters.responds_stale += 1,
            }
            (found, None)
        });
        let Some((result, waker)) = outcome else {
            return RespondOutcome::Stale;
        };
        if result == RespondOutcome::Ok {
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
        }
        result
    }

    pub fn stage_cancel(&self, token: ConnectionToken) -> CancelOutcome {
        let outcome = self.with_inner(|inner| {
            let worker = token.worker_id as usize;
            let slot = token.slot as usize;
            if inner.events.len() <= worker || token.generation == 0 {
                return (CancelOutcome::Invalid, None);
            }
            let matches = inner
                .parked
                .get(worker)
                .and_then(|table| table.get(slot))
                .and_then(|entry| entry.as_ref())
                .is_some_and(|entry| entry.token == token && entry.staged.is_none());
            if !matches {
                inner.counters.cancels_stale += 1;
                return (CancelOutcome::Stale, None);
            }
            if let Some(entry) = inner.parked[worker][slot].as_mut() {
                entry.staged = Some(StagedAction::Cancel);
            }
            retire_signals(inner, worker, token);
            inner.counters.cancels_ok += 1;
            let waker = inner.wakers.get(worker).and_then(|slot| slot.clone());
            (CancelOutcome::Ok, waker)
        });
        let Some((result, waker)) = outcome else {
            return CancelOutcome::Stale;
        };
        if result == CancelOutcome::Ok {
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
        }
        result
    }

    pub fn pull_body(&self, token: ConnectionToken, out: &mut [u8]) -> BodyPull {
        let done = self.with_inner(|inner| (pull_staged_body(inner, token, out), None));
        done.map(|(r, _)| r)
            .unwrap_or((RespondOutcome::Stale, 0, false))
    }

    // NOTE: coalesced writable staging for the re-arm call sites (take_stream_cmd in
    // connection/streams.rs and the H2 re-arm loop in h2/serve.rs): call once where the arm
    // releases. A repeat while the signal is pending reports true without duplicating; a full
    // queue reports false and the arm state persists, so the next release re-signals and the
    // is_writable query stays authoritative throughout.
    pub fn stage_writable(&self, token: ConnectionToken) -> bool {
        let done = self.with_inner(|inner| {
            let worker = token.worker_id as usize;
            if inner.events.len() <= worker || token.generation == 0 {
                return (false, None);
            }
            (stage_writable_locked(inner, worker, token), None)
        });
        let staged = done.is_some_and(|(ok, _)| ok);
        if staged {
            self.wake.notify_one();
        }
        staged
    }

    pub fn take_staged(&self, worker_id: u32) -> Vec<StagedWork> {
        let taken = self.with_inner(|inner| {
            let worker = worker_id as usize;
            if inner.parked.len() <= worker {
                return (Vec::new(), None);
            }
            let mut staged = Vec::new();
            for slot in 0..inner.parked[worker].len() {
                let action = inner.parked[worker][slot]
                    .as_mut()
                    // NOTE: stream entries stay parked for pump-pull; take_staged never consumes them.
                    .filter(|entry| entry.stream.is_none())
                    .and_then(|entry| entry.staged.take());
                let Some(action) = action else { continue };
                let entry = inner.parked[worker][slot].take();
                let Some(entry) = entry else { continue };
                let cause = match &action {
                    StagedAction::Respond { .. } => TombCause::Responded,
                    StagedAction::Cancel => TombCause::Cancelled,
                };
                push_tomb(inner, worker, entry.token, cause);
                let (status, reason, body, cancel) = match action {
                    StagedAction::Respond {
                        status,
                        reason,
                        body,
                    } => (status, reason, body, false),
                    StagedAction::Cancel => (0, Vec::new(), Vec::new(), true),
                };
                let base = ConnectionToken {
                    request_seq: 0,
                    ..entry.token
                };
                let grant = entry.body_pulled.min(u64::from(u32::MAX)) as u32;
                staged.push(StagedWork {
                    token: base,
                    slot,
                    status,
                    reason,
                    body,
                    cancel,
                    grant_owed: grant,
                });
            }
            (staged, None)
        });
        taken.map(|(staged, _)| staged).unwrap_or_default()
    }

    // NOTE: whole-event dequeue: the first unfitting event reports required capacities
    // and stays queued; a later one ends the batch silently. The wait is level-triggered:
    // every enqueue notifies and pending is re-checked after each wake.
    pub fn poll(
        &self,
        timeout_ns: u64,
        events: &mut [EventSlot],
        methods: &mut [u8],
        paths: &mut [u8],
    ) -> PollReport {
        if events.is_empty() {
            let pending = self
                .inner
                .lock()
                .map(|inner| pending_total(&inner))
                .unwrap_or(0);
            return PollReport {
                events_required: pending,
                ..PollReport::default()
            };
        }
        let deadline = (timeout_ns != 0 && timeout_ns != u64::MAX)
            .then(|| Instant::now() + Duration::from_nanos(timeout_ns));
        let unbounded = timeout_ns == u64::MAX;
        let Ok(mut guard) = self.inner.lock() else {
            return PollReport::default();
        };
        loop {
            let report = drain_pending(&mut guard, events, methods, paths);
            if report.copied > 0 || report.events_required > 0 || report.method_required > 0 {
                return report;
            }
            let timed_out = match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        true
                    } else {
                        let wait = deadline - now;
                        guard = match self.wake.wait_timeout(guard, wait) {
                            Ok((guard, _)) => guard,
                            Err(_) => return PollReport::default(),
                        };
                        false
                    }
                }
                None if unbounded => {
                    guard = match self.wake.wait(guard) {
                        Ok(guard) => guard,
                        Err(_) => return PollReport::default(),
                    };
                    false
                }
                None => true,
            };
            if timed_out {
                return drain_pending(&mut guard, events, methods, paths);
            }
        }
    }

    // NOTE: best-effort on a full queue; the host must keep draining. Bounded retries
    // so a worker that outlives its host still exits instead of wedging the join.
    pub fn push_shutdown(&self, worker_id: u32) {
        for _ in 0..SHUTDOWN_PUSH_RETRIES {
            let pushed = self.with_inner(|inner| {
                let worker = worker_id as usize;
                let room = inner
                    .events
                    .get(worker)
                    .is_some_and(|queue| queue.len() < HOST_QUEUE_DEPTH);
                if !room {
                    return (false, None);
                }
                inner.events[worker].push_back(HostEvent {
                    kind: HostEventKind::Shutdown,
                    token: ConnectionToken {
                        worker_id,
                        slot: u32::MAX,
                        request_seq: 0,
                        stream_id: 0,
                        worker_generation: 0,
                        generation: 0,
                    },
                    route_id: 0,
                    method_len: 0,
                    method: [0; HOST_MAX_METHOD_BYTES],
                    path_len: 0,
                    path: [0; HOST_MAX_PATH_BYTES],
                    body_ready: 0,
                    body_complete: false,
                });
                inner.counters.shutdowns += 1;
                (true, None)
            });
            if pushed.is_some_and(|(done, _)| done) {
                self.wake.notify_all();
                return;
            }
            std::thread::yield_now();
        }
    }

    pub fn counters(&self) -> HostCounters {
        self.inner
            .lock()
            .map(|inner| inner.counters)
            .unwrap_or_default()
    }

    pub fn parked_count(&self) -> usize {
        self.inner
            .lock()
            .map(|inner| {
                inner
                    .parked
                    .iter()
                    .map(|table| table.iter().filter(|entry| entry.is_some()).count())
                    .sum()
            })
            .unwrap_or(0)
    }

    // NOTE: the closure runs under the queue lock and must only copy bytes. The waker
    // clone leaves under the guard; wake() runs after unlock.
    pub(crate) fn with_inner<T>(
        &self,
        op: impl FnOnce(&mut MutexGuard<'_, HostInner>) -> (T, Option<WakeHandle>),
    ) -> Option<(T, Option<WakeHandle>)> {
        let mut guard = self.inner.lock().ok()?;
        Some(op(&mut guard))
    }
}

#[derive(Clone)]
pub struct HostHandle {
    pub(crate) shared: Arc<HostShared>,
}

impl HostHandle {
    pub fn new(shared: Arc<HostShared>) -> Self {
        Self { shared }
    }

    pub fn poll(
        &self,
        timeout_ns: u64,
        events: &mut [EventSlot],
        methods: &mut [u8],
        paths: &mut [u8],
    ) -> PollReport {
        self.shared.poll(timeout_ns, events, methods, paths)
    }

    pub fn respond(
        &self,
        token: ConnectionToken,
        status: u16,
        reason: &[u8],
        body: &[u8],
    ) -> RespondOutcome {
        self.shared.stage_respond(token, status, reason, body)
    }

    pub fn cancel(&self, token: ConnectionToken) -> CancelOutcome {
        self.shared.stage_cancel(token)
    }

    pub fn pull_body(&self, token: ConnectionToken, out: &mut [u8]) -> BodyPull {
        self.shared.pull_body(token, out)
    }

    pub fn counters(&self) -> HostCounters {
        self.shared.counters()
    }

    pub fn parked_count(&self) -> usize {
        self.shared.parked_count()
    }
}

fn push_tomb(inner: &mut HostInner, worker: usize, token: ConnectionToken, cause: TombCause) {
    let Some(queue) = inner.tombs.get_mut(worker) else {
        return;
    };
    if queue.len() >= HOST_TOMB_DEPTH {
        queue.pop_front();
    }
    queue.push_back(Tomb { token, cause });
}

pub(crate) fn find_tomb(
    inner: &HostInner,
    worker: usize,
    token: ConnectionToken,
) -> Option<TombCause> {
    inner
        .tombs
        .get(worker)?
        .iter()
        .rev()
        .find(|tomb| tomb.token == token)
        .map(|tomb| tomb.cause)
}

fn pending_total(inner: &HostInner) -> usize {
    inner.events.iter().map(|queue| queue.len()).sum()
}

fn drain_pending(
    inner: &mut HostInner,
    events: &mut [EventSlot],
    methods: &mut [u8],
    paths: &mut [u8],
) -> PollReport {
    let mut report = PollReport::default();
    if events.is_empty() || inner.events.is_empty() {
        return report;
    }
    let mut method_used = 0usize;
    let mut path_used = 0usize;
    let workers = inner.events.len();
    let mut empty_workers = 0;
    let mut cursor = inner.next_poll % workers;
    while report.copied < events.len() && empty_workers < workers {
        let front = inner.events[cursor].front();
        let Some(event) = front else {
            empty_workers += 1;
            cursor = (cursor + 1) % workers;
            continue;
        };
        let (kind, token, route_id) = (event.kind, event.token, event.route_id);
        // NOTE: signal events carry advisory counts in the reused length fields and no bytes:
        // BodyAvailable reports staged-ready bytes with the end flag, Writable carries the
        // token alone. Both always fit; only Request/Shutdown touch the caller buffers.
        let signal = matches!(kind, HostEventKind::BodyAvailable | HostEventKind::Writable);
        let (method_len, path_len) = if signal {
            (event.body_ready, usize::from(event.body_complete))
        } else {
            (usize::from(event.method_len), usize::from(event.path_len))
        };
        empty_workers = 0;
        if !signal
            && (method_used + method_len > methods.len() || path_used + path_len > paths.len())
        {
            if report.copied == 0 {
                report.method_required = method_len;
                report.path_required = path_len;
            }
            break;
        }
        let event = inner.events[cursor].pop_front();
        let Some(event) = event else { break };
        let (method_offset, path_offset) = if signal {
            (0, 0)
        } else {
            methods[method_used..method_used + method_len]
                .copy_from_slice(&event.method[..method_len]);
            paths[path_used..path_used + path_len].copy_from_slice(&event.path[..path_len]);
            (method_used, path_used)
        };
        events[report.copied] = EventSlot {
            kind,
            token,
            route_id,
            method_len,
            path_len,
            method_offset,
            path_offset,
        };
        if !signal {
            method_used += method_len;
            path_used += path_len;
        }
        report.copied += 1;
        cursor = (cursor + 1) % workers;
    }
    inner.next_poll = cursor;
    if report.method_required == 0 && report.copied == events.len() {
        let remaining: usize = inner.events.iter().map(|queue| queue.len()).sum();
        if remaining > 0 {
            report.events_required = remaining;
        }
    }
    report
}

pub(crate) fn valid_reason(reason: &[u8]) -> bool {
    !reason.is_empty()
        && !reason.iter().any(|byte| {
            *byte == b'\r' || *byte == b'\n' || (*byte < 0x20 && *byte != b'\t') || *byte == 0x7f
        })
}
