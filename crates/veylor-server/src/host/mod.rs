use std::collections::VecDeque;
use std::sync::{
    Arc, Condvar, Mutex, MutexGuard,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use veylor_http1::{is_token, valid_field_value};
use veylor_runtime::{ConnectionToken, WakeHandle};
use veylor_tls::{CertificateDer, PrivateKeyDer, TlsAcceptor, TlsError};

mod request;
mod signals;
mod staged;
mod tunnel;

// NOTE: advisory signal staging (BodyAvailable/Writable stage, admission rollback, signal
// retire) lives in signals.rs; re-exported here so existing crate::host:: paths resolve.
pub use request::{
    MAX_REQUEST_HEADER_BYTES, MAX_REQUEST_HEADERS, RequestHeadError, RequestHeadReport,
    RequestHeader, RequestSpan,
};
pub(crate) use signals::{stage_body_available, stage_tunnel_signal};

// NOTE: park/stage/take booking lives in staged.rs; re-exported here so existing
// crate::host:: paths resolve.
pub use staged::StagedWork;
pub(crate) use staged::{ParkedEntry, StagedAction};
pub(crate) use tunnel::HOST_TUNNEL_CAP;

// NOTE: bounded per-worker event queues with whole-event dequeue and required-capacity
// reporting, level-triggered wakeups, one Shutdown event per worker. Fixed-length bodies
// stage per parked record up to the shared static cap: the worker pushes wire bytes,
// the host pulls, completion is observed.

// NOTE: bounded per-worker event queue depth; a full queue reports required capacity
// instead of growing.
pub const HOST_QUEUE_DEPTH: usize = 64;
// NOTE: H2 stream-keyed park range appended to the parked table: 8 conns x 32 subslots,
// indexed MAX_CONNECTIONS + conn * 32 + subslot; sized from the H2 turn constants.
pub(crate) const H2_PARK_SLOTS: usize = crate::h2::MAX_H2_CONNS * crate::h2::STREAM_SLOTS;
pub const HOST_MAX_METHOD_BYTES: usize = 16;
pub const HOST_MAX_PATH_BYTES: usize = 2048;
pub const HOST_MAX_REASON_BYTES: usize = 64;
pub const MAX_STAGED_RESPONSE_HEADERS: usize = 8;
pub const MAX_STAGED_RESPONSE_BYTES: usize = 1024;
pub const MAX_STAGED_RESPONSE_NAME_BYTES: usize = 128;

pub(crate) fn valid_response_headers(headers: &[(&[u8], &[u8])]) -> bool {
    if headers.len() > MAX_STAGED_RESPONSE_HEADERS {
        return false;
    }
    let mut total = 0;
    for (name, value) in headers {
        if name.is_empty()
            || name.len() > MAX_STAGED_RESPONSE_NAME_BYTES
            || !name.iter().copied().all(is_token)
            || is_framing_header(name)
            || !valid_field_value(value)
        {
            return false;
        }
        total += name.len() + value.len();
        if total > MAX_STAGED_RESPONSE_BYTES {
            return false;
        }
    }
    true
}

fn is_framing_header(name: &[u8]) -> bool {
    name.eq_ignore_ascii_case(b"content-length")
        || name.eq_ignore_ascii_case(b"transfer-encoding")
        || name.eq_ignore_ascii_case(b"connection")
        || name.eq_ignore_ascii_case(b"keep-alive")
        || name.eq_ignore_ascii_case(b"proxy-connection")
        || name.eq_ignore_ascii_case(b"upgrade")
        || name.eq_ignore_ascii_case(b"date")
}
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
    TunnelData,
    TunnelClosed,
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
    pub shutdowns_dropped: u64,
    pub tombs_evicted: u64,
}

pub(crate) struct HostInner {
    pub(crate) events: Vec<VecDeque<HostEvent>>,
    pub(crate) parked: Vec<Vec<Option<ParkedEntry>>>,
    pub(crate) tombs: Vec<VecDeque<Tomb>>,
    seqs: Vec<Vec<u32>>,
    pub(crate) wakers: Vec<Option<WakeHandle>>,
    next_poll: usize,
    pub(crate) counters: HostCounters,
    tls_rotation: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    tls_rotation_epoch: u64,
    staged_epoch: Vec<u64>,
}

pub struct HostShared {
    inner: Mutex<HostInner>,
    wake: Condvar,
    tls_rotation_epoch: AtomicU64,
    staged_epoch: Vec<AtomicU64>,
    h2_tunnels: Vec<AtomicU64>,
}

impl HostShared {
    pub fn new(worker_count: usize) -> Self {
        let mut events = Vec::with_capacity(worker_count);
        let mut parked = Vec::with_capacity(worker_count);
        let mut tombs = Vec::with_capacity(worker_count);
        let mut seqs = Vec::with_capacity(worker_count);
        let mut wakers = Vec::with_capacity(worker_count);
        let mut staged_epoch = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            events.push(VecDeque::with_capacity(HOST_QUEUE_DEPTH));
            let park_cap = super::MAX_CONNECTIONS + H2_PARK_SLOTS;
            parked.push((0..park_cap).map(|_| None).collect());
            tombs.push(VecDeque::with_capacity(HOST_TOMB_DEPTH));
            seqs.push(vec![0u32; super::MAX_CONNECTIONS + H2_PARK_SLOTS]);
            wakers.push(None);
            staged_epoch.push(AtomicU64::new(0));
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
                tls_rotation: None,
                tls_rotation_epoch: 0,
                staged_epoch: vec![0; worker_count],
            }),
            wake: Condvar::new(),
            tls_rotation_epoch: AtomicU64::new(0),
            staged_epoch,
            h2_tunnels: (0..worker_count).map(|_| AtomicU64::new(0)).collect(),
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

    // NOTE: H2 DATA admission stages under with_inner and reports an outcome instead of a
    // bool, so the poll wake rides this helper to mirror the push_body notify shape.
    pub(crate) fn notify_pollers(&self) {
        self.wake.notify_one();
    }

    pub(crate) fn tls_rotation_epoch(&self) -> u64 {
        self.tls_rotation_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn stage_tls_rotation(
        &self,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<u64, TlsError> {
        TlsAcceptor::from_chain(chain.clone(), key.clone_key(), 1)?;

        let epoch = self
            .with_inner(|inner| {
                inner.tls_rotation_epoch += 1;
                inner.tls_rotation = Some((chain, key));

                (inner.tls_rotation_epoch, None)
            })
            .map(|(epoch, _)| epoch)
            .ok_or(TlsError::InvalidInput)?;

        self.tls_rotation_epoch.store(epoch, Ordering::Release);

        Ok(epoch)
    }

    pub(crate) fn take_tls_rotation(
        &self,
        seen_epoch: u64,
    ) -> Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>, u64)> {
        if self.tls_rotation_epoch() <= seen_epoch {
            return None;
        }

        self.with_inner(|inner| {
            let epoch = inner.tls_rotation_epoch;
            let rotation = if epoch > seen_epoch {
                inner
                    .tls_rotation
                    .as_ref()
                    .map(|(chain, key)| (chain.clone(), key.clone_key(), epoch))
            } else {
                None
            };

            (rotation, None)
        })
        .and_then(|(rotation, _)| rotation)
    }

    // NOTE: whole-event dequeue: the first unfitting event reports required capacities
    // and stays queued; a later one ends the batch silently. The wait is level-triggered:
    // every enqueue notifies and pending is re-checked after each wake.
    // Timeout convention: 0 takes a single non-blocking pass, u64::MAX waits for the
    // next wakeup; anything else bounds the wait. Mirrored in the C ABI docs.
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
        // NOTE: best-effort give-up is observable, not silent: a full queue across all
        // retries means the worker exits on host drop without its shutdown event.
        self.with_inner(|inner| {
            inner.counters.shutdowns_dropped += 1;
            ((), None)
        });
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
        self.shared.stage_respond(token, status, reason, &[], body)
    }

    pub fn respond_with_headers(
        &self,
        token: ConnectionToken,
        status: u16,
        reason: &[u8],
        headers: &[(&[u8], &[u8])],
        body: &[u8],
    ) -> RespondOutcome {
        self.shared
            .stage_respond(token, status, reason, headers, body)
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

pub(crate) fn push_tomb(
    inner: &mut HostInner,
    worker: usize,
    token: ConnectionToken,
    cause: TombCause,
) {
    let Some(queue) = inner.tombs.get_mut(worker) else {
        return;
    };
    if queue.len() >= HOST_TOMB_DEPTH {
        // NOTE: lossy by design (tombs are hints; correctness rides generations), but the
        // eviction is counted so a late poller missing a tomb is explainable, not silent.
        queue.pop_front();
        inner.counters.tombs_evicted += 1;
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
        let signal = matches!(
            kind,
            HostEventKind::BodyAvailable
                | HostEventKind::Writable
                | HostEventKind::TunnelData
                | HostEventKind::TunnelClosed
        );
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
