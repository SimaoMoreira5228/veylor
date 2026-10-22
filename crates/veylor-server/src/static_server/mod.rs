use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use veylor_router::RouteSlot;
use veylor_runtime::{
    AcceptedConnection, AdmittedConnection, ConnectionDeadline, ConnectionDeadlineKind,
    ConnectionPhase, ConnectionSlot, ConnectionTimeouts, ConnectionToken, PollInterest, Poller,
    Readiness, ShutdownPhase, ShutdownState, TIMER_INDEX_STORAGE_FACTOR, TcpListener,
    TimerExpiration, TurnConfig, TurnInput, WAKE_TOKEN, WakeHandle, Worker, WorkerError,
    WorkerStorage, bind_reuse_port,
};

use crate::connection::{ConnClose, ConnState, PumpOutcome, PumpProgress, pump_connection};
use crate::host::{HostHandle, HostShared};
use crate::multi_worker::{ThreadCtx, ThreadResult};
use crate::static_routes::{MAX_ROUTES, RouteBuildError, StaticRoutes};

pub const MAX_CONNECTIONS: usize = 128;
pub const MAX_WORKERS: usize = 16;
pub const LISTENER_TOKEN: usize = 0;
pub const SHUTDOWN_TIMER_ID: u64 = 0xFFFF_FFFE;
pub(crate) const MAX_EVENTS_PER_TURN: usize = 64;
const MAX_EXPIRED_PER_TURN: usize = 64;
const SHUTDOWN_GRACE_DEFAULT_NS: u64 = 5_000_000_000;
const SENTINEL_SLOT: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CloseCounts {
    pub completed: u64,
    pub peer_closed: u64,
    pub reset: u64,
    pub timeout: u64,
    pub parked_timeout: u64,
    pub queue_full: u64,
    pub cancelled: u64,
    pub protocol_error: u64,
    pub response_overflow: u64,
    pub transport_error: u64,
    pub shutdown: u64,
}

impl CloseCounts {
    fn add(&mut self, reason: ConnClose) {
        let count = match reason {
            ConnClose::Completed => &mut self.completed,
            ConnClose::PeerClosed => &mut self.peer_closed,
            ConnClose::Reset => &mut self.reset,
            ConnClose::Timeout => &mut self.timeout,
            // NOTE: genuinely distinct from Timeout/TransportError: an abandoned park
            // means the host never responded (host health), and a full host queue means
            // overload at the dispatch boundary (S4.2 quota recovery needs both split
            // out). Cancelled is host-initiated, unlike drain Shutdown closes.
            ConnClose::ParkedTimeout => &mut self.parked_timeout,
            ConnClose::QueueFull => &mut self.queue_full,
            ConnClose::Cancelled => &mut self.cancelled,
            ConnClose::ProtocolError => &mut self.protocol_error,
            ConnClose::ResponseOverflow => &mut self.response_overflow,
            ConnClose::TransportError => &mut self.transport_error,
            ConnClose::Shutdown => &mut self.shutdown,
        };
        *count += 1;
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    pub timeouts: ConnectionTimeouts,
    pub shutdown_grace_ns: u64,
    pub drain_idle_linger_ns: u64,
    pub turn: TurnConfig,
    pub poll_wait_ms: i32,
    pub worker_count: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            timeouts: ConnectionTimeouts {
                request_head_ns: 5_000_000_000,
                body_progress_ns: 5_000_000_000,
                response_write_ns: 5_000_000_000,
                keep_alive_idle_ns: 30_000_000_000,
            },
            shutdown_grace_ns: SHUTDOWN_GRACE_DEFAULT_NS,
            drain_idle_linger_ns: 200_000_000,
            turn: TurnConfig::DEFAULT,
            poll_wait_ms: 10,
            worker_count: 1,
        }
    }
}

#[derive(Debug)]
pub enum ServerError {
    InvalidConfig,
    InvalidRoute,
    DuplicateRoute,
    RouteCapacity,
    ResponseTooLarge,
    Bind(io::Error),
    Poll(io::Error),
    Thread(String),
    Join(String),
}

impl From<RouteBuildError> for ServerError {
    fn from(error: RouteBuildError) -> Self {
        match error {
            RouteBuildError::Duplicate => Self::DuplicateRoute,
            RouteBuildError::Full => Self::RouteCapacity,
            RouteBuildError::BodyTooLarge => Self::ResponseTooLarge,
            RouteBuildError::Invalid => Self::InvalidRoute,
        }
    }
}

pub struct Server {
    pub(crate) listener: TcpListener,
    pub(crate) config: ServerConfig,
    pub(crate) routes: StaticRoutes,
    pub(crate) host: Arc<HostShared>,
}

impl Server {
    pub fn bind(addr: SocketAddr, config: ServerConfig) -> Result<Self, ServerError> {
        validate_config(&config)?;
        let listener = if config.worker_count == 1 {
            TcpListener::bind(addr).map_err(ServerError::Bind)?
        } else {
            bind_reuse_port(addr).map_err(ServerError::Bind)?
        };
        Ok(Self {
            listener,
            config,
            routes: StaticRoutes::new(),
            host: Arc::new(HostShared::new(config.worker_count)),
        })
    }

    pub fn addr(&self) -> Result<SocketAddr, ServerError> {
        self.listener.local_addr().map_err(ServerError::Bind)
    }

    pub fn register_route(
        &mut self,
        method: &[u8],
        path: &[u8],
        status: u16,
        reason: &[u8],
        body: &[u8],
    ) -> Result<(), ServerError> {
        self.routes
            .register(method, path, status, reason, body)
            .map_err(ServerError::from)
    }

    pub fn register_managed(&mut self, method: &[u8], path: &[u8]) -> Result<(), ServerError> {
        self.routes
            .register_managed(method, path)
            .map_err(ServerError::from)
    }

    pub fn start(self) -> Result<RunningServer, ServerError> {
        freeze_check(&self.routes)?;
        if self.config.worker_count > 1 {
            return crate::multi_worker::start_multi(self);
        }
        let Server {
            mut listener,
            config,
            routes,
            host,
        } = self;
        let mut worker_poller = Poller::new(MAX_EVENTS_PER_TURN).map_err(ServerError::Poll)?;
        worker_poller
            .register(&mut listener, LISTENER_TOKEN, PollInterest::Readable)
            .map_err(ServerError::Poll)?;
        let waker = worker_poller.create_waker().map_err(ServerError::Poll)?;
        let addr = listener.local_addr().map_err(ServerError::Bind)?;
        let stop = Arc::new(AtomicBool::new(false));
        let force = Arc::new(AtomicBool::new(false));
        let ctx = ThreadCtx {
            worker_id: 0,
            listener,
            config,
            routes: Arc::new(routes),
            host: Arc::clone(&host),
            worker_poller,
            stop: Arc::clone(&stop),
            force: Arc::clone(&force),
            peers: Vec::new(),
        };
        let handle = thread::Builder::new()
            .name("veylor-worker".to_string())
            .spawn(|| crate::multi_worker::run_worker(ctx))
            .map_err(|error| ServerError::Thread(error.to_string()))?;
        Ok(RunningServer {
            handles: vec![handle],
            wakers: vec![waker],
            stop,
            force,
            addr,
            host: HostHandle::new(host),
        })
    }
}

pub struct RunningServer {
    pub(crate) handles: Vec<JoinHandle<ThreadResult>>,
    pub(crate) wakers: Vec<WakeHandle>,
    pub(crate) stop: Arc<AtomicBool>,
    pub(crate) force: Arc<AtomicBool>,
    pub(crate) addr: SocketAddr,
    pub(crate) host: HostHandle,
}

impl RunningServer {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn host(&self) -> HostHandle {
        self.host.clone()
    }

    pub fn stop_accepting(&self) {
        self.stop.store(true, Ordering::Release);
        for waker in &self.wakers {
            let _ = waker.wake();
        }
    }

    pub fn stop_force(&self) {
        self.force.store(true, Ordering::Release);
        self.stop.store(true, Ordering::Release);
        for waker in &self.wakers {
            let _ = waker.wake();
        }
    }

    pub fn join(self) -> Result<CloseCounts, ServerError> {
        let mut total = CloseCounts::default();
        for worker in self.join_per_worker()? {
            total.completed += worker.completed;
            total.peer_closed += worker.peer_closed;
            total.reset += worker.reset;
            total.timeout += worker.timeout;
            total.parked_timeout += worker.parked_timeout;
            total.queue_full += worker.queue_full;
            total.cancelled += worker.cancelled;
            total.protocol_error += worker.protocol_error;
            total.response_overflow += worker.response_overflow;
            total.transport_error += worker.transport_error;
            total.shutdown += worker.shutdown;
        }
        Ok(total)
    }

    pub fn join_per_worker(self) -> Result<Vec<CloseCounts>, ServerError> {
        let mut counts = Vec::with_capacity(self.handles.len());
        let mut first_error: Option<ServerError> = None;
        for handle in self.handles {
            match handle.join().map(ThreadResult::into_counts) {
                Ok(Ok(worker)) => counts.push(worker),
                Ok(Err(error)) => first_error = first_error.or(Some(error)),
                Err(_) => {
                    let panic = ServerError::Join("worker thread panicked".to_string());
                    first_error = first_error.or(Some(panic));
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(counts),
        }
    }
}

fn validate_config(config: &ServerConfig) -> Result<(), ServerError> {
    config
        .turn
        .validate()
        .map_err(|_| ServerError::InvalidConfig)?;
    if config.worker_count == 0 || config.worker_count > MAX_WORKERS {
        return Err(ServerError::InvalidConfig);
    }
    if config.poll_wait_ms <= 0 {
        return Err(ServerError::InvalidConfig);
    }
    let timeouts = config.timeouts;
    if timeouts.request_head_ns == 0
        || timeouts.body_progress_ns == 0
        || timeouts.response_write_ns == 0
        || timeouts.keep_alive_idle_ns == 0
    {
        return Err(ServerError::InvalidConfig);
    }
    Ok(())
}

fn freeze_check(routes: &StaticRoutes) -> Result<(), ServerError> {
    let mut slots = [RouteSlot::EMPTY; MAX_ROUTES];
    routes.build_table(&mut slots).map_err(ServerError::from)?;
    Ok(())
}

fn conn_poll_token(slot: usize) -> usize {
    slot + 1
}

pub(crate) fn worker_main(ctx: ThreadCtx) -> ThreadResult {
    let ThreadCtx {
        worker_id,
        mut listener,
        config,
        routes,
        host,
        mut worker_poller,
        stop,
        force,
        peers: _,
    } = ctx;
    let mut result = ThreadResult {
        counts: CloseCounts::default(),
        error: None,
    };
    let mut stream_poller = match Poller::new(MAX_CONNECTIONS * 2) {
        Ok(poller) => poller,
        Err(error) => {
            result.error = Some(error.to_string());
            return result;
        }
    };
    // NOTE: host respond/cancel stages a command and wakes this poller so a parked
    // worker picks it up promptly instead of sleeping out the poll cadence.
    match stream_poller.create_waker() {
        Ok(waker) => host.register_waker(worker_id, waker),
        Err(error) => {
            result.error = Some(error.to_string());
            return result;
        }
    };
    let mut route_slots = [RouteSlot::EMPTY; MAX_ROUTES];
    let table = match routes.build_table(&mut route_slots) {
        Ok(table) => table,
        Err(_) => {
            result.error = Some("route freeze failed".to_string());
            return result;
        }
    };

    let mut slots = vec![ConnectionSlot::EMPTY; MAX_CONNECTIONS];
    let mut free_stack = vec![0u32; MAX_CONNECTIONS];
    let timer_cap = MAX_CONNECTIONS + 2;
    let mut timer_storage = vec![TimerExpiration::default(); timer_cap];
    let mut timer_index = vec![0u32; timer_cap * TIMER_INDEX_STORAGE_FACTOR];
    let staging_cap = config.turn.max_accepts_per_turn.max(1);
    let mut staging: Vec<Option<AcceptedConnection>> = (0..staging_cap).map(|_| None).collect();
    let mut live: Vec<Option<ConnectionToken>> = (0..MAX_CONNECTIONS).map(|_| None).collect();
    let mut shutdown = ShutdownState::new();
    let storage = WorkerStorage {
        slots: &mut slots,
        free_stack: &mut free_stack,
        timer_storage: &mut timer_storage,
        timer_index_storage: &mut timer_index,
        staging: &mut staging,
        live: &mut live,
    };
    let mut worker = match Worker::new(
        worker_id,
        1,
        config.turn,
        config.timeouts,
        storage,
        &mut shutdown,
        &mut worker_poller,
    ) {
        Ok(worker) => worker,
        Err(_) => {
            result.error = Some("worker init failed".to_string());
            return result;
        }
    };

    let mut conns: Vec<Option<ConnState>> = (0..MAX_CONNECTIONS).map(|_| None).collect();
    let mut events: Vec<Readiness> = (0..MAX_EVENTS_PER_TURN)
        .map(|_| Readiness {
            token: WAKE_TOKEN,
            readable: false,
            writable: false,
            read_closed_hint: false,
            write_closed_hint: false,
            error_hint: false,
        })
        .collect();
    let mut expired: Vec<ConnectionDeadline> = (0..MAX_EXPIRED_PER_TURN)
        .map(|_| sentinel_deadline())
        .collect();
    let mut admitted: Vec<Option<AdmittedConnection>> = (0..staging_cap).map(|_| None).collect();
    let mut ready: Vec<Readiness> = Vec::new();
    let base = Instant::now();
    let mut turn_offset = 0usize;
    let mut stop_seen = false;
    let mut drain_start_ns = 0u64;

    loop {
        let now_ns = base.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        if force.load(Ordering::Acquire) {
            worker.force_shutdown();
        } else if stop.load(Ordering::Acquire) && worker.shutdown_phase() == ShutdownPhase::Running
        {
            // NOTE: the first observation runs one normal turn so SYNs already in
            // the backlog are admitted and their in-flight bytes served; only the
            // next iteration enters drain. Without the sweep, stop-before-accept
            // leaves backlog connections to RST on listener close.
            if !stop_seen {
                stop_seen = true;
            } else {
                let result =
                    worker.begin_shutdown(now_ns, config.shutdown_grace_ns, SHUTDOWN_TIMER_ID);
                if result == veylor_runtime::ShutdownResult::Draining {
                    drain_start_ns = now_ns;
                }
            }
        }
        expired.fill(sentinel_deadline());
        // NOTE: the loop wait rides the stream poller while streams are registered, so
        // stream readiness wakes the wait instead of every wave paying the worker-poller
        // cadence; Worker::turn then runs at zero wait and still drains accepts/expires
        // timers each iteration. With zero streams the stream poll is skipped and the
        // worker poller carries the wait as before (no spin either way). Stop/force wake
        // only worker-poller wakers, so a stop sleeping in the stream poll is observed at
        // loop top within one stream cadence; new accepts likewise wait out one cadence.
        let live_streams = worker.live_count();
        let pending = work_pending(&conns);
        let stream_wait_ms = if pending { 0 } else { config.poll_wait_ms };
        let worker_wait_ms = if live_streams == 0 { stream_wait_ms } else { 0 };
        if live_streams > 0 {
            ready.clear();
            let poll_error: Option<io::Error> =
                match stream_poller.poll(Some(Duration::from_millis(stream_wait_ms as u64))) {
                    Ok(readiness) => {
                        ready.extend(readiness.take(MAX_CONNECTIONS * 2));
                        None
                    }
                    Err(error) => Some(error),
                };
            for event in ready.drain(..) {
                if event.token == LISTENER_TOKEN || event.token == WAKE_TOKEN {
                    continue;
                }
                let Some(conn) = conns
                    .get_mut(event.token.wrapping_sub(1))
                    .and_then(|slot| slot.as_mut())
                else {
                    continue;
                };
                if event.readable || event.read_closed_hint {
                    conn.mark_readable();
                }
                if event.writable {
                    conn.mark_writable();
                }
                if event.error_hint {
                    conn.mark_error();
                }
            }
            if let Some(error) = poll_error {
                result.error = Some(error.to_string());
                cleanup_all(
                    &host,
                    &mut conns,
                    &mut stream_poller,
                    &mut worker,
                    &mut result.counts,
                );
                worker.destroy();
                host.push_shutdown(worker_id);
                break;
            }
        }
        let input = TurnInput {
            listener_token: LISTENER_TOKEN,
            now_ns,
            max_wait_ms: worker_wait_ms,
            events: &mut events,
            expired: &mut expired,
            admitted: &mut admitted,
        };
        let turn_result = worker.turn(&mut listener, input);
        drain_admitted(
            &mut admitted,
            &mut conns,
            &mut stream_poller,
            &mut worker,
            &mut result.counts,
        );
        drain_expired(
            &expired,
            &host,
            &mut conns,
            &mut stream_poller,
            &mut worker,
            &mut result.counts,
        );
        let draining = worker.shutdown_phase() != ShutdownPhase::Running;
        drain_host_staged(
            &host,
            worker_id,
            &mut conns,
            &mut stream_poller,
            &mut worker,
            &mut result.counts,
            now_ns,
        );
        match turn_result {
            Ok(outcome) => {
                if outcome.shutdown_complete {
                    cleanup_all(
                        &host,
                        &mut conns,
                        &mut stream_poller,
                        &mut worker,
                        &mut result.counts,
                    );
                    worker.destroy();
                    host.push_shutdown(worker_id);
                    break;
                }
            }
            Err(WorkerError::SlotCapacity) => {}
            Err(error) => {
                result.error = Some(format!("worker turn failed: {error:?}"));
                cleanup_all(
                    &host,
                    &mut conns,
                    &mut stream_poller,
                    &mut worker,
                    &mut result.counts,
                );
                worker.destroy();
                host.push_shutdown(worker_id);
                break;
            }
        }

        let live_count = worker.live_count().max(1) as u64;
        let per_conn_budget = config.turn.byte_budget_per_turn.div_ceil(live_count).max(1);
        for step in 0..MAX_CONNECTIONS {
            let index = (turn_offset + step) % MAX_CONNECTIONS;
            let Some(conn) = conns[index].as_mut() else {
                continue;
            };
            if draining {
                conn.begin_drain();
            }
            let hook_close = match pump_connection(
                conn,
                &routes,
                &table,
                per_conn_budget,
                worker_id,
                &host,
                draining,
            ) {
                PumpOutcome::Keep(progress) => {
                    let token = conn.token();
                    apply_pump_hooks(&mut worker, token, progress, now_ns)
                }
                PumpOutcome::Close(reason) => Some(reason),
            };
            if let Some(reason) = hook_close {
                close_conn(
                    index,
                    reason,
                    &host,
                    &mut conns,
                    &mut stream_poller,
                    &mut worker,
                    &mut result.counts,
                );
            }
        }
        turn_offset = (turn_offset + 1) % MAX_CONNECTIONS;

        // NOTE: drain serves in-flight exchanges to completion (each conn closes
        // after its current response via begin_drain); conns still idle past the
        // linger close promptly instead of waiting out their admission deadline.
        if draining
            && drain_start_ns > 0
            && now_ns.saturating_sub(drain_start_ns) >= config.drain_idle_linger_ns
        {
            close_idle_for_drain(
                &host,
                &mut conns,
                &mut stream_poller,
                &mut worker,
                &mut result.counts,
            );
        }
    }
    result
}

fn sentinel_deadline() -> ConnectionDeadline {
    ConnectionDeadline {
        token: ConnectionToken {
            worker_id: u32::MAX,
            slot: SENTINEL_SLOT,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 0,
            generation: 0,
        },
        timer_id: 0,
        kind: ConnectionDeadlineKind::RequestHead,
    }
}

fn drain_admitted(
    admitted: &mut [Option<AdmittedConnection>],
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
) {
    for entry in admitted.iter_mut() {
        let Some(admitted_conn) = entry.take() else {
            continue;
        };
        let slot = admitted_conn.token.slot as usize;
        let mut registered = false;
        if slot < conns.len() && conns[slot].is_none() {
            let mut stream = admitted_conn.stream;
            if stream_poller
                .register(&mut stream, conn_poll_token(slot), PollInterest::Readable)
                .is_ok()
            {
                conns[slot] = Some(ConnState::new(admitted_conn.token, stream));
                registered = true;
            } else {
                let _ = worker.close(admitted_conn.token);
            }
        } else {
            let _ = worker.close(admitted_conn.token);
        }
        if !registered {
            counts.add(ConnClose::TransportError);
        }
    }
}

fn drain_expired(
    expired: &[ConnectionDeadline],
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
) {
    for deadline in expired {
        if deadline.token.slot == SENTINEL_SLOT {
            break;
        }
        let slot = deadline.token.slot as usize;
        let timed_out = conns
            .get(slot)
            .and_then(|entry| entry.as_ref())
            .is_some_and(|conn| conn.token() == deadline.token);
        if timed_out {
            // NOTE: an abandoned park (host never responded) reports ParkedTimeout, not
            // Timeout, so host health is distinguishable from slow networks.
            let reason = if deadline.kind == ConnectionDeadlineKind::AwaitingHandler {
                ConnClose::ParkedTimeout
            } else {
                ConnClose::Timeout
            };
            close_conn(slot, reason, host, conns, stream_poller, worker, counts);
        }
    }
}

// NOTE: a staged validation failure drops the work without touching the slot: the entry
// is already tombed, and the host got its outcome at stage time. Single-threaded worker
// order makes this unreachable except under table divergence, so fail closed silently.
fn drain_host_staged(
    host: &HostShared,
    worker_id: u32,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
    now_ns: u64,
) {
    for work in host.take_staged(worker_id) {
        let valid = conns
            .get(work.slot)
            .and_then(|entry| entry.as_ref())
            .is_some_and(|conn| conn.token() == work.token && conn.is_parked())
            && worker
                .phase(work.token)
                .is_ok_and(|phase| phase == ConnectionPhase::AwaitingHandler);
        if !valid {
            continue;
        }
        if work.cancel {
            close_conn(
                work.slot,
                ConnClose::Cancelled,
                host,
                conns,
                stream_poller,
                worker,
                counts,
            );
            continue;
        }
        let encoded = conns
            .get_mut(work.slot)
            .and_then(|entry| entry.as_mut())
            .map(|conn| conn.complete_parked(work.status, &work.reason, &work.body));
        match encoded {
            Some(Ok(())) => {
                if worker
                    .transition_phase(work.token, ConnectionPhase::WritingResponse, now_ns)
                    .is_err()
                {
                    close_conn(
                        work.slot,
                        ConnClose::TransportError,
                        host,
                        conns,
                        stream_poller,
                        worker,
                        counts,
                    );
                }
            }
            Some(Err(reason)) => {
                close_conn(
                    work.slot,
                    reason,
                    host,
                    conns,
                    stream_poller,
                    worker,
                    counts,
                );
            }
            None => {}
        }
    }
}

// NOTE: hook failures mean the worker table diverged from connection state; a
// failed transition already parks the slot in Closing, so the server closes the
// connection instead of running it deadline-less or retrying into the park.
fn apply_pump_hooks(
    worker: &mut Worker<'_>,
    token: ConnectionToken,
    progress: PumpProgress,
    now_ns: u64,
) -> Option<ConnClose> {
    let bytes = progress.bytes_read.saturating_add(progress.bytes_written);
    if worker.note_progress(token, bytes, now_ns).is_err() {
        return Some(ConnClose::TransportError);
    }
    if progress.resumed_idle {
        // NOTE: pump marks a resume candidate on local head-phase read activity or an armed
        // buffered tail, so quiet turns cost zero lookups. Confirm via one phase query: idle
        // resumes in a single round-trip, head is a fresh read needing no transition, anything
        // else (or a stale token) fails closed without retrying into the park.
        match worker.phase(token) {
            Ok(ConnectionPhase::KeepAliveIdle) => {
                if worker.resume_idle(token, now_ns).is_err() {
                    return Some(ConnClose::TransportError);
                }
            }
            Ok(ConnectionPhase::ReadingRequestHead) => {}
            Ok(_) | Err(_) => return Some(ConnClose::TransportError),
        }
    }
    if progress.entered_body {
        let entered = worker.transition_phase(token, ConnectionPhase::ReadingRequestBody, now_ns);
        if entered.is_err() {
            return Some(ConnClose::TransportError);
        }
    }
    if progress.entered_response {
        let entered = worker.transition_phase(token, ConnectionPhase::WritingResponse, now_ns);
        if entered.is_err() {
            return Some(ConnClose::TransportError);
        }
    }
    // NOTE: the park enqueue already succeeded in the pump; this arms the parked
    // (AwaitingHandler) deadline. On failure close_conn unparks the entry, so the host
    // observes Gone rather than a wedged slot.
    if progress.entered_park {
        let entered = worker.transition_phase(token, ConnectionPhase::AwaitingHandler, now_ns);
        if entered.is_err() {
            return Some(ConnClose::TransportError);
        }
    }
    if progress.response_flushed {
        if worker.cancel_deadline(token).is_err() {
            return Some(ConnClose::TransportError);
        }
        if worker.schedule_idle(token, now_ns).is_err() {
            return Some(ConnClose::TransportError);
        }
    }
    None
}

fn close_conn(
    index: usize,
    reason: ConnClose,
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
) {
    let Some(conn) = conns[index].take() else {
        return;
    };
    let token = conn.token();
    let parked = conn.is_parked();
    let mut stream = conn.into_stream();
    let _ = stream_poller.deregister(&mut stream);
    let _ = worker.close(token);
    // NOTE: every parked close unparks exactly once (take_staged already removed staged
    // entries, so this is a no-op for applied responds/cancels): the host tomb records
    // Gone and a later respond can never report success for a dead connection.
    if parked {
        host.unpark_closed(token);
    }
    counts.add(reason);
}

fn cleanup_all(
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
) {
    for index in 0..conns.len() {
        if conns[index].is_some() {
            close_conn(
                index,
                ConnClose::Shutdown,
                host,
                conns,
                stream_poller,
                worker,
                counts,
            );
        }
    }
}

fn work_pending(conns: &[Option<ConnState>]) -> bool {
    conns
        .iter()
        .flatten()
        .any(|conn| conn.has_pending_output() || conn.has_unparsed_input() || conn.read_armed())
}

fn close_idle_for_drain(
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
) {
    for index in 0..conns.len() {
        let idle = conns[index].as_ref().is_some_and(|conn| conn.is_idle());
        if idle {
            close_conn(
                index,
                ConnClose::Shutdown,
                host,
                conns,
                stream_poller,
                worker,
                counts,
            );
        }
    }
}
