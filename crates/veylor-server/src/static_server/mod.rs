use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use veylor_router::RouteSlot;
use veylor_runtime::{
    ConnectionTimeouts, PollInterest, Poller, TcpListener, TurnConfig, WakeHandle, bind_reuse_port,
};

use crate::connection::ConnClose;
use crate::host::{HostHandle, HostShared};
use crate::multi_worker::{ThreadCtx, ThreadResult};
use crate::static_routes::{MAX_ROUTES, RouteBuildError, StaticRoutes};

mod turn;

pub(crate) use turn::worker_main;

pub const MAX_CONNECTIONS: usize = 128;
pub const MAX_WORKERS: usize = 16;
pub const LISTENER_TOKEN: usize = 0;
pub const SHUTDOWN_TIMER_ID: u64 = 0xFFFF_FFFE;
pub(crate) const MAX_EVENTS_PER_TURN: usize = 64;
const SHUTDOWN_GRACE_DEFAULT_NS: u64 = 5_000_000_000;

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
    pub(crate) fn add(&mut self, reason: ConnClose) {
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
        max_body_bytes: u64,
    ) -> Result<(), ServerError> {
        self.routes
            .register(method, path, status, reason, body, max_body_bytes)
            .map_err(ServerError::from)
    }

    pub fn register_managed(
        &mut self,
        method: &[u8],
        path: &[u8],
        max_body_bytes: u64,
    ) -> Result<(), ServerError> {
        self.routes
            .register_managed(method, path, max_body_bytes)
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
