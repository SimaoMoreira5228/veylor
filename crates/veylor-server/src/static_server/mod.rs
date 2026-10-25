use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
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
use crate::static_routes::{MAX_ROUTES, NativeHandler, RouteBuildError, StaticRoutes};

pub(crate) mod turn;

pub(crate) use turn::worker_main;

// NOTE: per-worker connection ceiling: 1024 makes the 10k-idle ladder reachable
// via fan-out (10 workers x 1024) within MAX_WORKERS. Beyond it the first breaks
// are fd limits (one fd per conn plus pollers), then the const-sized host park
// tables, then per-worker memory (~1 KiB fixed per slot, ~16 KiB slabs per active
// conn once input/output materialize). H1 tables ride ServerConfig::max_connections
// (1..=MAX_CONNECTIONS); the H2/host slot space still rides this const, so the
// config value can never index out of range there.
pub const MAX_CONNECTIONS: usize = 1024;
// NOTE: default() keeps the historical 128-conn behavior; opt in per worker via
// ServerConfig::max_connections. A named const keeps the literal in one place.
pub const DEFAULT_MAX_CONNECTIONS: usize = 128;
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
    pub h2_served: u64,
    pub h2_refused: u64,
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
    pub max_connections: usize,
    // NOTE: paired-port h2c: None disables the cleartext HTTP/2 listener, Some(0)
    // binds an ephemeral loopback port, Some(port) binds that port on the H1
    // bind address IP. One extra port (not ALPN/upgrades) keeps the H1 path
    // untouched; each worker binds its own reuse-port h2c listener like H1.
    pub h2c_port: Option<u16>,
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
            max_connections: DEFAULT_MAX_CONNECTIONS,
            h2c_port: None,
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
            RouteBuildError::NativePanic => Self::InvalidRoute,
            RouteBuildError::Invalid => Self::InvalidRoute,
        }
    }
}

pub struct Server {
    pub(crate) listener: TcpListener,
    pub(crate) h2c_listener: Option<TcpListener>,
    pub(crate) config: ServerConfig,
    pub(crate) routes: StaticRoutes,
    pub(crate) h2_routes: Vec<H2StaticRoute>,
    pub(crate) host: Arc<HostShared>,
}

// NOTE: owned route projection for the h2c turn: StaticRoutes stays read-only
// (frozen), so register paths mirror validated entries here and the worker projects
// borrows per turn. Managed routes mirror as park-only entries; file mounts mirror
// as prefix entries (path carries the prefix, file_root the root) and the H2 view
// skips them in exact-match lookups, so statics keep precedence exactly like H1.
#[derive(Clone, Debug)]
pub(crate) struct H2StaticRoute {
    pub(crate) method: Vec<u8>,
    pub(crate) path: Vec<u8>,
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
    pub(crate) managed: bool,
    pub(crate) native: Option<(NativeHandler, usize)>,
    pub(crate) file_root: Option<PathBuf>,
    pub(crate) trailers: Vec<Vec<u8>>,
}

impl Server {
    pub fn bind(addr: SocketAddr, config: ServerConfig) -> Result<Self, ServerError> {
        validate_config(&config)?;
        let listener = if config.worker_count == 1 {
            TcpListener::bind(addr).map_err(ServerError::Bind)?
        } else {
            bind_reuse_port(addr).map_err(ServerError::Bind)?
        };
        let h2c_listener = match config.h2c_port {
            None => None,
            Some(port) => {
                let h2c_addr = SocketAddr::new(addr.ip(), port);
                let bound = if config.worker_count == 1 {
                    TcpListener::bind(h2c_addr).map_err(ServerError::Bind)?
                } else {
                    bind_reuse_port(h2c_addr).map_err(ServerError::Bind)?
                };
                Some(bound)
            }
        };
        Ok(Self {
            listener,
            h2c_listener,
            config,
            routes: StaticRoutes::new(),
            h2_routes: Vec::new(),
            host: Arc::new(HostShared::new(config.worker_count)),
        })
    }

    pub fn addr(&self) -> Result<SocketAddr, ServerError> {
        self.listener.local_addr().map_err(ServerError::Bind)
    }

    pub fn h2c_addr(&self) -> Option<Result<SocketAddr, ServerError>> {
        self.h2c_listener
            .as_ref()
            .map(|listener| listener.local_addr().map_err(ServerError::Bind))
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
            .map_err(ServerError::from)?;
        // NOTE: register validated, so the mirror cannot fail: same bounds, same order.
        if self.h2_routes.len() < MAX_ROUTES {
            self.h2_routes.push(H2StaticRoute {
                method: method.to_vec(),
                path: path.to_vec(),
                status,
                body: body.to_vec(),
                managed: false,
                native: None,
                file_root: None,
                trailers: Vec::new(),
            });
        }
        Ok(())
    }

    pub fn register_managed(
        &mut self,
        method: &[u8],
        path: &[u8],
        max_body_bytes: u64,
    ) -> Result<(), ServerError> {
        self.routes
            .register_managed(method, path, max_body_bytes, &[])
            .map_err(ServerError::from)?;
        // NOTE: park-only mirror (valid dummy status/body pass the projection filter);
        // the turn parks these instead of serving them synchronously (H1 parity).
        if self.h2_routes.len() < MAX_ROUTES {
            self.h2_routes.push(H2StaticRoute {
                method: method.to_vec(),
                path: path.to_vec(),
                status: 200,
                body: Vec::new(),
                managed: true,
                native: None,
                file_root: None,
                trailers: Vec::new(),
            });
        }
        Ok(())
    }

    pub fn register_file_mount(
        &mut self,
        prefix: &[u8],
        root: &std::path::Path,
    ) -> Result<(), ServerError> {
        self.routes
            .register_file_mount(prefix, root)
            .map_err(ServerError::from)?;
        // NOTE: prefix entry for the H2 projection: method is a GET-shaped dummy (mounts
        // serve GET and HEAD through the prefix match, never the exact table), status and
        // body dummies pass the projection filter, and the view skips file entries there.
        if self.h2_routes.len() < MAX_ROUTES {
            self.h2_routes.push(H2StaticRoute {
                method: b"GET".to_vec(),
                path: prefix.to_vec(),
                status: 200,
                body: Vec::new(),
                managed: false,
                native: None,
                file_root: Some(root.to_path_buf()),
                trailers: Vec::new(),
            });
        }
        Ok(())
    }

    pub fn register_managed_with_trailers(
        &mut self,
        method: &[u8],
        path: &[u8],
        max_body_bytes: u64,
        trailers: &[&[u8]],
    ) -> Result<(), ServerError> {
        self.routes
            .register_managed(method, path, max_body_bytes, trailers)
            .map_err(ServerError::from)?;
        // NOTE: allowlist rides the mirror beside the park-only dummies: register validated
        // names and order, so the projection resolves the same list H1 reads at park.
        if self.h2_routes.len() < MAX_ROUTES {
            self.h2_routes.push(H2StaticRoute {
                method: method.to_vec(),
                path: path.to_vec(),
                status: 200,
                body: Vec::new(),
                managed: true,
                native: None,
                file_root: None,
                trailers: trailers.iter().map(|name| name.to_vec()).collect(),
            });
        }
        Ok(())
    }

    // NOTE: native handlers run on the owner worker only, over H1 and H2 alike; user_data
    // crosses as an address-sized value with the handler. Frozen at start like every other
    // register path.
    pub fn register_native(
        &mut self,
        method: &[u8],
        path: &[u8],
        handler: Option<NativeHandler>,
        user_data: usize,
    ) -> Result<(), ServerError> {
        self.routes
            .register_native(method, path, handler, user_data)
            .map_err(ServerError::from)?;
        // NOTE: register validated, so the mirror cannot fail: same bounds, same order.
        // Handler and user_data copy (both Copy); the H2 projection reads them straight.
        if self.h2_routes.len() < MAX_ROUTES {
            if let Some(handler) = handler {
                self.h2_routes.push(H2StaticRoute {
                    method: method.to_vec(),
                    path: path.to_vec(),
                    status: 200,
                    body: Vec::new(),
                    managed: false,
                    native: Some((handler, user_data)),
                    file_root: None,
                    trailers: Vec::new(),
                });
            }
        }
        Ok(())
    }

    pub fn start(self) -> Result<RunningServer, ServerError> {
        freeze_check(&self.routes)?;
        if self.config.worker_count > 1 {
            return crate::multi_worker::start_multi(self);
        }
        let Server {
            mut listener,
            mut h2c_listener,
            config,
            routes,
            h2_routes,
            host,
        } = self;
        let mut worker_poller = Poller::new(MAX_EVENTS_PER_TURN).map_err(ServerError::Poll)?;
        worker_poller
            .register(&mut listener, LISTENER_TOKEN, PollInterest::Readable)
            .map_err(ServerError::Poll)?;
        let waker = worker_poller.create_waker().map_err(ServerError::Poll)?;
        let addr = listener.local_addr().map_err(ServerError::Bind)?;
        let h2c_addr = h2c_listener
            .as_mut()
            .map(|bound| bound.local_addr().map_err(ServerError::Bind))
            .transpose()?;
        let stop = Arc::new(AtomicBool::new(false));
        let force = Arc::new(AtomicBool::new(false));
        let ctx = ThreadCtx {
            worker_id: 0,
            listener,
            h2c_listener,
            config,
            routes: Arc::new(routes),
            h2_routes: Arc::new(h2_routes),
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
            h2c_addr,
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
    pub(crate) h2c_addr: Option<SocketAddr>,
    pub(crate) host: HostHandle,
}

impl RunningServer {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn h2c_addr(&self) -> Option<SocketAddr> {
        self.h2c_addr
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
            total.h2_served += worker.h2_served;
            total.h2_refused += worker.h2_refused;
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
    if config.max_connections == 0 || config.max_connections > MAX_CONNECTIONS {
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
