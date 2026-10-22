use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use veylor_runtime::{PollInterest, Poller, TcpListener, WakeHandle, bind_reuse_port};

use crate::host::HostShared;
use crate::static_routes::StaticRoutes;
use crate::static_server::{
    CloseCounts, LISTENER_TOKEN, MAX_EVENTS_PER_TURN, RunningServer, Server, ServerConfig,
    ServerError, worker_main,
};

pub(crate) struct ThreadCtx {
    pub(crate) worker_id: u32,
    pub(crate) listener: TcpListener,
    pub(crate) config: ServerConfig,
    pub(crate) routes: Arc<StaticRoutes>,
    pub(crate) host: Arc<HostShared>,
    pub(crate) worker_poller: Poller,
    pub(crate) stop: Arc<AtomicBool>,
    pub(crate) force: Arc<AtomicBool>,
    pub(crate) peers: Vec<WakeHandle>,
}

pub(crate) struct ThreadResult {
    pub(crate) counts: CloseCounts,
    pub(crate) error: Option<String>,
}

impl ThreadResult {
    pub(crate) fn into_counts(self) -> Result<CloseCounts, ServerError> {
        match self.error {
            Some(error) => Err(ServerError::Thread(error)),
            None => Ok(self.counts),
        }
    }
}

// NOTE: a failing worker escalates to force, even over a user-started graceful
// drain: Draining still admits nothing new while a dead sibling would leave the
// server half-running, so the error always wins. Propagation only sets the
// shared flags and wakes sleepers; it never joins, so the erroring thread
// cannot deadlock on itself and still returns its own error for first-error
// attribution at join. Panics take the same path via run_worker below.
pub(crate) fn propagate_failure(stop: &AtomicBool, force: &AtomicBool, wakers: &[WakeHandle]) {
    force.store(true, Ordering::Release);
    stop.store(true, Ordering::Release);
    for waker in wakers {
        let _ = waker.wake();
    }
}

pub(crate) fn run_worker(ctx: ThreadCtx) -> ThreadResult {
    let stop = Arc::clone(&ctx.stop);
    let force = Arc::clone(&ctx.force);
    let peers = ctx.peers.clone();
    match catch_unwind(AssertUnwindSafe(|| worker_main(ctx))) {
        Ok(result) => {
            if result.error.is_some() {
                propagate_failure(&stop, &force, &peers);
            }
            result
        }
        Err(payload) => {
            propagate_failure(&stop, &force, &peers);
            resume_unwind(payload);
        }
    }
}

pub(crate) fn start_multi(server: Server) -> Result<RunningServer, ServerError> {
    let Server {
        listener: template,
        config,
        routes,
        host,
    } = server;
    let count = config.worker_count;
    let addr = template.local_addr().map_err(ServerError::Bind)?;
    let mut listeners = Vec::with_capacity(count);
    listeners.push(template);
    for _ in 1..count {
        match bind_reuse_port(addr) {
            Ok(listener) => listeners.push(listener),
            Err(error) => return Err(ServerError::Bind(error)),
        }
    }
    let routes = Arc::new(routes);
    let stop = Arc::new(AtomicBool::new(false));
    let force = Arc::new(AtomicBool::new(false));
    let mut prepared = Vec::with_capacity(count);
    for (index, listener) in listeners.into_iter().enumerate() {
        let prepared_worker =
            prepare_worker(index, listener, &config, &routes, &host, &stop, &force)?;
        prepared.push(prepared_worker);
    }
    let peers: Vec<WakeHandle> = prepared.iter().map(|(_, waker)| waker.clone()).collect();
    let mut handles: Vec<JoinHandle<ThreadResult>> = Vec::with_capacity(count);
    let mut wakers: Vec<WakeHandle> = Vec::with_capacity(count);
    for (index, (mut ctx, waker)) in prepared.into_iter().enumerate() {
        ctx.peers = peers.clone();
        match thread::Builder::new()
            .name(format!("veylor-worker-{index}"))
            .spawn(|| run_worker(ctx))
        {
            Ok(handle) => {
                handles.push(handle);
                wakers.push(waker);
            }
            Err(error) => {
                abort_started(handles, wakers, &stop, &force);
                return Err(ServerError::Thread(error.to_string()));
            }
        }
    }
    Ok(RunningServer {
        handles,
        wakers,
        stop,
        force,
        addr,
        host: crate::host::HostHandle::new(host),
    })
}

fn prepare_worker(
    index: usize,
    mut listener: TcpListener,
    config: &ServerConfig,
    routes: &Arc<StaticRoutes>,
    host: &Arc<HostShared>,
    stop: &Arc<AtomicBool>,
    force: &Arc<AtomicBool>,
) -> Result<(ThreadCtx, WakeHandle), ServerError> {
    let mut worker_poller = Poller::new(MAX_EVENTS_PER_TURN).map_err(ServerError::Poll)?;
    worker_poller
        .register(&mut listener, LISTENER_TOKEN, PollInterest::Readable)
        .map_err(ServerError::Poll)?;
    let waker = worker_poller.create_waker().map_err(ServerError::Poll)?;
    let ctx = ThreadCtx {
        worker_id: index as u32,
        listener,
        config: *config,
        routes: Arc::clone(routes),
        host: Arc::clone(host),
        worker_poller,
        stop: Arc::clone(stop),
        force: Arc::clone(force),
        peers: Vec::new(),
    };
    Ok((ctx, waker))
}

fn abort_started(
    handles: Vec<JoinHandle<ThreadResult>>,
    wakers: Vec<WakeHandle>,
    stop: &AtomicBool,
    force: &AtomicBool,
) {
    propagate_failure(stop, force, &wakers);
    for handle in handles {
        let _ = handle.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_waker() -> (Poller, WakeHandle) {
        let mut poller = Poller::new(4).unwrap();
        let waker = poller.create_waker().unwrap();
        (poller, waker)
    }

    #[test]
    fn failure_escalates_flags_and_wakes_sleepers() {
        let stop = AtomicBool::new(false);
        let force = AtomicBool::new(false);
        let (_poller, waker) = test_waker();
        propagate_failure(&stop, &force, &[waker]);
        assert!(stop.load(Ordering::Acquire));
        assert!(force.load(Ordering::Acquire));
    }

    #[test]
    fn failure_overrides_graceful_drain() {
        let stop = AtomicBool::new(true);
        let force = AtomicBool::new(false);
        let (_poller_a, first) = test_waker();
        let (_poller_b, second) = test_waker();
        propagate_failure(&stop, &force, &[first, second]);
        assert!(stop.load(Ordering::Acquire));
        assert!(force.load(Ordering::Acquire));
    }

    #[test]
    fn wake_signal_is_observable_on_sibling_poller() {
        let stop = AtomicBool::new(false);
        let force = AtomicBool::new(false);
        let (mut poller, waker) = test_waker();
        propagate_failure(&stop, &force, &[waker]);
        let mut seen_wake = false;
        for event in poller
            .poll(Some(std::time::Duration::from_millis(0)))
            .unwrap()
        {
            seen_wake = seen_wake || event.token == veylor_runtime::WAKE_TOKEN;
        }
        assert!(seen_wake);
    }
}
