use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use veylor_runtime::{PollInterest, Poller, TcpListener, WakeHandle, bind_reuse_port};

use crate::static_routes::StaticRoutes;
use crate::static_server::{
    LISTENER_TOKEN, MAX_EVENTS_PER_TURN, RunningServer, Server, ServerConfig, ServerError,
    ThreadCtx, ThreadResult, worker_main,
};

pub(crate) fn start_multi(server: Server) -> Result<RunningServer, ServerError> {
    let Server {
        listener: template,
        config,
        routes,
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
    let mut handles: Vec<JoinHandle<ThreadResult>> = Vec::with_capacity(count);
    let mut wakers: Vec<WakeHandle> = Vec::with_capacity(count);
    for (index, listener) in listeners.into_iter().enumerate() {
        let (ctx, waker) = match prepare_worker(index, listener, &config, &routes, &stop, &force) {
            Ok(prepared) => prepared,
            Err(error) => {
                abort_started(handles, wakers, &stop, &force);
                return Err(error);
            }
        };
        match thread::Builder::new()
            .name(format!("veylor-worker-{index}"))
            .spawn(|| worker_main(ctx))
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
    })
}

fn prepare_worker(
    index: usize,
    mut listener: TcpListener,
    config: &ServerConfig,
    routes: &Arc<StaticRoutes>,
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
        worker_poller,
        stop: Arc::clone(stop),
        force: Arc::clone(force),
    };
    Ok((ctx, waker))
}

fn abort_started(
    handles: Vec<JoinHandle<ThreadResult>>,
    wakers: Vec<WakeHandle>,
    stop: &AtomicBool,
    force: &AtomicBool,
) {
    stop.store(true, Ordering::Release);
    force.store(true, Ordering::Release);
    for waker in &wakers {
        let _ = waker.wake();
    }
    for handle in handles {
        let _ = handle.join();
    }
}
