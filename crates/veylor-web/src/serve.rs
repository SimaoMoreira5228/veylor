use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use veylor_server::{CloseCounts, RunningServer, Server, ServerError};

use crate::native::{App, Route, dispatch};
use crate::router::{RouteTarget, Router};

pub trait ServeExt: Sized {
    fn serve<S: Send + Sync + 'static>(self, app: Router<S>) -> Result<RunningApp, ServeError>;
}

impl ServeExt for Server {
    fn serve<S: Send + Sync + 'static>(mut self, app: Router<S>) -> Result<RunningApp, ServeError> {
        let Router {
            state,
            routes,
            mut mounts,
            fallback,
            body_cap,
            layers,
            executor,
        } = app;
        let layers: Arc<[Arc<dyn crate::middleware::Middleware<S>>]> = Arc::from(layers);

        let state: &'static S = Box::leak(Box::new(state));
        let mut registered = Vec::new();
        let mut managed = Vec::new();

        for spec in routes {
            if !veylor_router::valid_route_pattern(&spec.path) {
                return Err(ServeError::InvalidRoute(
                    String::from_utf8_lossy(&spec.path).into_owned(),
                ));
            }

            let methods = match spec.target {
                RouteTarget::Methods(methods) => methods,
                RouteTarget::Files(files) => {
                    let prefix = mount_prefix(&spec.path);
                    if prefix.is_empty() {
                        return Err(ServeError::InvalidRoute(
                            String::from_utf8_lossy(&spec.path).into_owned(),
                        ));
                    }
                    mounts.push((prefix.to_vec(), files.0));
                    continue;
                }
                RouteTarget::Sse(handler) => {
                    let route = Arc::new(Route {
                        layers: Arc::clone(&layers),
                        kind: crate::native::Kind::Sse,
                        sse: Some(handler),
                        async_handler: None,
                        pattern: spec.path.clone(),
                        state,
                        handler: Box::new(|_, _| crate::response::Response::not_found()),
                    });
                    let route_token = managed.len() as u64 + 1;
                    self.register_managed_with_token(
                        b"GET",
                        &spec.path,
                        body_cap as u64,
                        route_token,
                    )
                    .map_err(ServeError::Server)?;
                    managed.push(Arc::clone(&route));
                    registered.push(route);
                    continue;
                }
            };

            for entry in methods.entries {
                let route = Arc::new(Route {
                    layers: Arc::clone(&layers),
                    kind: entry.kind,
                    sse: None,
                    async_handler: entry.async_handler,
                    pattern: spec.path.clone(),
                    state,
                    handler: entry.handler,
                });

                if entry.managed || needs_managed(entry.method) {
                    // NOTE: 1-based so 0 stays "untagged" for routes registered directly.
                    let route_token = managed.len() as u64 + 1;
                    self.register_managed_with_token(
                        entry.method,
                        &spec.path,
                        body_cap as u64,
                        route_token,
                    )
                    .map_err(ServeError::Server)?;
                    managed.push(Arc::clone(&route));
                } else {
                    let user_data = Arc::as_ptr(&route) as usize;
                    self.register_native(entry.method, &spec.path, Some(dispatch::<S>), user_data)
                        .map_err(ServeError::Server)?;
                }

                registered.push(route);
            }
        }

        for (prefix, root) in mounts {
            self.register_file_mount(&prefix, &root)
                .map_err(ServeError::Server)?;
        }

        let inflight = crate::executor::Inflight::new();
        let app: &'static App<S> = Box::leak(Box::new(App {
            state,
            routes: registered,
            managed,
            fallback,
            body_cap,
            pending: std::sync::atomic::AtomicUsize::new(0),
            executor,
            inflight: std::sync::Arc::clone(&inflight),
        }));

        let workers = self.worker_count();
        let h3_worker = self.h3_worker_id();
        let server = self.start().map_err(ServeError::Server)?;
        let stop = Arc::new(AtomicBool::new(false));
        let mut pool = Vec::new();
        if !app.managed.is_empty() {
            for worker in (0..workers).chain(h3_worker) {
                pool.push(crate::managed::spawn(
                    worker,
                    server.host(),
                    app,
                    Arc::clone(&stop),
                ));
            }
        }

        Ok(RunningApp {
            server: Some(server),
            dispatchers: pool,
            stop,
            inflight,
        })
    }
}

// NOTE: "/static/*path" mounts at "/static/": the wildcard segment names the mount, it
// does not become part of the prefix.
fn mount_prefix(pattern: &[u8]) -> &[u8] {
    let last = pattern.rsplit(|byte| *byte == b'/').next().unwrap_or(&[]);
    if last.first() == Some(&b'*') || last.first() == Some(&b':') {
        return &pattern[..pattern.len() - last.len()];
    }
    pattern
}

// NOTE: a body-bearing method parks on the host dispatcher unless the route already asked
// for it; GET/HEAD/DELETE/OPTIONS have no body the handler can read.
fn needs_managed(method: &[u8]) -> bool {
    !matches!(method, b"GET" | b"HEAD" | b"DELETE" | b"OPTIONS")
}

pub struct RunningApp {
    server: Option<RunningServer>,
    dispatchers: Vec<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    inflight: Arc<crate::executor::Inflight>,
}

impl RunningApp {
    fn server(&self) -> &RunningServer {
        self.server.as_ref().expect("server joined")
    }

    pub fn addr(&self) -> std::net::SocketAddr {
        self.server().addr()
    }

    pub fn h2c_addr(&self) -> Option<std::net::SocketAddr> {
        self.server().h2c_addr()
    }

    pub fn h3_addr(&self) -> Option<std::net::SocketAddr> {
        self.server().h3_addr()
    }

    pub fn host(&self) -> veylor_server::HostHandle {
        self.server().host()
    }

    pub fn stop_accepting(&self) {
        self.server().stop_accepting();
    }

    pub fn stop(self) -> Result<CloseCounts, ServeError> {
        self.join()
    }

    pub fn join(mut self) -> Result<CloseCounts, ServeError> {
        let Some(server) = self.server.take() else {
            return Ok(CloseCounts::default());
        };
        server.stop_accepting();
        let joined = server.join().map_err(ServeError::Server);

        self.stop.store(true, Ordering::Release);
        for dispatcher in self.dispatchers.drain(..) {
            let _ = dispatcher.join();
        }
        self.inflight.wait();

        joined
    }
}

impl Drop for RunningApp {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

#[derive(Debug)]
pub enum ServeError {
    Server(ServerError),
    InvalidRoute(String),
}
