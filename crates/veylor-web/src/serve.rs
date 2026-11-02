use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use veylor_server::{CloseCounts, RunningServer, Server, ServerError};

use crate::native::{App, Route, dispatch};
use crate::router::Router;

pub trait ServeExt: Sized {
    fn serve<S: Send + Sync + 'static>(self, app: Router<S>) -> Result<RunningApp, ServeError>;
}

impl ServeExt for Server {
    fn serve<S: Send + Sync + 'static>(mut self, app: Router<S>) -> Result<RunningApp, ServeError> {
        let Router {
            state,
            routes,
            mounts,
            fallback,
            body_cap,
        } = app;

        let state: &'static S = Box::leak(Box::new(state));
        let mut registered = Vec::new();
        let mut managed = Vec::new();

        for spec in routes {
            if !veylor_router::valid_route_pattern(&spec.path) {
                return Err(ServeError::InvalidRoute(
                    String::from_utf8_lossy(&spec.path).into_owned(),
                ));
            }

            for entry in spec.methods.entries {
                let route = Arc::new(Route {
                    method: entry.method,
                    pattern: spec.path.clone(),
                    state,
                    handler: entry.handler,
                });

                if entry.managed || needs_managed(entry.method) {
                    self.register_managed(entry.method, &spec.path, body_cap as u64)
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

        let app: &'static App<S> = Box::leak(Box::new(App {
            state,
            routes: registered,
            managed,
            fallback,
            body_cap,
        }));

        let server = self.start().map_err(ServeError::Server)?;
        let stop = Arc::new(AtomicBool::new(false));
        let dispatcher = if app.managed.is_empty() {
            None
        } else {
            Some(crate::managed::spawn(server.host(), app, Arc::clone(&stop)))
        };

        Ok(RunningApp {
            server: Some(server),
            dispatcher,
            stop,
        })
    }
}

// NOTE: a body-bearing method parks on the host dispatcher unless the route already asked
// for it; GET/HEAD/DELETE/OPTIONS have no body the handler can read.
fn needs_managed(method: &[u8]) -> bool {
    !matches!(method, b"GET" | b"HEAD" | b"DELETE" | b"OPTIONS")
}

pub struct RunningApp {
    server: Option<RunningServer>,
    dispatcher: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
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
        if let Some(dispatcher) = self.dispatcher.take() {
            let _ = dispatcher.join();
        }

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
