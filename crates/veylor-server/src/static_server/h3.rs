use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use veylor_h3::http::{Request, Response, StatusCode};
use veylor_h3::{
    Body, Bytes, ConnectProtocol, ManagedOutcome, RequestStream, RequestTrailers, ResponseWriter,
    RunningServer, Server, Tls,
};
use veylor_runtime::ConnectionToken;

use crate::connection::streams::StreamCmd;
use crate::host::{H3_PARK_SLOTS, HostShared, RespondOutcome, StagedWork};

use super::{H2StaticRoute, ServerConfig, ServerError};
use response::{staged_response, text, with_date};
use route::{select, serve_route};

mod file;
mod response;
mod route;
mod tunnel;

type WaiterKey = (u32, u32, u64);

struct Waiter {
    token: ConnectionToken,
    sender: mpsc::Sender<HostWork>,
}

enum HostWork {
    Response(StagedWork),
    Command(StreamCmd),
}

type WaiterMap = Arc<Mutex<HashMap<WaiterKey, Waiter>>>;

pub(crate) struct H3Server {
    running: Option<RunningServer>,
    stop: Arc<AtomicBool>,
    dispatcher: Option<thread::JoinHandle<()>>,
}

impl H3Server {
    pub(crate) fn address(&self) -> SocketAddr {
        self.running.as_ref().expect("h3 running server").address()
    }

    pub(crate) fn rotate_tls(
        &self,
        certs: Vec<veylor_tls::Certificate>,
        key: veylor_tls::PrivateKey,
    ) -> Result<(), ServerError> {
        self.running
            .as_ref()
            .ok_or_else(|| ServerError::Thread("h3 stopped".to_owned()))?
            .rotate_tls(certs, key)
            .map_err(|error| ServerError::Thread(format!("h3: {error}")))
    }

    pub(crate) fn stop(&self) {
        self.stop.store(true, AtomicOrdering::Release);
        if let Some(running) = &self.running {
            running.stop();
        }
    }

    pub(crate) fn join(mut self) -> Result<(), ServerError> {
        self.stop();
        if let Some(running) = self.running.take() {
            running
                .join()
                .map_err(|error| ServerError::Thread(format!("h3: {error}")))?;
        }
        if let Some(dispatcher) = self.dispatcher.take() {
            dispatcher
                .join()
                .map_err(|_| ServerError::Join("h3 dispatcher panicked".to_owned()))?;
        }
        Ok(())
    }
}

impl Drop for H3Server {
    fn drop(&mut self) {
        self.stop.store(true, AtomicOrdering::Release);
    }
}

pub(crate) fn start(
    config: &ServerConfig,
    ip: IpAddr,
    routes: &[H2StaticRoute],
    host: &Arc<HostShared>,
) -> Result<Option<H3Server>, ServerError> {
    let Some(port) = config.h3_port else {
        return Ok(None);
    };
    let tls = config.tls.as_ref().ok_or(ServerError::InvalidConfig)?;
    let tls = Tls::new(tls.cert_chain.clone(), tls.key.clone());
    let worker = host.h3_worker_id();
    let waiters: WaiterMap = Arc::new(Mutex::new(HashMap::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let timeout = Duration::from_nanos(config.timeouts.body_progress_ns);
    let handler = Handler {
        routes: routes.to_vec(),
        host: Arc::clone(host),
        worker,
        slots: Mutex::new(Slots {
            free: (0..H3_PARK_SLOTS as u32).rev().collect(),
            generations: vec![1; H3_PARK_SLOTS],
        }),
        waiters: Arc::clone(&waiters),
        timeout,
    };

    let dispatcher = {
        let host = Arc::clone(host);
        let waiters = Arc::clone(&waiters);
        let stop = Arc::clone(&stop);
        thread::Builder::new()
            .name("veylor-h3-host".to_owned())
            .spawn(move || dispatch(host, worker, waiters, stop))
            .map_err(|error| ServerError::Thread(error.to_string()))?
    };

    let server = Server::bind(
        SocketAddr::new(ip, port),
        veylor_h3::Config {
            tls,
            limits: veylor_h3::Limits::default(),
            runtime_threads: if config.h3_runtime_threads == 0 {
                std::thread::available_parallelism()
                    .map(|count| count.get())
                    .unwrap_or(4)
            } else {
                config.h3_runtime_threads
            },
        },
    );
    match server.start(handler) {
        Ok(running) => Ok(Some(H3Server {
            running: Some(running),
            stop,
            dispatcher: Some(dispatcher),
        })),
        Err(error) => {
            stop.store(true, AtomicOrdering::Release);
            let _ = dispatcher.join();
            Err(ServerError::Thread(format!("h3: {error}")))
        }
    }
}

fn dispatch(host: Arc<HostShared>, worker: u32, waiters: WaiterMap, stop: Arc<AtomicBool>) {
    let mut keys = Vec::new();
    while !stop.load(AtomicOrdering::Acquire) {
        let staged = host.wait_staged(worker, Duration::from_millis(5));
        let mut waiters = waiters.lock();
        for work in staged {
            let key = (
                work.token.worker_id,
                work.slot as u32,
                work.token.generation,
            );
            if let Some(waiter) = waiters.remove(&key) {
                let _ = waiter.sender.blocking_send(HostWork::Response(work));
            }
        }
        keys.clear();
        keys.extend(waiters.keys().copied());
        for key in keys.iter() {
            let Some(token) = waiters.get(key).map(|waiter| waiter.token) else {
                continue;
            };
            let Some(command) = host.take_stream_command(token) else {
                continue;
            };
            if let Some(waiter) = waiters.get(key) {
                let _ = waiter.sender.blocking_send(HostWork::Command(command));
            }
        }
    }
}

struct Slots {
    free: Vec<u32>,
    generations: Vec<u64>,
}

impl Slots {
    fn acquire(&mut self) -> Option<(u32, u64)> {
        let slot = self.free.pop()?;
        Some((slot, self.generations[slot as usize]))
    }

    fn release(&mut self, slot: u32) {
        let generation = &mut self.generations[slot as usize];
        *generation = generation.wrapping_add(1).max(1);
        self.free.push(slot);
    }
}

struct Handler {
    routes: Vec<H2StaticRoute>,
    host: Arc<HostShared>,
    worker: u32,
    slots: Mutex<Slots>,
    waiters: WaiterMap,
    timeout: Duration,
}

struct RouteMatch<'a> {
    index: usize,
    route: &'a H2StaticRoute,
    native: bool,
}

impl veylor_h3::Handler for Handler {
    fn handle(&self, request: Request<()>, body: Bytes) -> Response<Body> {
        serve_route(&self.routes, request, body)
    }

    fn is_direct(&self, request: &Request<()>) -> bool {
        let method = request.method().as_str().as_bytes();
        let path = request.uri().path().as_bytes();
        select(&self.routes, method, path).is_none_or(|found| found.native || !found.route.managed)
    }

    fn connect<'a>(
        &'a self,
        request: Request<()>,
        stream: RequestStream,
        writer: &'a mut ResponseWriter<'_>,
    ) -> Pin<Box<dyn Future<Output = ManagedOutcome> + Send + 'a>> {
        let protocol = request
            .extensions()
            .get::<ConnectProtocol>()
            .map(|protocol| protocol.0.clone())
            .unwrap_or_default();
        if protocol != b"websocket" {
            return Box::pin(async move {
                ManagedOutcome::Handled(text(StatusCode::NOT_IMPLEMENTED, b"Not Implemented"))
            });
        }
        let path = request.uri().path().as_bytes().to_vec();
        let Some(found) = select(&self.routes, b"CONNECT", &path) else {
            return Box::pin(async move {
                ManagedOutcome::Handled(text(StatusCode::NOT_FOUND, b"Not Found"))
            });
        };
        if !found.route.managed {
            return Box::pin(async move {
                ManagedOutcome::Handled(text(StatusCode::NOT_FOUND, b"Not Found"))
            });
        }
        let route_id = found.index as u64;
        let trailer_allow = found.route.trailers.clone();
        Box::pin(async move {
            self.tunnel(request, stream, writer, route_id, &path, &trailer_allow)
                .await
        })
    }

    fn managed<'a>(
        &'a self,
        mut request: Request<()>,
        body: Bytes,
        writer: &'a mut ResponseWriter<'_>,
    ) -> Pin<Box<dyn Future<Output = ManagedOutcome> + Send + 'a>> {
        let trailers = request
            .extensions_mut()
            .remove::<RequestTrailers>()
            .map(|trailers| trailers.0)
            .unwrap_or_default();
        let method = request.method().as_str().as_bytes().to_vec();
        let path = request.uri().path().as_bytes().to_vec();
        let Some(found) = select(&self.routes, &method, &path) else {
            return Box::pin(async move { ManagedOutcome::Pass(request, body) });
        };
        if found.native || !found.route.managed {
            return Box::pin(async move { ManagedOutcome::Pass(request, body) });
        }
        if body.len() as u64 > found.route.max_body_bytes {
            return Box::pin(async move {
                ManagedOutcome::Handled(text(StatusCode::PAYLOAD_TOO_LARGE, b"Payload Too Large"))
            });
        }
        let route_id = found.index as u64;
        let route_token = found.route.token;
        let body_limit = found.route.max_body_bytes;
        let trailer_allow = found.route.trailers.clone();
        Box::pin(async move {
            self.park(
                request,
                body,
                route_id,
                route_token,
                &method,
                &path,
                body_limit,
                trailers,
                &trailer_allow,
                writer,
            )
            .await
        })
    }
}

impl Handler {
    #[allow(clippy::too_many_arguments)]
    async fn park(
        &self,
        request: Request<()>,
        body: Bytes,
        route_id: u64,
        route_token: u64,
        method: &[u8],
        path: &[u8],
        body_limit: u64,
        trailers: Vec<(Vec<u8>, Vec<u8>)>,
        trailer_allow: &[Vec<u8>],
        writer: &mut ResponseWriter<'_>,
    ) -> ManagedOutcome {
        let Some((slot, generation)) = self.slots.lock().acquire() else {
            return ManagedOutcome::Handled(text(StatusCode::SERVICE_UNAVAILABLE, b"busy"));
        };
        let key = (self.worker, slot, generation);
        let (sender, mut receiver) = mpsc::channel(2);
        let token = ConnectionToken {
            worker_id: self.worker,
            slot,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 1,
            generation,
        };
        let registered = self
            .waiters
            .lock()
            .insert(key, Waiter { token, sender })
            .is_none();
        if !registered {
            release(&self.slots, slot);
            return ManagedOutcome::Handled(text(StatusCode::SERVICE_UNAVAILABLE, b"busy"));
        }

        let header_pairs: Vec<(Vec<u8>, Vec<u8>)> = request
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect();
        let headers = header_pairs
            .iter()
            .map(|(name, value)| Some((name.as_slice(), value.as_slice())));
        let parked = self.host.try_park_with_request(
            self.worker,
            token,
            route_id,
            route_token,
            method,
            path,
            body.len() as u64,
            trailer_allow,
            Some(body_limit),
            false,
            headers,
        );
        let Ok(event_token) = parked else {
            self.remove_waiter(key);
            release(&self.slots, slot);
            return ManagedOutcome::Handled(text(StatusCode::SERVICE_UNAVAILABLE, b"busy"));
        };
        if let Some(waiter) = self.waiters.lock().get_mut(&key) {
            waiter.token = event_token;
        }
        if !body.is_empty() && !self.host.push_body(event_token, &body) {
            self.host.unpark_closed(self.worker, slot);
            self.remove_waiter(key);
            release(&self.slots, slot);
            return ManagedOutcome::Handled(text(StatusCode::SERVICE_UNAVAILABLE, b"busy"));
        }
        if !trailers.is_empty() {
            let fields: Vec<(&[u8], &[u8])> = trailers
                .iter()
                .map(|(name, value)| (name.as_slice(), value.as_slice()))
                .collect();
            if self.host.push_request_trailers(event_token, &fields) != RespondOutcome::Ok {
                self.retire(key, slot);
                return ManagedOutcome::Handled(text(StatusCode::BAD_REQUEST, b"Bad Request"));
            }
        }

        let mut streaming = false;
        loop {
            let signal = tokio::time::timeout(self.timeout, receiver.recv()).await;
            let work = match signal {
                Ok(Some(work)) => work,
                _ => {
                    if streaming {
                        writer.reset(0);
                    }
                    self.host.unpark_closed(self.worker, slot);
                    self.remove_waiter(key);
                    release(&self.slots, slot);
                    if streaming {
                        return ManagedOutcome::Streamed;
                    }
                    return ManagedOutcome::Handled(text(
                        StatusCode::SERVICE_UNAVAILABLE,
                        b"unavailable",
                    ));
                }
            };
            match work {
                HostWork::Response(work) => {
                    release(&self.slots, slot);
                    return ManagedOutcome::Handled(staged_response(work));
                }
                HostWork::Command(command) => {
                    let streamed = self
                        .apply_command(command, &mut streaming, writer, event_token, key, slot)
                        .await;
                    if let Some(outcome) = streamed {
                        return outcome;
                    }
                }
            }
        }
    }

    async fn apply_command(
        &self,
        command: StreamCmd,
        streaming: &mut bool,
        writer: &mut ResponseWriter<'_>,
        token: ConnectionToken,
        key: WaiterKey,
        slot: u32,
    ) -> Option<ManagedOutcome> {
        match command {
            StreamCmd::Begin {
                status, headers, ..
            } => {
                if *streaming {
                    writer.reset(0);
                    self.retire(key, slot);
                    return Some(ManagedOutcome::Streamed);
                }
                let status =
                    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                let headers = with_date(headers);
                if writer.send_headers(status, &headers).await.is_err() {
                    self.retire(key, slot);
                    return Some(ManagedOutcome::Streamed);
                }
                *streaming = true;
                None
            }
            StreamCmd::Write { chunk } => {
                if !*streaming || writer.write(&chunk).await.is_err() {
                    writer.reset(0);
                    self.retire(key, slot);
                    return Some(ManagedOutcome::Streamed);
                }
                self.host.stage_writable(token);
                None
            }
            StreamCmd::End => {
                let _ = writer.finish().await;
                self.retire(key, slot);
                Some(ManagedOutcome::Streamed)
            }
            StreamCmd::EndWithTrailers { trailers } => {
                let trailers: Vec<(Vec<u8>, Vec<u8>)> = trailers
                    .into_iter()
                    .map(|trailer| (trailer.name, trailer.value))
                    .collect();
                if writer.write_trailers(&trailers).await.is_ok() {
                    let _ = writer.finish().await;
                } else {
                    writer.reset(0);
                }
                self.retire(key, slot);
                Some(ManagedOutcome::Streamed)
            }
            StreamCmd::Cancel => {
                writer.reset(0);
                self.retire(key, slot);
                Some(ManagedOutcome::Streamed)
            }
            StreamCmd::Upgrade { .. } | StreamCmd::H2Upgrade { .. } => {
                writer.reset(0);
                self.retire(key, slot);
                Some(ManagedOutcome::Streamed)
            }
        }
    }

    fn retire(&self, key: WaiterKey, slot: u32) {
        self.host.unpark_closed(self.worker, slot);
        self.remove_waiter(key);
        release(&self.slots, slot);
    }

    fn remove_waiter(&self, key: WaiterKey) {
        self.waiters.lock().remove(&key);
    }
}

fn release(slots: &Mutex<Slots>, slot: u32) {
    slots.lock().release(slot);
}
