use std::cmp::Ordering;
use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use tokio::sync::mpsc;
use veylor_h3::http::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, VARY};
use veylor_h3::http::{HeaderName, HeaderValue, Request, Response, StatusCode};
use veylor_h3::{
    Body, Bytes, ConnectProtocol, ManagedOutcome, RequestStream, RequestTrailers, ResponseWriter,
    RunningServer, Server, Tls,
};
use veylor_http1::{ALLOW_BYTES, ALLOW_METHODS, join_allow_value};
use veylor_router::{MAX_ROUTE_CAPTURES, RouteCapture, compare_route_patterns, match_route};
use veylor_runtime::ConnectionToken;

use crate::connection::streams::StreamCmd;
use crate::host::{H3_PARK_SLOTS, HOST_MAX_REASON_BYTES, HostShared, RespondOutcome, StagedWork};
use crate::static_files::HTTP_DATE_BYTES;
use crate::static_routes::now_http_date;
use crate::static_routes::{
    MAX_RESPONSE_BODY, NativeRequestView, accepts_gzip_value, compress_route_body,
};

use super::{H2StaticRoute, ServerConfig, ServerError};

mod file;
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
        certs: Vec<veylor_tls::CertificateDer<'static>>,
        key: veylor_tls::PrivateKeyDer<'static>,
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
    let tls = Tls::new(tls.cert_chain.clone(), tls.key.clone_key());
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
        let mut waiters = match waiters.lock() {
            Ok(waiters) => waiters,
            Err(_) => return,
        };
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
        eprintln!(
            "connect protocol: {protocol:?} path: {}",
            request.uri().path()
        );
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
        let body_limit = found.route.max_body_bytes;
        let trailer_allow = found.route.trailers.clone();
        Box::pin(async move {
            self.park(
                request,
                body,
                route_id,
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
        method: &[u8],
        path: &[u8],
        body_limit: u64,
        trailers: Vec<(Vec<u8>, Vec<u8>)>,
        trailer_allow: &[Vec<u8>],
        writer: &mut ResponseWriter<'_>,
    ) -> ManagedOutcome {
        let Some((slot, generation)) = self.slots.lock().ok().and_then(|mut slots| slots.acquire())
        else {
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
            .map(|mut waiters| waiters.insert(key, Waiter { token, sender }).is_none())
            .unwrap_or(false);
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
        if let Ok(mut waiters) = self.waiters.lock() {
            if let Some(waiter) = waiters.get_mut(&key) {
                waiter.token = event_token;
            }
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
        if let Ok(mut waiters) = self.waiters.lock() {
            waiters.remove(&key);
        }
    }
}

fn release(slots: &Mutex<Slots>, slot: u32) {
    if let Ok(mut slots) = slots.lock() {
        slots.release(slot);
    }
}

fn staged_response(work: StagedWork) -> Response<Body> {
    if work.cancel {
        return text(StatusCode::SERVICE_UNAVAILABLE, b"cancelled");
    }
    let mut response = Response::new(Body::Bytes(Bytes::from(work.body)));
    *response.status_mut() =
        StatusCode::from_u16(work.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let length = response.body().content_length().unwrap_or(0).to_string();
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_bytes(length.as_bytes()).expect("length"),
    );
    for (name, value) in work.headers {
        if name.eq_ignore_ascii_case(b"content-length") {
            continue;
        }
        let Ok(name) = HeaderName::from_bytes(&name) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_bytes(&value) else {
            continue;
        };
        response.headers_mut().append(name, value);
    }
    response
}

fn serve_route(routes: &[H2StaticRoute], request: Request<()>, body: Bytes) -> Response<Body> {
    let method = request.method().as_str().as_bytes();
    let path = request.uri().path().as_bytes();
    let gzip = request
        .headers()
        .get_all(ACCEPT_ENCODING)
        .iter()
        .any(|value| accepts_gzip_value(value.as_bytes()));
    let Some(found) = select(routes, method, path) else {
        let range = header(&request, "range");
        let since = header(&request, "if-modified-since");
        let declared = body.len() as u64;
        if let Some(response) = file::serve(routes, method, path, declared, range, since) {
            return response;
        }
        if let Some((allow, len)) = allow_value(routes, path) {
            return method_not_allowed(&allow[..len]);
        }
        return text(StatusCode::NOT_FOUND, b"Not Found");
    };
    if found.native {
        return serve_native(found.route, &body, method, path, gzip);
    }
    if found.route.managed {
        return text(StatusCode::SERVICE_UNAVAILABLE, b"Service Unavailable");
    }
    if gzip {
        if let Some(twin) = found.route.gzip_body.as_deref() {
            return gzip_response(found.route.status, twin);
        }
    }
    status_response(found.route.status, &found.route.body)
}

fn serve_native(
    route: &H2StaticRoute,
    body: &Bytes,
    method: &[u8],
    path: &[u8],
    gzip: bool,
) -> Response<Body> {
    let Some((handler, user_data, native_gzip)) = route.native else {
        return text(StatusCode::INTERNAL_SERVER_ERROR, b"Internal Server Error");
    };
    let view = NativeRequestView {
        method: method.as_ptr(),
        method_len: method.len(),
        path: path.as_ptr(),
        path_len: path.len(),
        has_body: u8::from(!body.is_empty()),
        body_len: body.len() as u64,
        body: if body.is_empty() {
            ptr::null()
        } else {
            body.as_ptr()
        },
    };
    let mut reason = [0u8; HOST_MAX_REASON_BYTES];
    let mut body_scratch = [0u8; MAX_RESPONSE_BODY];
    match view.invoke(handler, user_data, &mut reason, &mut body_scratch) {
        Ok(invoked) => {
            if gzip && native_gzip {
                if let Ok(Some(twin)) = compress_route_body(invoked.body) {
                    return gzip_response(invoked.status, &twin);
                }
            }
            status_response(invoked.status, invoked.body)
        }
        Err(_) => text(StatusCode::INTERNAL_SERVER_ERROR, b"Internal Server Error"),
    }
}

fn select<'a>(routes: &'a [H2StaticRoute], method: &[u8], path: &[u8]) -> Option<RouteMatch<'a>> {
    let mut best: Option<(&'a [u8], RouteMatch<'a>)> = None;
    for (index, route) in routes.iter().enumerate() {
        if route.file_root.is_some() || route.method != method {
            continue;
        }
        if route_matches(&route.path, path) {
            select_route(&mut best, route, index);
        }
    }
    if best.is_none() && method == b"HEAD" {
        for (index, route) in routes.iter().enumerate() {
            if route.file_root.is_some() || route.method != b"GET" {
                continue;
            }
            if route_matches(&route.path, path) {
                select_route(&mut best, route, index);
            }
        }
    }
    best.map(|(_, found)| found)
}

fn select_route<'a>(
    best: &mut Option<(&'a [u8], RouteMatch<'a>)>,
    route: &'a H2StaticRoute,
    index: usize,
) {
    let better = best
        .as_ref()
        .is_none_or(|(current, _)| compare_route_patterns(&route.path, current) == Ordering::Less);
    if better {
        *best = Some((
            &route.path,
            RouteMatch {
                index,
                route,
                native: route.native.is_some(),
            },
        ));
    }
}

fn route_matches(pattern: &[u8], target: &[u8]) -> bool {
    let mut captures = [RouteCapture::default(); MAX_ROUTE_CAPTURES];
    match_route(pattern, target, &mut captures).is_some()
}

fn allow_value(routes: &[H2StaticRoute], path: &[u8]) -> Option<([u8; ALLOW_BYTES], usize)> {
    let mut methods: [&[u8]; ALLOW_METHODS] = [&[]; ALLOW_METHODS];
    let mut count = 0;
    for route in routes {
        if route.file_root.is_some() || !route_matches(&route.path, path) {
            continue;
        }
        if methods[..count].contains(&route.method.as_slice()) {
            continue;
        }
        let slot = methods.get_mut(count)?;
        *slot = &route.method;
        count += 1;
    }
    if count == 0 {
        return None;
    }
    let mut out = [0u8; ALLOW_BYTES];
    let len = join_allow_value(&methods[..count], &mut out)?;
    Some((out, len))
}

fn header<'a>(request: &'a Request<()>, name: &str) -> Option<&'a [u8]> {
    request
        .headers()
        .get_all(name)
        .iter()
        .next()
        .map(|value| value.as_bytes())
}

fn with_date(mut headers: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let has_date = headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case(b"date"));
    if !has_date {
        let mut date = [0u8; HTTP_DATE_BYTES];
        let len = now_http_date(&mut date);
        headers.push((b"date".to_vec(), date[..len].to_vec()));
    }
    headers
}

fn gzip_response(status: u16, body: &[u8]) -> Response<Body> {
    let mut response = status_response(status, body);
    response
        .headers_mut()
        .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    response
        .headers_mut()
        .insert(VARY, HeaderValue::from_static("accept-encoding"));
    response
}

fn method_not_allowed(allow: &[u8]) -> Response<Body> {
    let mut response = text(StatusCode::METHOD_NOT_ALLOWED, b"Method Not Allowed");
    if let Ok(value) = HeaderValue::from_bytes(allow) {
        response.headers_mut().insert("allow", value);
    }
    response
}

fn status_response(status: u16, body: &[u8]) -> Response<Body> {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    text(status, body)
}

fn text(status: StatusCode, body: &[u8]) -> Response<Body> {
    let mut response = Response::new(Body::Bytes(Bytes::copy_from_slice(body)));
    *response.status_mut() = status;
    let length = body.len().to_string();
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_bytes(length.as_bytes()).expect("length"),
    );
    response
}
