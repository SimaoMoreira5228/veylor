use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use veylor_router::{MAX_ROUTE_CAPTURES, RouteCapture, capture_route};
use veylor_server::{
    EventSlot, HostEventKind, HostHandle, MAX_REQUEST_HEADER_BYTES, MAX_REQUEST_HEADERS,
    RequestHeadError, RequestHeader, RespondOutcome,
};

use crate::native::{App, Kind, Route, internal_error};
use crate::request::{Param, Request};
use crate::response::{Response, reason};

const EVENT_SLOTS: usize = 64;
const METHOD_BYTES: usize = 64 * 16;
const PATH_BYTES: usize = 64 * 2048;
const BODY_CHUNK: usize = 4096;
const POLL_TIMEOUT_NS: u64 = 50_000_000;
const PENDING_CAP: usize = 128;

struct Pending {
    token: veylor_runtime::ConnectionToken,
    route_token: u64,
    method: Vec<u8>,
    target: Vec<u8>,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: Vec<u8>,
}

impl Pending {
    fn pull<S: Send + Sync + 'static>(&mut self, host: &HostHandle, app: &'static App<S>) -> Pull {
        let mut chunk = [0u8; BODY_CHUNK];
        loop {
            match host.pull_body(self.token, &mut chunk) {
                (RespondOutcome::Ok, count, done) => {
                    if count > 0 {
                        self.body.extend_from_slice(&chunk[..count]);
                    }
                    if self.body.len() > app.body_cap {
                        return Pull::TooLarge;
                    }
                    if done {
                        return Pull::Ready;
                    }
                    if count == 0 {
                        return Pull::Waiting;
                    }
                }
                _ => return Pull::Gone,
            }
        }
    }
}

enum Pull {
    Ready,
    Waiting,
    TooLarge,
    Gone,
}

pub(crate) fn spawn<S: Send + Sync + 'static>(
    worker: usize,
    host: HostHandle,
    app: &'static App<S>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name(format!("veylor-web-dispatch-{worker}"))
        .spawn(move || run(worker, host, app, stop))
        .expect("spawn web dispatcher")
}

fn run<S: Send + Sync + 'static>(
    worker: usize,
    host: HostHandle,
    app: &'static App<S>,
    stop: Arc<AtomicBool>,
) {
    let empty = EventSlot {
        kind: HostEventKind::Shutdown,
        token: veylor_runtime::ConnectionToken::default(),
        route_id: 0,
        route_token: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    };
    let mut events = [empty; EVENT_SLOTS];
    let mut methods = vec![0u8; METHOD_BYTES];
    let mut paths = vec![0u8; PATH_BYTES];
    let mut headers = vec![RequestHeader::default(); MAX_REQUEST_HEADERS];
    let mut bytes = vec![0u8; MAX_REQUEST_HEADER_BYTES];
    let mut pending: Vec<Pending> = Vec::new();

    while !stop.load(Ordering::Acquire) {
        let report = host.poll_worker(
            worker,
            POLL_TIMEOUT_NS,
            &mut events,
            &mut methods,
            &mut paths,
        );
        for event in &events[..report.copied] {
            if event.kind != HostEventKind::Request {
                continue;
            }
            let method = &methods[event.method_offset..event.method_offset + event.method_len];
            let target = &paths[event.path_offset..event.path_offset + event.path_len];
            accept(
                &host,
                app,
                event.token,
                event.route_token,
                method,
                target,
                &mut pending,
                &mut headers,
                &mut bytes,
            );
        }
        advance(&host, app, &mut pending);
    }
}

#[allow(clippy::too_many_arguments)]
fn accept<S: Send + Sync + 'static>(
    host: &HostHandle,
    app: &'static App<S>,
    token: veylor_runtime::ConnectionToken,
    route_token: u64,
    method: &[u8],
    target: &[u8],
    pending: &mut Vec<Pending>,
    headers: &mut [RequestHeader],
    bytes: &mut [u8],
) {
    let route = managed_route(app, route_token);
    let Some(route) = route else {
        fallback(host, app, token, method, target);
        return;
    };

    if route.kind == Kind::Sse {
        if let Some(sender) = crate::sse::SseSender::begin(host.clone(), token) {
            spawn_sse(route, sender);
        }
        return;
    }

    if route.kind == Kind::Echo {
        if host.upgrade_websocket_echo(token) == RespondOutcome::Ok {
            return;
        }
        respond(host, token, Response::new(400, &b"Bad Request"[..]));
        return;
    }

    if pending.len() >= PENDING_CAP {
        respond(host, token, Response::new(503, &b"Service Unavailable"[..]));
        return;
    }

    let report = match host.request_head(token, headers, bytes) {
        Ok(report) => report,
        Err(RequestHeadError::Unavailable(_)) => return,
        Err(RequestHeadError::Capacity { .. }) => {
            respond(
                host,
                token,
                Response::new(431, &b"Request Header Fields Too Large"[..]),
            );
            return;
        }
    };

    let target = &bytes[..report.target.length];
    let mut owned = Vec::with_capacity(report.header_count);
    for header in &headers[..report.header_count] {
        let name = &bytes[header.name.offset..header.name.offset + header.name.length];
        let value = &bytes[header.value.offset..header.value.offset + header.value.length];
        owned.push((name.to_vec(), value.to_vec()));
    }

    if app.pending.fetch_add(1, Ordering::AcqRel) >= PENDING_CAP {
        app.pending.fetch_sub(1, Ordering::AcqRel);
        respond(host, token, Response::new(503, &b"Service Unavailable"[..]));
        return;
    }

    pending.push(Pending {
        token,
        route_token,
        method: method.to_vec(),
        target: target.to_vec(),
        headers: owned,
        body: Vec::new(),
    });
}

fn advance<S: Send + Sync + 'static>(
    host: &HostHandle,
    app: &'static App<S>,
    pending: &mut Vec<Pending>,
) {
    let mut index = 0;
    while index < pending.len() {
        match pending[index].pull(host, app) {
            Pull::Ready => {
                let request = pending.swap_remove(index);
                app.pending.fetch_sub(1, Ordering::AcqRel);
                run_handler(host, app, request);
            }
            Pull::TooLarge => {
                let request = pending.swap_remove(index);
                app.pending.fetch_sub(1, Ordering::AcqRel);
                respond(
                    host,
                    request.token,
                    Response::new(413, &b"Content Too Large"[..]),
                );
            }
            Pull::Gone => {
                pending.swap_remove(index);
                app.pending.fetch_sub(1, Ordering::AcqRel);
            }
            Pull::Waiting => index += 1,
        }
    }
}

fn run_handler<S: Send + Sync + 'static>(
    host: &HostHandle,
    app: &'static App<S>,
    request: Pending,
) {
    let Some(route) = managed_route(app, request.route_token) else {
        respond(host, request.token, Response::not_found());
        return;
    };

    let mut captures = [RouteCapture::default(); MAX_ROUTE_CAPTURES];
    let count = capture_route(&route.pattern, &request.target, &mut captures).unwrap_or(0);
    let mut params = [Param {
        name: &[],
        value: &[],
    }; MAX_ROUTE_CAPTURES];
    for (index, capture) in captures.iter().take(count).enumerate() {
        params[index] = Param {
            name: capture.name(&route.pattern).unwrap_or(&[]),
            value: capture.value(&request.target).unwrap_or(&[]),
        };
    }

    let mut pairs = [(&b""[..], &b""[..]); MAX_REQUEST_HEADERS];
    for (slot, (name, value)) in pairs.iter_mut().zip(request.headers.iter()) {
        *slot = (name.as_slice(), value.as_slice());
    }
    let pairs = &pairs[..request.headers.len()];
    let incoming = Request::new(
        &request.method,
        &request.target,
        pairs,
        &request.body,
        &params[..count],
        route.state,
    );
    let response = catch_unwind(AssertUnwindSafe(|| {
        if route.layers.is_empty() {
            (route.handler)(&incoming, route.state)
        } else {
            crate::middleware::run_chain(&route.layers, &route.handler, incoming)
        }
    }))
    .unwrap_or_else(|_| internal_error());
    respond(host, request.token, response);
}

fn spawn_sse<S: Send + Sync + 'static>(route: &Route<S>, sender: crate::sse::SseSender) {
    let Some(handler) = route.sse.clone() else {
        return;
    };
    let state = route.state;
    let _ = thread::Builder::new()
        .name("veylor-web-sse".to_owned())
        .spawn(move || {
            let _ = catch_unwind(AssertUnwindSafe(|| handler(state, sender)));
        });
}

fn managed_route<S: Send + Sync + 'static>(
    app: &'static App<S>,
    token: u64,
) -> Option<&'static Route<S>> {
    token
        .checked_sub(1)
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| app.managed.get(index))
        .map(Arc::as_ref)
}

fn fallback<S: Send + Sync + 'static>(
    host: &HostHandle,
    app: &'static App<S>,
    token: veylor_runtime::ConnectionToken,
    method: &[u8],
    target: &[u8],
) {
    if let Some(fallback) = app.fallback.as_ref() {
        let request = Request::new(method, target, &[], &[], &[], app.state());
        let response = catch_unwind(AssertUnwindSafe(|| fallback(&request, app.state())))
            .unwrap_or_else(|_| internal_error());
        respond(host, token, response);
        return;
    }
    respond(host, token, Response::not_found());
}

fn respond(host: &HostHandle, token: veylor_runtime::ConnectionToken, response: Response) {
    let status = response.status_code();
    let mut headers = Vec::with_capacity(response.headers().len());
    for (name, value) in response.headers() {
        headers.push((name.as_slice(), value.as_slice()));
    }
    let _ = host.respond_with_headers(
        token,
        status,
        reason(status),
        &headers,
        response.body().as_bytes(),
    );
}
