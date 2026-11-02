use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use veylor_router::{MAX_ROUTE_CAPTURES, RouteCapture, capture_route, matches_route};
use veylor_server::{
    EventSlot, HostEventKind, HostHandle, MAX_REQUEST_HEADER_BYTES, MAX_REQUEST_HEADERS,
    RequestHeadError, RequestHeader, RespondOutcome,
};

use crate::native::{App, Kind, internal_error};
use crate::request::{Param, Request};
use crate::response::{Response, reason};

const EVENT_SLOTS: usize = 64;
const METHOD_BYTES: usize = 64 * 16;
const PATH_BYTES: usize = 64 * 2048;
const BODY_CHUNK: usize = 4096;
const POLL_TIMEOUT_NS: u64 = 50_000_000;

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
    let mut body = Vec::with_capacity(BODY_CHUNK);

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
            serve(
                &host,
                app,
                event.token,
                method,
                target,
                &mut headers,
                &mut bytes,
                &mut body,
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn serve<S: Send + Sync + 'static>(
    host: &HostHandle,
    app: &'static App<S>,
    token: veylor_runtime::ConnectionToken,
    method: &[u8],
    target: &[u8],
    headers: &mut [RequestHeader],
    bytes: &mut [u8],
    body: &mut Vec<u8>,
) {
    let route = app
        .managed
        .iter()
        .find(|route| route.method == method && matches_route(&route.pattern, target));
    let Some(route) = route else {
        if let Some(fallback) = app.fallback.as_ref() {
            let request = Request::new(method, target, &[], &[], &[], app.state());
            let response = catch_unwind(AssertUnwindSafe(|| fallback(&request, app.state())))
                .unwrap_or_else(|_| internal_error());
            respond(host, token, response);
            return;
        }
        respond(host, token, Response::not_found());
        return;
    };

    if route.kind == Kind::Echo {
        if host.upgrade_websocket_echo(token) == RespondOutcome::Ok {
            return;
        }
        respond(host, token, Response::new(400, &b"Bad Request"[..]));
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
    let head_fields = headers.get(..report.header_count).unwrap_or(&[]);
    let mut pairs = [(&b""[..], &b""[..]); MAX_REQUEST_HEADERS];
    for (index, header) in head_fields.iter().enumerate() {
        let name = &bytes[header.name.offset..header.name.offset + header.name.length];
        let value = &bytes[header.value.offset..header.value.offset + header.value.length];
        pairs[index] = (name, value);
    }
    let pairs = &pairs[..head_fields.len()];

    body.clear();
    let mut too_large = false;
    loop {
        let mut chunk = [0u8; BODY_CHUNK];
        match host.pull_body(token, &mut chunk) {
            (RespondOutcome::Ok, count, done) => {
                body.extend_from_slice(&chunk[..count]);
                if body.len() > app.body_cap {
                    too_large = true;
                    break;
                }
                if done {
                    break;
                }
            }
            _ => return,
        }
    }
    if too_large {
        respond(host, token, Response::new(413, &b"Content Too Large"[..]));
        return;
    }

    let mut captures = [RouteCapture::default(); MAX_ROUTE_CAPTURES];
    let count = capture_route(&route.pattern, target, &mut captures).unwrap_or(0);
    let mut params = [Param {
        name: &[],
        value: &[],
    }; MAX_ROUTE_CAPTURES];
    for (index, capture) in captures.iter().take(count).enumerate() {
        params[index] = Param {
            name: capture.name(&route.pattern).unwrap_or(&[]),
            value: capture.value(target).unwrap_or(&[]),
        };
    }

    let incoming = Request::new(method, target, pairs, body, &params[..count], route.state);
    let response = catch_unwind(AssertUnwindSafe(|| {
        if route.layers.is_empty() {
            (route.handler)(&incoming, route.state)
        } else {
            crate::middleware::run_chain(&route.layers, &route.handler, incoming)
        }
    }))
    .unwrap_or_else(|_| internal_error());
    respond(host, token, response);
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
