use std::cmp::Ordering;
use std::ptr;

use veylor_h3::http::header::{ACCEPT_ENCODING, CONTENT_ENCODING, VARY};
use veylor_h3::http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use veylor_h3::{Body, Bytes};
use veylor_http1::{ALLOW_BYTES, ALLOW_METHODS, join_allow_value};
use veylor_router::{
    MAX_ROUTE_CAPTURES, RouteCapture, capture_route, compare_route_patterns, matches_route,
};

use crate::host::HOST_MAX_REASON_BYTES;
use crate::static_routes::{
    MAX_NATIVE_HEADERS, MAX_NATIVE_PARAMS, NativeHeader, NativeParam, NativeRequestView,
    accepts_gzip_value, compress_route_body, engine_owned_request_header,
};

use super::RouteMatch;
use super::response::{
    gzip_response, method_not_allowed, status_response, status_response_with_headers, text,
};
use crate::static_server::H2StaticRoute;

pub(super) fn serve_route(
    routes: &[H2StaticRoute],
    request: Request<()>,
    body: Bytes,
) -> Response<Body> {
    let method = request.method().as_str().as_bytes();
    let path = request.uri().path().as_bytes();
    let target = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().as_bytes())
        .unwrap_or(path);
    let gzip = request
        .headers()
        .get_all(ACCEPT_ENCODING)
        .iter()
        .any(|value| accepts_gzip_value(value.as_bytes()));
    let Some(found) = select(routes, method, path) else {
        let accept = header(&request, "accept-encoding");
        let range = header(&request, "range");
        let since = header(&request, "if-modified-since");
        let declared = body.len() as u64;
        if let Some(response) =
            super::file::serve(routes, method, path, declared, accept, range, since)
        {
            return response;
        }
        if let Some((allow, len)) = allow_value(routes, path) {
            return method_not_allowed(&allow[..len]);
        }
        return text(StatusCode::NOT_FOUND, b"Not Found");
    };
    if found.native {
        return serve_native(found.route, &body, method, target, request.headers(), gzip);
    }
    if found.route.managed {
        return text(StatusCode::SERVICE_UNAVAILABLE, b"Service Unavailable");
    }
    if gzip && let Some(twin) = found.route.gzip_body.as_deref() {
        return gzip_response(found.route.status, twin);
    }
    status_response(found.route.status, &found.route.body)
}

fn serve_native(
    route: &H2StaticRoute,
    body: &Bytes,
    method: &[u8],
    path: &[u8],
    request_headers: &HeaderMap,
    gzip: bool,
) -> Response<Body> {
    let Some((handler, user_data, native_gzip)) = route.native else {
        return text(StatusCode::INTERNAL_SERVER_ERROR, b"Internal Server Error");
    };
    let mut native_headers = [NativeHeader::EMPTY; MAX_NATIVE_HEADERS];
    let mut native_header_count = 0usize;
    for (name, value) in request_headers {
        if native_header_count == MAX_NATIVE_HEADERS
            || engine_owned_request_header(name.as_str().as_bytes())
        {
            continue;
        }
        native_headers[native_header_count] = NativeHeader {
            name: name.as_str().as_ptr(),
            name_len: name.as_str().len(),
            value: value.as_bytes().as_ptr(),
            value_len: value.as_bytes().len(),
        };
        native_header_count += 1;
    }
    let mut captures = [RouteCapture::default(); MAX_ROUTE_CAPTURES];
    let capture_count = capture_route(&route.path, path, &mut captures).unwrap_or(0);
    let mut native_params = [NativeParam::EMPTY; MAX_NATIVE_PARAMS];
    let mut native_param_count = 0usize;
    for capture in captures.iter().take(capture_count.min(MAX_NATIVE_PARAMS)) {
        let (Some(name), Some(value)) = (capture.name(&route.path), capture.value(path)) else {
            continue;
        };
        native_params[native_param_count] = NativeParam {
            name: name.as_ptr(),
            name_len: name.len(),
            value: value.as_ptr(),
            value_len: value.len(),
        };
        native_param_count += 1;
    }
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
        headers: native_headers.as_ptr(),
        header_count: native_header_count,
        params: native_params.as_ptr(),
        param_count: native_param_count,
    };
    let mut reason = [0u8; HOST_MAX_REASON_BYTES];
    match view.invoke(handler, user_data, &mut reason) {
        Ok(invoked) => {
            let body = invoked.body();
            if gzip
                && native_gzip
                && let Ok(Some(twin)) = compress_route_body(body)
            {
                let mut response =
                    status_response_with_headers(invoked.status, &twin, invoked.headers());
                response
                    .headers_mut()
                    .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
                response
                    .headers_mut()
                    .insert(VARY, HeaderValue::from_static("accept-encoding"));
                return response;
            }
            status_response_with_headers(invoked.status, body, invoked.headers())
        }
        Err(_) => text(StatusCode::INTERNAL_SERVER_ERROR, b"Internal Server Error"),
    }
}

pub(super) fn select<'a>(
    routes: &'a [H2StaticRoute],
    method: &[u8],
    path: &[u8],
) -> Option<RouteMatch<'a>> {
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
    matches_route(pattern, target)
}

pub(super) fn allow_value(
    routes: &[H2StaticRoute],
    path: &[u8],
) -> Option<([u8; ALLOW_BYTES], usize)> {
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

pub(super) fn header<'a>(request: &'a Request<()>, name: &str) -> Option<&'a [u8]> {
    request
        .headers()
        .get_all(name)
        .iter()
        .next()
        .map(|value| value.as_bytes())
}
