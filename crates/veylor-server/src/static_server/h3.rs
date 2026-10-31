use std::net::{IpAddr, SocketAddr};
use std::ptr;
use std::sync::Arc;

use veylor_h3::http::{Method, Request, Response, StatusCode};

use crate::host::HOST_MAX_REASON_BYTES;
use crate::static_routes::{
    MAX_RESPONSE_BODY, NativeRequestView, accepts_gzip_value, compress_route_body,
};

use super::{H2StaticRoute, ServerConfig, ServerError};

pub(crate) fn start(
    config: &ServerConfig,
    ip: IpAddr,
    routes: &[H2StaticRoute],
) -> Result<Option<veylor_h3::RunningServer>, ServerError> {
    let Some(port) = config.h3_port else {
        return Ok(None);
    };
    let tls = config.tls.as_ref().ok_or(ServerError::InvalidConfig)?;
    let tls = veylor_h3::Tls::new(tls.cert_chain.clone(), tls.key.clone_key());
    let routes = Arc::new(routes.to_vec());
    let handler = move |request: Request<()>, body: veylor_h3::Bytes| serve(&routes, request, body);
    let server = veylor_h3::Server::bind(
        SocketAddr::new(ip, port),
        veylor_h3::Config {
            tls,
            limits: veylor_h3::Limits::default(),
        },
    );
    server
        .start(handler)
        .map(Some)
        .map_err(|error| ServerError::Thread(format!("h3: {error}")))
}

fn serve(
    routes: &[H2StaticRoute],
    request: Request<()>,
    body: veylor_h3::Bytes,
) -> Response<veylor_h3::Bytes> {
    let method = request.method().as_str().as_bytes();
    let path = request.uri().path().as_bytes();
    let gzip = request
        .headers()
        .get_all(veylor_h3::http::header::ACCEPT_ENCODING)
        .iter()
        .any(|value| accepts_gzip_value(value.as_bytes()));
    let Some(route) = find(routes, method, path) else {
        return text(StatusCode::NOT_FOUND, b"Not Found");
    };
    if route.managed {
        return text(StatusCode::SERVICE_UNAVAILABLE, b"Service Unavailable");
    }
    if route.file_root.is_some() {
        return text(StatusCode::NOT_FOUND, b"Not Found");
    }
    if let Some((handler, user_data, native_gzip)) = route.native {
        let view = NativeRequestView {
            method: request.method().as_str().as_ptr(),
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
        return match view.invoke(handler, user_data, &mut reason, &mut body_scratch) {
            Ok(invoked) => {
                if gzip && native_gzip {
                    if let Ok(Some(twin)) = compress_route_body(invoked.body) {
                        return gzip_response(invoked.status, &twin);
                    }
                }
                status_response(invoked.status, invoked.body)
            }
            Err(_) => text(StatusCode::INTERNAL_SERVER_ERROR, b"Internal Server Error"),
        };
    }
    if gzip {
        if let Some(twin) = route.gzip_body.as_deref() {
            return gzip_response(route.status, twin);
        }
    }
    status_response(route.status, &route.body)
}

fn find<'a>(routes: &'a [H2StaticRoute], method: &[u8], path: &[u8]) -> Option<&'a H2StaticRoute> {
    if let Some(route) = routes
        .iter()
        .find(|route| route.method == method && route.path == path)
    {
        return Some(route);
    }
    if method == Method::HEAD.as_str().as_bytes() {
        return routes
            .iter()
            .find(|route| route.method == b"GET" && route.path == path);
    }
    None
}

fn status_response(status: u16, body: &[u8]) -> Response<veylor_h3::Bytes> {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    text(status, body)
}

fn gzip_response(status: u16, body: &[u8]) -> Response<veylor_h3::Bytes> {
    let mut response = status_response(status, body);
    response.headers_mut().insert(
        veylor_h3::http::header::CONTENT_ENCODING,
        veylor_h3::http::HeaderValue::from_static("gzip"),
    );
    response.headers_mut().insert(
        veylor_h3::http::header::VARY,
        veylor_h3::http::HeaderValue::from_static("accept-encoding"),
    );
    response
}

fn text(status: StatusCode, body: &[u8]) -> Response<veylor_h3::Bytes> {
    Response::builder()
        .status(status)
        .header(
            veylor_h3::http::header::CONTENT_LENGTH,
            body.len().to_string(),
        )
        .body(veylor_h3::Bytes::copy_from_slice(body))
        .expect("valid response")
}
