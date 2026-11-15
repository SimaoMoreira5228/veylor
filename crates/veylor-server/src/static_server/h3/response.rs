use veylor_h3::http::header::{CONTENT_ENCODING, CONTENT_LENGTH, VARY};
use veylor_h3::http::{HeaderName, HeaderValue, Response, StatusCode};
use veylor_h3::{Body, Bytes};

use crate::host::StagedWork;
use crate::static_files::HTTP_DATE_BYTES;
use crate::static_routes::now_http_date;
use crate::static_routes::{
    NativeHeader, engine_owned_response_header, raw_bytes, valid_field_value,
};

pub(super) fn staged_response(work: StagedWork) -> Response<Body> {
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

pub(super) fn with_date(mut headers: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
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

pub(super) fn gzip_response(status: u16, body: &[u8]) -> Response<Body> {
    let mut response = status_response(status, body);
    response
        .headers_mut()
        .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    response
        .headers_mut()
        .insert(VARY, HeaderValue::from_static("accept-encoding"));
    response
}

pub(super) fn method_not_allowed(allow: &[u8]) -> Response<Body> {
    let mut response = text(StatusCode::METHOD_NOT_ALLOWED, b"Method Not Allowed");
    if let Ok(value) = HeaderValue::from_bytes(allow) {
        response.headers_mut().insert("allow", value);
    }
    response
}

pub(super) fn status_response_with_headers(
    status: u16,
    body: &[u8],
    headers: &[NativeHeader],
) -> Response<Body> {
    let mut response = status_response(status, body);
    for header in headers {
        let (Some(name), Some(value)) = (
            raw_bytes(header.name, header.name_len),
            raw_bytes(header.value, header.value_len),
        ) else {
            continue;
        };
        if engine_owned_response_header(name) || !valid_field_value(value) {
            continue;
        }
        let (Ok(name), Ok(value)) = (HeaderName::from_bytes(name), HeaderValue::from_bytes(value))
        else {
            continue;
        };
        response.headers_mut().append(name, value);
    }
    response
}

pub(super) fn status_response(status: u16, body: &[u8]) -> Response<Body> {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    text(status, body)
}

pub(super) fn text(status: StatusCode, body: &[u8]) -> Response<Body> {
    let mut response = Response::new(Body::Bytes(Bytes::copy_from_slice(body)));
    *response.status_mut() = status;
    let length = body.len().to_string();
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_bytes(length.as_bytes()).expect("length"),
    );
    response
}
