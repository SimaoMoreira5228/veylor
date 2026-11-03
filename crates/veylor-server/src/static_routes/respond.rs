// NOTE: response-assembly half of the static_routes seam: registry owns route storage and
// validation (super), this module owns response encoding over already-matched routes.
use veylor_http1::{
    ALLOW_BYTES, ALLOW_METHODS, ConnectionHeader, Field, Header, RequestHead, RequestVersion,
    ResponseFraming, ResponseToWrite, TargetForm, connection_header_for_response,
    encode_response_head_trusted, join_allow_value, response_permits_reuse,
};
#[cfg(test)]
use veylor_router::RouteCapture;
use veylor_router::{RouteError, RouteKind, RouteTable};

use crate::connection::trailers::field;
use crate::static_files::{FilePlan, HTTP_DATE_BYTES};

use super::file::respond_file;
use super::native::respond_native;
use super::{CONTENT_ENCODING_GZIP, accepts_gzip_value};
use super::{
    MAX_STATIC_BODY_BYTES, RouteBuildError, StaticRoutes, in_native_handler, now_http_date,
    route_cap,
};

#[cfg(test)]
const CAPTURE_SLOTS: usize = 8;
// NOTE: Allow join and range/multipart scratch live in veylor_http1 (response_write):
// this path shares one join_allow_value plus the four size bounds with H2 routes,
// so the 405 plus OPTIONS and file assembly cannot drift between engines.
const NOT_FOUND_BODY: &[u8] = b"Not Found";
const METHOD_NOT_ALLOWED_BODY: &[u8] = b"Method Not Allowed";
const BAD_REQUEST_BODY: &[u8] = b"Bad Request";
const URI_TOO_LONG_BODY: &[u8] = b"URI Too Long";
const REQUEST_HEADER_FIELDS_TOO_LARGE_BODY: &[u8] = b"Request Header Fields Too Large";
const PAYLOAD_TOO_LARGE_BODY: &[u8] = b"Payload Too Large";
const EXPECTATION_FAILED_BODY: &[u8] = b"Expectation Failed";

pub struct EncodedStaticResponse {
    pub length: usize,
    pub keep_open: bool,
    pub drain_total: u64,
    pub interim_len: usize,
    pub file: Option<FilePlan>,
}

#[cfg(test)]
pub fn respond_to_head(
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    head: RequestHead,
    headers: &[Header],
    input: &[u8],
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let mut captures = [RouteCapture::default(); CAPTURE_SLOTS];
    let found = table.find_request(head, input, &mut captures);
    respond_to_head_matched(routes, table, head, found, headers, input, output)
}

// NOTE: the connection path resolves the route once for the managed-vs-static verdict, so
// it hands that match down instead of paying a second frozen-table walk and path scan.
pub(crate) fn respond_to_head_matched(
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    head: RequestHead,
    found: Result<veylor_router::RouteMatch<'_, '_>, RouteError>,
    headers: &[Header],
    input: &[u8],
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    if in_native_handler() {
        return Err(RouteBuildError::Invalid);
    }
    let method = head.method(input).unwrap_or_default();
    if head.target_form == TargetForm::Asterisk
        && method == b"OPTIONS"
        && head.content_length.unwrap_or(0) == 0
        && !head.is_chunked
        && !head.expect_continue
    {
        return respond_server_options(table, head, input, output);
    }
    // NOTE: natives dispatch before statics on the same Static slot (same method+path can
    // never sit in both lists); either hit skips the file fallback below, so natives win
    // over file mounts exactly like statics do.
    if let Ok(matched) = &found
        && matched.kind == RouteKind::Static
        && let Some(native) = routes.lookup_native(method, matched.pattern)
    {
        let path = head.target(input).unwrap_or(matched.pattern);
        return respond_native(
            native.handler,
            native.user_data,
            head,
            method,
            path,
            input,
            headers,
            matched.captures,
            matched.pattern,
            native.gzip,
            output,
        );
    }
    // NOTE: exact static routes win; file mounts serve only table misses, so existing
    // routes never change behavior when mounts are added.
    if matches!(found, Err(RouteError::MethodNotAllowed)) {
        return respond_method_not_allowed(table, head, input, output);
    }
    if matches!(found, Err(RouteError::NotFound))
        && let Some(file) = respond_file(routes, head, headers, input, output)?
    {
        return Ok(file);
    }
    // NOTE: unmatched requests have no route cap of their own, so 404s keep the default.
    let (status, reason, body, gzip, cap) = match found {
        Ok(found) => {
            let cap = route_cap(found.body);
            match routes.lookup(method, found.pattern) {
                Some(route) => (
                    route.status,
                    route.reason.as_slice(),
                    route.body.as_slice(),
                    route.gzip_body.as_deref(),
                    cap,
                ),
                None => return Err(RouteBuildError::Invalid),
            }
        }
        Err(RouteError::NotFound) => (
            404,
            b"Not Found".as_slice(),
            NOT_FOUND_BODY,
            None,
            MAX_STATIC_BODY_BYTES,
        ),
        Err(error) => return Err(RouteBuildError::from(error)),
    };
    let request_version = head
        .protocol_version(input)
        .ok_or(RouteBuildError::Invalid)?;
    // NOTE: static routes cannot use trailers, so any chunked request rejects without reading;
    // oversized declarations reject the same way with no interim, both against the matched
    // route's cap (zero admits no body byte at all; u64::MAX is input-buffer bounded).
    if head.is_chunked {
        return reject_payload(output, method, request_version);
    }
    let drain_total = match head.content_length {
        Some(length) if length > cap => {
            return reject_payload(output, method, request_version);
        }
        Some(length) => length,
        None => 0,
    };
    // NOTE: like the managed park path, the interim stages on the bare Expect flag (see the
    // park_request NOTE for the deliberate bodyless split); the staged shape is shared policy
    // and rejections above carry no interim in either engine.
    let interim_len = if head.expect_continue {
        encode_continue(output)?
    } else {
        0
    };
    // NOTE: a declared body at or under the cap is drained by the connection before any response
    // byte is written, so framing treats it as complete now; chunked and oversized already left.
    // NOTE: status-driven close stays path-owned and deliberately split: an unmatched static
    // path closes its 404 (like the 405/416 siblings below) while a managed host answering
    // 404 reuses under its park-time verdict (pinned by
    // managed_date_present_and_shaped_one_shot_stream_and_reuse) and the conn crate leaves
    // the same choice to its host via response_close. Only the request-driven half (version,
    // Connection header, drain) converges through connection_header_for_response.
    let response_close = status == 404;
    let connection = connection_header_for_response(
        head,
        input,
        status,
        ResponseFraming::FixedLength,
        true,
        response_close,
    );
    // NOTE: Date rides first on every H1 assembly below (static/file/native/reject), one
    // response-instant read per response; 1xx interims carry no Date by RFC 9110 10.2.1.
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let encode_gzip = match gzip {
        Some(twin) => match header_value(headers, head, input, b"accept-encoding") {
            Some(value) if accepts_gzip_value(value) => Some(twin),
            _ => None,
        },
        None => None,
    };
    let (body, content_encoding) = match encode_gzip {
        Some(twin) => (twin, Some(CONTENT_ENCODING_GZIP)),
        None => (body, None),
    };
    let encoding_field = Field {
        name: b"Content-Encoding",
        value: CONTENT_ENCODING_GZIP,
    };
    let vary_field = Field {
        name: b"Vary",
        value: b"Accept-Encoding",
    };
    let fields: &[Field] = match (content_encoding, gzip) {
        (Some(_), _) => &[date[0], encoding_field, vary_field],
        (None, Some(_)) => &[date[0], vary_field],
        (None, None) => &date,
    };
    let response = ResponseToWrite {
        request_method: method,
        request_version,
        requested_upgrade: b"",
        status,
        reason,
        fields,
        framing: ResponseFraming::FixedLength,
        content_length: body.len() as u64,
        connection,
    };
    let final_buf = output
        .get_mut(interim_len..)
        .ok_or(RouteBuildError::Invalid)?;
    let encoded =
        encode_response_head_trusted(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
    let mut length = interim_len + encoded.length;
    if encoded.body_allowed && !body.is_empty() {
        let end = length + body.len();
        output
            .get_mut(length..end)
            .ok_or(RouteBuildError::Invalid)?
            .copy_from_slice(body);
        length = end;
    }
    Ok(EncodedStaticResponse {
        length,
        // NOTE: the keep-open verdict is shared (veylor_http1::response_permits_reuse): Close
        // pins the conn shut. The pump never upgrades (requested_upgrade is always empty on
        // this path), so the upgrade input stays false, and request-body drain rides
        // drain_total/DrainingBody below instead of the shared booleans.
        keep_open: response_permits_reuse(false, connection),
        drain_total,
        interim_len,
        file: None,
    })
}

pub(crate) fn header_value<'a>(
    headers: &'a [Header],
    head: RequestHead,
    input: &'a [u8],
    name: &[u8],
) -> Option<&'a [u8]> {
    headers.iter().take(head.header_count).find_map(|header| {
        let found = field(input, header.name)?;
        if !found.eq_ignore_ascii_case(name) {
            return None;
        }
        field(input, header.value)
    })
}

// NOTE: RFC 9110 15.5.6 mandates Allow on 405 and says nothing about connection, so this
// mirrors the 404 shape exactly (default drain cap, 413 on framing overflow, interim,
// Date-first, fixed body) with one added Allow field, and closes like a 404.
fn respond_method_not_allowed(
    table: &RouteTable<'_, '_>,
    head: RequestHead,
    input: &[u8],
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let method = head.method(input).unwrap_or_default();
    let request_version = head
        .protocol_version(input)
        .ok_or(RouteBuildError::Invalid)?;
    if head.is_chunked {
        return reject_payload(output, method, request_version);
    }
    let drain_total = match head.content_length {
        Some(length) if length > MAX_STATIC_BODY_BYTES => {
            return reject_payload(output, method, request_version);
        }
        Some(length) => length,
        None => 0,
    };
    let interim_len = if head.expect_continue {
        encode_continue(output)?
    } else {
        0
    };
    let mut methods: [&[u8]; ALLOW_METHODS] = [&[]; ALLOW_METHODS];
    let count = table
        .allowed_methods_for_request(head, input, &mut methods)
        .map_err(|_| RouteBuildError::Invalid)?;
    let mut allow = [0u8; ALLOW_BYTES];
    let allow_len =
        join_allow_value(&methods[..count], &mut allow).ok_or(RouteBuildError::Invalid)?;
    encode_method_not_allowed(
        head,
        input,
        method,
        &allow[..allow_len],
        drain_total,
        interim_len,
        output,
    )
}

pub(crate) fn encode_method_not_allowed(
    head: RequestHead,
    input: &[u8],
    method: &[u8],
    allow: &[u8],
    drain_total: u64,
    interim_len: usize,
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let request_version = head
        .protocol_version(input)
        .ok_or(RouteBuildError::Invalid)?;
    let connection =
        connection_header_for_response(head, input, 405, ResponseFraming::FixedLength, true, true);
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let fields = [
        Field {
            name: b"Date",
            value: &now[..now_len],
        },
        Field {
            name: b"Allow",
            value: allow,
        },
    ];
    let response = ResponseToWrite {
        request_method: method,
        request_version,
        requested_upgrade: b"",
        status: 405,
        reason: b"Method Not Allowed",
        fields: &fields,
        framing: ResponseFraming::FixedLength,
        content_length: METHOD_NOT_ALLOWED_BODY.len() as u64,
        connection,
    };
    let final_buf = output
        .get_mut(interim_len..)
        .ok_or(RouteBuildError::Invalid)?;
    let encoded =
        encode_response_head_trusted(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
    let mut length = interim_len + encoded.length;
    if encoded.body_allowed && !METHOD_NOT_ALLOWED_BODY.is_empty() {
        let end = length + METHOD_NOT_ALLOWED_BODY.len();
        output
            .get_mut(length..end)
            .ok_or(RouteBuildError::Invalid)?
            .copy_from_slice(METHOD_NOT_ALLOWED_BODY);
        length = end;
    }
    Ok(EncodedStaticResponse {
        length,
        keep_open: response_permits_reuse(false, connection),
        drain_total,
        interim_len,
        file: None,
    })
}

fn respond_server_options(
    table: &RouteTable<'_, '_>,
    head: RequestHead,
    input: &[u8],
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let method = head.method(input).unwrap_or_default();
    let mut methods: [&[u8]; ALLOW_METHODS] = [&[]; ALLOW_METHODS];
    let count = table
        .server_allow_methods(&mut methods)
        .map_err(|_| RouteBuildError::Invalid)?;
    let mut allow = [0u8; ALLOW_BYTES];
    let allow_len =
        join_allow_value(&methods[..count], &mut allow).ok_or(RouteBuildError::Invalid)?;
    let connection =
        connection_header_for_response(head, input, 200, ResponseFraming::FixedLength, true, false);
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let fields = [
        Field {
            name: b"Date",
            value: &now[..now_len],
        },
        Field {
            name: b"Allow",
            value: &allow[..allow_len],
        },
    ];
    let response = ResponseToWrite {
        request_method: method,
        request_version: head
            .protocol_version(input)
            .ok_or(RouteBuildError::Invalid)?,
        requested_upgrade: b"",
        status: 200,
        reason: b"OK",
        fields: &fields,
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        connection,
    };
    let encoded =
        encode_response_head_trusted(&response, output).map_err(|_| RouteBuildError::Invalid)?;
    Ok(EncodedStaticResponse {
        length: encoded.length,
        keep_open: response_permits_reuse(false, connection),
        drain_total: 0,
        interim_len: 0,
        file: None,
    })
}

// NOTE: rejection mapping stays inside the static path's 404/400+close and 413+close
// families: NotFound/Forbidden/NotAFile all read as 404+close (traversal gets no
// existence oracle), TooLarge reads as 413+close like any over-cap body. No new status.
// Bodies drain within the same default cap as unmatched static paths; chunked rejects.

pub(crate) fn reject_payload(
    output: &mut [u8],
    method: &[u8],
    request_version: RequestVersion,
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let length = respond_payload_too_large(output, method, request_version)?;
    Ok(EncodedStaticResponse {
        length,
        keep_open: false,
        drain_total: 0,
        interim_len: 0,
        file: None,
    })
}

pub(crate) fn encode_continue(output: &mut [u8]) -> Result<usize, RouteBuildError> {
    // NOTE: the interim head shape is shared (veylor_http1::continue_interim); this function
    // only stages those bytes into pump/host output buffers ahead of the final. Staging,
    // flush timing, and drain ordering stay transport-local in both engines.
    let interim = veylor_http1::continue_interim();
    let encoded =
        encode_response_head_trusted(&interim, output).map_err(|_| RouteBuildError::Invalid)?;
    Ok(encoded.length)
}

pub fn respond_payload_too_large(
    output: &mut [u8],
    method: &[u8],
    request_version: RequestVersion,
) -> Result<usize, RouteBuildError> {
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let response = ResponseToWrite {
        request_method: method,
        request_version,
        requested_upgrade: b"",
        status: 413,
        reason: b"Payload Too Large",
        fields: &date,
        framing: ResponseFraming::FixedLength,
        content_length: PAYLOAD_TOO_LARGE_BODY.len() as u64,
        connection: ConnectionHeader::Close,
    };
    let encoded =
        encode_response_head_trusted(&response, output).map_err(|_| RouteBuildError::Invalid)?;
    let mut length = encoded.length;
    if encoded.body_allowed && !PAYLOAD_TOO_LARGE_BODY.is_empty() {
        let end = length + PAYLOAD_TOO_LARGE_BODY.len();
        output
            .get_mut(length..end)
            .ok_or(RouteBuildError::Invalid)?
            .copy_from_slice(PAYLOAD_TOO_LARGE_BODY);
        length = end;
    }
    Ok(length)
}

pub fn respond_expectation_failed(
    output: &mut [u8],
    request_version: RequestVersion,
) -> Result<usize, RouteBuildError> {
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let response = ResponseToWrite {
        request_method: b"GET",
        request_version,
        requested_upgrade: b"",
        status: 417,
        reason: b"Expectation Failed",
        fields: &date,
        framing: ResponseFraming::FixedLength,
        content_length: EXPECTATION_FAILED_BODY.len() as u64,
        connection: ConnectionHeader::Close,
    };
    let encoded =
        encode_response_head_trusted(&response, output).map_err(|_| RouteBuildError::Invalid)?;
    let mut length = encoded.length;
    if encoded.body_allowed && !EXPECTATION_FAILED_BODY.is_empty() {
        let end = length + EXPECTATION_FAILED_BODY.len();
        output
            .get_mut(length..end)
            .ok_or(RouteBuildError::Invalid)?
            .copy_from_slice(EXPECTATION_FAILED_BODY);
        length = end;
    }
    Ok(length)
}

pub fn respond_bad_request(
    output: &mut [u8],
    request_version: RequestVersion,
) -> Result<usize, RouteBuildError> {
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let response = ResponseToWrite {
        request_method: b"GET",
        request_version,
        requested_upgrade: b"",
        status: 400,
        reason: b"Bad Request",
        fields: &date,
        framing: ResponseFraming::FixedLength,
        content_length: BAD_REQUEST_BODY.len() as u64,
        connection: ConnectionHeader::Close,
    };
    let encoded =
        encode_response_head_trusted(&response, output).map_err(|_| RouteBuildError::Invalid)?;
    let end = encoded.length + BAD_REQUEST_BODY.len();
    output
        .get_mut(encoded.length..end)
        .ok_or(RouteBuildError::Invalid)?
        .copy_from_slice(BAD_REQUEST_BODY);
    Ok(end)
}

pub fn respond_request_header_fields_too_large(
    output: &mut [u8],
    request_version: RequestVersion,
) -> Result<usize, RouteBuildError> {
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let response = ResponseToWrite {
        request_method: b"GET",
        request_version,
        requested_upgrade: b"",
        status: 431,
        reason: b"Request Header Fields Too Large",
        fields: &date,
        framing: ResponseFraming::FixedLength,
        content_length: REQUEST_HEADER_FIELDS_TOO_LARGE_BODY.len() as u64,
        connection: ConnectionHeader::Close,
    };
    let encoded =
        encode_response_head_trusted(&response, output).map_err(|_| RouteBuildError::Invalid)?;
    let end = encoded.length + REQUEST_HEADER_FIELDS_TOO_LARGE_BODY.len();
    output
        .get_mut(encoded.length..end)
        .ok_or(RouteBuildError::Invalid)?
        .copy_from_slice(REQUEST_HEADER_FIELDS_TOO_LARGE_BODY);
    Ok(end)
}

// NOTE: RFC 9110 15.5.15 scopes 414 to a long target URI specifically; Date-first with close
// mirrors the 431 sibling exactly (same error family, no keep-alive), method/version overflows
// stay on the 400 responder above.
pub fn respond_uri_too_long(
    output: &mut [u8],
    request_version: RequestVersion,
) -> Result<usize, RouteBuildError> {
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let response = ResponseToWrite {
        request_method: b"GET",
        request_version,
        requested_upgrade: b"",
        status: 414,
        reason: b"URI Too Long",
        fields: &date,
        framing: ResponseFraming::FixedLength,
        content_length: URI_TOO_LONG_BODY.len() as u64,
        connection: ConnectionHeader::Close,
    };
    let encoded =
        encode_response_head_trusted(&response, output).map_err(|_| RouteBuildError::Invalid)?;
    let end = encoded.length + URI_TOO_LONG_BODY.len();
    output
        .get_mut(encoded.length..end)
        .ok_or(RouteBuildError::Invalid)?
        .copy_from_slice(URI_TOO_LONG_BODY);
    Ok(end)
}

#[cfg(test)]
#[path = "respond/tests.rs"]
mod tests;
