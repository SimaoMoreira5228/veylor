// NOTE: response-assembly half of the static_routes seam: registry owns route storage and
// validation (super), this module owns response encoding over already-matched routes.
use veylor_http1::{
    ConnectionHeader, Field, Header, RequestHead, ResponseFraming, ResponseToWrite, TargetForm,
    connection_header_for_response, encode_response_head, response_permits_reuse,
};
use veylor_router::{RouteCapture, RouteError, RouteKind, RouteTable};

use crate::connection::field;
use crate::static_files::{
    FilePlan, FileReject, HTTP_DATE_BYTES, RangeApply, parse_http_date, write_http_date,
};

use super::native::respond_native;
use super::{CONTENT_ENCODING_GZIP, accepts_gzip_value};
use super::{
    MAX_STATIC_BODY_BYTES, RouteBuildError, StaticRoutes, in_native_handler, now_http_date,
    route_cap,
};

const CAPTURE_SLOTS: usize = 8;
// NOTE: at most this many distinct methods list in Allow; more truncate the tail, so size
// it at the route cap and the 405 below reports Invalid past the value buffer instead.
const ALLOW_METHODS: usize = 16;
const ALLOW_BYTES: usize = 320;
// NOTE: "bytes " plus three u64 fields plus separators tops out at 68 bytes; 80 leaves margin.
const CONTENT_RANGE_BYTES: usize = 80;
// NOTE: "multipart/byteranges; boundary=" plus the fixed token and three hex fields tops out near 91 bytes;
// 128 leaves margin, and the same buffer doubles as the Content-Range scratch since the two never coexist.
const MULTIPART_CTYPE_BYTES: usize = 128;
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

pub fn respond_to_head(
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    head: RequestHead,
    headers: &[Header],
    input: &[u8],
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    if in_native_handler() {
        return Err(RouteBuildError::Invalid);
    }
    if head.target_form == TargetForm::Asterisk
        && matches!(head.method(input), Some(m) if m == b"OPTIONS")
        && head.content_length.unwrap_or(0) == 0
        && !head.is_chunked
        && !head.expect_continue
    {
        return respond_server_options(table, head, input, output);
    }
    let mut captures = [RouteCapture::default(); CAPTURE_SLOTS];
    let found = table.find_request(head, input, &mut captures);
    // NOTE: natives dispatch before statics on the same Static slot (same method+path can
    // never sit in both lists); either hit skips the file fallback below, so natives win
    // over file mounts exactly like statics do.
    if let Ok(matched) = &found {
        if matched.kind == RouteKind::Static {
            let method = head.method(input).unwrap_or_default();
            if let Some(native) = routes.lookup_native(method, matched.pattern) {
                let path = head.target(input).unwrap_or(matched.pattern);
                return respond_native(
                    native.handler,
                    native.user_data,
                    head,
                    method,
                    path,
                    input,
                    headers,
                    native.gzip,
                    output,
                );
            }
        }
    }
    // NOTE: exact static routes win; file mounts serve only table misses, so existing
    // routes never change behavior when mounts are added.
    if matches!(found, Err(RouteError::MethodNotAllowed)) {
        return respond_method_not_allowed(table, head, input, output);
    }
    if matches!(found, Err(RouteError::NotFound)) {
        if let Some(file) = respond_file(routes, head, headers, input, output)? {
            return Ok(file);
        }
    }
    // NOTE: unmatched requests have no route cap of their own, so 404s keep the default.
    let (status, reason, body, gzip, cap) = match found {
        Ok(found) => {
            let cap = route_cap(found.body);
            match routes.lookup(head.method(input).unwrap_or_default(), found.pattern) {
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
    let method = head.method(input).unwrap_or_default();
    // NOTE: static routes cannot use trailers, so any chunked request rejects without reading;
    // oversized declarations reject the same way with no interim, both against the matched
    // route's cap (zero admits no body byte at all; u64::MAX is input-buffer bounded).
    if head.is_chunked {
        return reject_payload(output, method);
    }
    let drain_total = match head.content_length {
        Some(length) if length > cap => return reject_payload(output, method),
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
        encode_response_head(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
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
    if head.is_chunked {
        return reject_payload(output, method);
    }
    let drain_total = match head.content_length {
        Some(length) if length > MAX_STATIC_BODY_BYTES => return reject_payload(output, method),
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
    let allow_len = join_allow(&methods[..count], &mut allow).ok_or(RouteBuildError::Invalid)?;
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

fn join_allow(methods: &[&[u8]], out: &mut [u8]) -> Option<usize> {
    let mut len = 0;
    for (index, name) in methods.iter().enumerate() {
        if index > 0 {
            let end = len + 2;
            if end > out.len() {
                return None;
            }
            out[len..end].copy_from_slice(b", ");
            len = end;
        }
        let end = len + name.len();
        if end > out.len() {
            return None;
        }
        out[len..end].copy_from_slice(name);
        len = end;
    }
    Some(len)
}

fn encode_method_not_allowed(
    head: RequestHead,
    input: &[u8],
    method: &[u8],
    allow: &[u8],
    drain_total: u64,
    interim_len: usize,
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
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
        encode_response_head(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
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
    let allow_len = join_allow(&methods[..count], &mut allow).ok_or(RouteBuildError::Invalid)?;
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
        requested_upgrade: b"",
        status: 200,
        reason: b"OK",
        fields: &fields,
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        connection,
    };
    let encoded = encode_response_head(&response, output).map_err(|_| RouteBuildError::Invalid)?;
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
fn respond_file(
    routes: &StaticRoutes,
    head: RequestHead,
    headers: &[Header],
    input: &[u8],
    output: &mut [u8],
) -> Result<Option<EncodedStaticResponse>, RouteBuildError> {
    let Some((method, path)) = file_target(head, input) else {
        return Ok(None);
    };
    if head.is_chunked {
        return Ok(Some(reject_payload(output, method)?));
    }
    let drain_total = match head.content_length {
        Some(length) if length > MAX_STATIC_BODY_BYTES => {
            return Ok(Some(reject_payload(output, method)?));
        }
        Some(length) => length,
        None => 0,
    };
    let interim_len = if head.expect_continue {
        encode_continue(output)?
    } else {
        0
    };
    // NOTE: resolve reads non-GET/HEAD as no match without touching the fs; a GET probe
    // reuses the mount logic (prefix, trailing slash, decoding), so only a real file
    // resource answers 405 with Allow: GET, HEAD while traversal, directories, and
    // misses stay 404 with no existence oracle.
    if method != b"GET" && method != b"HEAD" {
        let probe = routes.files.resolve(b"GET", path);
        if matches!(probe, Ok(_) | Err(FileReject::TooLarge)) {
            let denied = encode_method_not_allowed(
                head,
                input,
                method,
                b"GET, HEAD",
                drain_total,
                interim_len,
                output,
            )?;
            return Ok(Some(denied));
        }
        return Ok(None);
    }
    let mut plan = match routes.files.resolve(method, path) {
        Ok(plan) => plan,
        Err(FileReject::TooLarge) => return Ok(Some(reject_payload(output, method)?)),
        Err(FileReject::NotFound | FileReject::Forbidden | FileReject::NotAFile) => {
            return Ok(None);
        }
    };
    // NOTE: a satisfiable single range narrows the plan window for a 206 while the pump
    // keeps 0-based offsets; ignored headers fall back to 200 full and unsatisfiable
    // windows answer 416 with bytes */size and no body, closed like a 404.
    // NOTE (RFC 9110 section 14.2): range handling is defined only for GET; HEAD (the only
    // other file method here) ignores Range and answers 200 full headers.
    // NOTE: If-None-Match ignores unconditionally in v1: no ETags emit anywhere, so comparison is
    // impossible and If-Modified-Since alone drives 304. Preconditions evaluate before Range, for GET
    // and HEAD alike; malformed dates ignore to 200 full while future dates 304 by the same mtime <=
    // date rule, both sides already whole seconds so the comparison is per-second per RFC 9110 13.1.
    let fresh = header_value(headers, head, input, b"if-modified-since")
        .and_then(parse_http_date)
        .is_some_and(|since| plan.mtime <= since);
    if fresh {
        return Ok(Some(respond_not_modified(
            head,
            input,
            method,
            &plan,
            drain_total,
            interim_len,
            output,
        )?));
    }
    let range = (method == b"GET")
        .then(|| range_header_value(headers, head, input))
        .flatten();
    let outcome = plan.apply_range(range);
    if outcome == RangeApply::Unsatisfiable {
        return Ok(Some(respond_range_unsatisfiable(
            head,
            input,
            method,
            &plan,
            drain_total,
            interim_len,
            output,
        )?));
    }
    // NOTE: two or more satisfiable ranges frame as one multipart body: the virtual length rides plan.size and the pump pages it
    // through read_chunk, so this path only swaps the Content-Type.
    let multipart = outcome == RangeApply::Multipart;
    let status = if plan.partial { 206 } else { 200 };
    let reason = if plan.partial {
        b"Partial Content".as_slice()
    } else {
        b"OK".as_slice()
    };
    let connection = connection_header_for_response(
        head,
        input,
        status,
        ResponseFraming::FixedLength,
        true,
        false,
    );
    let mut value = [0u8; MULTIPART_CTYPE_BYTES];
    let mut date = [0u8; HTTP_DATE_BYTES];
    let date_len = write_http_date(plan.mtime, &mut date);
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let range_len = if plan.partial && !multipart {
        write_content_range(
            &mut value,
            Some((plan.start, plan.start + plan.size - 1)),
            plan.total,
        )
    } else {
        0
    };
    let ctype_len = if multipart {
        plan.multipart_content_type(&mut value)
    } else {
        0
    };
    let ctype_value = if multipart {
        &value[..ctype_len]
    } else {
        plan.content_type.as_bytes()
    };
    let mut fields = [Field {
        name: b"Date",
        value: &now[..now_len],
    }; 5];
    fields[1] = Field {
        name: b"Content-Type",
        value: ctype_value,
    };
    fields[2] = Field {
        name: b"Accept-Ranges",
        value: b"bytes",
    };
    fields[3] = Field {
        name: b"Last-Modified",
        value: &date[..date_len],
    };
    let field_len = if plan.partial && !multipart {
        fields[4] = Field {
            name: b"Content-Range",
            value: &value[..range_len],
        };
        5
    } else {
        4
    };
    let response = ResponseToWrite {
        request_method: method,
        requested_upgrade: b"",
        status,
        reason,
        fields: &fields[..field_len],
        framing: ResponseFraming::FixedLength,
        content_length: plan.size,
        connection,
    };
    let final_buf = output
        .get_mut(interim_len..)
        .ok_or(RouteBuildError::Invalid)?;
    let encoded =
        encode_response_head(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
    let length = interim_len + encoded.length;
    // NOTE: HEAD and empty files complete with the head alone; only GET with bytes
    // streams, so the pump never stages zero-length transfers.
    let file = if plan.head_only || plan.size == 0 {
        None
    } else {
        Some(plan)
    };
    Ok(Some(EncodedStaticResponse {
        length,
        keep_open: response_permits_reuse(false, connection),
        drain_total,
        interim_len,
        file,
    }))
}

fn range_header_value<'a>(
    headers: &'a [Header],
    head: RequestHead,
    input: &'a [u8],
) -> Option<&'a [u8]> {
    header_value(headers, head, input, b"range")
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

// NOTE: 304 mirrors the 416 small-fixed-response shape (no body, no file staging) but keeps alive:
// a cache revalidation is a normal response, not an error, so only the Last-Modified validator that
// the 200 would have carried rides along per RFC 9110 13.1, and the connection reuses as usual.
fn respond_not_modified(
    head: RequestHead,
    input: &[u8],
    method: &[u8],
    plan: &FilePlan,
    drain_total: u64,
    interim_len: usize,
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let mut date = [0u8; HTTP_DATE_BYTES];
    let date_len = write_http_date(plan.mtime, &mut date);
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let fields = [
        Field {
            name: b"Date",
            value: &now[..now_len],
        },
        Field {
            name: b"Last-Modified",
            value: &date[..date_len],
        },
    ];
    let connection =
        connection_header_for_response(head, input, 304, ResponseFraming::FixedLength, true, false);
    let response = ResponseToWrite {
        request_method: method,
        requested_upgrade: b"",
        status: 304,
        reason: b"Not Modified",
        fields: &fields,
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        connection,
    };
    let final_buf = output
        .get_mut(interim_len..)
        .ok_or(RouteBuildError::Invalid)?;
    let encoded =
        encode_response_head(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
    Ok(EncodedStaticResponse {
        length: interim_len + encoded.length,
        keep_open: response_permits_reuse(false, connection),
        drain_total,
        interim_len,
        file: None,
    })
}

fn respond_range_unsatisfiable(
    head: RequestHead,
    input: &[u8],
    method: &[u8],
    plan: &FilePlan,
    drain_total: u64,
    interim_len: usize,
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let mut value = [0u8; CONTENT_RANGE_BYTES];
    let value_len = write_content_range(&mut value, None, plan.total);
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let fields = [
        Field {
            name: b"Date",
            value: &now[..now_len],
        },
        Field {
            name: b"Content-Range",
            value: &value[..value_len],
        },
    ];
    let connection =
        connection_header_for_response(head, input, 416, ResponseFraming::FixedLength, true, true);
    let response = ResponseToWrite {
        request_method: method,
        requested_upgrade: b"",
        status: 416,
        reason: b"Range Not Satisfiable",
        fields: &fields,
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        connection,
    };
    let final_buf = output
        .get_mut(interim_len..)
        .ok_or(RouteBuildError::Invalid)?;
    let encoded =
        encode_response_head(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
    Ok(EncodedStaticResponse {
        length: interim_len + encoded.length,
        keep_open: response_permits_reuse(false, connection),
        drain_total,
        interim_len,
        file: None,
    })
}

fn write_content_range(out: &mut [u8], range: Option<(u64, u64)>, total: u64) -> usize {
    use crate::static_files::{push_bytes, push_decimal};
    let mut len = push_bytes(out, 0, b"bytes ");
    match range {
        Some((start, last)) => {
            len = push_decimal(out, len, start);
            len = push_bytes(out, len, b"-");
            len = push_decimal(out, len, last);
            len = push_bytes(out, len, b"/");
        }
        None => len = push_bytes(out, len, b"*/"),
    }
    push_decimal(out, len, total)
}

// NOTE: query strings strip like the router so /assets/a.css?x=1 names the file;
// fragments never arrive on the wire and anything else rejects inside resolve.
fn file_target(head: RequestHead, input: &[u8]) -> Option<(&[u8], &[u8])> {
    let method = head.method(input)?;
    let target = head.target(input)?;
    let path = match head.target_form {
        TargetForm::Origin => strip_query(target),
        TargetForm::Absolute => absolute_path(target)?,
        TargetForm::Authority | TargetForm::Asterisk => return None,
    };
    if path.is_empty() || path[0] != b'/' {
        return None;
    }
    Some((method, path))
}

fn strip_query(target: &[u8]) -> &[u8] {
    let end = target
        .iter()
        .position(|byte| *byte == b'?')
        .unwrap_or(target.len());
    &target[..end]
}

fn absolute_path(target: &[u8]) -> Option<&[u8]> {
    let colon = target.iter().position(|byte| *byte == b':')?;
    let after = target.get(colon + 1..)?;
    let authority = after.strip_prefix(b"//")?;
    let end = authority
        .iter()
        .position(|byte| matches!(byte, b'/' | b'?'))
        .unwrap_or(authority.len());
    if end == authority.len() || authority[end] == b'?' {
        return Some(b"/");
    }
    Some(strip_query(&authority[end..]))
}

pub(crate) fn reject_payload(
    output: &mut [u8],
    method: &[u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let length = respond_payload_too_large(output, method)?;
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
    let encoded = encode_response_head(&interim, output).map_err(|_| RouteBuildError::Invalid)?;
    Ok(encoded.length)
}

pub fn respond_payload_too_large(
    output: &mut [u8],
    method: &[u8],
) -> Result<usize, RouteBuildError> {
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let response = ResponseToWrite {
        request_method: method,
        requested_upgrade: b"",
        status: 413,
        reason: b"Payload Too Large",
        fields: &date,
        framing: ResponseFraming::FixedLength,
        content_length: PAYLOAD_TOO_LARGE_BODY.len() as u64,
        connection: ConnectionHeader::Close,
    };
    let encoded = encode_response_head(&response, output).map_err(|_| RouteBuildError::Invalid)?;
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

pub fn respond_expectation_failed(output: &mut [u8]) -> Result<usize, RouteBuildError> {
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 417,
        reason: b"Expectation Failed",
        fields: &date,
        framing: ResponseFraming::FixedLength,
        content_length: EXPECTATION_FAILED_BODY.len() as u64,
        connection: ConnectionHeader::Close,
    };
    let encoded = encode_response_head(&response, output).map_err(|_| RouteBuildError::Invalid)?;
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

pub fn respond_bad_request(output: &mut [u8]) -> Result<usize, RouteBuildError> {
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 400,
        reason: b"Bad Request",
        fields: &date,
        framing: ResponseFraming::FixedLength,
        content_length: BAD_REQUEST_BODY.len() as u64,
        connection: ConnectionHeader::Close,
    };
    let encoded = encode_response_head(&response, output).map_err(|_| RouteBuildError::Invalid)?;
    let end = encoded.length + BAD_REQUEST_BODY.len();
    output
        .get_mut(encoded.length..end)
        .ok_or(RouteBuildError::Invalid)?
        .copy_from_slice(BAD_REQUEST_BODY);
    Ok(end)
}

pub fn respond_request_header_fields_too_large(
    output: &mut [u8],
) -> Result<usize, RouteBuildError> {
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 431,
        reason: b"Request Header Fields Too Large",
        fields: &date,
        framing: ResponseFraming::FixedLength,
        content_length: REQUEST_HEADER_FIELDS_TOO_LARGE_BODY.len() as u64,
        connection: ConnectionHeader::Close,
    };
    let encoded = encode_response_head(&response, output).map_err(|_| RouteBuildError::Invalid)?;
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
pub fn respond_uri_too_long(output: &mut [u8]) -> Result<usize, RouteBuildError> {
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let date = [Field {
        name: b"Date",
        value: &now[..now_len],
    }];
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 414,
        reason: b"URI Too Long",
        fields: &date,
        framing: ResponseFraming::FixedLength,
        content_length: URI_TOO_LONG_BODY.len() as u64,
        connection: ConnectionHeader::Close,
    };
    let encoded = encode_response_head(&response, output).map_err(|_| RouteBuildError::Invalid)?;
    let end = encoded.length + URI_TOO_LONG_BODY.len();
    output
        .get_mut(encoded.length..end)
        .ok_or(RouteBuildError::Invalid)?
        .copy_from_slice(URI_TOO_LONG_BODY);
    Ok(end)
}

#[cfg(test)]
mod tests {
    // NOTE: drift alarm for the converged 100-continue policy: the managed pump stages this
    // exact shape ahead of the final response while the host-driven conn crate encodes the
    // same shared shape for its continue handshake, so any shape edit upstream fails here
    // until the pump follows.
    use super::encode_continue;
    use veylor_http1::{continue_interim, encode_response_head};

    #[test]
    fn interim_stages_shared_continue_shape_ahead_of_final() {
        let mut staged = [0u8; 64];
        let len = encode_continue(&mut staged).expect("interim fits");
        let mut expected = [0u8; 64];
        let encoded = encode_response_head(&continue_interim(), &mut expected)
            .expect("shared interim encodes");
        assert_eq!(&staged[..len], &expected[..encoded.length]);
        assert_eq!(&staged[..len], b"HTTP/1.1 100 Continue\r\n\r\n");
    }

    #[test]
    fn keep_open_verdict_matches_shared_reuse_policy() {
        // NOTE: drift alarm for the converged keep-alive policy: every static keep_open below
        // must equal veylor_http1::response_permits_reuse on the same verdict, so a local
        // verdict edit fails here until the shared predicate follows. The upgrade input stays
        // false (this path never upgrades) and drain rides drain_total, not the booleans.
        use super::respond_to_head;
        use crate::static_routes::StaticRoutes;
        use veylor_http1::{
            Header, Limits, Parser, RequestHead, ResponseFraming, connection_header_for_response,
            response_permits_reuse,
        };
        use veylor_router::RouteSlot;

        fn drive(routes: &StaticRoutes, input: &[u8]) -> (RequestHead, bool) {
            let mut slots = [RouteSlot::EMPTY; 8];
            let table = routes.build_table(&mut slots).expect("table builds");
            let mut headers = [Header::default(); 16];
            let mut parser = Parser::new();
            let head = parser
                .parse(input, &mut headers, Limits::default())
                .expect("head parses")
                .expect("head complete");
            let mut output = [0u8; 2048];
            let encoded = respond_to_head(routes, &table, head, &headers, input, &mut output)
                .expect("static encodes");
            (head, encoded.keep_open)
        }

        let mut routes = StaticRoutes::new();
        routes
            .register(b"GET", b"/hello", 200, b"OK", b"hi", 1024)
            .expect("register static");

        let plain = b"GET /hello HTTP/1.1\r\nHost: test\r\n\r\n";
        let (head, keep_open) = drive(&routes, plain);
        let verdict = connection_header_for_response(
            head,
            plain,
            200,
            ResponseFraming::FixedLength,
            true,
            false,
        );
        assert!(keep_open);
        assert_eq!(keep_open, response_permits_reuse(false, verdict));

        let close = b"GET /hello HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
        let (head, keep_open) = drive(&routes, close);
        let verdict = connection_header_for_response(
            head,
            close,
            200,
            ResponseFraming::FixedLength,
            true,
            false,
        );
        assert!(!keep_open);
        assert_eq!(keep_open, response_permits_reuse(false, verdict));

        let missing = b"GET /nope HTTP/1.1\r\nHost: test\r\n\r\n";
        let (head, keep_open) = drive(&routes, missing);
        let verdict = connection_header_for_response(
            head,
            missing,
            404,
            ResponseFraming::FixedLength,
            true,
            true,
        );
        assert!(!keep_open);
        assert_eq!(keep_open, response_permits_reuse(false, verdict));
    }
}
