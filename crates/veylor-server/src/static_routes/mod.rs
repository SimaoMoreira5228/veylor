use std::path::Path;

use veylor_http1::{
    ConnectionHeader, Field, Header, RequestHead, ResponseFraming, ResponseToWrite, TargetForm,
    connection_header_for_response, encode_response_head, is_token,
};
use veylor_router::{
    RouteBodyPolicy, RouteCapture, RouteConfig, RouteError, RouteKind, RouteSlot, RouteTable,
};

use crate::connection::field;
use crate::static_files::{FileMounts, FilePlan, FileReject, MountError, RangeApply};

pub const MAX_ROUTES: usize = 64;
pub const MAX_RESPONSE_BODY: usize = 4096;
pub(crate) const MAX_TRAILER_NAMES: usize = 32;
pub(crate) const MAX_TRAILER_NAME_BYTES: usize = 128;
// NOTE: default per-route body cap for callers that keep the old behavior: static routes never
// consume request bodies, so the cap only bounds drain work. 64 KiB is eight full 8 KiB input
// buffers of skip work at most, while still admitting small form posts. A zero cap admits no
// body bytes at all; u64::MAX leaves the route uncapped except for input-buffer/deadline stops.
pub const MAX_STATIC_BODY_BYTES: u64 = 64 * 1024;
const CAPTURE_SLOTS: usize = 8;
// NOTE: "bytes " plus three u64 fields plus separators tops out at 68 bytes; 80 leaves margin.
const CONTENT_RANGE_BYTES: usize = 80;
const NOT_FOUND_BODY: &[u8] = b"Not Found";
const BAD_REQUEST_BODY: &[u8] = b"Bad Request";
const PAYLOAD_TOO_LARGE_BODY: &[u8] = b"Payload Too Large";
const EXPECTATION_FAILED_BODY: &[u8] = b"Expectation Failed";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteBuildError {
    Invalid,
    Duplicate,
    Full,
    BodyTooLarge,
}

impl From<RouteError> for RouteBuildError {
    fn from(error: RouteError) -> Self {
        match error {
            RouteError::Duplicate => Self::Duplicate,
            RouteError::Full => Self::Full,
            _ => Self::Invalid,
        }
    }
}

pub struct StaticRoute {
    method: Vec<u8>,
    path: Vec<u8>,
    status: u16,
    reason: Vec<u8>,
    body: Vec<u8>,
    max_body_bytes: u64,
}

pub struct ManagedRoute {
    method: Vec<u8>,
    path: Vec<u8>,
    max_body_bytes: u64,
    trailers: Vec<Vec<u8>>,
}

pub struct StaticRoutes {
    routes: Vec<StaticRoute>,
    managed: Vec<ManagedRoute>,
    files: FileMounts,
}

impl StaticRoutes {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            managed: Vec::new(),
            files: FileMounts::new(),
        }
    }

    pub fn register(
        &mut self,
        method: &[u8],
        path: &[u8],
        status: u16,
        reason: &[u8],
        body: &[u8],
        max_body_bytes: u64,
    ) -> Result<(), RouteBuildError> {
        if method.is_empty()
            || !method.iter().copied().all(is_token)
            || path.is_empty()
            || !(100..=599).contains(&status)
            || !valid_reason(reason)
        {
            return Err(RouteBuildError::Invalid);
        }
        if body.len() > MAX_RESPONSE_BODY {
            return Err(RouteBuildError::BodyTooLarge);
        }
        if self.routes.len() >= MAX_ROUTES {
            return Err(RouteBuildError::Full);
        }
        if self
            .routes
            .iter()
            .any(|route| route.method == method && route.path == path)
            || self
                .managed
                .iter()
                .any(|route| route.method == method && route.path == path)
        {
            return Err(RouteBuildError::Duplicate);
        }
        self.routes.push(StaticRoute {
            method: method.to_vec(),
            path: path.to_vec(),
            status,
            reason: reason.to_vec(),
            body: body.to_vec(),
            max_body_bytes,
        });
        Ok(())
    }

    pub fn register_managed(
        &mut self,
        method: &[u8],
        path: &[u8],
        max_body_bytes: u64,
        trailers: &[&[u8]],
    ) -> Result<(), RouteBuildError> {
        if method.is_empty()
            || method.len() > crate::host::HOST_MAX_METHOD_BYTES
            || !method.iter().copied().all(is_token)
            || path.is_empty()
            || path.len() > crate::host::HOST_MAX_PATH_BYTES
            || trailers.len() > MAX_TRAILER_NAMES
        {
            return Err(RouteBuildError::Invalid);
        }
        for (index, name) in trailers.iter().enumerate() {
            let dupe = trailers[..index]
                .iter()
                .any(|previous| previous.eq_ignore_ascii_case(name));
            if name.is_empty() || name.len() > MAX_TRAILER_NAME_BYTES || dupe {
                return Err(RouteBuildError::Invalid);
            }
            if !name.iter().copied().all(is_token) {
                return Err(RouteBuildError::Invalid);
            }
        }
        if self.routes.len() + self.managed.len() >= MAX_ROUTES {
            return Err(RouteBuildError::Full);
        }
        if self
            .routes
            .iter()
            .any(|route| route.method == method && route.path == path)
            || self
                .managed
                .iter()
                .any(|route| route.method == method && route.path == path)
        {
            return Err(RouteBuildError::Duplicate);
        }
        self.managed.push(ManagedRoute {
            method: method.to_vec(),
            path: path.to_vec(),
            max_body_bytes,
            trailers: trailers.iter().map(|name| name.to_vec()).collect(),
        });
        Ok(())
    }

    // NOTE: file mounts register alongside static routes and freeze structurally at
    // start: Server is consumed by start, so no &mut handle survives to register late.
    // Frozen therefore maps to Invalid; duplicates and capacity keep their own errors.
    pub fn register_file_mount(
        &mut self,
        prefix: &[u8],
        root: &Path,
    ) -> Result<(), RouteBuildError> {
        self.files
            .register(prefix, root)
            .map_err(|error| match error {
                MountError::Duplicate => RouteBuildError::Duplicate,
                MountError::Full => RouteBuildError::Full,
                MountError::Invalid | MountError::Frozen => RouteBuildError::Invalid,
            })
    }

    pub fn build_table<'slots, 'data>(
        &'data self,
        slots: &'slots mut [RouteSlot<'data>],
    ) -> Result<RouteTable<'slots, 'data>, RouteBuildError> {
        let mut table = RouteTable::new(slots);
        for route in self.routes.iter() {
            let config = RouteConfig {
                kind: RouteKind::Static,
                body: RouteBodyPolicy::Stream {
                    max_body_bytes: route.max_body_bytes,
                    permitted_trailers: &[],
                },
            };
            table.register(route.method.as_slice(), route.path.as_slice(), config)?;
        }
        // NOTE: managed routes share the frozen exact table with a distinct kind so the
        // connection layer can park on match; the Stream cap carries the per-route admission
        // limit (static v1 rejected bodies, now each route declares its own bound).
        for route in self.managed.iter() {
            let config = RouteConfig {
                kind: RouteKind::Managed,
                body: RouteBodyPolicy::Stream {
                    max_body_bytes: route.max_body_bytes,
                    permitted_trailers: &[],
                },
            };
            table.register(route.method.as_slice(), route.path.as_slice(), config)?;
        }
        table.freeze()?;
        Ok(table)
    }

    fn lookup(&self, method: &[u8], pattern: &[u8]) -> Option<&StaticRoute> {
        let found = self
            .routes
            .iter()
            .find(|route| route.method == method && route.path == pattern);
        match found {
            Some(route) => Some(route),
            None if method == b"HEAD" => self
                .routes
                .iter()
                .find(|route| route.method == b"GET" && route.path == pattern),
            None => None,
        }
    }

    // NOTE: the frozen table keeps an empty trailer view for managed routes, so the park path
    // reads the allowlist here instead: same match shape as the static lookup above, empty when
    // the route is gone so trailer staging fails closed without disturbing the body path.
    pub(crate) fn managed_trailers(&self, method: &[u8], pattern: &[u8]) -> &[Vec<u8>] {
        let found = self
            .managed
            .iter()
            .find(|route| route.method == method && route.path == pattern);
        match found {
            Some(route) => route.trailers.as_slice(),
            None if method == b"HEAD" => self
                .managed
                .iter()
                .find(|route| route.method == b"GET" && route.path == pattern)
                .map(|route| route.trailers.as_slice())
                .unwrap_or(&[]),
            None => &[],
        }
    }
}

impl Default for StaticRoutes {
    fn default() -> Self {
        Self::new()
    }
}

// NOTE: every registered route now carries a Stream cap, so Reject only appears on empty
// table slots that never match; it falls back to the default rather than failing open.
pub(crate) fn route_cap(body: RouteBodyPolicy<'_>) -> u64 {
    match body {
        RouteBodyPolicy::Stream { max_body_bytes, .. } => max_body_bytes,
        RouteBodyPolicy::Reject => MAX_STATIC_BODY_BYTES,
    }
}

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
    let mut captures = [RouteCapture::default(); CAPTURE_SLOTS];
    let found = table.find_request(head, input, &mut captures);
    // NOTE: exact static routes win; file mounts serve only table misses, so existing
    // routes never change behavior when mounts are added.
    if matches!(found, Err(RouteError::NotFound)) {
        if let Some(file) = respond_file(routes, head, headers, input, output)? {
            return Ok(file);
        }
    }
    // NOTE: unmatched requests have no route cap of their own, so 404s keep the default.
    let (status, reason, body, cap) = match found {
        Ok(found) => {
            let cap = route_cap(found.body);
            match routes.lookup(head.method(input).unwrap_or_default(), found.pattern) {
                Some(route) => (
                    route.status,
                    route.reason.as_slice(),
                    route.body.as_slice(),
                    cap,
                ),
                None => return Err(RouteBuildError::Invalid),
            }
        }
        Err(RouteError::NotFound) => (
            404,
            b"Not Found".as_slice(),
            NOT_FOUND_BODY,
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
    let response = ResponseToWrite {
        request_method: method,
        requested_upgrade: b"",
        status,
        reason,
        fields: &[],
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
        keep_open: connection != ConnectionHeader::Close,
        drain_total,
        interim_len,
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
    let range = (method == b"GET")
        .then(|| range_header_value(headers, head, input))
        .flatten();
    if plan.apply_range(range) == RangeApply::Unsatisfiable {
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
    let mut content_range = [0u8; CONTENT_RANGE_BYTES];
    let range_len = if plan.partial {
        write_content_range(
            &mut content_range,
            Some((plan.start, plan.start + plan.size - 1)),
            plan.total,
        )
    } else {
        0
    };
    let mut fields = [Field {
        name: b"Content-Type",
        value: plan.content_type.as_bytes(),
    }; 3];
    fields[1] = Field {
        name: b"Accept-Ranges",
        value: b"bytes",
    };
    let field_len = if plan.partial {
        fields[2] = Field {
            name: b"Content-Range",
            value: &content_range[..range_len],
        };
        3
    } else {
        2
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
        keep_open: connection != ConnectionHeader::Close,
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
    headers.iter().take(head.header_count).find_map(|header| {
        let name = field(input, header.name)?;
        if !name.eq_ignore_ascii_case(b"range") {
            return None;
        }
        field(input, header.value)
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
    let fields = [Field {
        name: b"Content-Range",
        value: &value[..value_len],
    }];
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
        keep_open: connection != ConnectionHeader::Close,
        drain_total,
        interim_len,
        file: None,
    })
}

fn write_content_range(out: &mut [u8], range: Option<(u64, u64)>, total: u64) -> usize {
    let mut len = push_field_bytes(out, 0, b"bytes ");
    match range {
        Some((start, last)) => {
            len = push_decimal(out, len, start);
            len = push_field_bytes(out, len, b"-");
            len = push_decimal(out, len, last);
            len = push_field_bytes(out, len, b"/");
        }
        None => len = push_field_bytes(out, len, b"*/"),
    }
    push_decimal(out, len, total)
}

fn push_field_bytes(out: &mut [u8], len: usize, bytes: &[u8]) -> usize {
    let end = len + bytes.len();
    out[len..end].copy_from_slice(bytes);
    end
}

fn push_decimal(out: &mut [u8], mut len: usize, value: u64) -> usize {
    let mut digits = [0u8; 20];
    let mut count = 0;
    let mut rest = value;
    loop {
        digits[count] = b'0' + (rest % 10) as u8;
        count += 1;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    while count > 0 {
        count -= 1;
        out[len] = digits[count];
        len += 1;
    }
    len
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

fn reject_payload(
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
    let interim = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 100,
        reason: b"Continue",
        fields: &[],
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        connection: ConnectionHeader::Omit,
    };
    let encoded = encode_response_head(&interim, output).map_err(|_| RouteBuildError::Invalid)?;
    Ok(encoded.length)
}

pub fn respond_payload_too_large(
    output: &mut [u8],
    method: &[u8],
) -> Result<usize, RouteBuildError> {
    let response = ResponseToWrite {
        request_method: method,
        requested_upgrade: b"",
        status: 413,
        reason: b"Payload Too Large",
        fields: &[],
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
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 417,
        reason: b"Expectation Failed",
        fields: &[],
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
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 400,
        reason: b"Bad Request",
        fields: &[],
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

fn valid_reason(reason: &[u8]) -> bool {
    !reason.is_empty()
        && !reason.iter().any(|byte| {
            *byte == b'\r' || *byte == b'\n' || (*byte < 0x20 && *byte != b'\t') || *byte == 0x7f
        })
}
