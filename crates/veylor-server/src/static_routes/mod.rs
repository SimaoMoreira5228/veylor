use veylor_http1::{
    ConnectionHeader, RequestHead, ResponseFraming, ResponseToWrite,
    connection_header_for_response, encode_response_head, is_token,
};
use veylor_router::{
    RouteBodyPolicy, RouteCapture, RouteConfig, RouteError, RouteKind, RouteSlot, RouteTable,
};

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
}

impl StaticRoutes {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            managed: Vec::new(),
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
}

pub fn respond_to_head(
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    head: RequestHead,
    input: &[u8],
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let mut captures = [RouteCapture::default(); CAPTURE_SLOTS];
    let found = table.find_request(head, input, &mut captures);
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
    })
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
