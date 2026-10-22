use veylor_http1::{
    ConnectionHeader, RequestHead, ResponseFraming, ResponseToWrite,
    connection_header_for_response, encode_response_head, is_token,
};
use veylor_router::{
    RouteBodyPolicy, RouteCapture, RouteConfig, RouteError, RouteKind, RouteSlot, RouteTable,
};

pub const MAX_ROUTES: usize = 64;
pub const MAX_RESPONSE_BODY: usize = 4096;
const CAPTURE_SLOTS: usize = 8;
const NOT_FOUND_BODY: &[u8] = b"Not Found";
const BAD_REQUEST_BODY: &[u8] = b"Bad Request";

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
}

pub struct StaticRoutes {
    routes: Vec<StaticRoute>,
}

impl StaticRoutes {
    pub fn new() -> Self {
        Self { routes: Vec::new() }
    }

    pub fn register(
        &mut self,
        method: &[u8],
        path: &[u8],
        status: u16,
        reason: &[u8],
        body: &[u8],
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
        {
            return Err(RouteBuildError::Duplicate);
        }
        self.routes.push(StaticRoute {
            method: method.to_vec(),
            path: path.to_vec(),
            status,
            reason: reason.to_vec(),
            body: body.to_vec(),
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
                body: RouteBodyPolicy::Reject,
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
}

impl Default for StaticRoutes {
    fn default() -> Self {
        Self::new()
    }
}

pub struct EncodedStaticResponse {
    pub length: usize,
    pub keep_open: bool,
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
    let (status, reason, body) = match found {
        Ok(found) => match routes.lookup(head.method(input).unwrap_or_default(), found.pattern) {
            Some(route) => (route.status, route.reason.as_slice(), route.body.as_slice()),
            None => return Err(RouteBuildError::Invalid),
        },
        Err(RouteError::NotFound) => (404, b"Not Found".as_slice(), NOT_FOUND_BODY),
        Err(error) => return Err(RouteBuildError::from(error)),
    };
    let method = head.method(input).unwrap_or_default();
    let body_complete = head.content_length.is_none() && !head.is_chunked;
    let response_close = status == 404;
    let connection = connection_header_for_response(
        head,
        input,
        status,
        ResponseFraming::FixedLength,
        body_complete,
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
    let encoded = encode_response_head(&response, output).map_err(|_| RouteBuildError::Invalid)?;
    let mut length = encoded.length;
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
        keep_open: connection != ConnectionHeader::Close && body_complete,
    })
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
