use veylor_http1::is_token;
use veylor_http2::{ErrorCode, H2ServerDriver, HpackFieldToEncode, HpackRepresentation};

use crate::static_routes::{MAX_RESPONSE_BODY, MAX_ROUTES, NativeHandler, RouteBuildError};

use super::{DECIMAL_CAP, HPACK_BLOCK_CAP};

pub(crate) const NOT_FOUND_BODY: &[u8] = b"Not Found";

#[derive(Clone, Copy, Debug)]
pub struct FrozenRoute<'a> {
    pub method: &'a [u8],
    pub path: &'a [u8],
    pub status: u16,
    pub body: &'a [u8],
    pub managed: bool,
}

// NOTE: native projection for the h2c turn: same method+path match shape as FrozenRoute,
// but dispatch invokes the handler synchronously instead of serving a stored body. Status
// and body dummies stay in FrozenRoute (natives never project there); validation and the
// HEAD-to-GET fallback mirror the static view so H1 precedence carries over.
#[derive(Clone, Copy, Debug)]
pub struct FrozenNativeRoute<'a> {
    pub method: &'a [u8],
    pub path: &'a [u8],
    pub handler: NativeHandler,
    pub user_data: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct FrozenRoutesView<'a> {
    routes: &'a [FrozenRoute<'a>],
    natives: &'a [FrozenNativeRoute<'a>],
}

impl<'a> FrozenRoutesView<'a> {
    pub fn new(routes: &'a [FrozenRoute<'a>]) -> Result<Self, RouteBuildError> {
        if routes.len() > MAX_ROUTES {
            return Err(RouteBuildError::Full);
        }
        for route in routes {
            if route.method.is_empty()
                || !route.method.iter().copied().all(is_token)
                || route.path.is_empty()
                || !(200..=599).contains(&route.status)
                || route.status == 204
            {
                return Err(RouteBuildError::Invalid);
            }
            if route.body.len() > MAX_RESPONSE_BODY {
                return Err(RouteBuildError::BodyTooLarge);
            }
        }
        if (1..routes.len()).any(|index| {
            routes[..index]
                .iter()
                .any(|prev| prev.method == routes[index].method && prev.path == routes[index].path)
        }) {
            return Err(RouteBuildError::Duplicate);
        }
        Ok(Self {
            routes,
            natives: &[],
        })
    }

    // NOTE: native attach runs after new so the frozen static table keeps its shape: same
    // method+path rules as the registry (bad method/path Invalid, any overlap Duplicate),
    // and natives overlap nothing since the registry already rejects cross-kind dupes.
    pub fn with_natives(
        mut self,
        natives: &'a [FrozenNativeRoute<'a>],
    ) -> Result<Self, RouteBuildError> {
        for native in natives {
            let bad = native.method.is_empty() || native.path.is_empty();
            let token = !native.method.iter().copied().all(is_token);
            if bad || token {
                return Err(RouteBuildError::Invalid);
            }
        }
        if (1..natives.len()).any(|index| {
            natives[..index].iter().any(|prev| {
                prev.method == natives[index].method && prev.path == natives[index].path
            })
        }) {
            return Err(RouteBuildError::Duplicate);
        }
        let overlap = natives.iter().any(|native| {
            self.routes
                .iter()
                .any(|route| route.method == native.method && route.path == native.path)
        });
        if overlap {
            return Err(RouteBuildError::Duplicate);
        }
        self.natives = natives;
        Ok(self)
    }

    pub(crate) fn find(&self, method: &[u8], path: &[u8]) -> Option<usize> {
        let exact = self
            .routes
            .iter()
            .position(|route| route.method == method && route.path == path);
        if exact.is_some() {
            return exact;
        }
        if method != b"HEAD" {
            return None;
        }
        self.routes
            .iter()
            .position(|route| route.method == b"GET" && route.path == path)
    }

    pub(crate) fn lookup(&self, method: &[u8], path: &[u8]) -> Option<(u16, &[u8])> {
        self.find(method, path)
            .map(|index| (self.routes[index].status, self.routes[index].body))
    }

    // NOTE: native-first dispatch like H1 (respond_to_head checks natives before statics):
    // same HEAD-to-GET fallback as find, so a HEAD static never shadows a GET native.
    pub(crate) fn lookup_native(
        &self,
        method: &[u8],
        path: &[u8],
    ) -> Option<(NativeHandler, usize)> {
        let direct = self
            .natives
            .iter()
            .find(|native| native.method == method && native.path == path);
        let fallback = direct.is_none() && method == b"HEAD";
        let found = if fallback {
            self.natives
                .iter()
                .find(|native| native.method == b"GET" && native.path == path)
        } else {
            direct
        };
        found.map(|native| (native.handler, native.user_data))
    }

    pub(crate) fn is_managed(&self, method: &[u8], path: &[u8]) -> bool {
        self.find(method, path)
            .is_some_and(|index| self.routes[index].managed)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn send_full(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    status: u16,
    body: &[u8],
    end_stream: bool,
    send_body: bool,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(status));
    let mut length_buf = [0; DECIMAL_CAP];
    let length_len = push_decimal(&mut length_buf, body.len() as u64);
    let response_fields = [
        field(b":status", &status_buf[..status_len]),
        field(b"content-length", &length_buf[..length_len]),
    ];
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields,
            &mut block,
            &mut output[*used..],
            end_stream,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    if end_stream || !send_body {
        return Ok(());
    }
    let mut offset = 0;
    while offset < body.len() {
        let room = output.len() - *used;
        if room < 10 {
            return Err(ErrorCode::EnhanceYourCalm);
        }
        let take = (body.len() - offset).min(room - 9);
        let last = offset + take == body.len();
        let sent = server
            .send_data(
                stream,
                &body[offset..offset + take],
                last,
                &mut output[*used..],
            )
            .map_err(|_| ErrorCode::FlowControlError)?;
        if sent.frame.payload_length == 0 {
            return Err(ErrorCode::FlowControlError);
        }
        *used += sent.writer.pending_bytes().len();
        offset += sent.frame.payload_length;
    }
    Ok(())
}

pub(crate) fn field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::Default,
        huffman_name: false,
        huffman_value: false,
    }
}

fn push_decimal(buf: &mut [u8; DECIMAL_CAP], mut value: u64) -> usize {
    let mut end = DECIMAL_CAP;
    loop {
        end -= 1;
        buf[end] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    let len = DECIMAL_CAP - end;
    buf.copy_within(end.., 0);
    len
}
