use std::cell::Cell;
use std::ffi::c_void;
use std::path::Path;

use veylor_http1::is_token;
use veylor_router::{RouteBodyPolicy, RouteConfig, RouteError, RouteKind, RouteSlot, RouteTable};

use crate::static_files::{FileMounts, MountError};

pub const MAX_ROUTES: usize = 64;
pub const MAX_RESPONSE_BODY: usize = 4096;
pub(crate) const MAX_TRAILER_NAMES: usize = 32;
pub(crate) const MAX_TRAILER_NAME_BYTES: usize = 128;
// NOTE: default per-route body cap for callers that keep the old behavior: static routes never
// consume request bodies, so the cap only bounds drain work. 64 KiB is eight full 8 KiB input
// buffers of skip work at most, while still admitting small form posts. A zero cap admits no
// body bytes at all; u64::MAX leaves the route uncapped except for input-buffer/deadline stops.
pub const MAX_STATIC_BODY_BYTES: u64 = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteBuildError {
    Invalid,
    Duplicate,
    Full,
    BodyTooLarge,
    // NOTE: dispatch-only bucket for a panicking native handler; registration never produces
    // it, so the server mapping reads it as InvalidRoute (unreachable there).
    NativePanic,
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
    pub(crate) method: Vec<u8>,
    pub(crate) path: Vec<u8>,
    pub(crate) status: u16,
    pub(crate) reason: Vec<u8>,
    pub(crate) body: Vec<u8>,
    pub(crate) gzip_body: Option<Vec<u8>>,
    pub(crate) max_body_bytes: u64,
}

pub struct ManagedRoute {
    method: Vec<u8>,
    path: Vec<u8>,
    max_body_bytes: u64,
    trailers: Vec<Vec<u8>>,
}

// NOTE: C-ABI native handlers run on the owner worker inside the pump (never on a host
// thread) with borrowed request views; the engine copies reason/body into caller buffers
// before returning and never retains handler memory. No headers in v1: method+path plus
// framing presence/length keeps the call FFI-trivial, with fixed-length bodies borrowed
// when fully buffered (see body). user_data crosses as an address-sized value next to the
// handler pointer (see below).
// NOTE: every pointer below borrows worker input bytes for the call only; a handler
// retaining method/path/body past return is UB, same as the out-param scratch rule.
#[repr(C)]
pub struct NativeRequestView {
    pub method: *const u8,
    pub method_len: usize,
    pub path: *const u8,
    pub path_len: usize,
    pub has_body: u8,
    pub body_len: u64,
    pub body: *const u8,
}

#[repr(C)]
pub struct NativeResponseOut {
    pub status: i32,
    pub reason: *const u8,
    pub reason_len: usize,
    pub body: *const u8,
    pub body_len: usize,
}

// NOTE: unsafe because the engine cannot verify the pointer; registration rejects None
// (null) so dispatch never calls null. Return is 0 on success, nonzero fails the request
// closed without a partial write. user_data crosses as an address-sized value (Send-safe,
// never dereferenced): the engine passes it back verbatim and its validity for the call is
// the registrant's contract.
pub type NativeHandler =
    unsafe extern "C" fn(*mut c_void, *const NativeRequestView, *mut NativeResponseOut) -> i32;

pub struct NativeRoute {
    pub(crate) method: Vec<u8>,
    pub(crate) path: Vec<u8>,
    pub(crate) handler: NativeHandler,
    pub(crate) user_data: usize,
    pub(crate) gzip: bool,
}

// NOTE: reentrancy guard: handlers must not call back into server APIs, so every registry
// and dispatch entry fails Invalid while a handler runs on this worker thread. Thread-local
// (workers own their pump), not per-connection: nested dispatch from any connection on the
// same worker trips it. Prompt return is documented, not enforced; join-from-handler stays
// a documented rule until the FFI slice owns join (no handle is reachable here to enforce).
thread_local! {
    pub(crate) static IN_NATIVE: Cell<bool> = const { Cell::new(false) };
}

pub fn in_native_handler() -> bool {
    IN_NATIVE.get()
}

pub struct StaticRoutes {
    routes: Vec<StaticRoute>,
    managed: Vec<ManagedRoute>,
    natives: Vec<NativeRoute>,
    pub(crate) files: FileMounts,
}

impl StaticRoutes {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            managed: Vec::new(),
            natives: Vec::new(),
            files: FileMounts::new(),
        }
    }

    fn is_registered(&self, method: &[u8], path: &[u8]) -> bool {
        self.routes
            .iter()
            .any(|route| route.method == method && route.path == path)
            || self
                .managed
                .iter()
                .any(|route| route.method == method && route.path == path)
            || self
                .natives
                .iter()
                .any(|route| route.method == method && route.path == path)
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
        if in_native_handler() {
            return Err(RouteBuildError::Invalid);
        }
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
        if self.is_registered(method, path) {
            return Err(RouteBuildError::Duplicate);
        }
        self.routes.push(StaticRoute {
            method: method.to_vec(),
            path: path.to_vec(),
            status,
            reason: reason.to_vec(),
            body: body.to_vec(),
            gzip_body: None,
            max_body_bytes,
        });
        Ok(())
    }

    pub fn register_gzip(
        &mut self,
        method: &[u8],
        path: &[u8],
        status: u16,
        reason: &[u8],
        body: &[u8],
        max_body_bytes: u64,
    ) -> Result<(), RouteBuildError> {
        self.register(method, path, status, reason, body, max_body_bytes)?;
        let route = self.routes.last_mut().ok_or(RouteBuildError::Invalid)?;
        route.gzip_body = gzip::compress_route_body(&route.body)?;
        // NOTE: currently unreachable-by-construction (static bodies stop at the 4 KiB
        // response cap above, and even incompressible input gains only deflate framing
        // overhead — far under the +64 twin bound), kept as defense-in-depth: if either
        // cap ever moves, this stays the fail-closed backstop instead of silent bloat.
        if route
            .gzip_body
            .as_ref()
            .is_some_and(|twin| twin.len() > gzip::GZIP_MAX_BYTES)
        {
            return Err(RouteBuildError::BodyTooLarge);
        }
        Ok(())
    }

    pub fn register_managed(
        &mut self,
        method: &[u8],
        path: &[u8],
        max_body_bytes: u64,
        trailers: &[&[u8]],
    ) -> Result<(), RouteBuildError> {
        if in_native_handler() {
            return Err(RouteBuildError::Invalid);
        }
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
        if self.is_registered(method, path) {
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
    // NOTE: native routes freeze the same way with a fixed drain cap (no per-route body
    // bound in v1); same method+path in any kind is Duplicate, and natives ride Static
    // table slots below so they win over file mounts, which serve only table misses.
    pub fn register_native(
        &mut self,
        method: &[u8],
        path: &[u8],
        handler: Option<NativeHandler>,
        user_data: usize,
    ) -> Result<(), RouteBuildError> {
        let Some(handler) = handler else {
            return Err(RouteBuildError::Invalid);
        };
        if in_native_handler() {
            return Err(RouteBuildError::Invalid);
        }
        if method.is_empty() || !method.iter().copied().all(is_token) || path.is_empty() {
            return Err(RouteBuildError::Invalid);
        }
        if self.routes.len() + self.managed.len() + self.natives.len() >= MAX_ROUTES {
            return Err(RouteBuildError::Full);
        }
        if self.is_registered(method, path) {
            return Err(RouteBuildError::Duplicate);
        }
        self.natives.push(NativeRoute {
            method: method.to_vec(),
            path: path.to_vec(),
            handler,
            user_data,
            gzip: false,
        });
        Ok(())
    }

    pub fn register_native_gzip(
        &mut self,
        method: &[u8],
        path: &[u8],
        handler: Option<NativeHandler>,
        user_data: usize,
    ) -> Result<(), RouteBuildError> {
        self.register_native(method, path, handler, user_data)?;
        let route = self.natives.last_mut().ok_or(RouteBuildError::Invalid)?;
        route.gzip = true;
        Ok(())
    }

    pub fn register_file_mount(
        &mut self,
        prefix: &[u8],
        root: &Path,
    ) -> Result<(), RouteBuildError> {
        if in_native_handler() {
            return Err(RouteBuildError::Invalid);
        }
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
        if in_native_handler() {
            return Err(RouteBuildError::Invalid);
        }
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
        // NOTE: natives ride Static slots (router crate untouched): same method+path can never
        // sit in both lists, so dispatch prefers the native list first with identical fallback.
        for route in self.natives.iter() {
            let config = RouteConfig {
                kind: RouteKind::Static,
                body: RouteBodyPolicy::Stream {
                    max_body_bytes: MAX_STATIC_BODY_BYTES,
                    permitted_trailers: &[],
                },
            };
            table.register(route.method.as_slice(), route.path.as_slice(), config)?;
        }
        table.freeze()?;
        Ok(table)
    }

    pub(crate) fn lookup(&self, method: &[u8], pattern: &[u8]) -> Option<&StaticRoute> {
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

    // NOTE: same HEAD-to-GET fallback shape as the static lookup so HEAD on a GET native
    // behaves identically; the table slot carries whichever method was registered.
    pub(crate) fn lookup_native(&self, method: &[u8], pattern: &[u8]) -> Option<&NativeRoute> {
        let found = self
            .natives
            .iter()
            .find(|route| route.method == method && route.path == pattern);
        match found {
            Some(route) => Some(route),
            None if method == b"HEAD" => self
                .natives
                .iter()
                .find(|route| route.method == b"GET" && route.path == pattern),
            None => None,
        }
    }

    pub(crate) fn gzip_body(&self, method: &[u8], path: &[u8]) -> Option<&[u8]> {
        self.routes
            .iter()
            .find(|route| route.method == method && route.path == path)
            .and_then(|route| route.gzip_body.as_deref())
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

// NOTE: one response-instant read per H1 assembly: SystemTime::now truncated to seconds like
// mtime, formatted as 29-byte IMF-fixdate into caller stack scratch; no allocs, one vDSO call.
// NOTE: host-dispatched (managed) responses assemble on the host-owned path, not here — Date
// there is a recorded follow-up, not this slice.
pub(crate) fn now_http_date(out: &mut [u8; crate::static_files::HTTP_DATE_BYTES]) -> usize {
    let instant = crate::static_files::system_time_secs(Some(std::time::SystemTime::now()));
    crate::static_files::write_http_date(instant, out)
}

pub(crate) fn valid_reason(reason: &[u8]) -> bool {
    !reason.is_empty()
        && !reason.iter().any(|byte| {
            *byte == b'\r' || *byte == b'\n' || (*byte < 0x20 && *byte != b'\t') || *byte == 0x7f
        })
}

mod gzip;
mod native;
mod respond;

pub(crate) use gzip::{CONTENT_ENCODING_GZIP, accepts_gzip_value, compress_route_body};

pub(crate) use respond::encode_continue;
pub(crate) use respond::header_value;
pub(crate) use respond::reject_payload;
pub use respond::{
    EncodedStaticResponse, respond_bad_request, respond_expectation_failed,
    respond_payload_too_large, respond_request_header_fields_too_large, respond_to_head,
    respond_uri_too_long,
};
