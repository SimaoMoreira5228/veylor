use std::cmp::Ordering;

use veylor_http1::{RequestHead, is_token};

use crate::RouteError;
use crate::path::{
    MAX_ROUTE_CAPTURES, RouteCapture, compare_paths, compare_patterns, inspect_pattern,
    match_pattern, request_path,
};

const MAX_TRAILER_NAMES: usize = 32;
const MAX_TRAILER_NAME_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteKind {
    Static,
    Managed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteBodyPolicy<'a> {
    Reject,
    Stream {
        max_body_bytes: u64,
        permitted_trailers: &'a [&'a [u8]],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteConfig<'a> {
    pub kind: RouteKind,
    pub body: RouteBodyPolicy<'a>,
}

impl RouteBodyPolicy<'_> {
    pub fn permits(self, request: RequestHead) -> bool {
        let request_has_body = request.is_chunked || request.content_length.unwrap_or(0) > 0;
        match self {
            Self::Reject => !request_has_body,
            Self::Stream { max_body_bytes, .. } => request
                .content_length
                .is_none_or(|length| length <= max_body_bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteSlot<'a> {
    method: &'a [u8],
    path: &'a [u8],
    kind: RouteKind,
    body: RouteBodyPolicy<'a>,
    route_id: u64,
    dynamic_segments: usize,
}

impl<'a> RouteSlot<'a> {
    pub const EMPTY: Self = Self {
        method: b"",
        path: b"",
        kind: RouteKind::Static,
        body: RouteBodyPolicy::Reject,
        route_id: 0,
        dynamic_segments: 0,
    };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Registering,
    Frozen,
    Invalid,
}

pub struct RouteTable<'slots, 'data> {
    slots: &'slots mut [RouteSlot<'data>],
    count: usize,
    exact_count: usize,
    state: State,
}

pub struct RouteMatch<'capture, 'data> {
    pub route_id: u64,
    pub kind: RouteKind,
    pub body: RouteBodyPolicy<'data>,
    pub pattern: &'data [u8],
    pub captures: &'capture [RouteCapture],
}

impl<'slots, 'data> RouteTable<'slots, 'data> {
    pub fn new(slots: &'slots mut [RouteSlot<'data>]) -> Self {
        slots.fill(RouteSlot::EMPTY);
        Self {
            slots,
            count: 0,
            exact_count: 0,
            state: State::Registering,
        }
    }

    pub fn register(
        &mut self,
        method: &'data [u8],
        path: &'data [u8],
        config: RouteConfig<'data>,
    ) -> Result<(), RouteError> {
        match self.state {
            State::Registering => {}
            State::Frozen => return Err(RouteError::Frozen),
            State::Invalid => return Err(RouteError::Invalid),
        }
        if method.is_empty() || !method.iter().copied().all(is_token) {
            return Err(RouteError::Invalid);
        }
        let Some(dynamic_segments) = inspect_pattern(path) else {
            return Err(RouteError::Invalid);
        };
        if !valid_body_policy(config.body) {
            return Err(RouteError::Invalid);
        }
        let Some(slot) = self.slots.get_mut(self.count) else {
            return Err(RouteError::Full);
        };
        *slot = RouteSlot {
            method,
            path,
            kind: config.kind,
            body: config.body,
            route_id: 0,
            dynamic_segments,
        };
        self.count += 1;
        Ok(())
    }

    pub fn freeze(&mut self) -> Result<(), RouteError> {
        match self.state {
            State::Registering => {}
            State::Frozen => return Err(RouteError::Frozen),
            State::Invalid => return Err(RouteError::Invalid),
        }
        if self.count > self.slots.len() {
            self.state = State::Invalid;
            return Err(RouteError::Invalid);
        }

        let routes = &mut self.slots[..self.count];
        let mut exact_count = 0;
        for index in 0..routes.len() {
            if routes[index].dynamic_segments == 0 {
                routes.swap(index, exact_count);
                exact_count += 1;
            }
        }
        routes[..exact_count].sort_unstable_by(compare_exact_routes);
        routes[exact_count..].sort_unstable_by(compare_dynamic_routes);

        for pair in routes[..exact_count].windows(2) {
            if compare_exact_routes(&pair[0], &pair[1]) == Ordering::Equal {
                self.state = State::Invalid;
                return Err(RouteError::Duplicate);
            }
        }
        for pair in routes[exact_count..].windows(2) {
            if compare_dynamic_routes(&pair[0], &pair[1]) == Ordering::Equal {
                self.state = State::Invalid;
                return Err(RouteError::Duplicate);
            }
        }

        for (index, route) in routes.iter_mut().enumerate() {
            route.route_id = u64::try_from(index + 1).map_err(|_| RouteError::Full)?;
        }
        self.exact_count = exact_count;
        self.state = State::Frozen;
        Ok(())
    }

    pub fn find_request<'capture>(
        &self,
        request: RequestHead,
        input: &[u8],
        capture_storage: &'capture mut [RouteCapture],
    ) -> Result<RouteMatch<'capture, 'data>, RouteError> {
        if self.state != State::Frozen {
            return Err(RouteError::NotFrozen);
        }
        let method = request.method(input).ok_or(RouteError::InvalidRequest)?;
        let path = request_path(request, input)?.ok_or(RouteError::NotFound)?;
        let mut captures = [RouteCapture::default(); MAX_ROUTE_CAPTURES];

        let found = self.find_method_path(
            method,
            path.bytes,
            path.input_offset,
            &mut captures,
            capture_storage,
        )?;
        let found = match found {
            Some(found) => Some(found),
            None if method == b"HEAD" => self.find_method_path(
                b"GET",
                path.bytes,
                path.input_offset,
                &mut captures,
                capture_storage,
            )?,
            None => None,
        };
        let Some(found) = found else {
            // NOTE: miss-only path scan, so hits return with no extra work: one bounded
            // O(n) pass over the frozen slots distinguishes a wrong method from a miss.
            if self.has_path_match(path.bytes, path.input_offset) {
                return Err(RouteError::MethodNotAllowed);
            }
            return Err(RouteError::NotFound);
        };
        let route = &self.slots[found.0];
        Ok(route_match(route, &capture_storage[..found.1]))
    }

    // NOTE: Allow companion to find_request for 405 assembly: same frozen slots and match
    // rules (exact compare_paths plus dynamic match_pattern), no alloc, miss-path only.
    // Fills unique methods in table order and truncates at out capacity; Ok(0) names no
    // resource. Exact registered methods only, never synthesized (HEAD fallback and any
    // other fallback stay dispatch-only, like unregistered OPTIONS).
    pub fn allowed_methods_for_request(
        &self,
        request: RequestHead,
        input: &[u8],
        out: &mut [&'data [u8]],
    ) -> Result<usize, RouteError> {
        if self.state != State::Frozen {
            return Err(RouteError::NotFrozen);
        }
        let Some(path) = request_path(request, input)? else {
            return Ok(0);
        };
        let mut scratch = [RouteCapture::default(); MAX_ROUTE_CAPTURES];
        let mut count = 0;
        for route in self.slots[..self.count].iter() {
            let matched = if route.dynamic_segments == 0 {
                compare_paths(route.path, path.bytes) == Ordering::Equal
            } else {
                match_pattern(route.path, path.bytes, path.input_offset, &mut scratch).is_some()
            };
            if !matched {
                continue;
            }
            if out[..count].contains(&route.method) {
                continue;
            }
            let Some(slot) = out.get_mut(count) else {
                break;
            };
            *slot = route.method;
            count += 1;
        }
        Ok(count)
    }

    pub fn server_allow_methods(&self, out: &mut [&'data [u8]]) -> Result<usize, RouteError> {
        if self.state != State::Frozen {
            return Err(RouteError::NotFrozen);
        }
        let mut count = 0;
        for route in self.slots[..self.count].iter() {
            if out[..count].contains(&route.method) {
                continue;
            }
            let Some(slot) = out.get_mut(count) else {
                break;
            };
            *slot = route.method;
            count += 1;
        }
        Ok(count)
    }

    fn has_path_match(&self, path: &[u8], input_offset: usize) -> bool {
        let mut scratch = [RouteCapture::default(); MAX_ROUTE_CAPTURES];
        for route in self.slots[..self.count].iter() {
            if route.dynamic_segments == 0 {
                if compare_paths(route.path, path) == Ordering::Equal {
                    return true;
                }
            } else if match_pattern(route.path, path, input_offset, &mut scratch).is_some() {
                return true;
            }
        }
        false
    }

    fn find_method_path(
        &self,
        method: &[u8],
        path: &[u8],
        input_offset: usize,
        scratch: &mut [RouteCapture; MAX_ROUTE_CAPTURES],
        capture_storage: &mut [RouteCapture],
    ) -> Result<Option<(usize, usize)>, RouteError> {
        if self.exact_count > 0 {
            let mut low = 0;
            let mut high = self.exact_count;
            while low < high {
                let middle = low + (high - low) / 2;
                let route = &self.slots[middle];
                match compare_route_key(route.method, route.path, method, path) {
                    Ordering::Less => low = middle + 1,
                    Ordering::Equal => {
                        return Ok(Some((middle, 0)));
                    }
                    Ordering::Greater => high = middle,
                }
            }
        }

        let dynamic = &self.slots[self.exact_count..self.count];
        let start = dynamic.partition_point(|route| route.method < method);
        for (dynamic_offset, route) in dynamic[start..].iter().enumerate() {
            if route.method != method {
                break;
            }
            let Some(count) = match_pattern(route.path, path, input_offset, scratch) else {
                continue;
            };
            if count > capture_storage.len() {
                return Err(RouteError::CaptureCapacity);
            }
            capture_storage[..count].copy_from_slice(&scratch[..count]);
            let index = self.exact_count + start + dynamic_offset;
            return Ok(Some((index, count)));
        }
        Ok(None)
    }
}

fn route_match<'capture, 'data>(
    route: &RouteSlot<'data>,
    captures: &'capture [RouteCapture],
) -> RouteMatch<'capture, 'data> {
    RouteMatch {
        route_id: route.route_id,
        kind: route.kind,
        body: route.body,
        pattern: route.path,
        captures,
    }
}

fn compare_exact_routes(left: &RouteSlot<'_>, right: &RouteSlot<'_>) -> Ordering {
    compare_route_key(left.method, left.path, right.method, right.path)
}

fn compare_dynamic_routes(left: &RouteSlot<'_>, right: &RouteSlot<'_>) -> Ordering {
    left.method
        .cmp(right.method)
        .then_with(|| compare_patterns(left.path, right.path))
}

fn compare_route_key(
    left_method: &[u8],
    left_path: &[u8],
    right_method: &[u8],
    right_path: &[u8],
) -> Ordering {
    left_method
        .cmp(right_method)
        .then_with(|| compare_paths(left_path, right_path))
}

fn valid_body_policy(policy: RouteBodyPolicy<'_>) -> bool {
    let RouteBodyPolicy::Stream {
        permitted_trailers, ..
    } = policy
    else {
        return true;
    };
    if permitted_trailers.len() > MAX_TRAILER_NAMES {
        return false;
    }
    for (index, name) in permitted_trailers.iter().enumerate() {
        if name.is_empty()
            || name.len() > MAX_TRAILER_NAME_BYTES
            || !name.iter().copied().all(is_token)
            || permitted_trailers[..index]
                .iter()
                .any(|previous| previous.eq_ignore_ascii_case(name))
        {
            return false;
        }
    }
    true
}
