use std::cell::RefCell;
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use veylor_server::{
    MAX_NATIVE_HEADERS, MAX_NATIVE_PARAMS, NativeHeader, NativeRequestView, NativeResponseOut,
};

use crate::handler::RouteHandler;
use crate::request::{Param, Request};
use crate::response::{Response, reason};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Kind {
    Handler,
    Echo,
}

pub(crate) struct Route<S: 'static> {
    pub(crate) layers: Arc<[Arc<dyn crate::middleware::Middleware<S>>]>,
    pub(crate) kind: Kind,
    pub(crate) method: &'static [u8],
    pub(crate) pattern: Vec<u8>,
    pub(crate) state: &'static S,
    pub(crate) handler: RouteHandler<S>,
}

pub(crate) struct App<S: 'static> {
    pub(crate) state: &'static S,
    // NOTE: owns the route allocations whose addresses were handed to the native registry.
    #[allow(dead_code)]
    pub(crate) routes: Vec<Arc<Route<S>>>,
    pub(crate) managed: Vec<Arc<Route<S>>>,
    pub(crate) fallback: Option<RouteHandler<S>>,
    pub(crate) body_cap: usize,
}

impl<S: 'static> App<S> {
    pub(crate) fn state(&self) -> &'static S {
        self.state
    }
}

struct Slot {
    response: Response,
    headers: [NativeHeader; MAX_NATIVE_HEADERS],
}

thread_local! {
    static OUTGOING: RefCell<Option<Slot>> = const { RefCell::new(None) };
}

pub(crate) unsafe extern "C" fn dispatch<S: Send + Sync + 'static>(
    user_data: *mut c_void,
    request: *const NativeRequestView,
    out: *mut NativeResponseOut,
) -> i32 {
    unsafe {
        if user_data.is_null() || request.is_null() || out.is_null() {
            return -1;
        }

        let route = &*(user_data as *const Route<S>);
        let view = &*request;
        let (Some(method), Some(target)) = (
            raw(view.method, view.method_len),
            raw(view.path, view.path_len),
        ) else {
            return -1;
        };
        let body = raw(view.body, view.body_len as usize).unwrap_or(&[]);

        let header_count = view.header_count.min(MAX_NATIVE_HEADERS);
        let mut headers = [(&b""[..], &b""[..]); MAX_NATIVE_HEADERS];
        for (slot, index) in headers.iter_mut().zip(0..header_count) {
            let header = &*view.headers.add(index);
            let (Some(name), Some(value)) = (
                raw(header.name, header.name_len),
                raw(header.value, header.value_len),
            ) else {
                continue;
            };
            *slot = (name, value);
        }
        let headers = &headers[..header_count];

        let count = view.param_count.min(MAX_NATIVE_PARAMS);
        let mut params = [Param {
            name: &[],
            value: &[],
        }; MAX_NATIVE_PARAMS];
        for (slot, index) in params.iter_mut().zip(0..count) {
            let param = &*view.params.add(index);
            let (Some(name), Some(value)) = (
                raw(param.name, param.name_len),
                raw(param.value, param.value_len),
            ) else {
                continue;
            };
            *slot = Param { name, value };
        }

        let incoming = Request::new(method, target, headers, body, &params[..count], route.state);
        let response = catch_unwind(AssertUnwindSafe(|| {
            if route.layers.is_empty() {
                (route.handler)(&incoming, route.state)
            } else {
                crate::middleware::run_chain(&route.layers, &route.handler, incoming)
            }
        }))
        .unwrap_or_else(|_| internal_error());

        write_out(response, out);
    }
    0
}

unsafe fn raw<'a>(ptr: *const u8, len: usize) -> Option<&'a [u8]> {
    if len == 0 {
        return Some(&[]);
    }
    if ptr.is_null() {
        return None;
    }
    Some(unsafe { std::slice::from_raw_parts(ptr, len) })
}

pub(crate) fn internal_error() -> Response {
    Response::new(500, &b"Internal Server Error"[..])
}

unsafe fn write_out(response: Response, out: *mut NativeResponseOut) {
    OUTGOING.with(|cell| {
        let mut slot = cell.borrow_mut();
        *slot = Some(Slot {
            response,
            headers: [NativeHeader::EMPTY; MAX_NATIVE_HEADERS],
        });
        let Some(slot) = slot.as_mut() else {
            return;
        };
        let Slot { response, headers } = slot;
        let mut count = 0usize;
        for (name, value) in response.headers() {
            if count == MAX_NATIVE_HEADERS {
                break;
            }
            headers[count] = NativeHeader {
                name: name.as_ptr(),
                name_len: name.len(),
                value: value.as_ptr(),
                value_len: value.len(),
            };
            count += 1;
        }
        let body = response.body().as_bytes();
        let phrase = reason(response.status_code());

        unsafe {
            (*out).status = i32::from(response.status_code());
            (*out).reason = phrase.as_ptr();
            (*out).reason_len = phrase.len();
            (*out).body = if body.is_empty() {
                std::ptr::null()
            } else {
                body.as_ptr()
            };
            (*out).body_len = body.len();
            (*out).headers = headers.as_ptr();
            (*out).header_count = count;
        }
    });
}
