use std::ffi::c_void;
use std::mem::{offset_of, size_of};

use veylor_server::{
    NativeHandler, NativeHeader, NativeParam, NativeRequestView, NativeResponseOut,
};

use super::api::read_bytes;
use super::{ManagedStaticState, ReadyRouteOp, VylManagedStaticServer, server_error_to_result};
use crate::abi::{VylBytes, VylResult, catch_ffi};

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct VylManagedStaticNativeRequest {
    pub method: *const u8,
    pub method_len: usize,
    pub path: *const u8,
    pub path_len: usize,
    pub has_body: u8,
    pub body_len: u64,
    pub body: *const u8,
    pub headers: *const NativeHeader,
    pub header_count: usize,
    pub params: *const NativeParam,
    pub param_count: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct VylManagedStaticNativeResponse {
    pub status: i32,
    pub reason: *const u8,
    pub reason_len: usize,
    pub body: *const u8,
    pub body_len: usize,
    pub headers: *const NativeHeader,
    pub header_count: usize,
}

pub type VylManagedStaticNativeHandlerFn = unsafe extern "C" fn(
    *mut c_void,
    *const VylManagedStaticNativeRequest,
    *mut VylManagedStaticNativeResponse,
) -> i32;

pub type VylManagedStaticNativeHandler = Option<VylManagedStaticNativeHandlerFn>;

const _: () = assert!(size_of::<VylManagedStaticNativeRequest>() == size_of::<NativeRequestView>());
const _: () =
    assert!(size_of::<VylManagedStaticNativeResponse>() == size_of::<NativeResponseOut>());
const _: () = assert!(size_of::<VylManagedStaticNativeRequest>() == 88);
const _: () = assert!(size_of::<VylManagedStaticNativeResponse>() == 56);
const _: () =
    assert!(offset_of!(VylManagedStaticNativeRequest, has_body) == 4 * size_of::<*const u8>());
const _: () =
    assert!(offset_of!(VylManagedStaticNativeRequest, body_len) == 5 * size_of::<*const u8>());
const _: () =
    assert!(offset_of!(VylManagedStaticNativeRequest, body) == 6 * size_of::<*const u8>());
const _: () = assert!(offset_of!(VylManagedStaticNativeResponse, reason) == size_of::<*const u8>());
const _: () =
    assert!(offset_of!(VylManagedStaticNativeResponse, body_len) == 4 * size_of::<*const u8>());
const _: () =
    assert!(offset_of!(VylManagedStaticNativeRequest, headers) == 7 * size_of::<*const u8>());
const _: () =
    assert!(offset_of!(VylManagedStaticNativeRequest, params) == 9 * size_of::<*const u8>());
const _: () =
    assert!(offset_of!(VylManagedStaticNativeResponse, headers) == 5 * size_of::<*const u8>());

// NOTE: the server registry carries the handler address plus an opaque user_data word, so
// no slot pool, trampoline table, or process-global state is needed: the C handler converts
// once here and user_data round-trips through the server untouched.

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_register_native_route(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    handler: VylManagedStaticNativeHandler,
    user_data: *mut c_void,
) -> VylResult {
    catch_ffi(|| unsafe { register_native_route(server, method, path, handler, user_data) })
}

unsafe fn register_native_route(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    handler: VylManagedStaticNativeHandler,
    user_data: *mut c_void,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let Some(handler) = handler else {
        return VylResult::InvalidArgument;
    };
    let handle = unsafe { &mut *server };
    let ManagedStaticState::Ready(ready) = &mut handle.state else {
        return VylResult::BadState;
    };
    let method = match unsafe { read_bytes(method) } {
        Ok(method) => method,
        Err(error) => return error,
    };
    let path = match unsafe { read_bytes(path) } {
        Ok(path) => path,
        Err(error) => return error,
    };

    let handler = convert_handler(handler);
    match ready
        .server
        .register_native(method, path, Some(handler), user_data as usize)
    {
        Ok(()) => {
            ready.ops.push(ReadyRouteOp::Native {
                method: method.to_vec(),
                path: path.to_vec(),
                handler,
                user_data: user_data as usize,
                gzip: false,
            });
            VylResult::Ok
        }
        Err(error) => server_error_to_result(error),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_register_native_route_with_gzip(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    handler: VylManagedStaticNativeHandler,
    user_data: *mut c_void,
) -> VylResult {
    catch_ffi(|| unsafe { register_native_gzip_route(server, method, path, handler, user_data) })
}

unsafe fn register_native_gzip_route(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    handler: VylManagedStaticNativeHandler,
    user_data: *mut c_void,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let Some(handler) = handler else {
        return VylResult::InvalidArgument;
    };
    let handle = unsafe { &mut *server };
    let ManagedStaticState::Ready(ready) = &mut handle.state else {
        return VylResult::BadState;
    };
    let method = match unsafe { read_bytes(method) } {
        Ok(method) => method,
        Err(error) => return error,
    };
    let path = match unsafe { read_bytes(path) } {
        Ok(path) => path,
        Err(error) => return error,
    };
    let handler = convert_handler(handler);
    match ready
        .server
        .register_native_gzip(method, path, Some(handler), user_data as usize)
    {
        Ok(()) => {
            ready.ops.push(ReadyRouteOp::Native {
                method: method.to_vec(),
                path: path.to_vec(),
                handler,
                user_data: user_data as usize,
                gzip: true,
            });
            VylResult::Ok
        }
        Err(error) => server_error_to_result(error),
    }
}

fn convert_handler(handler: VylManagedStaticNativeHandlerFn) -> NativeHandler {
    unsafe { std::mem::transmute::<VylManagedStaticNativeHandlerFn, NativeHandler>(handler) }
}
