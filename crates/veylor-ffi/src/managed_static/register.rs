use veylor_server::MAX_STATIC_BODY_BYTES;

use super::api::read_bytes;
use super::{ManagedStaticState, ReadyRouteOp, VylManagedStaticServer, server_error_to_result};
use crate::abi::{VylBytes, VylResult, catch_ffi, slice_length_valid};

// NOTE: mirrors the `veylor-server` static-route allowlist bounds; enforced here before any copy.
const MAX_TRAILER_NAMES: usize = 32;
const MAX_TRAILER_NAME_BYTES: usize = 128;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_register_static_route(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    status: u16,
    reason: VylBytes,
    body: VylBytes,
) -> VylResult {
    catch_ffi(|| unsafe {
        register_static_route(
            server,
            method,
            path,
            status,
            reason,
            body,
            MAX_STATIC_BODY_BYTES,
        )
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_register_static_route_with_body_cap(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    status: u16,
    reason: VylBytes,
    body: VylBytes,
    max_body_bytes: u64,
) -> VylResult {
    catch_ffi(|| unsafe {
        register_static_route(server, method, path, status, reason, body, max_body_bytes)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_register_static_route_with_gzip(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    status: u16,
    reason: VylBytes,
    body: VylBytes,
) -> VylResult {
    catch_ffi(|| unsafe {
        register_gzip_route(
            server,
            method,
            path,
            status,
            reason,
            body,
            MAX_STATIC_BODY_BYTES,
        )
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_register_managed_route(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    max_body_bytes: u64,
) -> VylResult {
    catch_ffi(|| unsafe { register_managed_route(server, method, path, max_body_bytes) })
}

unsafe fn register_static_route(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    status: u16,
    reason: VylBytes,
    body: VylBytes,
    max_body_bytes: u64,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
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
    let reason = match unsafe { read_bytes(reason) } {
        Ok(reason) => reason,
        Err(error) => return error,
    };
    let body = match unsafe { read_bytes(body) } {
        Ok(body) => body,
        Err(error) => return error,
    };
    match ready
        .server
        .register_route(method, path, status, reason, body, max_body_bytes)
    {
        Ok(()) => {
            ready.ops.push(ReadyRouteOp::Static {
                method: method.to_vec(),
                path: path.to_vec(),
                status,
                reason: reason.to_vec(),
                body: body.to_vec(),
                max_body_bytes,
            });
            VylResult::Ok
        }
        Err(error) => server_error_to_result(error),
    }
}

unsafe fn register_gzip_route(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    status: u16,
    reason: VylBytes,
    body: VylBytes,
    max_body_bytes: u64,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
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
    let reason = match unsafe { read_bytes(reason) } {
        Ok(reason) => reason,
        Err(error) => return error,
    };
    let body = match unsafe { read_bytes(body) } {
        Ok(body) => body,
        Err(error) => return error,
    };
    match ready
        .server
        .register_gzip_route(method, path, status, reason, body, max_body_bytes)
    {
        Ok(()) => {
            ready.ops.push(ReadyRouteOp::Gzip {
                method: method.to_vec(),
                path: path.to_vec(),
                status,
                reason: reason.to_vec(),
                body: body.to_vec(),
                max_body_bytes,
            });
            VylResult::Ok
        }
        Err(error) => server_error_to_result(error),
    }
}

unsafe fn register_managed_route(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    max_body_bytes: u64,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
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
    match ready.server.register_managed(method, path, max_body_bytes) {
        Ok(()) => {
            ready.ops.push(ReadyRouteOp::Managed {
                method: method.to_vec(),
                path: path.to_vec(),
                max_body_bytes,
            });
            VylResult::Ok
        }
        Err(error) => server_error_to_result(error),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_register_managed_route_with_trailers(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    max_body_bytes: u64,
    trailer_names: *const VylBytes,
    trailer_count: usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        register_managed_with_trailers(
            server,
            method,
            path,
            max_body_bytes,
            trailer_names,
            trailer_count,
        )
    })
}

unsafe fn register_managed_with_trailers(
    server: *mut VylManagedStaticServer,
    method: VylBytes,
    path: VylBytes,
    max_body_bytes: u64,
    trailer_names: *const VylBytes,
    trailer_count: usize,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
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
    if (trailer_names.is_null() && trailer_count != 0)
        || !slice_length_valid::<VylBytes>(trailer_count)
    {
        return VylResult::InvalidArgument;
    }
    if trailer_count > MAX_TRAILER_NAMES {
        return VylResult::InvalidArgument;
    }
    let mut trailers = Vec::with_capacity(trailer_count);
    for index in 0..trailer_count {
        let raw = unsafe { *trailer_names.add(index) };
        let name = match unsafe { read_bytes(raw) } {
            Ok(name) => name,
            Err(error) => return error,
        };
        if name.is_empty() || name.len() > MAX_TRAILER_NAME_BYTES {
            return VylResult::InvalidArgument;
        }
        trailers.push(name);
    }
    match ready
        .server
        .register_managed_with_trailers(method, path, max_body_bytes, &trailers)
    {
        Ok(()) => {
            ready.ops.push(ReadyRouteOp::ManagedWithTrailers {
                method: method.to_vec(),
                path: path.to_vec(),
                max_body_bytes,
                trailers: trailers.iter().map(|name| name.to_vec()).collect(),
            });
            VylResult::Ok
        }
        Err(error) => server_error_to_result(error),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_register_file_mount(
    server: *mut VylManagedStaticServer,
    prefix: VylBytes,
    root: VylBytes,
) -> VylResult {
    catch_ffi(|| unsafe { register_file_mount(server, prefix, root) })
}

unsafe fn register_file_mount(
    server: *mut VylManagedStaticServer,
    prefix: VylBytes,
    root: VylBytes,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &mut *server };
    let ManagedStaticState::Ready(ready) = &mut handle.state else {
        return VylResult::BadState;
    };
    let prefix = match unsafe { read_bytes(prefix) } {
        Ok(prefix) => prefix,
        Err(error) => return error,
    };
    let root = match unsafe { read_bytes(root) } {
        Ok(root) => root,
        Err(error) => return error,
    };
    let root = match bytes_to_path(root) {
        Ok(root) => root,
        Err(error) => return error,
    };
    match ready.server.register_file_mount(prefix, root) {
        Ok(()) => {
            ready.ops.push(ReadyRouteOp::FileMount {
                prefix: prefix.to_vec(),
                root: root.to_path_buf(),
            });
            VylResult::Ok
        }
        Err(error) => server_error_to_result(error),
    }
}

fn bytes_to_path(root: &[u8]) -> Result<&std::path::Path, VylResult> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Ok(std::path::Path::new(std::ffi::OsStr::from_bytes(root)))
    }
    #[cfg(not(unix))]
    {
        match std::str::from_utf8(root) {
            Ok(valid) => Ok(std::path::Path::new(valid)),
            Err(_) => Err(VylResult::InvalidArgument),
        }
    }
}
