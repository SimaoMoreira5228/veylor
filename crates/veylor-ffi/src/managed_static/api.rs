use std::boxed::Box;
use std::net::{Ipv4Addr, SocketAddr};
use std::ptr;
use std::slice;

use veylor_server::{MAX_STATIC_BODY_BYTES, Server};

use super::{ManagedStaticState, VylManagedStaticCloseCounts, VylManagedStaticConfig};
use super::{VylManagedStaticServer, server_error_to_result};
use crate::abi::{VylBytes, VylResult, catch_ffi, slice_length_valid};







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_new(
    config: *const VylManagedStaticConfig,
    out_server: *mut *mut VylManagedStaticServer,
) -> VylResult {
    catch_ffi(|| unsafe { create_server(config, out_server) })
}

unsafe fn create_server(
    config: *const VylManagedStaticConfig,
    out_server: *mut *mut VylManagedStaticServer,
) -> VylResult {
    if config.is_null() || out_server.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_server.write(ptr::null_mut());
    }
    let owned = unsafe { *config };
    let (server_config, port) = match owned.to_server_config() {
        Ok(config) => config,
        Err(error) => return error,
    };
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let server = match Server::bind(addr, server_config) {
        Ok(server) => server,
        Err(error) => return server_error_to_result(error),
    };
    let handle = Box::new(VylManagedStaticServer {
        state: ManagedStaticState::Ready(server),
    });
    unsafe {
        out_server.write(Box::into_raw(handle));
    }
    VylResult::Ok
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_destroy(
    server: *mut VylManagedStaticServer,
) -> VylResult {
    catch_ffi(|| unsafe { destroy_server(server) })
}

unsafe fn destroy_server(server: *mut VylManagedStaticServer) -> VylResult {
    if server.is_null() {
        return VylResult::Ok;
    }
    let handle = unsafe { &*server };
    if matches!(handle.state, ManagedStaticState::Running(_)) {
        return VylResult::BadState;
    }
    unsafe {
        drop(Box::from_raw(server));
    }
    VylResult::Ok
}






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
    let ManagedStaticState::Ready(routes) = &mut handle.state else {
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
    match routes.register_route(method, path, status, reason, body, max_body_bytes) {
        Ok(()) => VylResult::Ok,
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
    let ManagedStaticState::Ready(routes) = &mut handle.state else {
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
    match routes.register_managed(method, path, max_body_bytes) {
        Ok(()) => VylResult::Ok,
        Err(error) => server_error_to_result(error),
    }
}





#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_start(
    server: *mut VylManagedStaticServer,
) -> VylResult {
    catch_ffi(|| unsafe { start_server(server) })
}

unsafe fn start_server(server: *mut VylManagedStaticServer) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &mut *server };
    let state = std::mem::replace(&mut handle.state, ManagedStaticState::Joined);
    let ManagedStaticState::Ready(routes) = state else {
        handle.state = state;
        return VylResult::BadState;
    };
    match routes.start() {
        Ok(running) => {
            handle.state = ManagedStaticState::Running(running);
            VylResult::Ok
        }
        Err(error) => server_error_to_result(error),
    }
}





#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_stop_accepting(
    server: *mut VylManagedStaticServer,
) -> VylResult {
    catch_ffi(|| unsafe { stop_server(server, false) })
}





#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_stop_force(
    server: *mut VylManagedStaticServer,
) -> VylResult {
    catch_ffi(|| unsafe { stop_server(server, true) })
}

unsafe fn stop_server(server: *mut VylManagedStaticServer, force: bool) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    if force {
        running.stop_force();
    } else {
        running.stop_accepting();
    }
    VylResult::Ok
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_join(
    server: *mut VylManagedStaticServer,
    out_counts: *mut VylManagedStaticCloseCounts,
) -> VylResult {
    catch_ffi(|| unsafe { join_server(server, out_counts) })
}

unsafe fn join_server(
    server: *mut VylManagedStaticServer,
    out_counts: *mut VylManagedStaticCloseCounts,
) -> VylResult {
    if server.is_null() || out_counts.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_counts.write(VylManagedStaticCloseCounts::default());
    }
    let handle = unsafe { &mut *server };
    let state = std::mem::replace(&mut handle.state, ManagedStaticState::Joined);
    let ManagedStaticState::Running(running) = state else {
        handle.state = state;
        return VylResult::BadState;
    };
    match running.join() {
        Ok(counts) => {
            unsafe {
                out_counts.write(VylManagedStaticCloseCounts::from(counts));
            }
            VylResult::Ok
        }
        Err(error) => server_error_to_result(error),
    }
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_local_addr(
    server: *mut VylManagedStaticServer,
    out_port: *mut u16,
) -> VylResult {
    catch_ffi(|| unsafe { local_addr(server, out_port) })
}

unsafe fn local_addr(server: *mut VylManagedStaticServer, out_port: *mut u16) -> VylResult {
    if server.is_null() || out_port.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_port.write(0);
    }
    let handle = unsafe { &*server };
    let port = match &handle.state {
        ManagedStaticState::Ready(routes) => match routes.addr() {
            Ok(addr) => addr.port(),
            Err(error) => return server_error_to_result(error),
        },
        ManagedStaticState::Running(running) => running.addr().port(),
        ManagedStaticState::Joined => return VylResult::BadState,
    };
    unsafe {
        out_port.write(port);
    }
    VylResult::Ok
}

unsafe fn read_bytes<'a>(bytes: VylBytes) -> Result<&'a [u8], VylResult> {
    if (bytes.data.is_null() && bytes.length != 0) || !slice_length_valid::<u8>(bytes.length) {
        return Err(VylResult::InvalidArgument);
    }
    if bytes.length == 0 {
        return Ok(&[]);
    }
    Ok(unsafe { slice::from_raw_parts(bytes.data, bytes.length) })
}
