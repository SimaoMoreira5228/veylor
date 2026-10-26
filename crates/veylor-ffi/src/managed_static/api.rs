use std::boxed::Box;
use std::net::{Ipv4Addr, SocketAddr};
use std::ptr;
use std::slice;

use veylor_server::{MAX_STATIC_BODY_BYTES, MAX_WORKERS, Server, in_native_handler};

use super::{
    ManagedStaticState, ReadyRouteOp, ReadyServer, VylManagedStaticCloseCounts,
    VylManagedStaticConfig,
};
use super::{VylManagedStaticServer, server_error_to_result};
use crate::abi::{VylBytes, VylResult, catch_ffi, slice_length_valid};

// NOTE: mirrors the `veylor-server` static-route allowlist bounds; enforced here before any copy.
const MAX_TRAILER_NAMES: usize = 32;
const MAX_TRAILER_NAME_BYTES: usize = 128;







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
    let server = match Server::bind(addr, server_config.clone()) {
        Ok(server) => server,
        Err(error) => return server_error_to_result(error),
    };
    let handle = Box::new(VylManagedStaticServer {
        state: ManagedStaticState::Ready(Box::new(ReadyServer {
            server,
            config: server_config,
            addr,
            h2c_port: None,
            worker_count: 1,
            tls: None,
            ops: Vec::new(),
        })),
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








#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_enable_h2c(
    server: *mut VylManagedStaticServer,
    port: u16,
) -> VylResult {
    catch_ffi(|| unsafe { enable_h2c(server, port) })
}

unsafe fn enable_h2c(server: *mut VylManagedStaticServer, port: u16) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &mut *server };
    let ManagedStaticState::Ready(ready) = &mut handle.state else {
        return VylResult::BadState;
    };
    ready.h2c_port = Some(port);
    VylResult::Ok
}

#[unsafe(no_mangle)]






pub unsafe extern "C" fn vyl_managed_static_enable_tls(
    server: *mut VylManagedStaticServer,
    cert_ptr: *const u8,
    cert_len: usize,
    key_ptr: *const u8,
    key_len: usize,
    max_handshakes: usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        enable_tls(server, cert_ptr, cert_len, key_ptr, key_len, max_handshakes)
    })
}

unsafe fn enable_tls(
    server: *mut VylManagedStaticServer,
    cert_ptr: *const u8,
    cert_len: usize,
    key_ptr: *const u8,
    key_len: usize,
    max_handshakes: usize,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    if max_handshakes == 0 {
        return VylResult::InvalidArgument;
    }
    if cert_len == 0 || key_len == 0 {
        return VylResult::InvalidArgument;
    }
    if cert_ptr.is_null() || key_ptr.is_null() {
        return VylResult::InvalidArgument;
    }
    if !slice_length_valid::<u8>(cert_len) || !slice_length_valid::<u8>(key_len) {
        return VylResult::InvalidArgument;
    }
    let cert = unsafe { std::slice::from_raw_parts(cert_ptr, cert_len) };
    let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
    let handle = unsafe { &mut *server };
    let ManagedStaticState::Ready(ready) = &mut handle.state else {
        return VylResult::BadState;
    };
    ready.tls = Some((cert.to_vec(), key.to_vec(), max_handshakes));
    VylResult::Ok
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_rotate_tls(
    server: *mut VylManagedStaticServer,
    cert_ptr: *const u8,
    cert_len: usize,
    key_ptr: *const u8,
    key_len: usize,
) -> VylResult {
    catch_ffi(|| unsafe { rotate_tls(server, cert_ptr, cert_len, key_ptr, key_len) })
}

unsafe fn rotate_tls(
    server: *mut VylManagedStaticServer,
    cert_ptr: *const u8,
    cert_len: usize,
    key_ptr: *const u8,
    key_len: usize,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    if cert_len == 0 || key_len == 0 {
        return VylResult::InvalidArgument;
    }
    if cert_ptr.is_null() || key_ptr.is_null() {
        return VylResult::InvalidArgument;
    }
    if !slice_length_valid::<u8>(cert_len) || !slice_length_valid::<u8>(key_len) {
        return VylResult::InvalidArgument;
    }
    let cert = unsafe { std::slice::from_raw_parts(cert_ptr, cert_len) };
    let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
    let handle = unsafe { &mut *server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    match running.rotate_tls(cert, key) {
        Ok(()) => VylResult::Ok,
        Err(_) => VylResult::InvalidArgument,
    }
}








#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_set_worker_count(
    server: *mut VylManagedStaticServer,
    count: usize,
) -> VylResult {
    catch_ffi(|| unsafe { set_worker_count(server, count) })
}

unsafe fn set_worker_count(server: *mut VylManagedStaticServer, count: usize) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &mut *server };
    let ManagedStaticState::Ready(ready) = &mut handle.state else {
        return VylResult::BadState;
    };
    if count == 0 || count > MAX_WORKERS {
        return VylResult::InvalidArgument;
    }
    ready.worker_count = count;
    VylResult::Ok
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
    let ManagedStaticState::Ready(ready) = state else {
        handle.state = state;
        return VylResult::BadState;
    };
    let ReadyServer {
        server,
        config,
        addr,
        h2c_port,
        worker_count,
        tls,
        ops,
    } = *ready;
    if h2c_port.is_none() && worker_count == 1 && tls.is_none() {
        return match server.start() {
            Ok(running) => {
                handle.state = ManagedStaticState::Running(running);
                VylResult::Ok
            }
            Err(error) => server_error_to_result(error),
        };
    }
    let mut staged = config;
    staged.worker_count = worker_count;
    staged.h2c_port = h2c_port;
    if let Some((cert_der, key_der, max_handshakes)) = tls {
        staged.tls = Some(veylor_server::TlsConfig {
            cert_chain: vec![veylor_server::CertificateDer::from(cert_der)],
            key: veylor_server::PrivateKeyDer::Pkcs8(key_der.into()),
            max_handshakes,
        });
    }
    let bind_addr = server.addr().unwrap_or(addr);
    drop(server);
    let mut rebound = match Server::bind(bind_addr, staged) {
        Ok(rebound) => rebound,
        Err(error) => return server_error_to_result(error),
    };
    for op in &ops {
        if let Err(error) = op.apply(&mut rebound) {
            return error;
        }
    }
    match rebound.start() {
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
    if in_native_handler() {
        return VylResult::Busy;
    }
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
        ManagedStaticState::Ready(ready) => match ready.server.addr() {
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









#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_h2c_addr(
    server: *mut VylManagedStaticServer,
    out_port: *mut u16,
) -> VylResult {
    catch_ffi(|| unsafe { h2c_addr(server, out_port) })
}

unsafe fn h2c_addr(server: *mut VylManagedStaticServer, out_port: *mut u16) -> VylResult {
    if server.is_null() || out_port.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_port.write(0);
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    let Some(addr) = running.h2c_addr() else {
        return VylResult::BadState;
    };
    unsafe {
        out_port.write(addr.port());
    }
    VylResult::Ok
}

pub(super) unsafe fn read_bytes<'a>(bytes: VylBytes) -> Result<&'a [u8], VylResult> {
    if (bytes.data.is_null() && bytes.length != 0) || !slice_length_valid::<u8>(bytes.length) {
        return Err(VylResult::InvalidArgument);
    }
    if bytes.length == 0 {
        return Ok(&[]);
    }
    Ok(unsafe { slice::from_raw_parts(bytes.data, bytes.length) })
}
