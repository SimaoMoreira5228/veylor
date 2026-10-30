use std::slice;

use veylor_server::RespondOutcome;

use super::dispatch::{respond_outcome_to_result, token_to_server};
use super::{ManagedStaticState, VylManagedStaticServer, VylManagedStaticToken};
use crate::abi::{VylBytes, VylResult, catch_ffi, slice_length_valid};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_upgrade_websocket(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) -> VylResult {
    catch_ffi(|| unsafe { upgrade_websocket(server, token) })
}

unsafe fn upgrade_websocket(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    respond_outcome_to_result(running.host().upgrade_websocket(token_to_server(token)))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_upgrade_websocket_with_protocol(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    protocol: VylBytes,
) -> VylResult {
    catch_ffi(|| unsafe { upgrade_websocket_with_protocol(server, token, protocol) })
}

unsafe fn upgrade_websocket_with_protocol(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    protocol: VylBytes,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    if protocol.length > 0 && protocol.data.is_null() {
        return VylResult::InvalidArgument;
    }
    if !slice_length_valid::<u8>(protocol.length) {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    let protocol = unsafe { slice::from_raw_parts(protocol.data, protocol.length) };
    respond_outcome_to_result(
        running
            .host()
            .upgrade_websocket_with_protocol(token_to_server(token), protocol),
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_tunnel_read(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    destination: *mut u8,
    capacity: usize,
    out_read: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe { tunnel_read(server, token, destination, capacity, out_read) })
}

unsafe fn tunnel_read(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    destination: *mut u8,
    capacity: usize,
    out_read: *mut usize,
) -> VylResult {
    if server.is_null() || out_read.is_null() {
        return VylResult::InvalidArgument;
    }
    if capacity > 0 && destination.is_null() {
        return VylResult::InvalidArgument;
    }
    if !slice_length_valid::<u8>(capacity) {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_read.write(0);
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    let output = unsafe { slice::from_raw_parts_mut(destination, capacity) };
    let (outcome, count) = running.host().tunnel_read(token_to_server(token), output);
    unsafe {
        out_read.write(count);
    }
    respond_outcome_to_result(outcome)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_tunnel_write(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    bytes: VylBytes,
    out_accepted: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe { tunnel_write(server, token, bytes, out_accepted) })
}

unsafe fn tunnel_write(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    bytes: VylBytes,
    out_accepted: *mut usize,
) -> VylResult {
    if server.is_null() || out_accepted.is_null() {
        return VylResult::InvalidArgument;
    }
    if bytes.length > 0 && bytes.data.is_null() {
        return VylResult::InvalidArgument;
    }
    if !slice_length_valid::<u8>(bytes.length) {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_accepted.write(0);
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    let input = unsafe { slice::from_raw_parts(bytes.data, bytes.length) };
    let outcome = running.host().tunnel_write(token_to_server(token), input);
    if outcome == RespondOutcome::Ok {
        unsafe {
            out_accepted.write(bytes.length);
        }
    }
    respond_outcome_to_result(outcome)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_tunnel_close(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) -> VylResult {
    catch_ffi(|| unsafe { tunnel_close(server, token) })
}

unsafe fn tunnel_close(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    respond_outcome_to_result(running.host().tunnel_close(token_to_server(token)))
}
