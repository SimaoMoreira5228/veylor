use crate::abi::{VylBytes, VylField, VylResult, catch_ffi, slice_length_valid};

use super::dispatch::{
    MAX_RESPONSE_TRAILER_BYTES, MAX_RESPONSE_TRAILER_FIELDS, raw_trailer_fields, read_bytes,
    respond_outcome_to_result, token_to_server,
};
use super::{ManagedStaticState, VylManagedStaticServer, VylManagedStaticToken};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_begin_stream(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    status: u16,
    reason: VylBytes,
) -> VylResult {
    catch_ffi(|| unsafe { host_begin_stream(server, token, status, reason) })
}

unsafe fn host_begin_stream(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    status: u16,
    reason: VylBytes,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let reason = match unsafe { read_bytes(reason) } {
        Ok(reason) => reason,
        Err(error) => return error,
    };
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    respond_outcome_to_result(
        running
            .host()
            .begin_stream(token_to_server(token), status, reason),
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_begin_stream_with_headers(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    status: u16,
    reason: VylBytes,
    names: *const VylBytes,
    values: *const VylBytes,
    count: usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        host_begin_stream_with_headers(server, token, status, reason, names, values, count)
    })
}

#[allow(clippy::too_many_arguments)]
unsafe fn host_begin_stream_with_headers(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    status: u16,
    reason: VylBytes,
    names: *const VylBytes,
    values: *const VylBytes,
    count: usize,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    if count > veylor_server::MAX_STAGED_RESPONSE_HEADERS {
        return VylResult::InvalidArgument;
    }
    if count > 0 && (names.is_null() || values.is_null()) {
        return VylResult::InvalidArgument;
    }
    if !slice_length_valid::<VylBytes>(count) {
        return VylResult::InvalidArgument;
    }
    let reason = match unsafe { read_bytes(reason) } {
        Ok(reason) => reason,
        Err(error) => return error,
    };
    let names = if count == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(names, count) }
    };
    let values = if count == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(values, count) }
    };
    let mut headers = Vec::with_capacity(count);
    for index in 0..count {
        let name = match unsafe { read_bytes(names[index]) } {
            Ok(name) => name,
            Err(error) => return error,
        };
        let value = match unsafe { read_bytes(values[index]) } {
            Ok(value) => value,
            Err(error) => return error,
        };
        headers.push((name, value));
    }
    let handle = unsafe { &mut *server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    respond_outcome_to_result(running.host().begin_stream_with_headers(
        token_to_server(token),
        status,
        reason,
        &headers,
    ))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_write_stream(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    chunk: VylBytes,
) -> VylResult {
    catch_ffi(|| unsafe { host_write_stream(server, token, chunk) })
}

unsafe fn host_write_stream(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    chunk: VylBytes,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let chunk = match unsafe { read_bytes(chunk) } {
        Ok(chunk) => chunk,
        Err(error) => return error,
    };
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    respond_outcome_to_result(running.host().write_stream(token_to_server(token), chunk))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_end_stream(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) -> VylResult {
    catch_ffi(|| unsafe { host_end_stream(server, token) })
}

unsafe fn host_end_stream(
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
    respond_outcome_to_result(running.host().end_stream(token_to_server(token)))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_end_stream_with_trailers(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    trailers: *const VylField,
    trailer_count: usize,
) -> VylResult {
    catch_ffi(|| unsafe { host_end_stream_with_trailers(server, token, trailers, trailer_count) })
}

unsafe fn host_end_stream_with_trailers(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    trailers: *const VylField,
    trailer_count: usize,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    if (trailers.is_null() && trailer_count != 0) || !slice_length_valid::<VylField>(trailer_count)
    {
        return VylResult::InvalidArgument;
    }
    if trailer_count > MAX_RESPONSE_TRAILER_FIELDS {
        return VylResult::InvalidArgument;
    }
    let raw = unsafe { raw_trailer_fields(trailers, trailer_count) };
    let mut total = 0usize;
    let mut fields = Vec::with_capacity(trailer_count);
    for field in raw {
        let name = match unsafe { read_bytes(field.name) } {
            Ok(name) => name,
            Err(error) => return error,
        };
        let value = match unsafe { read_bytes(field.value) } {
            Ok(value) => value,
            Err(error) => return error,
        };
        total = total.saturating_add(name.len()).saturating_add(value.len());
        fields.push((name, value));
    }
    if total > MAX_RESPONSE_TRAILER_BYTES {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    respond_outcome_to_result(
        running
            .host()
            .end_stream_with_trailers(token_to_server(token), &fields),
    )
}

// NOTE: the Rust query returns a bare bool with no invalid signal, so the shape check lives here:
// a zero-generation token reports bad-state, while an unknown-but-well-formed token answers Ok
// with false — absence of a live writable stream is a negative answer, never an error.

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_is_writable(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    out_writable: *mut u32,
) -> VylResult {
    catch_ffi(|| unsafe { host_is_writable(server, token, out_writable) })
}

unsafe fn host_is_writable(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    out_writable: *mut u32,
) -> VylResult {
    if server.is_null() || out_writable.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_writable.write(0);
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    let token = token_to_server(token);
    if token.generation == 0 {
        return VylResult::BadState;
    }
    unsafe {
        out_writable.write(u32::from(running.host().is_writable(token)));
    }
    VylResult::Ok
}
