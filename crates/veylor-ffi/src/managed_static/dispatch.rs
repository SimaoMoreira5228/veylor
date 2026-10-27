use std::slice;

use veylor_runtime::ConnectionToken;
use veylor_server::{
    CancelOutcome, EventSlot, HostEventKind, PollReport, RespondOutcome, TrailerOutcome,
    TrailerReport,
};

use super::VylManagedStaticServer;
use super::{
    ManagedStaticState, VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE, VYL_MANAGED_STATIC_EVENT_REQUEST,
    VYL_MANAGED_STATIC_EVENT_SHUTDOWN, VYL_MANAGED_STATIC_EVENT_WRITABLE, VylManagedStaticEvent,
    VylManagedStaticPollReport, VylManagedStaticToken, VylManagedStaticTrailerReport,
};
use crate::abi::{VylBytes, VylField, VylResult, catch_ffi, slice_length_valid};

// NOTE: mirrors the `veylor-server` staged response-trailer bounds; enforced here before any copy.
const MAX_RESPONSE_TRAILER_FIELDS: usize = 8;
const MAX_RESPONSE_TRAILER_BYTES: usize = 1024;














#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_poll_events(
    server: *mut VylManagedStaticServer,
    timeout_ns: u64,
    events: *mut VylManagedStaticEvent,
    event_capacity: usize,
    methods: *mut u8,
    method_capacity: usize,
    paths: *mut u8,
    path_capacity: usize,
    out_report: *mut VylManagedStaticPollReport,
) -> VylResult {
    catch_ffi(|| unsafe {
        poll_events(
            server,
            timeout_ns,
            events,
            event_capacity,
            methods,
            method_capacity,
            paths,
            path_capacity,
            out_report,
        )
    })
}

#[allow(clippy::too_many_arguments)]
unsafe fn poll_events(
    server: *mut VylManagedStaticServer,
    timeout_ns: u64,
    events: *mut VylManagedStaticEvent,
    event_capacity: usize,
    methods: *mut u8,
    method_capacity: usize,
    paths: *mut u8,
    path_capacity: usize,
    out_report: *mut VylManagedStaticPollReport,
) -> VylResult {
    if server.is_null() || out_report.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_report.write(VylManagedStaticPollReport::default());
    }
    if (events.is_null() && event_capacity != 0) || !slice_length_valid::<EventSlot>(event_capacity)
    {
        return VylResult::InvalidArgument;
    }
    if (methods.is_null() && method_capacity != 0) || !slice_length_valid::<u8>(method_capacity) {
        return VylResult::InvalidArgument;
    }
    if (paths.is_null() && path_capacity != 0) || !slice_length_valid::<u8>(path_capacity) {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    let blank = EventSlot {
        kind: HostEventKind::Request,
        token: ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 0,
            generation: 0,
        },
        route_id: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    };
    let mut slots = vec![blank; event_capacity];
    let methods = unsafe { mutable_bytes(methods, method_capacity) };
    let paths = unsafe { mutable_bytes(paths, path_capacity) };
    let report = running.host().poll(timeout_ns, &mut slots, methods, paths);
    let out_events = unsafe { mutable_events(events, event_capacity) };
    for (index, slot) in slots.iter().take(report.copied).enumerate() {
        out_events[index] = event_from_slot(*slot);
    }
    unsafe {
        out_report.write(report_from_poll(report));
    }
    VylResult::Ok
}








#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_respond(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    status: u16,
    reason: VylBytes,
    body: VylBytes,
) -> VylResult {
    catch_ffi(|| unsafe { host_respond(server, token, status, reason, body) })
}

unsafe fn host_respond(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    status: u16,
    reason: VylBytes,
    body: VylBytes,
) -> VylResult {
    if server.is_null() {
        return VylResult::InvalidArgument;
    }
    let reason = match unsafe { read_bytes(reason) } {
        Ok(reason) => reason,
        Err(error) => return error,
    };
    let body = match unsafe { read_bytes(body) } {
        Ok(body) => body,
        Err(error) => return error,
    };
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    respond_outcome_to_result(
        running
            .host()
            .respond(token_to_server(token), status, reason, body),
    )
}








#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_respond_with_headers(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    status: u16,
    reason: VylBytes,
    names: *const VylBytes,
    values: *const VylBytes,
    count: usize,
    body: VylBytes,
) -> VylResult {
    catch_ffi(|| unsafe {
        host_respond_with_headers(server, token, status, reason, names, values, count, body)
    })
}

#[allow(clippy::too_many_arguments)]
unsafe fn host_respond_with_headers(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    status: u16,
    reason: VylBytes,
    names: *const VylBytes,
    values: *const VylBytes,
    count: usize,
    body: VylBytes,
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
    let body = match unsafe { read_bytes(body) } {
        Ok(body) => body,
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
    respond_outcome_to_result(running.host().respond_with_headers(
        token_to_server(token),
        status,
        reason,
        &headers,
        body,
    ))
}





#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_cancel(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
) -> VylResult {
    catch_ffi(|| unsafe { host_cancel(server, token) })
}

unsafe fn host_cancel(
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
    match running.host().cancel(token_to_server(token)) {
        CancelOutcome::Ok => VylResult::Ok,
        CancelOutcome::Stale => VylResult::StaleToken,
        CancelOutcome::Invalid => VylResult::InvalidArgument,
    }
}








#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_pull_body(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    out: *mut u8,
    out_capacity: usize,
    out_pulled: *mut usize,
    out_end: *mut u32,
) -> VylResult {
    catch_ffi(|| unsafe { host_pull_body(server, token, out, out_capacity, out_pulled, out_end) })
}

unsafe fn host_pull_body(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    out: *mut u8,
    out_capacity: usize,
    out_pulled: *mut usize,
    out_end: *mut u32,
) -> VylResult {
    if server.is_null() || out_pulled.is_null() || out_end.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_pulled.write(0);
        out_end.write(0);
    }
    if (out.is_null() && out_capacity != 0) || !slice_length_valid::<u8>(out_capacity) {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    let output = unsafe { mutable_bytes(out, out_capacity) };
    let (outcome, pulled, end) = running.host().pull_body(token_to_server(token), output);
    unsafe {
        out_pulled.write(pulled);
        out_end.write(u32::from(end));
    }
    respond_outcome_to_result(outcome)
}

fn respond_outcome_to_result(outcome: RespondOutcome) -> VylResult {
    match outcome {
        RespondOutcome::Ok => VylResult::Ok,
        RespondOutcome::Stale => VylResult::StaleToken,
        RespondOutcome::Duplicate => VylResult::Duplicate,
        RespondOutcome::Gone => VylResult::Gone,
        RespondOutcome::Invalid => VylResult::InvalidArgument,
        RespondOutcome::BodyTooLarge => VylResult::BodyTooLong,
        RespondOutcome::NotWritable => VylResult::BadState,
        RespondOutcome::WouldBlock => VylResult::WouldBlock,
    }
}












#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_managed_static_host_pull_trailers(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    names: *mut u8,
    names_capacity: usize,
    values: *mut u8,
    values_capacity: usize,
    out_report: *mut VylManagedStaticTrailerReport,
) -> VylResult {
    catch_ffi(|| unsafe {
        host_pull_trailers(
            server,
            token,
            names,
            names_capacity,
            values,
            values_capacity,
            out_report,
        )
    })
}

#[allow(clippy::too_many_arguments)]
unsafe fn host_pull_trailers(
    server: *mut VylManagedStaticServer,
    token: VylManagedStaticToken,
    names: *mut u8,
    names_capacity: usize,
    values: *mut u8,
    values_capacity: usize,
    out_report: *mut VylManagedStaticTrailerReport,
) -> VylResult {
    if server.is_null() || out_report.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_report.write(VylManagedStaticTrailerReport::default());
    }
    if (names.is_null() && names_capacity != 0) || !slice_length_valid::<u8>(names_capacity) {
        return VylResult::InvalidArgument;
    }
    if (values.is_null() && values_capacity != 0) || !slice_length_valid::<u8>(values_capacity) {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &*server };
    let ManagedStaticState::Running(running) = &handle.state else {
        return VylResult::BadState;
    };
    let names = unsafe { mutable_bytes(names, names_capacity) };
    let values = unsafe { mutable_bytes(values, values_capacity) };
    let report = running
        .host()
        .pull_trailers(token_to_server(token), names, values);
    unsafe {
        out_report.write(report_from_trailers(report));
    }
    trailer_outcome_to_result(report.outcome)
}

fn trailer_outcome_to_result(outcome: TrailerOutcome) -> VylResult {
    match outcome {
        TrailerOutcome::Ok => VylResult::Ok,
        TrailerOutcome::NotReady => VylResult::WouldBlock,
        TrailerOutcome::Stale => VylResult::StaleToken,
        TrailerOutcome::Duplicate => VylResult::Duplicate,
        TrailerOutcome::Gone => VylResult::Gone,
    }
}








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

fn token_to_server(token: VylManagedStaticToken) -> ConnectionToken {
    ConnectionToken {
        worker_id: token.worker_id,
        slot: token.slot,
        request_seq: token.request_seq,
        stream_id: token.stream_id,
        worker_generation: token.worker_generation,
        generation: token.generation,
    }
}

fn token_from_server(token: ConnectionToken) -> VylManagedStaticToken {
    VylManagedStaticToken {
        worker_id: token.worker_id,
        slot: token.slot,
        request_seq: token.request_seq,
        stream_id: token.stream_id,
        worker_generation: token.worker_generation,
        generation: token.generation,
    }
}

// NOTE: pub(super) so the in-module tests pin the signal field reuse without an engine round trip.
pub(super) fn event_from_slot(slot: EventSlot) -> VylManagedStaticEvent {
    let kind = match slot.kind {
        HostEventKind::Request => VYL_MANAGED_STATIC_EVENT_REQUEST,
        HostEventKind::Shutdown => VYL_MANAGED_STATIC_EVENT_SHUTDOWN,
        HostEventKind::BodyAvailable => VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE,
        HostEventKind::Writable => VYL_MANAGED_STATIC_EVENT_WRITABLE,
    };
    VylManagedStaticEvent {
        kind,
        token: token_from_server(slot.token),
        route_id: slot.route_id,
        method_len: slot.method_len,
        path_len: slot.path_len,
        method_offset: slot.method_offset,
        path_offset: slot.path_offset,
    }
}

fn report_from_poll(report: PollReport) -> VylManagedStaticPollReport {
    VylManagedStaticPollReport {
        copied: report.copied,
        events_required: report.events_required,
        method_required: report.method_required,
        path_required: report.path_required,
    }
}

fn report_from_trailers(report: TrailerReport) -> VylManagedStaticTrailerReport {
    VylManagedStaticTrailerReport {
        fields: report.fields,
        names_copied: report.names_copied,
        values_copied: report.values_copied,
        names_required: report.names_required,
        values_required: report.values_required,
    }
}

unsafe fn mutable_bytes<'a>(data: *mut u8, length: usize) -> &'a mut [u8] {
    if length == 0 {
        return &mut [];
    }
    unsafe { slice::from_raw_parts_mut(data, length) }
}

unsafe fn mutable_events<'a>(
    data: *mut VylManagedStaticEvent,
    length: usize,
) -> &'a mut [VylManagedStaticEvent] {
    if length == 0 {
        return &mut [];
    }
    unsafe { slice::from_raw_parts_mut(data, length) }
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

unsafe fn raw_trailer_fields<'a>(
    trailers: *const VylField,
    trailer_count: usize,
) -> &'a [VylField] {
    if trailer_count == 0 {
        return &[];
    }
    unsafe { slice::from_raw_parts(trailers, trailer_count) }
}

const _: () = assert!(std::mem::size_of::<VylManagedStaticEvent>() == 4 + 4 + 32 + 8 + 8 * 4);
