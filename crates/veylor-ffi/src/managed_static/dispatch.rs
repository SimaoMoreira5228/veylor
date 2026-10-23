use std::slice;

use veylor_runtime::ConnectionToken;
use veylor_server::{CancelOutcome, EventSlot, HostEventKind, PollReport, RespondOutcome};

use super::VylManagedStaticServer;
use super::{
    ManagedStaticState, VYL_MANAGED_STATIC_EVENT_REQUEST, VYL_MANAGED_STATIC_EVENT_SHUTDOWN,
    VylManagedStaticEvent, VylManagedStaticPollReport, VylManagedStaticToken,
};
use crate::abi::{VylBytes, VylResult, catch_ffi, slice_length_valid};











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
    }
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

fn event_from_slot(slot: EventSlot) -> VylManagedStaticEvent {
    let kind = match slot.kind {
        HostEventKind::Request => VYL_MANAGED_STATIC_EVENT_REQUEST,
        HostEventKind::Shutdown => VYL_MANAGED_STATIC_EVENT_SHUTDOWN,
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

const _: () = assert!(std::mem::size_of::<VylManagedStaticEvent>() == 4 + 4 + 32 + 8 + 8 * 4);
