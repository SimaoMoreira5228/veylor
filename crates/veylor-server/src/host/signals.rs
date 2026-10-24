use veylor_runtime::ConnectionToken;

use super::{
    HOST_MAX_METHOD_BYTES, HOST_MAX_PATH_BYTES, HOST_QUEUE_DEPTH, HostEvent, HostEventKind,
    HostInner,
};

// NOTE: advisory body signal staged at admission under the queue lock: at most one pending
// BodyAvailable per token, a repeat updates the staged counts latest-wins. A full queue with
// no coalesce target reports false and bumps queue-full; the caller rolls the byte admission
// back, so bytes and signals stay atomic and no signal ever strands staged bytes.
pub(crate) fn stage_body_available(
    inner: &mut HostInner,
    worker: usize,
    token: ConnectionToken,
) -> bool {
    let Some(queue) = inner.events.get_mut(worker) else {
        return false;
    };
    let staged = inner
        .parked
        .get(worker)
        .and_then(|table| table.get(token.slot as usize))
        .and_then(|entry| entry.as_ref())
        .filter(|entry| entry.token == token)
        .map(|entry| {
            let ready = entry.body.len();
            let complete = entry.body_pulled + ready as u64 == entry.body_total;
            (ready, complete)
        });
    let Some((ready, complete)) = staged else {
        return false;
    };
    if let Some(pending) = queue
        .iter_mut()
        .find(|event| event.kind == HostEventKind::BodyAvailable && event.token == token)
    {
        pending.body_ready = ready;
        pending.body_complete = complete;
        return true;
    }
    if queue.len() >= HOST_QUEUE_DEPTH {
        inner.counters.queue_full += 1;
        return false;
    }
    queue.push_back(HostEvent {
        kind: HostEventKind::BodyAvailable,
        token,
        route_id: 0,
        method_len: 0,
        method: [0; HOST_MAX_METHOD_BYTES],
        path_len: 0,
        path: [0; HOST_MAX_PATH_BYTES],
        body_ready: ready,
        body_complete: complete,
    });
    true
}

pub(crate) fn stage_writable_locked(
    inner: &mut HostInner,
    worker: usize,
    token: ConnectionToken,
) -> bool {
    let Some(queue) = inner.events.get_mut(worker) else {
        return false;
    };
    let live = inner
        .parked
        .get(worker)
        .and_then(|table| table.get(token.slot as usize))
        .and_then(|entry| entry.as_ref())
        .is_some_and(|entry| entry.token == token);
    if !live {
        return false;
    }
    let pending = queue
        .iter()
        .any(|event| event.kind == HostEventKind::Writable && event.token == token);
    if pending {
        return true;
    }
    if queue.len() >= HOST_QUEUE_DEPTH {
        inner.counters.queue_full += 1;
        return false;
    }
    queue.push_back(HostEvent {
        kind: HostEventKind::Writable,
        token,
        route_id: 0,
        method_len: 0,
        method: [0; HOST_MAX_METHOD_BYTES],
        path_len: 0,
        path: [0; HOST_MAX_PATH_BYTES],
        body_ready: 0,
        body_complete: false,
    });
    true
}

// NOTE: admission rollback: the appended tail belongs to this call (append-only under the
// lock), so truncating it restores the pre-push staging exactly.
pub(crate) fn retract_body_append(inner: &mut HostInner, token: ConnectionToken, appended: usize) {
    let Some(entry) = inner
        .parked
        .get_mut(token.worker_id as usize)
        .and_then(|table| table.get_mut(token.slot as usize))
        .and_then(|entry| entry.as_mut())
        .filter(|entry| entry.token == token)
    else {
        return;
    };
    let staged = entry.body.len();
    entry.body.truncate(staged.saturating_sub(appended));
}

// NOTE: signal hygiene: answering or closing an exchange retires its pending advisory signals
// under the same lock, so a later poll never observes a stale signal ahead of fresh events.
pub(crate) fn retire_signals(inner: &mut HostInner, worker: usize, token: ConnectionToken) {
    if let Some(queue) = inner.events.get_mut(worker) {
        queue.retain(|event| {
            !matches!(
                event.kind,
                HostEventKind::BodyAvailable | HostEventKind::Writable
            ) || event.token != token
        });
    }
}
