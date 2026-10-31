use std::sync::atomic::Ordering;
use std::time::Duration;

use veylor_runtime::ConnectionToken;

use crate::connection::parked::{BodyPushOutcome, MAX_STAGED_TRAILER_BYTES, TRAILER_SLOTS};
use crate::connection::trailers::{StagedTrailer, trailer_allowed, valid_trailer_field};
use crate::connection::{pull_staged_body, push_staged_body};
use crate::static_routes::MAX_RESPONSE_BODY;

use super::request::RequestSnapshot;
use super::signals::{
    retire_signals, retract_body_append, stage_body_available, stage_writable_locked,
};
use super::{
    BodyPull, CancelOutcome, HOST_MAX_METHOD_BYTES, HOST_MAX_PATH_BYTES, HOST_MAX_REASON_BYTES,
    HOST_QUEUE_DEPTH, HostEvent, HostEventKind, HostShared, ParkError, RespondOutcome, TombCause,
    find_tomb, push_tomb, valid_reason, valid_response_headers,
};

#[derive(Clone, Debug)]
pub(crate) enum StagedAction {
    Respond {
        status: u16,
        reason: Vec<u8>,
        body: Vec<u8>,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Cancel,
}

#[derive(Clone, Debug)]
pub(crate) struct ParkedEntry {
    pub(crate) token: ConnectionToken,
    pub(crate) method_len: u8,
    pub(crate) method: [u8; HOST_MAX_METHOD_BYTES],
    pub(crate) staged: Option<StagedAction>,
    pub(crate) stream_cmd: Option<crate::connection::parked::StreamCmd>,
    pub(crate) stream: Option<crate::connection::parked::StreamState>,
    pub(crate) body_total: u64,
    pub(crate) body_limit: Option<u64>,
    pub(crate) body: Vec<u8>,
    pub(crate) body_pulled: u64,
    pub(crate) request: Option<RequestSnapshot>,
    pub(crate) trailers: Option<Vec<crate::connection::trailers::StagedTrailer>>,
    pub(crate) trailer_allow: Vec<Vec<u8>>,
    pub(crate) tunnel_in: Vec<u8>,
    pub(crate) tunnel_in_start: usize,
    pub(crate) tunnel_out: Vec<u8>,
    pub(crate) tunnel_out_start: usize,
    pub(crate) tunnel_eof: bool,
    pub(crate) tunnel_close: bool,
    pub(crate) tunnel_grant: u32,
    pub(crate) h2_tunnel: bool,
}

pub struct StagedWork {
    pub token: ConnectionToken,
    pub slot: usize,
    pub status: u16,
    pub reason: Vec<u8>,
    pub body: Vec<u8>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub cancel: bool,
    pub grant_owed: u32,
}

// NOTE: the parked deadline reuses the AwaitingHandler (body-progress) timeout so an
// abandoned park times out like an abandoned body. Only the event copy and the staged
// response cross this lock, never parser pointers or IO.
impl HostShared {
    #[allow(clippy::too_many_arguments)]
    pub fn try_park(
        &self,
        worker_id: u32,
        token: ConnectionToken,
        route_id: u64,
        method: &[u8],
        path: &[u8],
        body_total: u64,
        trailer_allow: &[Vec<u8>],
    ) -> Result<ConnectionToken, ParkError> {
        self.try_park_inner(
            worker_id,
            token,
            route_id,
            method,
            path,
            body_total,
            trailer_allow,
            None,
            false,
            || Ok(None),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_park_with_request<'a>(
        &self,
        worker_id: u32,
        token: ConnectionToken,
        route_id: u64,
        method: &[u8],
        path: &[u8],
        body_total: u64,
        trailer_allow: &[Vec<u8>],
        body_limit: Option<u64>,
        tunnel: bool,
        headers: impl Iterator<Item = Option<(&'a [u8], &'a [u8])>> + Clone,
    ) -> Result<ConnectionToken, ParkError> {
        if method.is_empty()
            || method.len() > HOST_MAX_METHOD_BYTES
            || path.is_empty()
            || path.len() > HOST_MAX_PATH_BYTES
        {
            return Err(ParkError::SnapshotTooLarge);
        }

        self.try_park_inner(
            worker_id,
            token,
            route_id,
            method,
            path,
            body_total,
            trailer_allow,
            body_limit,
            tunnel,
            || {
                RequestSnapshot::new(path, headers)
                    .map(Some)
                    .ok_or(ParkError::SnapshotTooLarge)
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_park_inner(
        &self,
        worker_id: u32,
        token: ConnectionToken,
        route_id: u64,
        method: &[u8],
        path: &[u8],
        body_total: u64,
        trailer_allow: &[Vec<u8>],
        body_limit: Option<u64>,
        tunnel: bool,
        request_snapshot: impl FnOnce() -> Result<Option<RequestSnapshot>, ParkError>,
    ) -> Result<ConnectionToken, ParkError> {
        if method.is_empty()
            || method.len() > HOST_MAX_METHOD_BYTES
            || path.is_empty()
            || path.len() > HOST_MAX_PATH_BYTES
        {
            return Err(ParkError::SnapshotTooLarge);
        }

        let outcome = self.with_inner(|inner| {
            let worker = worker_id as usize;
            let slot = token.slot as usize;
            let vacant = inner
                .parked
                .get(worker)
                .and_then(|table| table.get(slot))
                .is_some_and(|entry| entry.is_none());
            let room = inner
                .events
                .get(worker)
                .is_some_and(|queue| queue.len() < HOST_QUEUE_DEPTH);
            if !vacant || !room {
                inner.counters.queue_full += 1;
                return (Err(ParkError::Full), None);
            }

            let request = match request_snapshot() {
                Ok(request) => request,
                Err(error) => return (Err(error), None),
            };

            let seq = inner.seqs[worker][slot].wrapping_add(1);
            let seq = if seq == 0 { 1 } else { seq };
            inner.seqs[worker][slot] = seq;
            let event_token = ConnectionToken {
                request_seq: seq,
                ..token
            };
            let mut event = HostEvent {
                kind: HostEventKind::Request,
                token: event_token,
                route_id,
                method_len: method.len() as u8,
                method: [0; HOST_MAX_METHOD_BYTES],
                path_len: path.len() as u16,
                path: [0; HOST_MAX_PATH_BYTES],
                body_ready: 0,
                body_complete: false,
            };
            event.method[..method.len()].copy_from_slice(method);
            event.path[..path.len()].copy_from_slice(path);
            inner.events[worker].push_back(event);
            let mut parked = ParkedEntry::new(event_token, body_total, method, trailer_allow);
            parked.body_limit = body_limit;
            parked.h2_tunnel = tunnel;
            parked.request = request;
            inner.parked[worker][slot] = Some(parked);
            inner.counters.parks += 1;
            let waker = inner.wakers.get(worker).and_then(|slot| slot.clone());
            (Ok(event_token), waker)
        });
        let Some((result, waker)) = outcome else {
            return Err(ParkError::Full);
        };
        if result.is_ok() && tunnel {
            if let Some(count) = self.h2_tunnels.get(worker_id as usize) {
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        if result.is_ok() {
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
            self.wake.notify_one();
        }
        result
    }

    pub fn unpark_closed(&self, worker_id: u32, slot: u32) -> bool {
        let done = self.with_inner(|inner| {
            let worker = worker_id as usize;
            let entry = inner
                .parked
                .get_mut(worker)
                .and_then(|table| table.get_mut(slot as usize))
                .and_then(|entry| entry.take());
            // NOTE: the tomb records the sequenced token: only the exact parked exchange
            // observes Gone; anything else is Stale.
            match entry {
                Some(entry) => {
                    retire_signals(inner, worker, entry.token);
                    push_tomb(inner, worker, entry.token, TombCause::Gone);
                    ((true, entry.h2_tunnel), None)
                }
                None => ((false, false), None),
            }
        });
        let Some(((closed, tunnel), _)) = done else {
            return false;
        };
        if tunnel {
            if let Some(count) = self.h2_tunnels.get(worker_id as usize) {
                count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        closed
    }

    pub fn push_body(&self, token: ConnectionToken, bytes: &[u8]) -> bool {
        let done = self.with_inner(|inner| {
            if !push_staged_body(inner, token, bytes) {
                return (false, None);
            }
            if bytes.is_empty() {
                return (true, None);
            }
            if stage_body_available(inner, token.worker_id as usize, token) {
                return (true, None);
            }
            // NOTE: atomic admission: the signal has no coalesce target and no room, so the
            // appended bytes roll back and the caller stalls or fails closed like any refusal.
            retract_body_append(inner, token, bytes.len());
            (false, None)
        });
        let staged = done.is_some_and(|(ok, _)| ok);
        if staged && !bytes.is_empty() {
            self.wake.notify_one();
        }
        staged
    }

    pub(crate) fn push_body_or_answered(
        &self,
        token: ConnectionToken,
        bytes: &[u8],
    ) -> BodyPushOutcome {
        let done = self.with_inner(|inner| {
            let outcome = crate::connection::streams::push_body_or_answered(inner, token, bytes);
            if outcome != BodyPushOutcome::Pushed || bytes.is_empty() {
                return (outcome, None);
            }
            if stage_body_available(inner, token.worker_id as usize, token) {
                return (outcome, None);
            }
            retract_body_append(inner, token, bytes.len());
            (BodyPushOutcome::Refused, None)
        });
        let outcome = done
            .map(|(outcome, _)| outcome)
            .unwrap_or(BodyPushOutcome::Refused);
        if outcome == BodyPushOutcome::Pushed && !bytes.is_empty() {
            self.wake.notify_one();
        }
        outcome
    }

    pub fn stage_respond(
        &self,
        token: ConnectionToken,
        status: u16,
        reason: &[u8],
        headers: &[(&[u8], &[u8])],
        body: &[u8],
    ) -> RespondOutcome {
        if !(200..=599).contains(&status)
            || reason.is_empty()
            || reason.len() > HOST_MAX_REASON_BYTES
            || !valid_reason(reason)
            || !valid_response_headers(headers)
        {
            return RespondOutcome::Invalid;
        }
        if body.len() > MAX_RESPONSE_BODY {
            return RespondOutcome::BodyTooLarge;
        }
        let outcome = self.with_inner(|inner| {
            let worker = token.worker_id as usize;
            let slot = token.slot as usize;
            if inner.events.len() <= worker || token.generation == 0 {
                return (RespondOutcome::Invalid, None);
            }
            if let Some(entry) = inner
                .parked
                .get_mut(worker)
                .and_then(|table| table.get_mut(slot))
                .and_then(|entry| entry.as_mut())
            {
                // NOTE: a token mismatch falls through to the tomb search: the slot may hold
                // a newer exchange while the tomb records this exact token.
                if entry.token == token {
                    // NOTE: one-shot respond loses to an open or staged stream: the entry stays
                    // parked for pump-pull, so a late fixed respond reports Duplicate.
                    if entry.staged.is_some() || entry.stream.is_some() {
                        inner.counters.responds_duplicate += 1;
                        return (RespondOutcome::Duplicate, None);
                    }
                    entry.staged = Some(StagedAction::Respond {
                        status,
                        reason: reason.to_vec(),
                        body: body.to_vec(),
                        headers: headers
                            .iter()
                            .map(|(name, value)| (name.to_vec(), value.to_vec()))
                            .collect(),
                    });
                    if let Some(epoch) = inner.staged_epoch.get_mut(worker) {
                        *epoch += 1;
                    }
                    retire_signals(inner, worker, token);
                    inner.counters.responds_ok += 1;
                    let waker = inner.wakers.get(worker).and_then(|slot| slot.clone());
                    return (RespondOutcome::Ok, waker);
                }
            }
            let found = match find_tomb(inner, worker, token) {
                Some(TombCause::Responded) => RespondOutcome::Duplicate,
                Some(TombCause::Gone) => RespondOutcome::Gone,
                Some(TombCause::Cancelled) | None => RespondOutcome::Stale,
            };
            match found {
                RespondOutcome::Duplicate => inner.counters.responds_duplicate += 1,
                RespondOutcome::Gone => inner.counters.responds_gone += 1,
                _ => inner.counters.responds_stale += 1,
            }
            (found, None)
        });
        let Some((result, waker)) = outcome else {
            return RespondOutcome::Stale;
        };
        if result == RespondOutcome::Ok {
            self.note_staged(token.worker_id as usize);
            self.wake.notify_all();
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
        }
        result
    }

    pub fn stage_cancel(&self, token: ConnectionToken) -> CancelOutcome {
        let outcome = self.with_inner(|inner| {
            let worker = token.worker_id as usize;
            let slot = token.slot as usize;
            if inner.events.len() <= worker || token.generation == 0 {
                return (CancelOutcome::Invalid, None);
            }
            let matches = inner
                .parked
                .get(worker)
                .and_then(|table| table.get(slot))
                .and_then(|entry| entry.as_ref())
                .is_some_and(|entry| entry.token == token && entry.staged.is_none());
            if !matches {
                inner.counters.cancels_stale += 1;
                return (CancelOutcome::Stale, None);
            }
            if let Some(entry) = inner.parked[worker][slot].as_mut() {
                if entry.stream.as_ref().is_some_and(|stream| stream.upgraded) {
                    entry.tunnel_close = true;
                } else {
                    entry.staged = Some(StagedAction::Cancel);
                }
            }
            if let Some(epoch) = inner.staged_epoch.get_mut(worker) {
                *epoch += 1;
            }
            retire_signals(inner, worker, token);
            inner.counters.cancels_ok += 1;
            let waker = inner.wakers.get(worker).and_then(|slot| slot.clone());
            (CancelOutcome::Ok, waker)
        });
        let Some((result, waker)) = outcome else {
            return CancelOutcome::Stale;
        };
        if result == CancelOutcome::Ok {
            self.note_staged(token.worker_id as usize);
            self.wake.notify_all();
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
        }
        result
    }

    pub(crate) fn push_request_trailers(
        &self,
        token: ConnectionToken,
        fields: &[(&[u8], &[u8])],
    ) -> RespondOutcome {
        if fields.len() > TRAILER_SLOTS {
            return RespondOutcome::Invalid;
        }
        let outcome = self.with_inner(|inner| {
            let worker = token.worker_id as usize;
            let slot = token.slot as usize;
            let entry = inner
                .parked
                .get_mut(worker)
                .and_then(|table| table.get_mut(slot))
                .and_then(|entry| entry.as_mut())
                .filter(|entry| entry.token == token);
            let Some(entry) = entry else {
                let outcome = match find_tomb(inner, worker, token) {
                    Some(TombCause::Responded) => RespondOutcome::Duplicate,
                    Some(TombCause::Gone) => RespondOutcome::Gone,
                    Some(TombCause::Cancelled) | None => RespondOutcome::Stale,
                };
                return (outcome, None);
            };
            if entry.staged.is_some() {
                return (RespondOutcome::Duplicate, None);
            }
            let have = entry.trailers.as_ref().map_or(0, Vec::len);
            if have.saturating_add(fields.len()) > TRAILER_SLOTS {
                return (RespondOutcome::Invalid, None);
            }
            let mut total: usize = entry
                .trailers
                .iter()
                .flatten()
                .map(|field| field.name.len() + field.value.len())
                .sum();
            for (name, value) in fields {
                if !valid_trailer_field(name, value)
                    || !trailer_allowed(&entry.trailer_allow, b"", name)
                {
                    return (RespondOutcome::Invalid, None);
                }
                total = total.saturating_add(name.len()).saturating_add(value.len());
                if total > MAX_STAGED_TRAILER_BYTES {
                    return (RespondOutcome::Invalid, None);
                }
            }
            let staged = entry.trailers.get_or_insert_with(Vec::new);
            for (name, value) in fields {
                staged.push(StagedTrailer {
                    name: name.to_vec(),
                    value: value.to_vec(),
                });
            }
            (RespondOutcome::Ok, None)
        });
        outcome
            .map(|(outcome, _)| outcome)
            .unwrap_or(RespondOutcome::Stale)
    }

    pub fn pull_body(&self, token: ConnectionToken, out: &mut [u8]) -> BodyPull {
        let done = self.with_inner(|inner| (pull_staged_body(inner, token, out), None));
        done.map(|(r, _)| r)
            .unwrap_or((RespondOutcome::Stale, 0, false))
    }

    // NOTE: coalesced writable staging for the re-arm call sites (take_stream_cmd in
    // connection/streams.rs and the H2 re-arm loop in h2/serve.rs): call once where the arm
    // releases. A repeat while the signal is pending reports true without duplicating; a full
    // queue reports false and the arm state persists, so the next release re-signals and the
    // is_writable query stays authoritative throughout.
    pub fn stage_writable(&self, token: ConnectionToken) -> bool {
        let done = self.with_inner(|inner| {
            let worker = token.worker_id as usize;
            if inner.events.len() <= worker || token.generation == 0 {
                return (false, None);
            }
            (stage_writable_locked(inner, worker, token), None)
        });
        let staged = done.is_some_and(|(ok, _)| ok);
        if staged {
            self.wake.notify_one();
        }
        staged
    }

    pub(crate) fn staged_epoch(&self, worker: usize) -> u64 {
        self.staged_epoch
            .get(worker)
            .map(|epoch| epoch.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    pub(crate) fn note_staged(&self, worker: usize) {
        if let Some(epoch) = self.staged_epoch.get(worker) {
            epoch.fetch_add(1, Ordering::Release);
        }
    }

    pub(crate) fn take_stream_command(
        &self,
        token: ConnectionToken,
    ) -> Option<crate::connection::streams::StreamCmd> {
        self.with_inner(|inner| {
            (
                crate::connection::streams::take_stream_cmd(inner, token),
                None,
            )
        })
        .and_then(|(command, _)| command)
    }

    pub(crate) fn wait_staged(&self, worker_id: u32, timeout: Duration) -> Vec<StagedWork> {
        let (staged, _) = self.take_staged(worker_id);
        if !staged.is_empty() {
            return staged;
        }
        if let Ok(guard) = self.inner.lock() {
            let _ = self.wake.wait_timeout(guard, timeout);
        }
        self.take_staged(worker_id).0
    }

    pub fn take_staged(&self, worker_id: u32) -> (Vec<StagedWork>, u64) {
        let taken = self.with_inner(|inner| {
            let worker = worker_id as usize;
            if inner.parked.len() <= worker {
                return ((Vec::new(), 0), None);
            }
            let mut staged = Vec::new();
            for slot in 0..inner.parked[worker].len() {
                let action = inner.parked[worker][slot]
                    .as_mut()
                    // NOTE: stream entries stay parked for pump-pull; take_staged never consumes them.
                    .filter(|entry| entry.stream.is_none())
                    .and_then(|entry| entry.staged.take());
                let Some(action) = action else { continue };
                let entry = inner.parked[worker][slot].take();
                let Some(entry) = entry else { continue };
                let cause = match &action {
                    StagedAction::Respond { .. } => TombCause::Responded,
                    StagedAction::Cancel => TombCause::Cancelled,
                };
                push_tomb(inner, worker, entry.token, cause);
                let (status, reason, body, headers, cancel) = match action {
                    StagedAction::Respond {
                        status,
                        reason,
                        body,
                        headers,
                    } => (status, reason, body, headers, false),
                    StagedAction::Cancel => (0, Vec::new(), Vec::new(), Vec::new(), true),
                };
                let base = ConnectionToken {
                    request_seq: 0,
                    ..entry.token
                };
                let grant = entry.body_pulled.min(u64::from(u32::MAX)) as u32;
                staged.push(StagedWork {
                    token: base,
                    slot,
                    status,
                    reason,
                    body,
                    headers,
                    cancel,
                    grant_owed: grant,
                });
            }
            (
                (staged, inner.staged_epoch.get(worker).copied().unwrap_or(0)),
                None,
            )
        });
        taken
            .map(|((staged, epoch), _)| (staged, epoch))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::connection::trailers::TrailerOutcome;

    use super::*;

    fn token() -> ConnectionToken {
        ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 1,
            generation: 1,
        }
    }

    #[test]
    fn staged_request_trailers_follow_allowlist_and_body_completion() {
        let shared = Arc::new(HostShared::new(1));
        let token = token();
        let event = shared
            .try_park(0, token, 0, b"POST", b"/upload", 3, &[b"x-check".to_vec()])
            .expect("park");
        let handle = crate::host::HostHandle::new(Arc::clone(&shared));

        assert!(shared.push_body(event, b"abc"));
        let mut body = [0u8; 3];
        assert_eq!(
            handle.pull_body(event, &mut body),
            (RespondOutcome::Ok, 3, true)
        );

        assert_eq!(
            shared.push_request_trailers(event, &[(&b"x-check"[..], &b"yes"[..])]),
            RespondOutcome::Ok
        );
        assert_eq!(
            shared.push_request_trailers(event, &[(&b"x-other"[..], &b"no"[..])]),
            RespondOutcome::Invalid
        );

        let mut names = [0u8; 8];
        let mut values = [0u8; 8];
        let report = handle.pull_trailers(event, &mut names, &mut values);
        assert_eq!(report.outcome, TrailerOutcome::Ok);
        assert_eq!(report.fields, 1);
        assert_eq!(&names[..report.names_copied], b"x-check");
        assert_eq!(&values[..report.values_copied], b"yes");
    }
}
