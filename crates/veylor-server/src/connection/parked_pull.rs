use veylor_runtime::ConnectionToken;

use crate::host::{
    HOST_MAX_METHOD_BYTES, HostInner, ParkedEntry, RespondOutcome, TombCause, find_tomb,
};

use super::trailers::{TrailerOutcome, TrailerReport};

impl ParkedEntry {
    pub(crate) fn new(
        token: ConnectionToken,
        body_total: u64,
        method: &[u8],
        trailer_allow: &[Vec<u8>],
    ) -> Self {
        let take = method.len().min(HOST_MAX_METHOD_BYTES);
        let mut snapshot = [0; HOST_MAX_METHOD_BYTES];
        snapshot[..take].copy_from_slice(&method[..take]);
        Self {
            token,
            method_len: take as u8,
            method: snapshot,
            staged: None,
            stream_cmd: None,
            stream: None,
            body_total,
            body_limit: None,
            body: Vec::new(),
            body_pulled: 0,
            request: None,
            trailers: None,
            trailer_allow: trailer_allow.to_vec(),
            tunnel_in: Vec::new(),
            tunnel_in_start: 0,
            tunnel_out: Vec::new(),
            tunnel_out_start: 0,
            tunnel_eof: false,
            tunnel_close: false,
            tunnel_grant: 0,
            h2_tunnel: false,
        }
    }
}

pub(crate) fn pull_staged_body(
    inner: &mut HostInner,
    token: ConnectionToken,
    out: &mut [u8],
) -> (RespondOutcome, usize, bool) {
    // NOTE: a staged-but-unapplied stream command fails body pulls closed with Stale; the
    // cause is known-wrong (the entry is live) but serving body mid-response risks order.
    let live = inner
        .parked
        .get(token.worker_id as usize)
        .and_then(|table| table.get(token.slot as usize))
        .and_then(|entry| entry.as_ref())
        .is_some_and(|entry| {
            entry.token == token && entry.staged.is_none() && entry.stream_cmd.is_none()
        });
    if !live {
        let outcome = match find_tomb(inner, token.worker_id as usize, token) {
            Some(TombCause::Gone) => RespondOutcome::Gone,
            Some(TombCause::Responded) => RespondOutcome::Duplicate,
            Some(TombCause::Cancelled) | None => RespondOutcome::Stale,
        };
        return (outcome, 0, false);
    }
    let Some(entry) = inner
        .parked
        .get_mut(token.worker_id as usize)
        .and_then(|table| table.get_mut(token.slot as usize))
        .and_then(|entry| entry.as_mut())
    else {
        return (RespondOutcome::Stale, 0, false);
    };
    let take = out.len().min(entry.body.len());
    out[..take].copy_from_slice(&entry.body[..take]);
    entry.body.drain(..take);
    entry.body_pulled += take as u64;
    let complete = entry.body_pulled == entry.body_total;
    (RespondOutcome::Ok, take, complete)
}

impl crate::host::HostHandle {
    pub fn pull_trailers(
        &self,
        token: ConnectionToken,
        names: &mut [u8],
        values: &mut [u8],
    ) -> TrailerReport {
        let pulled = self
            .shared
            .with_inner(|inner| (pull_parked_trailers(inner, token, names, values), None));
        pulled.0
    }
}

// NOTE: whole-set trailer copy after end-of-body: names and values join with NUL separators and
// copy only when both buffers fit, otherwise nothing copies and the report carries the required
// capacities; staged bytes are peeked, never drained, so repeated pulls observe the same set.
fn pull_parked_trailers(
    inner: &mut HostInner,
    token: ConnectionToken,
    names: &mut [u8],
    values: &mut [u8],
) -> TrailerReport {
    let entry = inner
        .parked
        .get(token.worker_id as usize)
        .and_then(|table| table.get(token.slot as usize))
        .and_then(|entry| entry.as_ref())
        .filter(|entry| entry.token == token && entry.staged.is_none());
    let Some(entry) = entry else {
        let outcome = match find_tomb(inner, token.worker_id as usize, token) {
            Some(TombCause::Gone) => TrailerOutcome::Gone,
            Some(TombCause::Responded) => TrailerOutcome::Duplicate,
            Some(TombCause::Cancelled) | None => TrailerOutcome::Stale,
        };
        return TrailerReport {
            outcome,
            ..TrailerReport::default()
        };
    };
    if entry.trailers.is_none() || entry.body_pulled != entry.body_total {
        return TrailerReport {
            outcome: TrailerOutcome::NotReady,
            ..TrailerReport::default()
        };
    }
    let fields = entry.trailers.as_deref().unwrap_or(&[]);
    let mut names_need = 0usize;
    let mut values_need = 0usize;
    for (index, field) in fields.iter().enumerate() {
        names_need += field.name.len() + usize::from(index > 0);
        values_need += field.value.len() + usize::from(index > 0);
    }
    if names.len() < names_need || values.len() < values_need {
        return TrailerReport {
            outcome: TrailerOutcome::Ok,
            fields: fields.len(),
            names_required: names_need,
            values_required: values_need,
            ..TrailerReport::default()
        };
    }
    let mut names_used = 0usize;
    let mut values_used = 0usize;
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            names[names_used] = b'\0';
            names_used += 1;
            values[values_used] = b'\0';
            values_used += 1;
        }
        names[names_used..names_used + field.name.len()].copy_from_slice(&field.name);
        names_used += field.name.len();
        values[values_used..values_used + field.value.len()].copy_from_slice(&field.value);
        values_used += field.value.len();
    }
    TrailerReport {
        outcome: TrailerOutcome::Ok,
        fields: fields.len(),
        names_copied: names_used,
        values_copied: values_used,
        ..TrailerReport::default()
    }
}
