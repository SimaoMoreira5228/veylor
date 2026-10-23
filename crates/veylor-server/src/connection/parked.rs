use veylor_http1::{
    BodyDecoder, BodyEvent, BodyLimits, ConnectionHeader, Header, Limits, ResponseBody,
    ResponseFraming, ResponseToWrite, connection_header_for_response, encode_response_head,
};
use veylor_runtime::{CloseReason, ConnectionToken, IoBatch, ReadStatus, WriteStatus, dispatch};

use crate::host::{
    HOST_MAX_METHOD_BYTES, HostInner, HostShared, ParkError, ParkedEntry, RespondOutcome,
    TombCause, find_tomb,
};
use crate::static_routes::{encode_continue, respond_payload_too_large};

use super::{
    ConnClose, ConnPhase, ConnState, ExchangeOutcome, INPUT_CAP, PumpOutcome, PumpProgress,
    StagedTrailer, TrailerOutcome, TrailerReport, connection_value, field, trailer_allowed,
};

// NOTE: request-trailer staging bounds: at most TRAILER_SLOTS fields and MAX_STAGED_TRAILER_BYTES
// of staged name/value bytes per park; the first field past either cap fails the connection closed.
pub(crate) const TRAILER_SLOTS: usize = 8;
pub(crate) const MAX_STAGED_TRAILER_BYTES: usize = 1024;

impl ConnState {
    // NOTE: one-shot bounded response over the parked snapshot: the static encode path
    // with the parked method, so HEAD keeps length metadata without body bytes. A fully
    // staged body keeps the parked keep-alive; unread wire bytes drain within the cap first.
    pub fn complete_parked(
        &mut self,
        status: u16,
        reason: &[u8],
        body: &[u8],
    ) -> Result<(), ConnClose> {
        let ConnPhase::Parked {
            method_len,
            method,
            keep_open,
            body_total,
            body_received,
            chunked,
            chunked_done,
            ..
        } = self.phase
        else {
            return Err(ConnClose::TransportError);
        };
        let request_method = &method[..usize::from(method_len)];
        let connection = if keep_open {
            ConnectionHeader::Omit
        } else {
            ConnectionHeader::Close
        };
        let response = ResponseToWrite {
            request_method,
            requested_upgrade: b"",
            status,
            reason,
            fields: &[],
            framing: ResponseFraming::FixedLength,
            content_length: body.len() as u64,
            connection,
        };
        let encoded = encode_response_head(&response, &mut self.output[..])
            .map_err(|_| ConnClose::TransportError)?;
        let mut length = encoded.length;
        if encoded.body_allowed && !body.is_empty() {
            let end = length + body.len();
            self.output
                .get_mut(length..end)
                .ok_or(ConnClose::ResponseOverflow)?
                .copy_from_slice(body);
            length = end;
        }
        self.out_sent = 0;
        self.pending_len = 0;
        self.write_blocked = false;
        self.closing = !keep_open;
        self.close_reason = ConnClose::Completed;
        // NOTE: chunked completion is terminal-chunk observed, not declared-total: a respond
        // before the terminal chunk closes (framing cannot be skipped without decoding), while
        // a terminal-complete chunked body answers with no wire drain left to do.
        let body_complete = if chunked {
            chunked_done
        } else {
            body_total.saturating_sub(body_received) == 0
        };
        if body_complete {
            self.out_len = length;
            self.phase = ConnPhase::SendingResponse;
            return Ok(());
        }
        if chunked {
            return Err(ConnClose::TransportError);
        }
        self.out_len = 0;
        self.pending_len = length;
        self.read_ready = true;
        self.phase = ConnPhase::DrainingBody {
            remaining: body_total.saturating_sub(body_received),
            responded: true,
        };
        Ok(())
    }
}

// NOTE: a parked connection owns no final response yet, so the pump only stages: an admitted
// 100-continue interim flushes first with reads paused, then wire bytes are pushed into host
// staging and compacted; the byte count feeds the parked body-progress deadline via hooks.
pub(crate) fn pump_parked(conn: &mut ConnState, host: &HostShared) -> PumpOutcome {
    let token = conn.token;
    let ConnPhase::Parked {
        body_total,
        body_received,
        event_token,
        chunked,
        chunked_done,
        ..
    } = conn.phase
    else {
        return PumpOutcome::Close(ConnClose::TransportError);
    };
    let mut progress = PumpProgress::default();
    if conn.out_sent < conn.out_len {
        let out_sent = conn.out_sent;
        let out_len = conn.out_len;
        let write_ready = !conn.write_blocked;
        let mut batch = [IoBatch::new(
            token,
            &mut conn.stream,
            &mut [],
            false,
            &conn.output[out_sent..out_len],
            write_ready,
        )];
        dispatch(u64::MAX, &mut batch);
        let accepted = match batch[0].write {
            WriteStatus::Progress(accepted)
            | WriteStatus::WouldBlock(accepted)
            | WriteStatus::BudgetExhausted(accepted)
            | WriteStatus::Interrupted(accepted) => {
                if matches!(batch[0].write, WriteStatus::WouldBlock(_)) {
                    conn.write_blocked = true;
                }
                accepted
            }
            WriteStatus::Closed(CloseReason::PeerClosed) => {
                return PumpOutcome::Close(ConnClose::PeerClosed);
            }
            WriteStatus::Closed(CloseReason::Reset) => {
                return PumpOutcome::Close(ConnClose::Reset);
            }
            WriteStatus::Closed(CloseReason::Error(_)) | WriteStatus::Error(_) => {
                return PumpOutcome::Close(ConnClose::TransportError);
            }
            WriteStatus::Idle => 0,
        };
        conn.out_sent += accepted;
        progress.bytes_written = accepted as u64;
        return PumpOutcome::Keep(progress);
    }
    let read_ready = conn.read_ready;
    let read_scratch = if conn.len < INPUT_CAP {
        &mut conn.input[conn.len..]
    } else {
        &mut [] as &mut [u8]
    };
    let mut batch = [IoBatch::new(
        token,
        &mut conn.stream,
        read_scratch,
        read_ready,
        &[],
        false,
    )];
    dispatch(u64::MAX, &mut batch);
    let read = batch[0].read;
    let write = batch[0].write;
    conn.read_ready = matches!(read, ReadStatus::Progress(_) | ReadStatus::BudgetExhausted);
    match read {
        ReadStatus::Progress(bytes) => {
            conn.len += bytes;
            progress.bytes_read = bytes as u64;
        }
        ReadStatus::Eof => return PumpOutcome::Close(ConnClose::PeerClosed),
        ReadStatus::Closed(CloseReason::PeerClosed) => {
            return PumpOutcome::Close(ConnClose::PeerClosed);
        }
        ReadStatus::Closed(CloseReason::Reset) => return PumpOutcome::Close(ConnClose::Reset),
        ReadStatus::Closed(CloseReason::Error(_)) | ReadStatus::Error(_) => {
            return PumpOutcome::Close(ConnClose::TransportError);
        }
        ReadStatus::Idle
        | ReadStatus::WouldBlock
        | ReadStatus::BudgetExhausted
        | ReadStatus::PausedForWrite
        | ReadStatus::Interrupted => {}
    }
    match write {
        WriteStatus::Progress(_)
        | WriteStatus::WouldBlock(_)
        | WriteStatus::BudgetExhausted(_)
        | WriteStatus::Interrupted(_) => {}
        WriteStatus::Closed(CloseReason::PeerClosed) => {
            return PumpOutcome::Close(ConnClose::PeerClosed);
        }
        WriteStatus::Closed(CloseReason::Reset) => return PumpOutcome::Close(ConnClose::Reset),
        WriteStatus::Closed(CloseReason::Error(_)) | WriteStatus::Error(_) => {
            return PumpOutcome::Close(ConnClose::TransportError);
        }
        WriteStatus::Idle => {}
    }
    if chunked {
        if !chunked_done {
            let feed = decode_chunked(conn, host, event_token);
            if feed.failed {
                return PumpOutcome::Close(ConnClose::TransportError);
            }
            if feed.consumed > 0 {
                conn.input.copy_within(feed.consumed..conn.len, 0);
                conn.len -= feed.consumed;
            }
            if feed.decoded > 0 {
                if let ConnPhase::Parked { body_received, .. } = &mut conn.phase {
                    *body_received += feed.decoded;
                }
            }
            if feed.done {
                if let ConnPhase::Parked { chunked_done, .. } = &mut conn.phase {
                    *chunked_done = true;
                }
            }
        }
        return PumpOutcome::Keep(progress);
    }
    let take = (conn.len as u64).min(body_total.saturating_sub(body_received)) as usize;
    if take > 0 && host.push_body(event_token, &conn.input[..take]) {
        conn.input.copy_within(take..conn.len, 0);
        conn.len -= take;
        if let ConnPhase::Parked { body_received, .. } = &mut conn.phase {
            *body_received += take as u64;
        }
    }
    PumpOutcome::Keep(progress)
}

// NOTE: drain stops new parks: a head that arrives mid-drain closes instead of waiting
// on a host that may have stopped polling. Already-parked requests are in-flight and
// may still be responded during drain; unresponded parks close at the force deadline.
#[allow(clippy::too_many_arguments)]
pub(crate) fn park_request(
    conn: &mut ConnState,
    head: veylor_http1::RequestHead,
    route_id: u64,
    worker_id: u32,
    host: &HostShared,
    draining: bool,
    max_body_bytes: u64,
    trailers: &[Vec<u8>],
) -> ExchangeOutcome {
    if draining {
        return ExchangeOutcome::Close(ConnClose::Shutdown);
    }
    let len = conn.len;
    let input = &conn.input[..len];
    let method_len = head.method(input).map(|method| method.len()).unwrap_or(0);
    let target_len = head.target(input).map(|target| target.len()).unwrap_or(0);
    if method_len > HOST_MAX_METHOD_BYTES || method_len == 0 || target_len == 0 {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    }
    // NOTE: fixed-length bodies at or under the route cap park with admission; over-cap
    // declarations reject with 413 and no interim, mirroring the static v1 rule. Chunked parks
    // with incremental admission (no 413: there is no declared length to judge) and decodes
    // across pumps into staging under the same cap; the head parser already accepted the final
    // chunked coding, so no Transfer-Encoding syntax is re-checked here.
    // NOTE: 413 stays a declared-length verdict only: a chunked aggregate that breaches the
    // route cap mid-stream fails the connection closed instead, since no length was declared.
    let chunked = head.is_chunked;
    let body_total = if chunked {
        u64::MAX
    } else {
        head.content_length.unwrap_or(0)
    };
    if !chunked && body_total > max_body_bytes {
        return reject_managed_body(conn, head);
    }
    // NOTE: a zero-cap route admits no body bytes at all: chunked has no declared length to
    // judge, so it rejects up front like an over-cap declaration instead of parking.
    if chunked && max_body_bytes == 0 {
        return reject_managed_body(conn, head);
    }
    let connection =
        connection_header_for_response(head, input, 200, ResponseFraming::FixedLength, true, false);
    let keep_open = connection != ConnectionHeader::Close;
    let token = conn.token;
    let mut snapshot = [0u8; HOST_MAX_METHOD_BYTES];
    snapshot[..method_len]
        .copy_from_slice(&input[head.method.offset..head.method.offset + method_len]);
    let target_start = head.target.offset;
    let event_token = match host.try_park(
        worker_id,
        token,
        route_id,
        &snapshot[..method_len],
        &conn.input[target_start..target_start + target_len],
        body_total,
    ) {
        Ok(event_token) => event_token,
        Err(ParkError::Full) => return ExchangeOutcome::Close(ConnClose::QueueFull),
        Err(ParkError::SnapshotTooLarge) => {
            return ExchangeOutcome::Close(ConnClose::TransportError);
        }
    };
    // NOTE: the interim stages before any staged body byte so Expect always sees 100 first;
    // it flushes on the next parked turn ahead of further reads, exactly once per park.
    // A chunked park also arms its worker-side incremental decoder now; buffered framing bytes
    // decode on the following parked turns, never staged raw.
    if head.expect_continue {
        match encode_continue(&mut conn.output[..]) {
            Ok(interim_len) => {
                conn.out_len = interim_len;
                conn.out_sent = 0;
            }
            Err(_) => {
                host.unpark_closed(token);
                return ExchangeOutcome::Close(ConnClose::ResponseOverflow);
            }
        }
    } else {
        conn.out_len = 0;
        conn.out_sent = 0;
    }
    conn.write_blocked = false;
    conn.pending_len = 0;
    conn.trailer_allow = trailers.to_vec();
    conn.trailer_conn = connection_value(&conn.headers[..], head, &conn.input[..len]).to_vec();
    conn.trailer_slots = [Header::default(); TRAILER_SLOTS];
    conn.trailer_staged = 0;
    let buffered = (conn.len - conn.head_len) as u64;
    let take = if chunked {
        0
    } else {
        buffered.min(body_total) as usize
    };
    let mut body_received = 0u64;
    if take > 0 {
        if !host.push_body(
            event_token,
            &conn.input[conn.head_len..conn.head_len + take],
        ) {
            host.unpark_closed(token);
            return ExchangeOutcome::Close(ConnClose::TransportError);
        }
        body_received = take as u64;
    }
    conn.input.copy_within(conn.head_len + take..conn.len, 0);
    conn.len -= conn.head_len + take;
    conn.head_len = 0;
    if chunked {
        conn.chunked_decoder = Some(BodyDecoder::with_limits(
            ResponseBody::Chunked,
            BodyLimits {
                total_body_bytes: max_body_bytes,
                ..BodyLimits::default()
            },
        ));
    }
    conn.phase = ConnPhase::Parked {
        route_id,
        method_len: method_len as u8,
        method: snapshot,
        keep_open,
        body_total,
        body_received,
        event_token,
        chunked,
        chunked_done: false,
    };
    ExchangeOutcome::Parked
}

fn reject_managed_body(conn: &mut ConnState, head: veylor_http1::RequestHead) -> ExchangeOutcome {
    let len = conn.len;
    let method = head.method(&conn.input[..len]).unwrap_or_default();
    match respond_payload_too_large(&mut conn.output[..], method) {
        Ok(length) => {
            conn.out_len = length;
            conn.out_sent = 0;
            conn.write_blocked = false;
            conn.pending_len = 0;
            conn.closing = true;
            conn.close_reason = ConnClose::Completed;
            conn.phase = ConnPhase::SendingResponse;
            ExchangeOutcome::Responded
        }
        Err(_) => ExchangeOutcome::Close(ConnClose::ResponseOverflow),
    }
}

impl ParkedEntry {
    pub(crate) fn new(token: ConnectionToken, body_total: u64) -> Self {
        Self {
            token,
            staged: None,
            body_total,
            body: Vec::new(),
            body_pulled: 0,
            trailers: None,
        }
    }
}

pub(crate) fn push_staged_body(
    inner: &mut HostInner,
    token: ConnectionToken,
    bytes: &[u8],
) -> bool {
    let Some(entry) = inner
        .parked
        .get_mut(token.worker_id as usize)
        .and_then(|table| table.get_mut(token.slot as usize))
        .and_then(|entry| entry.as_mut())
    else {
        return false;
    };
    if entry.token != token || entry.staged.is_some() {
        return false;
    }
    if entry.body_pulled + entry.body.len() as u64 + bytes.len() as u64 > entry.body_total {
        return false;
    }
    entry.body.extend_from_slice(bytes);
    true
}

pub(crate) fn pull_staged_body(
    inner: &mut HostInner,
    token: ConnectionToken,
    out: &mut [u8],
) -> (RespondOutcome, usize, bool) {
    let live = inner
        .parked
        .get(token.worker_id as usize)
        .and_then(|table| table.get(token.slot as usize))
        .and_then(|entry| entry.as_ref())
        .is_some_and(|entry| entry.token == token && entry.staged.is_none());
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

// NOTE: chunked completion reuses the declared-total observation with no new pull flag: a
// chunked park starts unknown (body_total u64::MAX, so pulls never report complete early)
// and the terminal 0-chunk collapses the total onto the exact staged byte count, making
// pulled == total true exactly when the host drains the last decoded byte. A separate
// chunked_done triple flag would fork every BodyPull consumer for zero new information,
// since "no more bytes will arrive" already is the predicate; the phase-side chunked_done
// flag exists only because complete_parked cannot query the entry.
pub(crate) struct ChunkedFeed {
    pub(crate) consumed: usize,
    pub(crate) decoded: u64,
    pub(crate) done: bool,
    pub(crate) failed: bool,
}

impl HostShared {
    pub(crate) fn mark_chunked_done(&self, token: ConnectionToken) -> bool {
        let marked = self.with_inner(|inner| {
            let done = inner
                .parked
                .get_mut(token.worker_id as usize)
                .and_then(|table| table.get_mut(token.slot as usize))
                .and_then(|entry| entry.as_mut())
                .is_some_and(|entry| {
                    if entry.token != token {
                        return false;
                    }
                    entry.body_total = entry.body_pulled + entry.body.len() as u64;
                    entry.trailers.get_or_insert_with(Vec::new);
                    true
                });
            (done, None)
        });
        marked.is_some_and(|(done, _)| done)
    }

    pub(crate) fn stage_trailer_field(
        &self,
        token: ConnectionToken,
        name: &[u8],
        value: &[u8],
    ) -> bool {
        let staged = self.with_inner(|inner| {
            let entry = inner
                .parked
                .get_mut(token.worker_id as usize)
                .and_then(|table| table.get_mut(token.slot as usize))
                .and_then(|entry| entry.as_mut());
            let Some(entry) = entry else {
                return (false, None);
            };
            if entry.token != token || entry.staged.is_some() {
                return (false, None);
            }
            if entry
                .trailers
                .as_ref()
                .is_some_and(|fields| fields.len() >= TRAILER_SLOTS)
            {
                return (false, None);
            }
            let mut total = 0usize;
            for field in entry.trailers.iter().flatten() {
                total += field.name.len() + field.value.len();
            }
            if total + name.len() + value.len() > MAX_STAGED_TRAILER_BYTES {
                return (false, None);
            }
            entry
                .trailers
                .get_or_insert_with(Vec::new)
                .push(StagedTrailer {
                    name: name.to_vec(),
                    value: value.to_vec(),
                });
            (true, None)
        });
        staged.is_some_and(|(done, _)| done)
    }
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
        pulled.map(|(report, _)| report).unwrap_or_default()
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

fn stage_new_trailers(
    conn: &mut ConnState,
    host: &HostShared,
    event_token: ConnectionToken,
    base: usize,
    end: usize,
) -> bool {
    while conn.trailer_staged < TRAILER_SLOTS {
        let staged = conn.trailer_staged;
        let header = conn.trailer_slots[staged];
        if header.name.length == 0 {
            break;
        }
        let input = &conn.input[base..end];
        let (Some(name), Some(value)) = (field(input, header.name), field(input, header.value))
        else {
            return false;
        };
        if !trailer_allowed(&conn.trailer_allow, &conn.trailer_conn, name) {
            return false;
        }
        if !host.stage_trailer_field(event_token, name, value) {
            return false;
        }
        conn.trailer_staged = staged + 1;
    }
    true
}

// NOTE: the decoder is driven directly, not via the request wrapper: the route allowlist copied at
// park gates each stored field alongside Connection-nominated and request-prohibited names, while
// staged bytes copy into the host entry for whole-set pulls after end-of-body. Unlisted or refused
// fields fail closed here; prohibited names and quota breaches also fail inside the decoder verdicts,
// and chunk extensions are consumed and ignored within the extension quota by the same verdicts. No
// framing syntax is re-implemented: size lines, CRLF strictness, and overflow checks are the http1
// layer's.
// NOTE: the decoder lives worker-side because BodyDecoder is !Send on this toolchain (raw
// trailer-base pointer) while HostInner crosses threads; ConnState never leaves its worker,
// matching the existing parser/headers placement. The slots buffer lives in ConnState so the base
// pointer stays stable across turns; spans never escape the turn that stored them because each new
// field copies out to host staging before any compaction runs.
fn decode_chunked(
    conn: &mut ConnState,
    host: &HostShared,
    event_token: ConnectionToken,
) -> ChunkedFeed {
    let mut feed = ChunkedFeed {
        consumed: 0,
        decoded: 0,
        done: false,
        failed: false,
    };
    loop {
        let Some(decoder) = conn.chunked_decoder.as_mut() else {
            feed.failed = true;
            break;
        };
        let base = feed.consumed;
        let end = conn.len;
        let next = match decoder.next(
            &conn.input[base..end],
            false,
            &mut conn.trailer_slots,
            Limits::default(),
        ) {
            Ok(next) => next,
            Err(_) => {
                feed.failed = true;
                break;
            }
        };
        if !stage_new_trailers(conn, host, event_token, base, end) {
            feed.failed = true;
            break;
        }
        match next.event {
            Some(BodyEvent::Data(span)) => {
                let start = base + span.offset;
                if !host.push_body(event_token, &conn.input[start..start + span.length]) {
                    break;
                }
                feed.decoded += span.length as u64;
            }
            Some(BodyEvent::Trailers { count }) => {
                if count != conn.trailer_staged || !host.mark_chunked_done(event_token) {
                    feed.failed = true;
                    break;
                }
                feed.done = true;
            }
            Some(BodyEvent::Complete) => {}
            None => {}
        }
        feed.consumed += next.consumed;
        if feed.done || next.event.is_none() && next.consumed == 0 {
            break;
        }
    }
    feed
}
