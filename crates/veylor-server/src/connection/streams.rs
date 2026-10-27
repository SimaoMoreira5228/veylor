use veylor_http1::{
    BodyEvent, ConnectionHeader, Field, Limits, ResponseFraming, ResponseToWrite,
    encode_response_chunk, encode_response_chunk_with_required_length,
    encode_response_final_chunk_with_required_length, encode_response_head,
    prohibited_request_trailer,
};
use veylor_runtime::ConnectionToken;

use crate::host::{
    HOST_MAX_REASON_BYTES, HostInner, HostShared, MAX_STAGED_RESPONSE_HEADERS, ParkedEntry,
    RespondOutcome, StagedAction, TombCause, find_tomb, valid_reason, valid_response_headers,
};

use crate::static_files::HTTP_DATE_BYTES;
use crate::static_routes::now_http_date;

use super::parked::{MAX_STAGED_TRAILER_BYTES, TRAILER_SLOTS};
use super::{
    ConnClose, ConnPhase, ConnState, OUTPUT_CAP, PumpOutcome, PumpProgress, StagedTrailer, field,
    trailer_allowed,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BodyPushOutcome {
    Pushed,
    Answered,
    Refused,
}

pub(crate) fn push_staged_body(
    inner: &mut HostInner,
    token: ConnectionToken,
    bytes: &[u8],
) -> bool {
    matches!(
        push_body_or_answered(inner, token, bytes),
        BodyPushOutcome::Pushed
    )
}

// NOTE: park-time body bytes race the host's staged respond: try_park queues the event, the
// host can answer before the worker pushes the already-read bytes, and killing the connection
// for that ordering loses a healthy exchange. Answering first is legitimate, so an entry that
// already holds a staged decision reports Answered (the worker drains instead of dying);
// anything else absent or unusable reports Refused and the caller fails closed.
pub(crate) fn push_body_or_answered(
    inner: &mut HostInner,
    token: ConnectionToken,
    bytes: &[u8],
) -> BodyPushOutcome {
    let Some(entry) = inner
        .parked
        .get_mut(token.worker_id as usize)
        .and_then(|table| table.get_mut(token.slot as usize))
        .and_then(|entry| entry.as_mut())
    else {
        return BodyPushOutcome::Refused;
    };
    if entry.token != token {
        return BodyPushOutcome::Refused;
    }
    if entry.staged.is_some() {
        return BodyPushOutcome::Answered;
    }
    if entry.body_pulled + entry.body.len() as u64 + bytes.len() as u64 > entry.body_total {
        return BodyPushOutcome::Refused;
    }
    entry.body.extend_from_slice(bytes);
    BodyPushOutcome::Pushed
}

// NOTE: stream writes reuse the staged channel with pump-pull delivery: take_staged skips stream
// entries (parked, no tomb); pump_parked pulls one stream command per turn once output is fully
// flushed. Pacing is a query (is_writable): begin arms at take, each admitted write consumes,
// flushed turns re-arm. Idle open streams trip the armed WritingResponse deadline and close, so
// an abandoned stream never wedges its slot.
pub const MAX_STREAM_WRITE: usize = 16_384;

#[derive(Clone, Debug, Default)]
pub(crate) struct StreamState {
    pub(crate) head_sent: bool,
    pub(crate) writable: bool,
    pub(crate) ending: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum StreamCmd {
    Begin {
        status: u16,
        reason: Vec<u8>,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Write {
        chunk: Vec<u8>,
    },
    End,
    EndWithTrailers {
        trailers: Vec<StagedTrailer>,
    },
    Cancel,
}

fn stream_entry(inner: &mut HostInner, token: ConnectionToken) -> Option<&mut ParkedEntry> {
    inner
        .parked
        .get_mut(token.worker_id as usize)
        .and_then(|table| table.get_mut(token.slot as usize))
        .and_then(|entry| entry.as_mut())
        .filter(|entry| entry.token == token && entry.stream.is_some())
}

pub(crate) fn take_stream_cmd(inner: &mut HostInner, token: ConnectionToken) -> Option<StreamCmd> {
    let entry = stream_entry(inner, token)?;
    if let Some(cmd) = entry.stream_cmd.take() {
        if matches!(cmd, StreamCmd::Begin { .. }) {
            if let Some(stream) = entry.stream.as_mut() {
                stream.head_sent = true;
                stream.writable = true;
            }
        }
        return Some(cmd);
    }
    if matches!(entry.staged, Some(StagedAction::Cancel)) {
        entry.staged.take();
        return Some(StreamCmd::Cancel);
    }
    // NOTE: no queued command on an open stream means the last flush completed: re-arm.
    if entry.stream.as_ref().is_some_and(|stream| stream.head_sent) {
        if let Some(stream) = entry.stream.as_mut() {
            stream.writable = true;
        }
    }
    None
}

impl HostShared {
    pub fn begin_stream(
        &self,
        token: ConnectionToken,
        status: u16,
        reason: &[u8],
    ) -> RespondOutcome {
        self.begin_stream_with_headers(token, status, reason, &[])
    }

    pub fn begin_stream_with_headers(
        &self,
        token: ConnectionToken,
        status: u16,
        reason: &[u8],
        headers: &[(&[u8], &[u8])],
    ) -> RespondOutcome {
        if !(200..=599).contains(&status) || status == 204 || status == 205 || status == 304 {
            return RespondOutcome::Invalid;
        }
        if reason.len() > HOST_MAX_REASON_BYTES || !valid_reason(reason) {
            return RespondOutcome::Invalid;
        }
        if !valid_response_headers(headers) {
            return RespondOutcome::Invalid;
        }
        self.stage_stream(
            token,
            StreamCmd::Begin {
                status,
                reason: reason.to_vec(),
                headers: headers
                    .iter()
                    .map(|(name, value)| (name.to_vec(), value.to_vec()))
                    .collect(),
            },
            RespondOutcome::Stale,
        )
    }

    pub fn write_stream(&self, token: ConnectionToken, chunk: &[u8]) -> RespondOutcome {
        if chunk.is_empty() {
            return RespondOutcome::Invalid;
        }
        if chunk.len() > MAX_STREAM_WRITE {
            return RespondOutcome::BodyTooLarge;
        }
        // NOTE: the probe rejects chunks whose single frame cannot fit the output buffer; the
        // largest admittable chunk is 8184 bytes (8192 minus hex digits and two CRLFs).
        let mut required = 0usize;
        let _ = encode_response_chunk_with_required_length(chunk, &mut [], &mut required);
        if required > OUTPUT_CAP {
            return RespondOutcome::BodyTooLarge;
        }
        self.stage_stream(
            token,
            StreamCmd::Write {
                chunk: chunk.to_vec(),
            },
            RespondOutcome::NotWritable,
        )
    }

    pub fn end_stream(&self, token: ConnectionToken) -> RespondOutcome {
        self.stage_stream(token, StreamCmd::End, RespondOutcome::Stale)
    }

    // NOTE: response-trailer stage validation is two layers: the request-trailer prohibited
    // set below (RFC 9112 trailers carry no framing/routing/auth metadata; the http1 terminal
    // encoder's own response set omits content-length, so the stage gate must name it), then the
    // http1 terminal encoder as an empty-output probe for name/value syntax, pseudo-headers (`:`
    // is no token char), and its wider response set. A nonzero required length proves the staged
    // set always encodes. Staged caps mirror the request-trailer bounds (TRAILER_SLOTS fields, 1
    // KiB name/value bytes); anything past either cap, or any invalid field, rejects the whole
    // set with nothing staged.
    pub fn end_stream_with_trailers(
        &self,
        token: ConnectionToken,
        fields: &[(&[u8], &[u8])],
    ) -> RespondOutcome {
        if fields.len() > TRAILER_SLOTS {
            return RespondOutcome::Invalid;
        }
        let mut total = 0usize;
        for (name, value) in fields {
            if prohibited_request_trailer(name) {
                return RespondOutcome::Invalid;
            }
            total += name.len() + value.len();
        }
        if total > MAX_STAGED_TRAILER_BYTES {
            return RespondOutcome::Invalid;
        }
        let borrowed: Vec<Field> = fields
            .iter()
            .copied()
            .map(|(name, value)| Field { name, value })
            .collect();
        let mut required = 0usize;
        let _ = encode_response_final_chunk_with_required_length(&borrowed, &mut [], &mut required);
        if required == 0 {
            return RespondOutcome::Invalid;
        }
        if required > OUTPUT_CAP {
            return RespondOutcome::BodyTooLarge;
        }
        let staged = borrowed
            .iter()
            .map(|field| StagedTrailer {
                name: field.name.to_vec(),
                value: field.value.to_vec(),
            })
            .collect();
        self.stage_stream(
            token,
            StreamCmd::EndWithTrailers { trailers: staged },
            RespondOutcome::Stale,
        )
    }

    pub fn is_writable(&self, token: ConnectionToken) -> bool {
        let done = self.with_inner(|inner| {
            let live = inner
                .parked
                .get(token.worker_id as usize)
                .and_then(|table| table.get(token.slot as usize))
                .and_then(|entry| entry.as_ref())
                .is_some_and(|entry| {
                    entry.token == token
                        && entry.staged.is_none()
                        && entry.stream_cmd.is_none()
                        && entry.stream.as_ref().is_some_and(|stream| stream.head_sent)
                        && entry.stream.as_ref().is_some_and(|stream| stream.writable)
                        && entry.stream.as_ref().is_some_and(|stream| !stream.ending)
                });
            (live, None)
        });
        done.is_some_and(|(live, _)| live)
    }

    fn stage_stream(
        &self,
        token: ConnectionToken,
        cmd: StreamCmd,
        missing: RespondOutcome,
    ) -> RespondOutcome {
        let outcome = self.with_inner(|inner| {
            let worker = token.worker_id as usize;
            let slot = token.slot as usize;
            if inner.events.len() <= worker || token.generation == 0 {
                return (RespondOutcome::Invalid, None);
            }
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
                    Some(TombCause::Cancelled) => RespondOutcome::Stale,
                    None => missing,
                };
                match outcome {
                    RespondOutcome::Duplicate => inner.counters.responds_duplicate += 1,
                    RespondOutcome::Gone => inner.counters.responds_gone += 1,
                    RespondOutcome::Stale => inner.counters.responds_stale += 1,
                    _ => {}
                }
                return (outcome, None);
            };
            match cmd {
                StreamCmd::Begin { .. } => {
                    if entry.stream.is_some() || entry.staged.is_some() {
                        inner.counters.responds_duplicate += 1;
                        return (RespondOutcome::Duplicate, None);
                    }
                    entry.stream_cmd = Some(cmd);
                    entry.stream = Some(StreamState::default());
                }
                StreamCmd::Write { .. } => {
                    let open = entry.stream.as_ref().is_some_and(|stream| stream.head_sent);
                    if !open {
                        return (RespondOutcome::NotWritable, None);
                    }
                    if entry.stream_cmd.is_some() || entry.staged.is_some() {
                        return (RespondOutcome::WouldBlock, None);
                    }
                    let armed = entry.stream.as_ref().is_some_and(|stream| stream.writable);
                    let ending = entry.stream.as_ref().is_some_and(|stream| stream.ending);
                    if !armed || ending {
                        return (RespondOutcome::NotWritable, None);
                    }
                    entry.stream_cmd = Some(cmd);
                    if let Some(stream) = entry.stream.as_mut() {
                        stream.writable = false;
                    }
                }
                StreamCmd::End | StreamCmd::EndWithTrailers { .. } => {
                    let open = entry.stream.as_ref().is_some_and(|stream| stream.head_sent);
                    if !open {
                        return (RespondOutcome::Invalid, None);
                    }
                    let ending = entry.stream.as_ref().is_some_and(|stream| stream.ending);
                    if ending || entry.stream_cmd.is_some() || entry.staged.is_some() {
                        inner.counters.responds_duplicate += 1;
                        return (RespondOutcome::Duplicate, None);
                    }
                    entry.stream_cmd = Some(cmd);
                    // NOTE: take→apply runs without the lock, so a command staged in between
                    // must fail here instead of being swallowed with a false Ok at unpark.
                    if let Some(stream) = entry.stream.as_mut() {
                        stream.ending = true;
                    }
                }
                StreamCmd::Cancel => {
                    return (RespondOutcome::Invalid, None);
                }
            }
            inner.counters.responds_ok += 1;
            let waker = inner.wakers.get(worker).and_then(|slot| slot.clone());
            (RespondOutcome::Ok, waker)
        });
        let Some((result, waker)) = outcome else {
            return RespondOutcome::Stale;
        };
        if result == RespondOutcome::Ok {
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
        }
        result
    }
}

// NOTE: one stream command per turn, applied only with output fully flushed. Begin moves the
// worker AwaitingHandler->WritingResponse through entered_response; end unparks (later ops see
// Gone) and hands the final flush to the SendingResponse path for keep-alive reuse. Cancel on
// a stream-open entry closes here: drain_host_staged is frozen and demands AwaitingHandler,
// which begin-apply already left, so it would silently drop the cancel.
pub(crate) fn apply_stream_cmd(
    conn: &mut ConnState,
    host: &HostShared,
    token: ConnectionToken,
    cmd: StreamCmd,
    mut progress: PumpProgress,
) -> PumpOutcome {
    let ConnPhase::Parked { keep_open, .. } = conn.phase else {
        return PumpOutcome::Close(ConnClose::TransportError);
    };
    ConnState::materialize_output(&mut conn.output);
    let length = match cmd {
        StreamCmd::Begin {
            status,
            reason,
            headers,
        } => {
            // NOTE: the entry holds no method snapshot, so HEAD cannot reject at stage; the
            // parked snapshot here fails the connection closed instead.
            let ConnPhase::Parked {
                method_len, method, ..
            } = conn.phase
            else {
                return PumpOutcome::Close(ConnClose::TransportError);
            };
            if method[..usize::from(method_len)].eq_ignore_ascii_case(b"HEAD") {
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Close(ConnClose::TransportError);
            }
            let connection = if keep_open {
                ConnectionHeader::Omit
            } else {
                ConnectionHeader::Close
            };
            let mut now = [0u8; HTTP_DATE_BYTES];
            let now_len = now_http_date(&mut now);
            let mut fields = [Field {
                name: b"",
                value: &[][..],
            }; 1 + MAX_STAGED_RESPONSE_HEADERS];
            fields[0] = Field {
                name: b"Date",
                value: &now[..now_len],
            };
            let mut field_count = 1;
            for (name, value) in headers.iter().take(fields.len() - 1) {
                fields[field_count] = Field {
                    name: name.as_slice(),
                    value: value.as_slice(),
                };
                field_count += 1;
            }
            let response = ResponseToWrite {
                request_method: &method[..usize::from(method_len)],
                requested_upgrade: b"",
                status,
                reason: &reason,
                fields: &fields[..field_count],
                framing: ResponseFraming::Chunked,
                content_length: 0,
                connection,
            };
            let Some(output) = conn.output.as_mut() else {
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Close(ConnClose::TransportError);
            };
            let Ok(encoded) = encode_response_head(&response, &mut output[..]) else {
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Close(ConnClose::TransportError);
            };
            progress.entered_response = true;
            encoded.length
        }
        StreamCmd::Write { chunk } => {
            // NOTE: the stage probe guarantees fit, so BufferTooSmall here is unreachable and
            // permanent: close and tomb Gone instead of wedging the stream.
            let Some(output) = conn.output.as_mut() else {
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Close(ConnClose::TransportError);
            };
            let Ok(length) = encode_response_chunk(&chunk, &mut output[..]) else {
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Close(ConnClose::TransportError);
            };
            length
        }
        StreamCmd::End | StreamCmd::EndWithTrailers { .. } => {
            // NOTE: stage validation plus the staged caps prove the trailer block fits, so the
            // required-length probe here is the no-truncation guarantee, never a fit check: any
            // encode failure is permanent and fails the connection closed like the chunk path.
            let trailers: Vec<Field> = match &cmd {
                StreamCmd::EndWithTrailers { trailers } => trailers
                    .iter()
                    .map(|field| Field {
                        name: &field.name,
                        value: &field.value,
                    })
                    .collect(),
                _ => Vec::new(),
            };
            let mut required = 0usize;
            let Some(output) = conn.output.as_mut() else {
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Close(ConnClose::TransportError);
            };
            let Ok(length) = encode_response_final_chunk_with_required_length(
                &trailers,
                &mut output[..],
                &mut required,
            ) else {
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Close(ConnClose::TransportError);
            };
            conn.pending_len = 0;
            conn.closing = !keep_open;
            conn.close_reason = ConnClose::Completed;
            conn.phase = ConnPhase::SendingResponse;
            host.unpark_closed(token.worker_id, token.slot);
            length
        }
        StreamCmd::Cancel => return PumpOutcome::Close(ConnClose::Cancelled),
    };
    conn.out_len = length;
    conn.out_sent = 0;
    conn.write_blocked = false;
    PumpOutcome::Keep(progress)
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

// NOTE: the request-trailer prohibited set lives in veylor_http1::prohibited_request_trailer
// (decoder policy, shared with the H2 gate below): the response terminal refuses the same
// framing/routing/auth metadata, and the response encoder's own set omits content-length,
// so the stage gate must name it. Staged caps stay local: 8 fields / 1 KiB bound host
// staging and are intentionally stricter than the decoder's wider BodyLimits (32 fields,
// 8 KiB); narrowing the decoder would change wire behavior, so the caps are not unified.

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
    pub fn begin_stream(
        &self,
        token: ConnectionToken,
        status: u16,
        reason: &[u8],
    ) -> RespondOutcome {
        self.shared.begin_stream(token, status, reason)
    }

    pub fn begin_stream_with_headers(
        &self,
        token: ConnectionToken,
        status: u16,
        reason: &[u8],
        headers: &[(&[u8], &[u8])],
    ) -> RespondOutcome {
        self.shared
            .begin_stream_with_headers(token, status, reason, headers)
    }

    pub fn write_stream(&self, token: ConnectionToken, chunk: &[u8]) -> RespondOutcome {
        self.shared.write_stream(token, chunk)
    }

    pub fn end_stream(&self, token: ConnectionToken) -> RespondOutcome {
        self.shared.end_stream(token)
    }

    pub fn end_stream_with_trailers(
        &self,
        token: ConnectionToken,
        fields: &[(&[u8], &[u8])],
    ) -> RespondOutcome {
        self.shared.end_stream_with_trailers(token, fields)
    }

    pub fn is_writable(&self, token: ConnectionToken) -> bool {
        self.shared.is_writable(token)
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
        let Some(input) = conn.input.as_ref() else {
            return false;
        };
        let input = &input[base..end];
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
pub(crate) fn decode_chunked(
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
        let Some(input) = conn.input.as_ref() else {
            feed.failed = true;
            break;
        };
        let next = match decoder.next(
            &input[base..end],
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
                let Some(input) = conn.input.as_ref() else {
                    break;
                };
                if !host.push_body(event_token, &input[start..start + span.length]) {
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

#[cfg(test)]
mod tests {
    // NOTE: drift alarm for the converged trailer policy: the stage path calls the shared
    // veylor_http1 predicate, so this pins the exact 15-name membership plus the staged
    // caps (8 fields / 1 KiB) at the H1 consumer. A deliberate policy edit must update
    // the shared set and this test together.
    use super::{MAX_STAGED_TRAILER_BYTES, TRAILER_SLOTS};
    use veylor_http1::prohibited_request_trailer;

    const PROHIBITED: [&[u8]; 15] = [
        b"authorization",
        b"connection",
        b"content-encoding",
        b"content-length",
        b"content-range",
        b"content-type",
        b"expect",
        b"host",
        b"keep-alive",
        b"proxy-authorization",
        b"proxy-connection",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
    ];

    #[test]
    fn staged_request_trailer_policy_matches_decoder_truth() {
        for name in PROHIBITED {
            assert!(prohibited_request_trailer(name), "must refuse {name:?}");
        }
        assert!(!prohibited_request_trailer(b"x-checksum"));
        assert!(!prohibited_request_trailer(b"digest"));
        assert_eq!(TRAILER_SLOTS, 8);
        assert_eq!(MAX_STAGED_TRAILER_BYTES, 1024);
    }
}
