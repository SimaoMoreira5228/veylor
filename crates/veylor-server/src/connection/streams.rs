use veylor_http1::{
    ConnectionHeader, Field, RequestVersion, ResponseFraming, ResponseToWrite,
    encode_response_chunk, encode_response_chunk_with_required_length,
    encode_response_final_chunk_with_required_length, encode_response_head,
    prohibited_request_trailer, response_permits_reuse,
};
use veylor_runtime::ConnectionToken;

use crate::host::{
    HOST_MAX_REASON_BYTES, HostInner, HostShared, MAX_STAGED_RESPONSE_HEADERS, ParkedEntry,
    RespondOutcome, StagedAction, TombCause, find_tomb, valid_reason, valid_response_headers,
};

use crate::static_files::HTTP_DATE_BYTES;
use crate::static_routes::now_http_date;

use super::parked::{MAX_STAGED_TRAILER_BYTES, TRAILER_SLOTS};
use super::trailers::StagedTrailer;
use super::{ConnClose, ConnPhase, ConnState, OUTPUT_CAP, PumpOutcome, PumpProgress};

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
    pub(crate) upgraded: bool,
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
    Upgrade {
        response: Vec<u8>,
        echo: bool,
    },
    H2Upgrade {
        protocol: Option<Vec<u8>>,
    },
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
        if matches!(
            cmd,
            StreamCmd::Begin { .. } | StreamCmd::Upgrade { .. } | StreamCmd::H2Upgrade { .. }
        ) {
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
    pub(crate) fn defer_stream_write(&self, token: ConnectionToken, chunk: Vec<u8>) -> bool {
        if chunk.is_empty() || chunk.len() > MAX_STREAM_WRITE {
            return false;
        }

        let Some((queued, waker)) = self.with_inner(|inner| {
            let Some(entry) = stream_entry(inner, token) else {
                return (false, None);
            };
            let open = entry
                .stream
                .as_ref()
                .is_some_and(|stream| stream.head_sent && !stream.upgraded);
            if !open || entry.stream_cmd.is_some() || entry.staged.is_some() {
                return (false, None);
            }

            entry.stream_cmd = Some(StreamCmd::Write { chunk });
            let waker = inner
                .wakers
                .get(token.worker_id as usize)
                .and_then(Clone::clone);
            (true, waker)
        }) else {
            return false;
        };

        if queued {
            self.note_staged(token.worker_id as usize);
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
        }

        queued
    }

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
                    let Some(stream) = entry.stream.as_ref() else {
                        return false;
                    };
                    if entry.token != token || entry.staged.is_some() {
                        return false;
                    }
                    if stream.upgraded {
                        return !entry.tunnel_close
                            && entry.tunnel_out.len() - entry.tunnel_out_start
                                < crate::host::HOST_TUNNEL_CAP;
                    }
                    stream.head_sent
                        && stream.writable
                        && !stream.ending
                        && entry.stream_cmd.is_none()
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
                    let open = entry
                        .stream
                        .as_ref()
                        .is_some_and(|stream| stream.head_sent && !stream.upgraded);
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
                    let open = entry
                        .stream
                        .as_ref()
                        .is_some_and(|stream| stream.head_sent && !stream.upgraded);
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
                StreamCmd::H2Upgrade { .. } | StreamCmd::Upgrade { .. } => {
                    if entry.stream.is_some() || entry.staged.is_some() {
                        inner.counters.responds_duplicate += 1;
                        return (RespondOutcome::Duplicate, None);
                    }
                    entry.stream = Some(StreamState {
                        upgraded: true,
                        ..StreamState::default()
                    });
                    entry.stream_cmd = Some(cmd);
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
            self.wake.notify_all();
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
    let ConnPhase::Parked {
        connection,
        request_version,
        ..
    } = conn.phase
    else {
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
            // NOTE: HTTP/1.0 streams use close-delimited framing; HTTP/1.1 keeps the parked
            // connection verdict and uses chunked framing.
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
                request_version,
                requested_upgrade: b"",
                status,
                reason: &reason,
                fields: &fields[..field_count],
                framing: match request_version {
                    RequestVersion::Http10 => ResponseFraming::CloseDelimited,
                    RequestVersion::Http11 => ResponseFraming::Chunked,
                },
                content_length: 0,
                connection: match request_version {
                    RequestVersion::Http10 => ConnectionHeader::Close,
                    RequestVersion::Http11 => connection,
                },
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
            match request_version {
                RequestVersion::Http10 if chunk.len() <= output.len() => {
                    output[..chunk.len()].copy_from_slice(&chunk);
                    chunk.len()
                }
                RequestVersion::Http10 => {
                    host.unpark_closed(token.worker_id, token.slot);
                    return PumpOutcome::Close(ConnClose::ResponseOverflow);
                }
                RequestVersion::Http11 => match encode_response_chunk(&chunk, &mut output[..]) {
                    Ok(length) => length,
                    Err(_) => {
                        host.unpark_closed(token.worker_id, token.slot);
                        return PumpOutcome::Close(ConnClose::TransportError);
                    }
                },
            }
        }
        StreamCmd::End | StreamCmd::EndWithTrailers { .. } => {
            if request_version == RequestVersion::Http10 {
                if matches!(cmd, StreamCmd::EndWithTrailers { .. }) {
                    host.unpark_closed(token.worker_id, token.slot);
                    return PumpOutcome::Close(ConnClose::ProtocolError);
                }
                conn.pending_len = 0;
                conn.closing = true;
                conn.close_reason = ConnClose::Completed;
                conn.phase = ConnPhase::SendingResponse;
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Keep(progress);
            }
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
            conn.closing = !response_permits_reuse(false, connection);
            conn.close_reason = ConnClose::Completed;
            conn.phase = ConnPhase::SendingResponse;
            host.unpark_closed(token.worker_id, token.slot);
            length
        }
        StreamCmd::H2Upgrade { .. } => {
            return PumpOutcome::Close(ConnClose::ProtocolError);
        }
        StreamCmd::Upgrade { response, echo } => {
            let ConnPhase::Parked {
                request_version,
                body_total,
                body_received,
                chunked,
                chunked_done,
                ..
            } = conn.phase
            else {
                return PumpOutcome::Close(ConnClose::TransportError);
            };
            if request_version != RequestVersion::Http11 {
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Close(ConnClose::ProtocolError);
            }
            let body_complete = if chunked {
                chunked_done
            } else {
                body_total == body_received
            };
            if !body_complete || response.len() > OUTPUT_CAP {
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Close(ConnClose::ProtocolError);
            }
            let Some(output) = conn.output.as_mut() else {
                host.unpark_closed(token.worker_id, token.slot);
                return PumpOutcome::Close(ConnClose::TransportError);
            };
            output[..response.len()].copy_from_slice(&response);
            conn.out_len = response.len();
            conn.out_sent = 0;
            conn.write_blocked = false;
            conn.closing = false;
            conn.tunnel_token = token;
            conn.tunnel_eof = false;
            conn.tunnel_echo = echo
                .then(|| veylor_websocket::WebSocket::server(veylor_websocket::Limits::default()));
            conn.phase = ConnPhase::Tunnel;
            progress.tunnel_upgraded = true;
            return PumpOutcome::Keep(progress);
        }
        StreamCmd::Cancel => return PumpOutcome::Close(ConnClose::Cancelled),
    };
    conn.out_len = length;
    conn.out_sent = 0;
    conn.write_blocked = false;
    PumpOutcome::Keep(progress)
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
