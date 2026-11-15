use veylor_http1::{
    Field, ResponseFraming, ResponseToWrite, encode_response_head, response_body_allowed_for,
    response_permits_reuse,
};
use veylor_runtime::{CloseReason, IoBatch, ReadStatus, WriteStatus, dispatch};

use crate::host::{HostShared, MAX_STAGED_RESPONSE_HEADERS};
use crate::static_files::HTTP_DATE_BYTES;
use crate::static_routes::now_http_date;

use super::chunked::decode_chunked;
use super::streams::{apply_stream_cmd, take_stream_cmd};
use super::tls::{tls_encrypt_output, tls_flush_pending, tls_generate_flight, tls_read_available};
use super::{ConnClose, ConnPhase, ConnState, INPUT_CAP, PumpOutcome, PumpProgress};

pub(crate) use super::streams::{BodyPushOutcome, StreamCmd, StreamState, push_staged_body};

// NOTE: request-trailer staging bounds: at most TRAILER_SLOTS fields and MAX_STAGED_TRAILER_BYTES
// of staged name/value bytes per park; the first field past either cap fails the connection closed.
// NOTE: these bound host staging only and are intentionally stricter than the decoder's wider
// BodyLimits (32 fields, 8 KiB): the staged layer fails first, so the values are not unified
// with the decoder (narrowing it would change wire behavior).
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
        headers: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), ConnClose> {
        let ConnPhase::Parked {
            method_len,
            method,
            request_version,
            connection,
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
        // NOTE: shed staged bodies the shared gate forbids (same rule as the native path):
        // without this a host-staged 204/304/205 body fails encoder validation and closes
        // the connection instead of answering cleanly. HEAD keeps the would-be length.
        let (body, content_length) =
            if response_body_allowed_for(request_method, status) || request_method == b"HEAD" {
                (body, body.len() as u64)
            } else {
                (&b""[..], 0)
            };
        // NOTE: H2 managed Date is the recorded follow-up (h2/serve.rs Begin/send_full), not this slice.
        let mut now = [0u8; HTTP_DATE_BYTES];
        let now_len = now_http_date(&mut now);
        let mut fields = [Field {
            name: b"",
            value: b"",
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
            request_method,
            request_version,
            requested_upgrade: b"",
            status,
            reason,
            fields: &fields[..field_count],
            framing: ResponseFraming::FixedLength,
            content_length,
            connection,
        };
        ConnState::materialize_output(&mut self.output);
        let Some(output) = self.output.as_mut() else {
            return Err(ConnClose::TransportError);
        };
        let encoded = encode_response_head(&response, &mut output[..])
            .map_err(|_| ConnClose::TransportError)?;
        let mut length = encoded.length;
        if encoded.body_allowed && !body.is_empty() {
            let end = length + body.len();
            output
                .get_mut(length..end)
                .ok_or(ConnClose::ResponseOverflow)?
                .copy_from_slice(body);
            length = end;
        }
        self.out_sent = 0;
        self.pending_len = 0;
        self.write_blocked = false;
        // NOTE: closing follows the shared verdict (response_permits_reuse), which the park
        // stored whole: an HTTP/1.0 keep-alive request echoes Connection: keep-alive above
        // instead of a bool-erased Omit, matching the static path and the conn crate.
        self.closing = !response_permits_reuse(false, connection);
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
    if conn.out_sent < conn.out_len || conn.tls_unflushed() > 0 {
        if conn.has_tls() {
            if conn.out_sent < conn.out_len
                && let Err(reason) = tls_encrypt_output(conn)
            {
                return PumpOutcome::Close(reason);
            }

            if let Err(reason) = tls_generate_flight(conn) {
                return PumpOutcome::Close(reason);
            }

            match tls_flush_pending(conn, u64::MAX) {
                Ok(wrote) => {
                    progress.bytes_written = wrote;

                    return PumpOutcome::Keep(progress);
                }
                Err(reason) => return PumpOutcome::Close(reason),
            }
        }

        let out_sent = conn.out_sent;
        let out_len = conn.out_len;
        let write_ready = !conn.write_blocked;
        let Some(output) = conn.output.as_ref() else {
            conn.out_sent = conn.out_len;
            return PumpOutcome::Keep(progress);
        };
        let mut batch = [IoBatch::new(
            token,
            &mut conn.stream,
            &mut [],
            false,
            &output[out_sent..out_len],
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
    let (staged, _) = host.with_inner(|inner| (take_stream_cmd(inner, event_token), None));
    if let Some(cmd) = staged {
        return apply_stream_cmd(conn, host, event_token, cmd, progress);
    }
    // NOTE: stream re-arm emits one coalesced WRITABLE per release. The emission lives here,
    // not in take_stream_cmd: the take holds the queue lock (HostShared::stage_writable would
    // deadlock) and the queue fields are private to host/. The is_writable gate admits only
    // open armed streams, and a full queue leaves the arm set for the next turn to re-signal.
    if host.is_writable(event_token) {
        host.stage_writable(event_token);
    }
    if conn.has_tls() {
        if conn.len < INPUT_CAP {
            ConnState::materialize_input(&mut conn.input);
        }

        match tls_read_available(conn, u64::MAX) {
            Ok((wire, _)) => {
                progress.bytes_read = wire;
            }
            Err(reason) => return PumpOutcome::Close(reason),
        }

        match tls_flush_pending(conn, u64::MAX) {
            Ok(wrote) => {
                progress.bytes_written = wrote;
            }
            Err(reason) => return PumpOutcome::Close(reason),
        }
    } else {
        let read_ready = conn.read_ready;
        if conn.len < INPUT_CAP {
            ConnState::materialize_input(&mut conn.input);
        }
        let len = conn.len;
        let read_scratch: &mut [u8] = match conn.input.as_mut() {
            Some(input) => &mut input[len..],
            None => &mut [],
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
    }
    if chunked {
        if !chunked_done {
            let feed = decode_chunked(conn, host, event_token);
            if feed.failed {
                return PumpOutcome::Close(ConnClose::TransportError);
            }
            if feed.consumed > 0 {
                if let Some(input) = conn.input.as_mut() {
                    input.copy_within(feed.consumed..conn.len, 0);
                }
                conn.len -= feed.consumed;
            }
            if feed.decoded > 0
                && let ConnPhase::Parked { body_received, .. } = &mut conn.phase
            {
                *body_received += feed.decoded;
            }
            if feed.done
                && let ConnPhase::Parked { chunked_done, .. } = &mut conn.phase
            {
                *chunked_done = true;
            }
        }
        return PumpOutcome::Keep(progress);
    }
    let take = (conn.len as u64).min(body_total.saturating_sub(body_received)) as usize;
    let staged = take > 0
        && match conn.input.as_ref() {
            Some(input) => host.push_body(event_token, &input[..take]),
            None => false,
        };
    if staged {
        if let Some(input) = conn.input.as_mut() {
            input.copy_within(take..conn.len, 0);
        }
        conn.len -= take;
        if let ConnPhase::Parked { body_received, .. } = &mut conn.phase {
            *body_received += take as u64;
        }
    }
    PumpOutcome::Keep(progress)
}
