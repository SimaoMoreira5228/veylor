use veylor_http1::{
    BodyDecoder, BodyLimits, ConnectionHeader, RequestVersion, ResponseBody, ResponseFraming,
    connection_header_for_response,
};

use crate::host::{HOST_MAX_METHOD_BYTES, HostShared, ParkError};
use crate::static_routes::{encode_continue, respond_payload_too_large};

use super::streams::BodyPushOutcome;
use super::trailers::connection_value;
use super::{ConnClose, ConnPhase, ConnState, ExchangeOutcome};

// NOTE: drain stops new parks: a head that arrives mid-drain closes instead of waiting
// on a host that may have stopped polling. Already-parked requests are in-flight and
// may still be responded during drain; unresponded parks close at the force deadline.
#[allow(clippy::too_many_arguments)]
pub(crate) fn park_request(
    conn: &mut ConnState,
    head: veylor_http1::RequestHead,
    route_id: u64,
    route_token: u64,
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
    let Some(input) = conn.input.as_ref() else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    let input = &input[..len];
    let Some(headers) = conn.headers.as_ref() else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    let Some(headers) = headers.get(..head.header_count) else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    let method_len = head.method(input).map(|method| method.len()).unwrap_or(0);
    let Some(request_version) = head.protocol_version(input) else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
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
    let connection: ConnectionHeader =
        connection_header_for_response(head, input, 200, ResponseFraming::FixedLength, true, false);
    // NOTE: shared keep-alive verdict (see respond.rs keep_open NOTE): the park stores this
    // provisional response-half verdict whole — not a bool — so fixed responses match the
    // static path. HTTP/1.1 streaming uses it too; HTTP/1.0 streaming must close because it
    // uses close-delimited framing. The body half rides body_total/body_received (or
    // chunked_done) through the DrainingBody phase instead of the shared booleans.
    // NOTE: status-driven closes stay path-owned and deliberately split: a static 404/405/416
    // closes (response_close=true at the encode sites) while a managed host answering the
    // same status reuses under this provisional verdict (pinned by
    // managed_date_present_and_shaped_one_shot_stream_and_reuse); the conn crate leaves the
    // same choice to its host via response_close. Only the request-driven half converges here.
    let token = conn.token;
    let mut snapshot = [0u8; HOST_MAX_METHOD_BYTES];
    snapshot[..method_len]
        .copy_from_slice(&input[head.method.offset..head.method.offset + method_len]);
    let target_start = head.target.offset;
    let request_headers = headers.iter().map(|header| {
        let name_end = header.name.offset.checked_add(header.name.length)?;
        let value_end = header.value.offset.checked_add(header.value.length)?;

        Some((
            input.get(header.name.offset..name_end)?,
            input.get(header.value.offset..value_end)?,
        ))
    });
    let event_token = match host.try_park_with_request(
        worker_id,
        token,
        route_id,
        route_token,
        &snapshot[..method_len],
        &input[target_start..target_start + target_len],
        body_total,
        &[],
        None,
        false,
        request_headers,
    ) {
        Ok(event_token) => event_token,
        Err(ParkError::Full) => return ExchangeOutcome::Close(ConnClose::QueueFull),
        Err(ParkError::SnapshotTooLarge) => {
            // NOTE: attribute like the parser (request_target_too_long): the route matched
            // on the stripped path but the staged copy carries the whole target, so an
            // over-long query string lands here, not in the line parser. Answer 414 with
            // Date and close exactly like the static path instead of closing silent;
            // other snapshot overflows (method) keep the silent transport close.
            if target_len > crate::host::HOST_MAX_PATH_BYTES {
                super::exchange::send_uri_too_long(conn, request_version);
                return ExchangeOutcome::Responded;
            }
            return ExchangeOutcome::Close(ConnClose::TransportError);
        }
    };
    // NOTE: the interim stages before any staged body byte so Expect always sees 100 first;
    // it flushes on the next parked turn ahead of further reads, exactly once per park.
    // A chunked park also arms its worker-side incremental decoder now; buffered framing bytes
    // decode on the following parked turns, never staged raw.
    // NOTE: the arming verdict is shared (veylor_http1::admission_needs_interim) but this
    // staging keys on the bare Expect flag, not the body-gated decoder rule: a bodyless Expect
    // still stages the interim here while the decoder admits Ready with no interim. The split
    // stays for this change (unifying it would alter GET-with-Expect wire bytes); the staged
    // shape is the shared continue_interim, only the staging decision and flush timing are local.
    if head.expect_continue {
        ConnState::materialize_output(&mut conn.output);
        let interim = match conn.output.as_mut() {
            Some(output) => encode_continue(&mut output[..]).ok(),
            None => None,
        };
        match interim {
            Some(interim_len) => {
                conn.out_len = interim_len;
                conn.out_sent = 0;
            }
            None => {
                host.unpark_closed(token.worker_id, token.slot);
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
    let Some(headers) = conn.headers.as_ref() else {
        host.unpark_closed(token.worker_id, token.slot);
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    conn.trailer_conn = connection_value(&headers[..], head, input).to_vec();
    conn.trailer_slots = None;
    conn.trailer_staged = 0;
    let buffered = (conn.len - conn.head_len) as u64;
    let take = if chunked {
        0
    } else {
        buffered.min(body_total) as usize
    };
    let mut body_received = 0u64;
    if take > 0 {
        // NOTE: the host may stage its respond between try_park above and this push (same
        // reason the flake hunt chased): Answered means the bytes were never staged, so count
        // them as drain-skipped here and let the normal drain publish the held response.
        // Only a genuinely unusable entry still fails closed.
        match host.push_body_or_answered(event_token, &input[conn.head_len..conn.head_len + take]) {
            BodyPushOutcome::Refused => {
                host.unpark_closed(token.worker_id, token.slot);
                return ExchangeOutcome::Close(ConnClose::TransportError);
            }
            BodyPushOutcome::Pushed | BodyPushOutcome::Answered => {
                body_received = take as u64;
            }
        }
    }
    if let Some(buffer) = conn.input.as_mut() {
        buffer.copy_within(conn.head_len + take..conn.len, 0);
    }
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
        request_version,
        connection,
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
    let Some(input) = conn.input.as_ref() else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    let method = head.method(&input[..len]).unwrap_or_default();
    let request_version = head
        .protocol_version(&input[..len])
        .unwrap_or(RequestVersion::Http11);
    ConnState::materialize_output(&mut conn.output);
    let length = match conn.output.as_mut() {
        Some(output) => respond_payload_too_large(&mut output[..], method, request_version).ok(),
        None => None,
    };
    match length {
        Some(length) => {
            conn.out_len = length;
            conn.out_sent = 0;
            conn.write_blocked = false;
            conn.pending_len = 0;
            conn.closing = true;
            conn.close_reason = ConnClose::Completed;
            conn.close_drain_remaining = if head.is_chunked {
                u64::MAX
            } else {
                head.content_length.unwrap_or(0)
            };
            conn.phase = ConnPhase::SendingResponse;
            ExchangeOutcome::Responded
        }
        None => ExchangeOutcome::Close(ConnClose::ResponseOverflow),
    }
}
