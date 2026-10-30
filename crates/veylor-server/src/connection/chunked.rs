use veylor_http1::{BodyEvent, Header, Limits};
use veylor_runtime::ConnectionToken;

use crate::host::HostShared;

use super::ConnState;
use super::parked::{MAX_STAGED_TRAILER_BYTES, TRAILER_SLOTS};
use super::trailers::{StagedTrailer, field, trailer_allowed};

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

fn stage_new_trailers(
    conn: &mut ConnState,
    host: &HostShared,
    event_token: ConnectionToken,
    base: usize,
    end: usize,
) -> bool {
    while conn.trailer_staged < TRAILER_SLOTS {
        let staged = conn.trailer_staged;
        let header = conn.trailer_slots()[staged];
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
    if conn.trailer_slots.is_none() {
        conn.trailer_slots = Some(Box::new([Header::default(); TRAILER_SLOTS]));
    }
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
        let Some(trailer_slots) = conn.trailer_slots.as_mut() else {
            feed.failed = true;
            break;
        };
        let next = match decoder.next(
            &input[base..end],
            false,
            &mut **trailer_slots,
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
