use veylor_http2::{ErrorCode, HeaderRange};

use crate::connection::parked::{MAX_STAGED_TRAILER_BYTES, TRAILER_SLOTS};
use crate::connection::trailers::StagedTrailer;
use crate::connection::trailers::{trailer_allowed, valid_trailer_field};

use super::Serve;
use crate::h2::STREAM_SLOTS;

impl<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> Serve<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> {
    pub(super) fn stage_request_trailers(
        &mut self,
        stream: u32,
        range: HeaderRange,
        end: bool,
    ) -> H2Trailer {
        let Some(ctx) = self.host else {
            return H2Trailer::Conn;
        };
        // NOTE: a trailer block without END_STREAM is malformed framing (RFC 9113 §8.1),
        // never a partial trailer set: the client receive side already rejects the same
        // shape as TrailerNotFinal, so the server RSTs here instead of staging trailers
        // before content-end.
        if !end {
            return H2Trailer::Stream(ErrorCode::ProtocolError);
        }
        let Some(fields) = self.headers.get(range.start..range.start + range.count) else {
            return H2Trailer::Conn;
        };
        let mut staged = Vec::new();
        for header in fields {
            let Some((name, value)) = self.server.driver().resolve_header(*header) else {
                return H2Trailer::Stream(ErrorCode::ProtocolError);
            };
            if staged.len() >= TRAILER_SLOTS || !valid_trailer_field(name, value) {
                return H2Trailer::Stream(ErrorCode::ProtocolError);
            }
            staged.push((name, value));
        }
        let sub = (stream >> 1) as usize % STREAM_SLOTS;
        let slot = ctx.base.slot + sub as u32;
        let worker = ctx.base.worker_id;
        let verdict = ctx.host.with_inner(|inner| {
            let entry = inner
                .parked
                .get_mut(worker as usize)
                .and_then(|table| table.get_mut(slot as usize))
                .and_then(|entry| entry.as_mut());
            let Some(entry) = entry else {
                return (H2Trailer::Conn, None);
            };
            if entry.token.stream_id != stream || entry.token.slot != slot {
                return (H2Trailer::Conn, None);
            }
            if entry.staged.is_some() {
                return (H2Trailer::Stream(ErrorCode::RefusedStream), None);
            }
            // NOTE: allowlist second, after the prohibited+syntax gate above (H1 order): the
            // shared predicate owns listedness, and the empty connection value reads H2
            // framing truth (connection-specific fields never survive accept_request).
            for (name, _) in &staged {
                if !trailer_allowed(&entry.trailer_allow, b"", name) {
                    return (H2Trailer::Stream(ErrorCode::ProtocolError), None);
                }
            }
            let have = entry.trailers.as_ref().map_or(0, Vec::len);
            if have + staged.len() > TRAILER_SLOTS {
                return (H2Trailer::Stream(ErrorCode::ProtocolError), None);
            }
            let mut total = 0usize;
            for field in entry.trailers.iter().flatten() {
                total += field.name.len() + field.value.len();
            }
            for (name, value) in &staged {
                total += name.len() + value.len();
                if total > MAX_STAGED_TRAILER_BYTES {
                    return (H2Trailer::Stream(ErrorCode::ProtocolError), None);
                }
            }
            if end && entry.body_total == u64::MAX {
                entry.body_total = entry.body_pulled + entry.body.len() as u64;
            }
            let fields = entry.trailers.get_or_insert_with(Vec::new);
            for (name, value) in &staged {
                fields.push(StagedTrailer {
                    name: name.to_vec(),
                    value: value.to_vec(),
                });
            }
            if end && entry.body_pulled + entry.body.len() as u64 != entry.body_total {
                return (H2Trailer::Stream(ErrorCode::ProtocolError), None);
            }
            (H2Trailer::Staged, None)
        });
        verdict.0
    }
}

#[derive(Clone, Copy)]
pub(super) enum H2Trailer {
    Staged,
    Stream(ErrorCode),
    Conn,
}

// NOTE: the H2 request-trailer prohibited set is the same 15-name decoder policy as H1
// (veylor_http1::prohibited_request_trailer, byte-identical membership): H2 framing has no
// connection-specific fields (they never survive accept_request), so no H2-only names exist
// and the set is shared, not copied. The connection-specific handling lives in
// trailer_allowed, which H2 calls with an empty connection value for exactly that reason.
// Staged caps (TRAILER_SLOTS / MAX_STAGED_TRAILER_BYTES) are the shared H1 host-staging
// bounds, intentionally stricter than the decoder's wider BodyLimits.

#[cfg(test)]
mod tests {
    // NOTE: drift alarm for the converged trailer policy: the H2 gate shares the H1/decoder
    // 15-name predicate (no H2-only names: connection-specific fields never survive
    // accept_request), so this pins valid_trailer_field against the exact membership plus
    // the shared staged caps at the H2 consumer.
    use super::{MAX_STAGED_TRAILER_BYTES, TRAILER_SLOTS, valid_trailer_field};

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
    fn h2_request_trailer_policy_matches_decoder_truth() {
        for name in PROHIBITED {
            assert!(!valid_trailer_field(name, b"v"), "must refuse {name:?}");
        }
        assert!(valid_trailer_field(b"x-checksum", b"v"));
        assert!(!valid_trailer_field(b"", b"v"));
        assert!(!valid_trailer_field(b":pseudo", b"v"));
        assert_eq!(TRAILER_SLOTS, 8);
        assert_eq!(MAX_STAGED_TRAILER_BYTES, 1024);
    }
}
