use std::borrow::Cow;

use veylor_http2::ErrorCode;

use crate::host::HOST_MAX_REASON_BYTES;
use crate::static_routes::{
    CONTENT_ENCODING_GZIP, MAX_NATIVE_HEADERS, MAX_STATIC_BODY_BYTES, NativeHandler,
    NativeRequestView, compress_route_body, engine_owned_response_header, raw_bytes,
    valid_field_value,
};

use super::Serve;
use crate::h2::send::send_full;

impl<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> Serve<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> {
    // NOTE: H2 native dispatch with the H1 invocation contract: same match, same
    // native-first precedence, same guard/panic/fail-closed semantics via the shared
    // invoke. H1 mappings translate per stream (the conn multiplexes on): overflow and
    // handler failure RST InternalError where H1 closes response-overflow, panic RSTs
    // where H1 closes protocol-error, never partial DATA since HEADERS emit only after
    // a successful invoke. Every reason validates but never encodes: H2 has no reason
    // phrase. A 204 serves :status-only HEADERS with END_STREAM through send_full: RFC 9113
    // forbids content-length on 204/304 (status_sheds_length) while every other status here
    // carries it, so the shared sender sheds the length and any handler body there (H1 drops
    // 204 bodies the same way); the
    // static projection still rejects 204 outright, so only handler/managed 204s land here.
    // NOTE: request bytes live in HPACK-decoded buffers and DATA has not arrived at
    // HEADERS time, so every invoke is framing-only (null body, has_body/body_len
    // intact). Tails RST rather than drain: sync routes hold no streaming state, so an
    // open stream answers HEADERS-open then refuses exactly like a static body refusal.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn serve_native(
        &mut self,
        stream: u32,
        view: NativeRequestView,
        end: bool,
        is_head: bool,
        handler: NativeHandler,
        user_data: usize,
        gzip: bool,
        encode: bool,
    ) {
        let declared = view.body_len;
        if declared > MAX_STATIC_BODY_BYTES {
            return self.deny(stream, ErrorCode::RefusedStream, false);
        }
        if end && declared > 0 {
            return self.deny(stream, ErrorCode::ProtocolError, false);
        }
        let mut reason_scratch = [0u8; HOST_MAX_REASON_BYTES];
        let invoked = match view.invoke(handler, user_data, &mut reason_scratch) {
            Ok(invoked) => invoked,
            Err(_) => return self.deny(stream, ErrorCode::InternalError, false),
        };
        let mut extra = [(&b""[..], &b""[..]); MAX_NATIVE_HEADERS];
        let mut extra_count = 0usize;
        for header in invoked.headers() {
            let (Some(name), Some(value)) = (
                raw_bytes(header.name, header.name_len),
                raw_bytes(header.value, header.value_len),
            ) else {
                return self.deny(stream, ErrorCode::InternalError, false);
            };
            if engine_owned_response_header(name) {
                continue;
            }
            if extra_count == MAX_NATIVE_HEADERS
                || !name.iter().all(|byte| veylor_http1::is_token(*byte))
                || !valid_field_value(value)
            {
                return self.deny(stream, ErrorCode::InternalError, false);
            }
            extra[extra_count] = (name, value);
            extra_count += 1;
        }
        let extra = &extra[..extra_count];
        let body = invoked.body();
        let twin = if encode {
            match compress_route_body(body) {
                Ok(twin) => twin,
                Err(_) => return self.fail(ErrorCode::InternalError),
            }
        } else {
            None
        };
        let (body, content_encoding, vary) = match twin {
            Some(ref twin) => (twin.as_slice(), Some(CONTENT_ENCODING_GZIP), true),
            None => (body, None, gzip),
        };
        if !end {
            let refused = send_full(
                self.server,
                stream,
                invoked.status,
                Cow::Borrowed(body),
                content_encoding,
                vary,
                extra,
                false,
                false,
                self.output,
                self.used,
                self.pending,
            );
            if refused.is_err() {
                return self.fail(ErrorCode::InternalError);
            }
            return self.deny(stream, ErrorCode::RefusedStream, true);
        }
        match send_full(
            self.server,
            stream,
            invoked.status,
            Cow::Borrowed(body),
            content_encoding,
            vary,
            extra,
            is_head || body.is_empty(),
            true,
            self.output,
            self.used,
            self.pending,
        ) {
            Ok(complete) => {
                self.outcome.served += u16::from(complete);
                self.outcome.pending |= !complete;
            }
            Err(ErrorCode::RefusedStream) => {
                self.deny(stream, ErrorCode::RefusedStream, false);
            }
            Err(code) => self.fail(code),
        }
    }
}
