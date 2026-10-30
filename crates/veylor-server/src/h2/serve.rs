use veylor_http1::{ALLOW_BYTES, is_token, prohibited_request_trailer};
use veylor_http2::{
    ErrorCode, H2ServerDriver, HeaderRange, HpackHeader, ServerError, ValidatedServerRequest,
};
use veylor_runtime::ConnectionToken;

use crate::connection::parked::{MAX_STAGED_TRAILER_BYTES, TRAILER_SLOTS};
use crate::connection::trailers::StagedTrailer;
use crate::connection::trailers::trailer_allowed;
use crate::host::HOST_MAX_REASON_BYTES;
use crate::static_files::FileReject;
use crate::static_routes::{
    CONTENT_ENCODING_GZIP, MAX_RESPONSE_BODY, MAX_STATIC_BODY_BYTES, NativeHandler,
    NativeRequestView, compress_route_body,
};

use super::file::FileDecision;
use super::routes::{
    FrozenRoutesView, MAX_REQUEST_TARGET_BYTES, NOT_FOUND_BODY, URI_TOO_LONG_BODY,
};
use super::send::{send_full, send_method_not_allowed, send_options};
use super::{
    H2_BODY_CAP, H2FileTransfer, H2HostCtx, STREAM_SLOTS, StepOutcome, StreamEvent, fail_goaway,
};

pub(crate) struct Serve<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> {
    pub(crate) server: &'s mut H2ServerDriver<'b, 't, 'd, 'e>,
    pub(crate) headers: &'s [HpackHeader],
    pub(crate) data: &'u [u8],
    pub(crate) terminal: &'s mut bool,
    pub(crate) routes: &'o FrozenRoutesView<'o>,
    pub(crate) host: Option<H2HostCtx<'h>>,
    pub(crate) output: &'o mut [u8],
    pub(crate) used: &'o mut usize,
    pub(crate) outcome: &'o mut StepOutcome,
    pub(crate) files: &'o mut [Option<H2FileTransfer>; STREAM_SLOTS],
    pub(crate) scratch: &'o mut [u8],
}

impl<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> Serve<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> {
    pub(crate) fn fail(&mut self, code: ErrorCode) {
        let last = self.server.driver().streams().highest_remote();
        let max = self
            .server
            .driver()
            .streams()
            .remote_settings()
            .max_frame_size;
        fail_goaway(
            last,
            max,
            self.terminal,
            self.output,
            self.used,
            self.outcome,
            code,
        );
    }

    pub(crate) fn deny(&mut self, stream: u32, code: ErrorCode, close: bool) {
        let staged = if code == ErrorCode::RefusedStream {
            self.server.refuse(stream, &mut self.output[*self.used..])
        } else {
            self.server
                .rst_stream(stream, code, &mut self.output[*self.used..])
        };
        match staged {
            Ok(writer) => {
                *self.used += writer.pending_bytes().len();
                self.outcome.refused += 1;
            }
            Err(ServerError::StreamClosed | ServerError::UnknownStream) => {}
            Err(_) => self.fail(ErrorCode::InternalError),
        }
        if close {
            self.outcome.closed = true;
        }
    }

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
    fn serve_native(
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
        let mut body_scratch = [0u8; MAX_RESPONSE_BODY];
        let invoked = match view.invoke(handler, user_data, &mut reason_scratch, &mut body_scratch)
        {
            Ok(invoked) => invoked,
            Err(_) => return self.deny(stream, ErrorCode::InternalError, false),
        };
        let twin = if encode {
            match compress_route_body(invoked.body) {
                Ok(twin) => twin,
                Err(_) => return self.fail(ErrorCode::InternalError),
            }
        } else {
            None
        };
        let (body, content_encoding, vary) = match twin {
            Some(ref twin) => (twin.as_slice(), Some(CONTENT_ENCODING_GZIP), true),
            None => (invoked.body, None, gzip),
        };
        if !end {
            let refused = send_full(
                self.server,
                stream,
                invoked.status,
                body,
                content_encoding,
                vary,
                &[],
                false,
                false,
                self.output,
                self.used,
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
            body,
            content_encoding,
            vary,
            &[],
            is_head || body.is_empty(),
            true,
            self.output,
            self.used,
        ) {
            Ok(()) => self.outcome.served += 1,
            Err(code) => self.fail(code),
        }
    }

    // NOTE: 405 assembly beside the static 404 tail: the caller joins the exact registered
    // methods for the path (write_allow) before this call so no request borrow crosses the
    // mutable dispatch; overflow RSTs the stream fail-closed with the conn surviving (never
    // GOAWAY for a 405), and the !end refusal mirrors the static tail exactly (HEADERS-open
    // then RST plus the same close). Date rides the shared 405 sender first after :status
    // (H1 Date-first parity); HEAD answers headers-only with the 405 length like any static HEAD.
    fn serve_server_options(&mut self, stream: u32, end: bool, allow: &[u8]) {
        if !end {
            if send_options(self.server, stream, allow, false, self.output, self.used).is_err() {
                return self.fail(ErrorCode::InternalError);
            }
            return self.deny(stream, ErrorCode::RefusedStream, true);
        }
        match send_options(self.server, stream, allow, true, self.output, self.used) {
            Ok(()) => self.outcome.served += 1,
            Err(code) => self.fail(code),
        }
    }

    fn serve_allow_value(&mut self, stream: u32, end: bool, is_head: bool, allow: &[u8]) {
        if !end {
            if send_method_not_allowed(
                self.server,
                stream,
                allow,
                false,
                false,
                self.output,
                self.used,
            )
            .is_err()
            {
                return self.fail(ErrorCode::InternalError);
            }
            return self.deny(stream, ErrorCode::RefusedStream, true);
        }
        match send_method_not_allowed(
            self.server,
            stream,
            allow,
            is_head,
            true,
            self.output,
            self.used,
        ) {
            Ok(()) => self.outcome.served += 1,
            Err(code) => self.fail(code),
        }
    }

    fn stage_request_trailers(&mut self, stream: u32, range: HeaderRange, end: bool) -> H2Trailer {
        let Some(ctx) = self.host else {
            return H2Trailer::Conn;
        };
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
        verdict
            .map(|(verdict, _)| verdict)
            .unwrap_or(H2Trailer::Conn)
    }

    pub(crate) fn serve_one(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::Headers { stream, range, end } => {
                let fields = match self.headers.get(range.start..range.start + range.count) {
                    Some(fields) => fields,
                    None => return self.fail(ErrorCode::ProtocolError),
                };
                let request = match self.server.accept_request(stream, fields) {
                    Ok(request) => request,
                    Err(ServerError::RefusedStream) => {
                        return self.deny(stream, ErrorCode::RefusedStream, false);
                    }
                    Err(
                        ServerError::StreamClosed
                        | ServerError::UnknownStream
                        | ServerError::NewStreamsClosed,
                    ) => return,
                    Err(ServerError::Request(_)) => {
                        match self.stage_request_trailers(stream, range, end) {
                            H2Trailer::Staged => return,
                            H2Trailer::Stream(code) => return self.deny(stream, code, false),
                            H2Trailer::Conn => return self.fail(ErrorCode::ProtocolError),
                        }
                    }
                    Err(_) => return self.fail(ErrorCode::InternalError),
                };
                let asterisk = matches!(request.path, Some(path) if path == b"*");
                if request.method == b"OPTIONS" && asterisk {
                    let mut allow = [0u8; ALLOW_BYTES];
                    let Some(len) = self.routes.write_server_allow(&mut allow) else {
                        self.deny(stream, ErrorCode::InternalError, false);
                        return;
                    };
                    self.serve_server_options(stream, end, &allow[..len]);
                    return;
                }
                let path = match request.path {
                    Some(path) if path.first() == Some(&b'/') => path,
                    _ => return self.deny(stream, ErrorCode::RefusedStream, false),
                };
                let is_head = request.method == b"HEAD";
                // NOTE: 414 before every dispatch (static, native, managed, file): the decoded
                // :path length is known exactly, so an over-long target answers here instead of
                // missing to 404. Per-stream fail-closed like the 404 tail below (HEADERS+DATA
                // with Date through send_full, never GOAWAY for a per-stream verdict); the !end
                // refusal matches the 404 tail too (HEADERS-open then RST plus the same close).
                if path.len() > MAX_REQUEST_TARGET_BYTES {
                    if !end {
                        if send_full(
                            self.server,
                            stream,
                            414,
                            URI_TOO_LONG_BODY,
                            None,
                            false,
                            &[],
                            false,
                            false,
                            self.output,
                            self.used,
                        )
                        .is_err()
                        {
                            return self.fail(ErrorCode::InternalError);
                        }
                        return self.deny(stream, ErrorCode::RefusedStream, true);
                    }
                    match send_full(
                        self.server,
                        stream,
                        414,
                        URI_TOO_LONG_BODY,
                        None,
                        false,
                        &[],
                        is_head,
                        true,
                        self.output,
                        self.used,
                    ) {
                        Ok(()) => self.outcome.served += 1,
                        Err(code) => self.fail(code),
                    }
                    return;
                }
                if self.routes.is_managed(request.method, path) {
                    if let Some(ctx) = self.host {
                        let declared = request.content_length;
                        if end && declared.unwrap_or(0) > 0 {
                            return self.deny(stream, ErrorCode::ProtocolError, false);
                        }
                        if end || declared.is_some() {
                            let capped = declared.unwrap_or(0) <= H2_BODY_CAP;
                            if capped
                                && park_stream(ctx, self.routes, stream, request, self.outcome)
                            {
                                return;
                            }
                            return self.deny(stream, ErrorCode::RefusedStream, false);
                        }
                    }
                }
                if let Some((handler, user_data, gzip)) =
                    self.routes.lookup_native(request.method, path)
                {
                    let declared = request.content_length.unwrap_or(0);
                    let view = NativeRequestView {
                        method: request.method.as_ptr(),
                        method_len: request.method.len(),
                        path: path.as_ptr(),
                        path_len: path.len(),
                        has_body: u8::from(declared > 0),
                        body_len: declared,
                        body: std::ptr::null(),
                    };
                    let encode = gzip && FrozenRoutesView::accepts_gzip(fields, self.server);
                    return self.serve_native(
                        stream, view, end, is_head, handler, user_data, gzip, encode,
                    );
                }
                // NOTE: exact statics win; file mounts serve only table misses, so existing
                // routes never change behavior when mounts are added (H1 respond_to_head parity).
                // NOTE: method-mismatch 405 beside the 404 below (H1 MethodNotAllowed parity):
                // a path match with no method match answers 405 with the exact registered
                // methods, never synthesized; managed parks and native dispatch already missed
                // above, so this covers static, managed, and native kinds together. File
                // entries never match the scan, so file 405 probes only after plan_file below
                // reports Miss. Unknown paths fall through to the 404 tail untouched.
                let found = self.routes.lookup(request.method, path);
                if found.is_none() {
                    let routes = self.routes;
                    if routes.has_path_match(path) {
                        let mut allow = [0u8; ALLOW_BYTES];
                        let Some(len) = routes.write_allow(path, &mut allow) else {
                            self.deny(stream, ErrorCode::InternalError, false);
                            return;
                        };
                        self.serve_allow_value(stream, end, is_head, &allow[..len]);
                        return;
                    }
                }
                if found.is_none() {
                    let declared = request.content_length.unwrap_or(0);
                    match self.plan_file(request.method, path, declared, fields) {
                        FileDecision::Miss => {
                            // NOTE: file GET-probe mirror of H1 respond_file: non-GET/HEAD over
                            // an existing file resource answers 405 with Allow: GET, HEAD while
                            // traversal, directories, and misses stay 404 with no existence
                            // oracle. The GET probe reuses resolve_file (prefix, decoding, and
                            // stat stay single-sourced), so only a real file (Ok) or an
                            // over-cap file (TooLarge) reports a resource; the static 405 scan
                            // above already missed, so no exact route shadows this probe.
                            if request.method != b"GET" && request.method != b"HEAD" {
                                let probe = self.routes.resolve_file(b"GET", path);
                                let exists = matches!(probe, Ok(_) | Err(FileReject::TooLarge));
                                if exists {
                                    self.serve_allow_value(stream, end, is_head, b"GET, HEAD");
                                    return;
                                }
                            }
                        }
                        FileDecision::TooLarge => {
                            match send_full(
                                self.server,
                                stream,
                                413,
                                b"Payload Too Large",
                                None,
                                false,
                                &[],
                                false,
                                true,
                                self.output,
                                self.used,
                            ) {
                                Ok(()) => self.outcome.served += 1,
                                Err(code) => self.fail(code),
                            }
                            return;
                        }
                        FileDecision::Serve(plan, head, send_body) => {
                            self.emit_file(stream, end, plan, head, send_body);
                            return;
                        }
                    }
                }
                let (status, body, gzip) = found.unwrap_or((404, NOT_FOUND_BODY, None));
                let twin = match gzip {
                    Some(twin) if FrozenRoutesView::accepts_gzip(fields, self.server) => Some(twin),
                    _ => None,
                };
                let (body, encoding, vary) = match twin {
                    Some(twin) => (twin, Some(CONTENT_ENCODING_GZIP), true),
                    None => (body, None, gzip.is_some()),
                };
                if !end {
                    if send_full(
                        self.server,
                        stream,
                        status,
                        body,
                        encoding,
                        vary,
                        &[],
                        false,
                        false,
                        self.output,
                        self.used,
                    )
                    .is_err()
                    {
                        return self.fail(ErrorCode::InternalError);
                    }
                    return self.deny(stream, ErrorCode::RefusedStream, true);
                }
                match send_full(
                    self.server,
                    stream,
                    status,
                    body,
                    encoding,
                    vary,
                    &[],
                    is_head || body.is_empty(),
                    true,
                    self.output,
                    self.used,
                ) {
                    Ok(()) => self.outcome.served += 1,
                    Err(code) => self.fail(code),
                }
            }
            StreamEvent::Data(stream, start, len, end) => {
                let Some(bytes) = self.data.get(start..start + len) else {
                    return self.fail(ErrorCode::InternalError);
                };
                let Some(ctx) = self.host else {
                    return self.deny(stream, ErrorCode::RefusedStream, true);
                };
                match push_h2_data(ctx, stream, bytes, end) {
                    H2DataOutcome::Stored => {}
                    H2DataOutcome::Short => self.deny(stream, ErrorCode::ProtocolError, false),
                    H2DataOutcome::Over => self.deny(stream, ErrorCode::FlowControlError, false),
                    H2DataOutcome::Gone => self.deny(stream, ErrorCode::RefusedStream, false),
                }
            }
            StreamEvent::PeerReset => {}
            StreamEvent::PeerGoAway => self.outcome.closed = true,
        }
    }
}

// NOTE: queue-full split: H1 closes the conn when the host queue is full, but one H2 conn
// carries siblings, so a refused park RSTs only its stream and multiplexing continues.
fn park_stream(
    ctx: H2HostCtx<'_>,
    routes: &FrozenRoutesView<'_>,
    stream: u32,
    request: ValidatedServerRequest<'_>,
    outcome: &mut StepOutcome,
) -> bool {
    let total = request.content_length.unwrap_or(0);
    let path = request.path.unwrap_or(b"/");
    let route_id = routes.find(request.method, path).map(|index| index as u64);
    let route_id = route_id.unwrap_or(u64::MAX);
    let trailers = routes.managed_trailers(request.method, path);
    let sub = (stream >> 1) as usize % STREAM_SLOTS;
    let base = ConnectionToken {
        stream_id: stream,
        request_seq: 0,
        slot: ctx.base.slot + sub as u32,
        ..ctx.base
    };
    match ctx.host.try_park(
        ctx.base.worker_id,
        base,
        route_id,
        request.method,
        path,
        total,
        trailers,
    ) {
        Ok(_) => {
            outcome.parked += 1;
            true
        }
        Err(_) => false,
    }
}

#[derive(Clone, Copy)]
enum H2Trailer {
    Staged,
    Stream(ErrorCode),
    Conn,
}

pub(crate) fn valid_trailer_field(name: &[u8], value: &[u8]) -> bool {
    if name.is_empty() || name.first() == Some(&b':') || prohibited_request_trailer(name) {
        return false;
    }
    if !name
        .iter()
        .copied()
        .all(|byte| is_token(byte) && !byte.is_ascii_uppercase())
    {
        return false;
    }
    let ows = |byte: &u8| *byte == b' ' || *byte == b'\t';
    if value.first().is_some_and(ows) || value.last().is_some_and(ows) {
        return false;
    }
    !value.iter().any(|byte| {
        *byte == 0
            || *byte == b'\r'
            || *byte == b'\n'
            || *byte == 0x7f
            || (*byte < 0x20 && *byte != b'\t')
    })
}

// NOTE: the H2 request-trailer prohibited set is the same 15-name decoder policy as H1
// (veylor_http1::prohibited_request_trailer, byte-identical membership): H2 framing has no
// connection-specific fields (they never survive accept_request), so no H2-only names exist
// and the set is shared, not copied. The connection-specific handling lives in
// trailer_allowed, which H2 calls with an empty connection value for exactly that reason.
// Staged caps (TRAILER_SLOTS / MAX_STAGED_TRAILER_BYTES) are the shared H1 host-staging
// bounds, intentionally stricter than the decoder's wider BodyLimits.

enum H2DataOutcome {
    Stored,
    Short,
    Over,
    Gone,
}

fn push_h2_data(ctx: H2HostCtx<'_>, stream: u32, bytes: &[u8], end: bool) -> H2DataOutcome {
    let slot = ctx.base.slot + ((stream >> 1) as usize % STREAM_SLOTS) as u32;
    let outcome = ctx.host.with_inner(|inner| {
        let live = inner
            .parked
            .get_mut(ctx.base.worker_id as usize)
            .and_then(|table| table.get_mut(slot as usize))
            .and_then(|entry| entry.as_mut())
            .filter(|live| live.token.stream_id == stream);
        let Some(live) = live else {
            return (H2DataOutcome::Gone, None);
        };
        let room = live
            .body_total
            .saturating_sub(live.body_pulled + live.body.len() as u64);
        let fit = room.min(bytes.len() as u64) as usize;
        live.body.extend_from_slice(&bytes[..fit]);
        if fit < bytes.len() {
            return (H2DataOutcome::Over, None);
        }
        if end && live.body_pulled + live.body.len() as u64 != live.body_total {
            return (H2DataOutcome::Short, None);
        }
        // NOTE: H2 body admission signals like the H1 fixed/chunked/park paths: an empty
        // terminal frame stages no signal (completion is pull-observed), and a full queue
        // retracts the admission and refuses the stream, so bytes and signals stay atomic.
        if bytes.is_empty() {
            return (H2DataOutcome::Stored, None);
        }
        let token = live.token;
        if crate::host::stage_body_available(inner, ctx.base.worker_id as usize, token) {
            return (H2DataOutcome::Stored, None);
        }
        let back = inner
            .parked
            .get_mut(ctx.base.worker_id as usize)
            .and_then(|table| table.get_mut(slot as usize))
            .and_then(|entry| entry.as_mut())
            .filter(|entry| entry.token == token);
        if let Some(entry) = back {
            entry.body.truncate(entry.body.len().saturating_sub(fit));
        }
        (H2DataOutcome::Gone, None)
    });
    let Some((result, _)) = outcome else {
        return H2DataOutcome::Gone;
    };
    if matches!(result, H2DataOutcome::Stored) && !bytes.is_empty() {
        ctx.host.notify_pollers();
    }
    result
}

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
