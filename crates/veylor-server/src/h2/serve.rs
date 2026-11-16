use std::borrow::Cow;

use veylor_http1::ALLOW_BYTES;
use veylor_http2::{ErrorCode, H2ServerDriver, HpackHeader, ServerError};
use veylor_router::{MAX_ROUTE_CAPTURES, RouteCapture};

use crate::host::HOST_TUNNEL_CAP;
use crate::static_files::FileReject;
use crate::static_routes::{
    CONTENT_ENCODING_GZIP, MAX_NATIVE_HEADERS, MAX_NATIVE_PARAMS, NativeHeader, NativeParam,
    NativeRequestView, engine_owned_request_header,
};

use super::file::FileDecision;
use super::pending::PendingResponses;
use super::routes::{
    FrozenRoutesView, MAX_REQUEST_TARGET_BYTES, NOT_FOUND_BODY, RouteTarget, URI_TOO_LONG_BODY,
};
use super::send::{send_full, send_method_not_allowed, send_options};
use super::{
    H2_BODY_CAP, H2FileTransfer, H2HostCtx, STREAM_SLOTS, StepOutcome, StreamEvent, fail_goaway,
};

mod native;
mod park;
mod trailers;

use park::{H2DataOutcome, park_stream, push_h2_data};
use trailers::H2Trailer;

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
    pub(crate) pending: &'o mut PendingResponses,
    pub(crate) files: &'o mut Option<Box<[Option<H2FileTransfer>; STREAM_SLOTS]>>,
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
                self.pending,
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
                // NOTE: one route selection per request; the winner's kind is the dispatch
                // order H1 uses (managed parks, native handles, static serves).
                let target = self
                    .routes
                    .target(request.method, path, self.host.is_some());
                if request.method == b"CONNECT" {
                    if request.protocol != Some(&b"websocket"[..]) {
                        return self.deny(stream, ErrorCode::RefusedStream, false);
                    }
                    if request.content_length.is_some() {
                        return self.deny(stream, ErrorCode::ProtocolError, false);
                    }
                    let Some(ctx) = self.host else {
                        return self.deny(stream, ErrorCode::RefusedStream, false);
                    };
                    let Some(RouteTarget::Managed(index)) = target else {
                        return self.deny(stream, ErrorCode::RefusedStream, false);
                    };
                    if park_stream(
                        ctx,
                        self.routes,
                        index,
                        stream,
                        request,
                        end,
                        HOST_TUNNEL_CAP as u64,
                        fields,
                        self.server,
                        self.outcome,
                        true,
                    ) {
                        return;
                    }
                    return self.deny(stream, ErrorCode::RefusedStream, false);
                }
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
                            Cow::Borrowed(URI_TOO_LONG_BODY),
                            None,
                            false,
                            &[],
                            false,
                            false,
                            self.output,
                            self.used,
                            self.pending,
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
                        Cow::Borrowed(URI_TOO_LONG_BODY),
                        None,
                        false,
                        &[],
                        is_head,
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
                    return;
                }
                if let Some(RouteTarget::Managed(index)) = target
                    && let Some(ctx) = self.host
                {
                    let declared = request.content_length;
                    if end && declared.unwrap_or(0) > 0 {
                        return self.deny(stream, ErrorCode::ProtocolError, false);
                    }
                    let body_limit = self.routes.body_limit(index).min(H2_BODY_CAP);

                    if declared.is_some_and(|length| length > body_limit) {
                        self.serve_managed_body_too_large(stream, is_head);
                        return;
                    }

                    if park_stream(
                        ctx,
                        self.routes,
                        index,
                        stream,
                        request,
                        end,
                        body_limit,
                        fields,
                        self.server,
                        self.outcome,
                        false,
                    ) {
                        return;
                    }
                    return self.deny(stream, ErrorCode::RefusedStream, false);
                }
                if let Some(RouteTarget::Native(index)) = target
                    && let Some((handler, user_data, gzip, pattern)) =
                        self.routes.native_route(index)
                {
                    let declared = request.content_length.unwrap_or(0);
                    let mut native_headers = [NativeHeader::EMPTY; MAX_NATIVE_HEADERS];
                    let mut native_header_count = 0usize;
                    for header in fields {
                        let Some((name, value)) = self.server.driver().resolve_header(*header)
                        else {
                            continue;
                        };
                        if native_header_count == MAX_NATIVE_HEADERS
                            || engine_owned_request_header(name)
                        {
                            continue;
                        }
                        native_headers[native_header_count] = NativeHeader {
                            name: name.as_ptr(),
                            name_len: name.len(),
                            value: value.as_ptr(),
                            value_len: value.len(),
                        };
                        native_header_count += 1;
                    }
                    let mut captures = [RouteCapture::default(); MAX_ROUTE_CAPTURES];
                    let capture_count =
                        veylor_router::capture_route(pattern, path, &mut captures).unwrap_or(0);
                    let mut native_params = [NativeParam::EMPTY; MAX_NATIVE_PARAMS];
                    let mut native_param_count = 0usize;
                    for capture in captures.iter().take(capture_count.min(MAX_NATIVE_PARAMS)) {
                        let (Some(name), Some(value)) =
                            (capture.name(pattern), capture.value(path))
                        else {
                            continue;
                        };
                        native_params[native_param_count] = NativeParam {
                            name: name.as_ptr(),
                            name_len: name.len(),
                            value: value.as_ptr(),
                            value_len: value.len(),
                        };
                        native_param_count += 1;
                    }
                    let view = NativeRequestView {
                        method: request.method.as_ptr(),
                        method_len: request.method.len(),
                        path: path.as_ptr(),
                        path_len: path.len(),
                        has_body: u8::from(declared > 0),
                        body_len: declared,
                        body: std::ptr::null(),
                        headers: native_headers.as_ptr(),
                        header_count: native_header_count,
                        params: native_params.as_ptr(),
                        param_count: native_param_count,
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
                let found = match target {
                    Some(RouteTarget::Static(index) | RouteTarget::Managed(index)) => {
                        self.routes.stored(index)
                    }
                    _ => None,
                };
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
                                Cow::Borrowed(b"Payload Too Large"),
                                None,
                                false,
                                &[],
                                false,
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
                        Cow::Borrowed(body),
                        encoding,
                        vary,
                        &[],
                        false,
                        false,
                        self.output,
                        self.used,
                        self.pending,
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
                    Cow::Borrowed(body),
                    encoding,
                    vary,
                    &[],
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
                    H2DataOutcome::OverLimit => {
                        let slot = ctx.base.slot + ((stream >> 1) as usize % STREAM_SLOTS) as u32;
                        ctx.host.unpark_closed(ctx.base.worker_id, slot);
                        self.deny(stream, ErrorCode::Cancel, false);
                    }
                    H2DataOutcome::OverDeclared => {
                        let slot = ctx.base.slot + ((stream >> 1) as usize % STREAM_SLOTS) as u32;
                        ctx.host.unpark_closed(ctx.base.worker_id, slot);
                        self.deny(stream, ErrorCode::ProtocolError, false);
                    }
                    H2DataOutcome::Gone => self.deny(stream, ErrorCode::RefusedStream, false),
                }
            }
            StreamEvent::PeerReset => {}
            StreamEvent::PeerGoAway => {}
        }
    }

    fn serve_managed_body_too_large(&mut self, stream: u32, is_head: bool) {
        match send_full(
            self.server,
            stream,
            413,
            Cow::Borrowed(b"Payload Too Large"),
            None,
            false,
            &[],
            is_head,
            !is_head,
            self.output,
            self.used,
            self.pending,
        ) {
            Ok(complete) => {
                self.outcome.served += u16::from(complete);
                self.outcome.pending |= !complete;
            }
            Err(code) => self.fail(code),
        }
    }
}
