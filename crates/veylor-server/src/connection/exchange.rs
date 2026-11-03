use veylor_http1::{Limits, ParseError, RequestVersion};
use veylor_router::{RouteKind, RouteTable};

use crate::host::{HOST_MAX_METHOD_BYTES, HostShared};
use crate::static_routes::{
    EncodedStaticResponse, RouteBuildError, StaticRoutes, respond_bad_request,
    respond_expectation_failed, respond_request_header_fields_too_large, respond_to_head_matched,
    respond_uri_too_long, route_cap,
};

use super::file::FileTransfer;
use super::parked::park_request;
use super::{ConnClose, ConnPhase, ConnState, INPUT_CAP};

// NOTE: minimal request-line frame around a target (1-byte method + 2 spaces + 8-byte version);
// a target side longer than the line limit behind this frame is target-caused (RFC 9110 15.5.15).
const MIN_REQUEST_LINE_FRAME: usize = 11;

pub(crate) enum ExchangeOutcome {
    NeedMore,
    Responded,
    Parked,
    Close(ConnClose),
}

pub(crate) fn parse_exchange(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    worker_id: u32,
    host: &HostShared,
    draining: bool,
) -> ExchangeOutcome {
    // NOTE: parsing empty input can only yield NeedMore, so quiet turns skip the parse
    // (and its header materialization) outright; behavior-identical, zero slabs when idle.
    if conn.len == 0 {
        return ExchangeOutcome::NeedMore;
    }
    let input = ConnState::materialize_input(&mut conn.input);
    let headers = ConnState::materialize_headers(&mut conn.headers);
    let parsed = conn
        .parser
        .parse(&input[..conn.len], &mut headers[..], Limits::default());
    match parsed {
        Ok(Some(head)) => {
            conn.head_len = head.length;
            park_or_respond(conn, routes, table, head, worker_id, host, draining)
        }
        Ok(None) if conn.len < INPUT_CAP => ExchangeOutcome::NeedMore,
        Err(ParseError::ExpectationFailed) => {
            // NOTE: 417 precedence over admission is structural and shared: the veylor_http1
            // head parser rejects an unsupported Expect before any head exists to admit, so
            // neither engine can stage an interim past this point. The 417+close rendering
            // itself stays managed-side (status-shape territory, not this policy); the
            // host-driven conn crate maps the failed head to ProtocolError and fails every
            // follow-up closed instead.
            let version = response_version(conn);
            send_expectation_failed(conn, version);
            ExchangeOutcome::Responded
        }
        Err(ParseError::HeaderSectionTooLarge) => {
            send_request_header_fields_too_large(conn, response_version(conn));
            ExchangeOutcome::Responded
        }
        // NOTE: RFC 9110 15.5.15 scopes 414 to a long target URI specifically, so an over-long
        // request line attributes by component lengths below: target-side overflow answers 414 with
        // Date and close exactly like 400/431, while method/version overflows stay 400. The parser
        // keeps its undifferentiated RequestLineTooLarge (a new variant would break three exhaustive
        // ParseError matches outside this slice), and a full input buffer surfaces as NeedMore, so
        // this arm attributes the buffered line itself; the hot scan is unaffected (error path only).
        Ok(None) | Err(ParseError::RequestLineTooLarge) => {
            let version = response_version(conn);
            if request_target_too_long(conn) {
                send_uri_too_long(conn, version);
            } else {
                send_bad_request(conn, version);
            }
            ExchangeOutcome::Responded
        }
        Err(_) => {
            let version = response_version(conn);
            send_bad_request(conn, version);
            ExchangeOutcome::Responded
        }
    }
}

fn park_or_respond(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    head: veylor_http1::RequestHead,
    worker_id: u32,
    host: &HostShared,
    draining: bool,
) -> ExchangeOutcome {
    let mut captures = [veylor_router::RouteCapture::default(); 8];
    let Some(input) = conn.input.as_ref() else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    let input = &input[..conn.len];
    match table.find_request(head, input, &mut captures) {
        Ok(found) if found.kind == RouteKind::Managed => {
            let cap = route_cap(found.body);
            let method = head.method(input).unwrap_or_default();
            let mut snapshot = [0u8; HOST_MAX_METHOD_BYTES];
            let take = method.len().min(HOST_MAX_METHOD_BYTES);
            snapshot[..take].copy_from_slice(&method[..take]);
            let trailers = routes.managed_trailers(&snapshot[..take], found.pattern);
            park_request(
                conn,
                head,
                found.route_id,
                found.token,
                worker_id,
                host,
                draining,
                cap,
                trailers,
            )
        }
        found => encode_static(conn, routes, table, head, found),
    }
}

fn encode_static(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    head: veylor_http1::RequestHead,
    found: Result<veylor_router::RouteMatch<'_, '_>, veylor_router::RouteError>,
) -> ExchangeOutcome {
    let Some(headers) = conn.headers.as_ref() else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    let Some(input) = conn.input.as_ref() else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    let output = ConnState::materialize_output(&mut conn.output);
    match respond_to_head_matched(
        routes,
        table,
        head,
        found,
        &headers[..],
        &input[..conn.len],
        &mut output[..],
    ) {
        Ok(EncodedStaticResponse {
            length,
            keep_open,
            drain_total,
            interim_len,
            file,
        }) => {
            conn.out_sent = 0;
            conn.write_blocked = false;
            conn.closing = !keep_open;
            // NOTE: a rejection closes without draining the declared body (413/417 keep
            // the early response); remember what is still in flight so the close can
            // consume it instead of resetting the peer's response.
            conn.close_drain_remaining = if keep_open {
                0
            } else if drain_total == 0 {
                if head.is_chunked {
                    u64::MAX
                } else {
                    head.content_length.unwrap_or(0)
                }
            } else {
                0
            };
            conn.close_reason = ConnClose::Completed;
            // NOTE: bytes already buffered past the head are body bytes by position, so
            // skip them in place and drain the rest across pumps; the head is dropped now
            // and keep-alive reuse below treats the remainder as pipelined tail. The
            // encoded response stays unpublished until the drain finishes, so an idle
            // drain waits on poll instead of spinning the worker turn.
            let buffered = (conn.len - conn.head_len) as u64;
            let skip = buffered.min(drain_total) as usize;
            let consumed = conn.head_len + skip;
            if consumed < conn.len
                && let Some(input) = conn.input.as_mut()
            {
                input.copy_within(consumed..conn.len, 0);
            }
            conn.len -= consumed;
            conn.head_len = 0;
            let remaining = drain_total - skip as u64;
            // NOTE: HEAD and empty files complete with the head alone; only GET bodies
            // with bytes stage, so the pump never opens a file it will not read from.
            conn.file = file
                .filter(|plan| !plan.head_only && plan.size > 0)
                .map(|plan| FileTransfer { plan, offset: 0 });
            let sending_file = conn.file.is_some();
            if remaining == 0 {
                conn.out_len = length;
                conn.phase = if sending_file {
                    ConnPhase::SendingFile
                } else {
                    ConnPhase::SendingResponse
                };
            } else {
                conn.out_len = interim_len;
                conn.pending_len = length;
                conn.phase = ConnPhase::DrainingBody {
                    remaining,
                    responded: false,
                };
                conn.read_ready = true;
            }
            ExchangeOutcome::Responded
        }
        // NOTE: handler-reported failure closes as response-overflow (bounded-response family,
        // like static); NativePanic is defense-in-depth for the boundary closure itself —
        // a panic inside the handler aborts nounwind before it can reach here.
        Err(RouteBuildError::NativePanic) => ExchangeOutcome::Close(ConnClose::ProtocolError),
        Err(_) => ExchangeOutcome::Close(ConnClose::ResponseOverflow),
    }
}

fn response_version(conn: &ConnState) -> RequestVersion {
    let Some(input) = conn.input.as_ref() else {
        return RequestVersion::Http11;
    };
    let input = &input[..conn.len];
    if let Some(version) = conn.parser.protocol_version(input) {
        return version;
    }

    let line_end = input
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(input.len());
    let line = input[..line_end]
        .strip_suffix(b"\r")
        .unwrap_or(&input[..line_end]);
    let Some(version_start) = line
        .iter()
        .rposition(|byte| *byte == b' ')
        .map(|index| index + 1)
    else {
        return RequestVersion::Http11;
    };
    RequestVersion::from_bytes(&line[version_start..]).unwrap_or(RequestVersion::Http11)
}

fn send_bad_request(conn: &mut ConnState, request_version: RequestVersion) {
    ConnState::materialize_output(&mut conn.output);
    let staged = match conn.output.as_mut() {
        Some(output) => respond_bad_request(&mut output[..], request_version).ok(),
        None => None,
    };
    finish_simple(conn, ConnClose::ProtocolError, staged);
}

fn send_request_header_fields_too_large(conn: &mut ConnState, request_version: RequestVersion) {
    ConnState::materialize_output(&mut conn.output);
    let staged = match conn.output.as_mut() {
        Some(output) => {
            respond_request_header_fields_too_large(&mut output[..], request_version).ok()
        }
        None => None,
    };
    finish_simple(conn, ConnClose::ProtocolError, staged);
}

// NOTE: RFC 9110 15.5.15 blames only the target URI: with no newline the buffer is still on the
// request line, so a missing or leading space reads as method overflow (400) while a target side
// longer than the line limit behind the minimal frame reads as 414; a second space pins a short
// target, so version-side overflow stays 400.
fn request_target_too_long(conn: &ConnState) -> bool {
    let Some(input) = conn.input.as_ref() else {
        return false;
    };
    let buffered = &input[..conn.len];
    if buffered.contains(&b'\n') {
        return false;
    }
    let Some(method_end) = buffered.iter().position(|byte| *byte == b' ') else {
        return false;
    };
    if method_end == 0 {
        return false;
    }
    let target_start = method_end + 1;
    if target_start >= buffered.len() {
        return false;
    }
    let rest = &buffered[target_start..];
    let target_len = rest
        .iter()
        .position(|byte| *byte == b' ')
        .unwrap_or(rest.len());
    target_len + MIN_REQUEST_LINE_FRAME > Limits::default().request_line_bytes
}

pub(crate) fn send_uri_too_long(conn: &mut ConnState, request_version: RequestVersion) {
    ConnState::materialize_output(&mut conn.output);
    let staged = match conn.output.as_mut() {
        Some(output) => respond_uri_too_long(&mut output[..], request_version).ok(),
        None => None,
    };
    finish_simple(conn, ConnClose::ProtocolError, staged);
}

fn send_expectation_failed(conn: &mut ConnState, request_version: RequestVersion) {
    ConnState::materialize_output(&mut conn.output);
    let staged = match conn.output.as_mut() {
        Some(output) => respond_expectation_failed(&mut output[..], request_version).ok(),
        None => None,
    };
    finish_simple(conn, ConnClose::Completed, staged);
}

fn finish_simple(conn: &mut ConnState, reason: ConnClose, staged: Option<usize>) {
    conn.close_reason = reason;
    conn.out_len = staged.unwrap_or(0);
    conn.out_sent = 0;
    conn.phase = ConnPhase::SendingResponse;
    conn.closing = true;
    if staged.is_some() {
        conn.write_blocked = false;
    }
}
