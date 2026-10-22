use veylor_http1::{
    ConnectionHeader, Header, Limits, ParseError, Parser, ResponseFraming, ResponseToWrite,
    connection_header_for_response, encode_response_head,
};
use veylor_router::{RouteKind, RouteTable};
use veylor_runtime::{
    CloseReason, ConnectionToken, IoBatch, ReadStatus, TcpStream, WriteStatus, dispatch,
};

use crate::host::{HOST_MAX_METHOD_BYTES, HostShared, ParkError};
use crate::static_routes::{
    EncodedStaticResponse, StaticRoutes, respond_bad_request, respond_expectation_failed,
    respond_to_head,
};

pub const INPUT_CAP: usize = 8192;
pub const OUTPUT_CAP: usize = 8192;
pub const HEADER_SLOTS: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnPhase {
    ReadingHead,
    DrainingBody {
        remaining: u64,
    },
    SendingResponse,
    // NOTE: the owner-side parked snapshot: method copy for HEAD semantics at respond
    // time, keep-open decided at park (body framing forces close after the response).
    Parked {
        route_id: u64,
        method_len: u8,
        method: [u8; HOST_MAX_METHOD_BYTES],
        has_body: bool,
        keep_open: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnClose {
    PeerClosed,
    Reset,
    Timeout,
    ParkedTimeout,
    QueueFull,
    Cancelled,
    ProtocolError,
    ResponseOverflow,
    TransportError,
    Shutdown,
    Completed,
}

pub struct ConnState {
    token: ConnectionToken,
    stream: TcpStream,
    parser: Parser,
    headers: Box<[Header; HEADER_SLOTS]>,
    input: Box<[u8; INPUT_CAP]>,
    len: usize,
    head_len: usize,
    output: Box<[u8; OUTPUT_CAP]>,
    out_len: usize,
    out_sent: usize,
    pending_len: usize,
    phase: ConnPhase,
    read_ready: bool,
    write_blocked: bool,
    closing: bool,
    close_reason: ConnClose,
    error_hint: bool,
}

impl ConnState {
    pub fn new(token: ConnectionToken, stream: TcpStream) -> Self {
        Self {
            token,
            stream,
            parser: Parser::new(),
            headers: Box::new([Header::default(); HEADER_SLOTS]),
            input: Box::new([0; INPUT_CAP]),
            len: 0,
            head_len: 0,
            output: Box::new([0; OUTPUT_CAP]),
            out_len: 0,
            out_sent: 0,
            pending_len: 0,
            phase: ConnPhase::ReadingHead,
            read_ready: true,
            write_blocked: false,
            closing: false,
            close_reason: ConnClose::Completed,
            error_hint: false,
        }
    }

    pub fn token(&self) -> ConnectionToken {
        self.token
    }

    pub fn mark_readable(&mut self) {
        self.read_ready = true;
    }

    pub fn mark_writable(&mut self) {
        self.write_blocked = false;
    }

    pub fn mark_error(&mut self) {
        self.error_hint = true;
    }

    pub fn has_pending_output(&self) -> bool {
        self.out_sent < self.out_len
    }

    pub fn has_unparsed_input(&self) -> bool {
        self.phase == ConnPhase::ReadingHead && self.len > 0
    }

    pub fn read_armed(&self) -> bool {
        self.read_ready
    }

    pub fn is_idle(&self) -> bool {
        self.phase == ConnPhase::ReadingHead && !self.has_pending_output() && self.len == 0
    }

    pub fn is_parked(&self) -> bool {
        matches!(self.phase, ConnPhase::Parked { .. })
    }

    // NOTE: one-shot bounded response over the parked snapshot: the static encode path
    // with the parked method, so HEAD keeps length metadata without body bytes.
    pub fn complete_parked(
        &mut self,
        status: u16,
        reason: &[u8],
        body: &[u8],
    ) -> Result<(), ConnClose> {
        let ConnPhase::Parked {
            method_len,
            method,
            has_body,
            keep_open,
            ..
        } = self.phase
        else {
            return Err(ConnClose::TransportError);
        };
        let request_method = &method[..usize::from(method_len)];
        let connection = if keep_open && !has_body {
            ConnectionHeader::Omit
        } else {
            ConnectionHeader::Close
        };
        let response = ResponseToWrite {
            request_method,
            requested_upgrade: b"",
            status,
            reason,
            fields: &[],
            framing: ResponseFraming::FixedLength,
            content_length: body.len() as u64,
            connection,
        };
        let encoded = encode_response_head(&response, &mut self.output[..])
            .map_err(|_| ConnClose::TransportError)?;
        let mut length = encoded.length;
        if encoded.body_allowed && !body.is_empty() {
            let end = length + body.len();
            self.output
                .get_mut(length..end)
                .ok_or(ConnClose::ResponseOverflow)?
                .copy_from_slice(body);
            length = end;
        }
        self.out_len = length;
        self.out_sent = 0;
        self.pending_len = 0;
        self.write_blocked = false;
        self.closing = !keep_open || has_body;
        self.close_reason = ConnClose::Completed;
        self.phase = ConnPhase::SendingResponse;
        Ok(())
    }

    pub fn begin_drain(&mut self) {
        self.closing = true;
    }

    pub fn into_stream(self) -> TcpStream {
        self.stream
    }
}

pub enum PumpOutcome {
    Keep(PumpProgress),
    Close(ConnClose),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PumpProgress {
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub resumed_idle: bool,
    pub entered_body: bool,
    pub entered_response: bool,
    pub entered_park: bool,
    pub response_flushed: bool,
}

pub fn pump_connection(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    byte_budget: u64,
    worker_id: u32,
    host: &HostShared,
    draining: bool,
) -> PumpOutcome {
    if conn.error_hint {
        return PumpOutcome::Close(ConnClose::TransportError);
    }
    if conn.is_parked() {
        return pump_parked(conn);
    }
    let draining_body = matches!(conn.phase, ConnPhase::DrainingBody { .. });
    let reading = (conn.phase == ConnPhase::ReadingHead || draining_body) && conn.len < INPUT_CAP;
    let token = conn.token;
    let mut progress = PumpProgress::default();
    let read_ready = conn.read_ready;
    let out_sent = conn.out_sent;
    let out_len = conn.out_len;
    // NOTE: the encoded final response is held while draining and no final byte is written until
    // the declared body is fully skipped, so a drained exchange keeps single-outstanding-response
    // order. An admitted 100-continue interim is staged ahead of the final and stays writable
    // during the drain; dispatch serves the write first and pauses reads while it is pending, so
    // the interim always flushes before further drain bytes are consumed from the wire.
    let sending = conn.phase == ConnPhase::SendingResponse;
    let flushing_interim =
        matches!(conn.phase, ConnPhase::DrainingBody { .. }) && out_sent < out_len;
    let write_ready = (sending || flushing_interim) && out_sent < out_len && !conn.write_blocked;
    let read_scratch = if reading {
        &mut conn.input[conn.len..]
    } else {
        &mut [] as &mut [u8]
    };
    let mut batch = [IoBatch::new(
        token,
        &mut conn.stream,
        read_scratch,
        read_ready,
        &conn.output[out_sent..out_len],
        write_ready,
    )];
    dispatch(byte_budget, &mut batch);
    let read = batch[0].read;
    let write = batch[0].write;
    let closed = batch[0].closed;
    // NOTE: edge-triggered readiness fires only on new arrivals, so a read that stopped on
    // partial progress or budget keeps its claim; a drained socket (WouldBlock) waits for the
    // next edge instead. Without the re-arm, budget-split heads stall until more data arrives.
    conn.read_ready = matches!(read, ReadStatus::Progress(_) | ReadStatus::BudgetExhausted);
    let read_drained = matches!(read, ReadStatus::WouldBlock);
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
    let accepted = match write {
        WriteStatus::Progress(accepted)
        | WriteStatus::WouldBlock(accepted)
        | WriteStatus::BudgetExhausted(accepted)
        | WriteStatus::Interrupted(accepted) => {
            if matches!(write, WriteStatus::WouldBlock(_)) {
                conn.write_blocked = true;
            }
            accepted
        }
        WriteStatus::Closed(CloseReason::PeerClosed) => {
            return PumpOutcome::Close(ConnClose::PeerClosed);
        }
        WriteStatus::Closed(CloseReason::Reset) => return PumpOutcome::Close(ConnClose::Reset),
        WriteStatus::Closed(CloseReason::Error(_)) | WriteStatus::Error(_) => {
            return PumpOutcome::Close(ConnClose::TransportError);
        }
        WriteStatus::Idle => 0,
    };
    conn.out_sent += accepted;
    progress.bytes_written = accepted as u64;
    if closed.is_some() && !conn.has_pending_output() && conn.phase == ConnPhase::ReadingHead {
        return PumpOutcome::Close(ConnClose::PeerClosed);
    }
    // NOTE: resume candidate only: local head phase plus new bytes or an armed buffered tail.
    // Quiet turns (no bytes, drained tail) stay silent so the hooks skip the phase lookup.
    if conn.phase == ConnPhase::ReadingHead
        && (progress.bytes_read > 0 || (conn.len > 0 && read_ready))
    {
        progress.resumed_idle = true;
    }

    if let ConnPhase::DrainingBody { remaining } = conn.phase {
        let skip = (conn.len as u64).min(remaining) as usize;
        conn.input.copy_within(skip..conn.len, 0);
        conn.len -= skip;
        let left = remaining - skip as u64;
        if left == 0 {
            conn.out_len = conn.pending_len;
            conn.pending_len = 0;
            conn.phase = ConnPhase::SendingResponse;
            conn.write_blocked = false;
            progress.entered_response = true;
        } else {
            conn.phase = ConnPhase::DrainingBody { remaining: left };
            // NOTE: edge-triggered readiness fires only on new arrivals, so a drain that stopped
            // short of EAGAIN re-arms its read; a drained socket waits for the next edge instead.
            // An Idle read attempted nothing (no readiness claim), so it leaves the flag alone.
            if !matches!(read, ReadStatus::Idle) {
                conn.read_ready = !read_drained;
            }
        }
    }

    if conn.phase == ConnPhase::ReadingHead {
        match parse_exchange(conn, routes, table, worker_id, host, draining) {
            ExchangeOutcome::NeedMore => {}
            ExchangeOutcome::Responded => {}
            ExchangeOutcome::Parked => {
                progress.entered_park = true;
            }
            ExchangeOutcome::Close(reason) => return PumpOutcome::Close(reason),
        }
        if conn.phase == ConnPhase::SendingResponse {
            progress.entered_response = true;
        } else if matches!(conn.phase, ConnPhase::DrainingBody { .. }) {
            progress.entered_body = true;
        }
    }

    if conn.phase == ConnPhase::SendingResponse && !conn.has_pending_output() {
        if conn.closing {
            return PumpOutcome::Close(conn.close_reason);
        }
        reuse_keep_alive(conn);
        progress.response_flushed = true;
    }
    PumpOutcome::Keep(progress)
}

enum ExchangeOutcome {
    NeedMore,
    Responded,
    Parked,
    Close(ConnClose),
}

fn parse_exchange(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    worker_id: u32,
    host: &HostShared,
    draining: bool,
) -> ExchangeOutcome {
    let parsed = conn.parser.parse(
        &conn.input[..conn.len],
        &mut conn.headers[..],
        Limits::default(),
    );
    match parsed {
        Ok(Some(head)) => {
            conn.head_len = head.length;
            park_or_respond(conn, routes, table, head, worker_id, host, draining)
        }
        Ok(None) => {
            if conn.len == INPUT_CAP {
                send_bad_request(conn);
                ExchangeOutcome::Responded
            } else {
                ExchangeOutcome::NeedMore
            }
        }
        Err(ParseError::ExpectationFailed) => {
            send_expectation_failed(conn);
            ExchangeOutcome::Responded
        }
        Err(ParseError::HeaderCapacity) => {
            send_bad_request(conn);
            ExchangeOutcome::Responded
        }
        Err(_) => {
            send_bad_request(conn);
            ExchangeOutcome::Responded
        }
    }
}

// NOTE: a parked connection owns no response bytes yet, so the pump only reads: it
// buffers pipelined tail bytes and observes peer close/reset so the slot frees and a
// later host respond lands on the tomb as Gone instead of reporting success for bytes
// that went nowhere. Nothing is parsed until the host responds or cancels.
fn pump_parked(conn: &mut ConnState) -> PumpOutcome {
    let token = conn.token;
    let read_ready = conn.read_ready;
    let read_scratch = if conn.len < INPUT_CAP {
        &mut conn.input[conn.len..]
    } else {
        &mut [] as &mut [u8]
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
    PumpOutcome::Keep(PumpProgress::default())
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
    let found = table.find_request(head, &conn.input[..conn.len], &mut captures);
    match found {
        Ok(found) if found.kind == RouteKind::Managed => {
            park_request(conn, head, found.route_id, worker_id, host, draining)
        }
        Ok(_) | Err(_) => encode_static(conn, routes, table, head),
    }
}

// NOTE: drain stops new parks: a head that arrives mid-drain closes instead of waiting
// on a host that may have stopped polling. Already-parked requests are in-flight and
// may still be responded during drain; unresponded parks close at the force deadline.
fn park_request(
    conn: &mut ConnState,
    head: veylor_http1::RequestHead,
    route_id: u64,
    worker_id: u32,
    host: &HostShared,
    draining: bool,
) -> ExchangeOutcome {
    if draining {
        return ExchangeOutcome::Close(ConnClose::Shutdown);
    }
    let len = conn.len;
    let input = &conn.input[..len];
    let method_len = head.method(input).map(|method| method.len()).unwrap_or(0);
    let target_len = head.target(input).map(|target| target.len()).unwrap_or(0);
    if method_len > HOST_MAX_METHOD_BYTES || method_len == 0 || target_len == 0 {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    }
    // NOTE: bodies stay out: framing is only recorded. The connection closes after the
    // parked response decision instead of draining, and an admitted 100-continue gets
    // no interim on a managed route in this slice.
    let has_body = head.is_chunked || head.content_length.unwrap_or(0) > 0;
    let connection =
        connection_header_for_response(head, input, 200, ResponseFraming::FixedLength, true, false);
    let keep_open = connection != ConnectionHeader::Close && !has_body;
    let token = conn.token;
    let mut snapshot = [0u8; HOST_MAX_METHOD_BYTES];
    snapshot[..method_len]
        .copy_from_slice(&input[head.method.offset..head.method.offset + method_len]);
    let target_start = head.target.offset;
    match host.try_park(
        worker_id,
        token,
        route_id,
        &snapshot[..method_len],
        &conn.input[target_start..target_start + target_len],
    ) {
        Ok(_) => {
            conn.input.copy_within(conn.head_len..conn.len, 0);
            conn.len -= conn.head_len;
            conn.head_len = 0;
            conn.phase = ConnPhase::Parked {
                route_id,
                method_len: method_len as u8,
                method: snapshot,
                has_body,
                keep_open,
            };
            ExchangeOutcome::Parked
        }
        Err(ParkError::Full) => ExchangeOutcome::Close(ConnClose::QueueFull),
        Err(ParkError::SnapshotTooLarge) => ExchangeOutcome::Close(ConnClose::TransportError),
    }
}

fn encode_static(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    head: veylor_http1::RequestHead,
) -> ExchangeOutcome {
    match respond_to_head(
        routes,
        table,
        head,
        &conn.input[..conn.len],
        &mut conn.output[..],
    ) {
        Ok(EncodedStaticResponse {
            length,
            keep_open,
            drain_total,
            interim_len,
        }) => {
            conn.out_sent = 0;
            conn.write_blocked = false;
            conn.closing = !keep_open;
            conn.close_reason = ConnClose::Completed;
            // NOTE: bytes already buffered past the head are body bytes by position, so
            // skip them in place and drain the rest across pumps; the head is dropped now
            // and keep-alive reuse below treats the remainder as pipelined tail. The
            // encoded response stays unpublished until the drain finishes, so an idle
            // drain waits on poll instead of spinning the worker turn.
            let buffered = (conn.len - conn.head_len) as u64;
            let skip = buffered.min(drain_total) as usize;
            conn.input.copy_within(conn.head_len + skip..conn.len, 0);
            conn.len -= conn.head_len + skip;
            conn.head_len = 0;
            let remaining = drain_total - skip as u64;
            if remaining == 0 {
                conn.out_len = length;
                conn.phase = ConnPhase::SendingResponse;
            } else {
                conn.out_len = interim_len;
                conn.pending_len = length;
                conn.phase = ConnPhase::DrainingBody { remaining };
                conn.read_ready = true;
            }
            ExchangeOutcome::Responded
        }
        Err(_) => ExchangeOutcome::Close(ConnClose::ResponseOverflow),
    }
}

fn send_bad_request(conn: &mut ConnState) {
    conn.close_reason = ConnClose::ProtocolError;
    match respond_bad_request(&mut conn.output[..]) {
        Ok(length) => {
            conn.out_len = length;
            conn.out_sent = 0;
            conn.phase = ConnPhase::SendingResponse;
            conn.write_blocked = false;
            conn.closing = true;
        }
        Err(_) => {
            conn.out_len = 0;
            conn.out_sent = 0;
            conn.phase = ConnPhase::SendingResponse;
            conn.closing = true;
        }
    }
}

fn send_expectation_failed(conn: &mut ConnState) {
    conn.close_reason = ConnClose::Completed;
    match respond_expectation_failed(&mut conn.output[..]) {
        Ok(length) => {
            conn.out_len = length;
            conn.out_sent = 0;
            conn.phase = ConnPhase::SendingResponse;
            conn.write_blocked = false;
            conn.closing = true;
        }
        Err(_) => {
            conn.out_len = 0;
            conn.out_sent = 0;
            conn.phase = ConnPhase::SendingResponse;
            conn.closing = true;
        }
    }
}

fn reuse_keep_alive(conn: &mut ConnState) {
    let tail = conn.len - conn.head_len;
    conn.input.copy_within(conn.head_len..conn.len, 0);
    conn.len = tail;
    conn.head_len = 0;
    conn.out_len = 0;
    conn.out_sent = 0;
    conn.phase = ConnPhase::ReadingHead;
    conn.parser.reset();
    conn.read_ready = tail > 0;
}
