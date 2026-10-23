pub(crate) mod parked;
pub(crate) mod streams;

pub(crate) use parked::{pull_staged_body, push_staged_body};

use veylor_http1::{BodyDecoder, Header, Limits, ParseError, Parser, Span, is_token};
use veylor_router::{RouteKind, RouteTable};
use veylor_runtime::{
    CloseReason, ConnectionToken, IoBatch, ReadStatus, TcpStream, WriteStatus, dispatch,
};

use crate::host::{HOST_MAX_METHOD_BYTES, HostShared};
use crate::static_files::FilePlan;
#[cfg(not(target_os = "linux"))]
use crate::static_files::read_chunk;
use crate::static_routes::{
    EncodedStaticResponse, StaticRoutes, respond_bad_request, respond_expectation_failed,
    respond_to_head,
};

pub const INPUT_CAP: usize = 8192;
pub const OUTPUT_CAP: usize = 8192;
pub const HEADER_SLOTS: usize = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedTrailer {
    pub(crate) name: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TrailerOutcome {
    Ok,
    NotReady,
    #[default]
    Stale,
    Gone,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TrailerReport {
    pub outcome: TrailerOutcome,
    pub fields: usize,
    pub names_copied: usize,
    pub values_copied: usize,
    pub names_required: usize,
    pub values_required: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnPhase {
    ReadingHead,
    DrainingBody {
        remaining: u64,
        responded: bool,
    },
    SendingResponse,
    // NOTE: in-flight file body after its head flushed; offsets ride conn.file and every
    // body byte stages through the output buffer, so this variant stays fieldless + Copy.
    SendingFile,
    // NOTE: the owner-side parked snapshot: method copy for HEAD semantics at respond time,
    // keep-open from the request line alone, fixed-length body admission with the event token
    // so worker turns can stage wire bytes for host pulls; unread bytes drain within the cap.
    Parked {
        route_id: u64,
        method_len: u8,
        method: [u8; HOST_MAX_METHOD_BYTES],
        keep_open: bool,
        body_total: u64,
        body_received: u64,
        event_token: ConnectionToken,
        chunked: bool,
        chunked_done: bool,
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

#[derive(Clone, Debug)]
pub(crate) struct FileTransfer {
    plan: FilePlan,
    offset: u64,
}

pub struct ConnState {
    pub(crate) token: ConnectionToken,
    pub(crate) stream: TcpStream,
    pub(crate) parser: Parser,
    pub(crate) headers: Box<[Header; HEADER_SLOTS]>,
    pub(crate) input: Box<[u8; INPUT_CAP]>,
    pub(crate) chunked_decoder: Option<BodyDecoder>,
    pub(crate) trailer_allow: Vec<Vec<u8>>,
    pub(crate) trailer_conn: Vec<u8>,
    pub(crate) trailer_slots: [Header; crate::connection::parked::TRAILER_SLOTS],
    pub(crate) trailer_staged: usize,
    pub(crate) len: usize,
    pub(crate) head_len: usize,
    pub(crate) output: Box<[u8; OUTPUT_CAP]>,
    pub(crate) out_len: usize,
    pub(crate) out_sent: usize,
    pub(crate) pending_len: usize,
    pub(crate) phase: ConnPhase,
    pub(crate) file: Option<FileTransfer>,
    pub(crate) read_ready: bool,
    pub(crate) write_blocked: bool,
    pub(crate) closing: bool,
    pub(crate) close_reason: ConnClose,
    pub(crate) error_hint: bool,
}

impl ConnState {
    pub fn new(token: ConnectionToken, stream: TcpStream) -> Self {
        Self {
            token,
            stream,
            parser: Parser::new(),
            headers: Box::new([Header::default(); HEADER_SLOTS]),
            input: Box::new([0; INPUT_CAP]),
            chunked_decoder: None,
            trailer_allow: Vec::new(),
            trailer_conn: Vec::new(),
            trailer_slots: [Header::default(); crate::connection::parked::TRAILER_SLOTS],
            trailer_staged: 0,
            len: 0,
            head_len: 0,
            output: Box::new([0; OUTPUT_CAP]),
            out_len: 0,
            out_sent: 0,
            pending_len: 0,
            phase: ConnPhase::ReadingHead,
            file: None,
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

    pub fn begin_drain(&mut self) {
        self.closing = true;
    }

    pub(crate) fn file_complete(&self) -> bool {
        self.file
            .as_ref()
            .is_some_and(|transfer| transfer.offset >= transfer.plan.size)
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
        return parked::pump_parked(conn, host);
    }
    // NOTE: zero-copy staging split: Linux moves file bytes straight to the socket with
    // sendfile while other targets stage one read_chunk slice through the output buffer;
    // both cap a turn at the same byte width off the same transfer offset, so keep-alive
    // reuse, pipelined tail, and shutdown drain stay byte-identical while a short send
    // still fails closed as truncation. Rust ignores SIGPIPE process-wide, so a closed
    // receiver surfaces here as EPIPE/PeerClosed, never as a signal; EAGAIN backpressures
    // through write_blocked exactly like a blocked buffer flush.
    #[cfg(target_os = "linux")]
    let mut file_sent: u64 = 0;
    #[cfg(not(target_os = "linux"))]
    let file_sent: u64 = 0;
    #[cfg(target_os = "linux")]
    if conn.phase == ConnPhase::SendingFile && !conn.has_pending_output() {
        match sendfile_file_chunk(conn, byte_budget) {
            Ok(sent) => file_sent = sent,
            Err(reason) => return PumpOutcome::Close(reason),
        }
    }
    #[cfg(not(target_os = "linux"))]
    if conn.phase == ConnPhase::SendingFile && !conn.has_pending_output() {
        if let Err(reason) = stage_file_chunk(conn) {
            return PumpOutcome::Close(reason);
        }
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
    let sending = conn.phase == ConnPhase::SendingResponse || conn.phase == ConnPhase::SendingFile;
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
    progress.bytes_written = accepted as u64 + file_sent;
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

    if let ConnPhase::DrainingBody {
        remaining,
        responded,
    } = conn.phase
    {
        let skip = (conn.len as u64).min(remaining) as usize;
        conn.input.copy_within(skip..conn.len, 0);
        conn.len -= skip;
        let left = remaining - skip as u64;
        if left == 0 {
            conn.out_len = conn.pending_len;
            conn.pending_len = 0;
            conn.phase = if conn.file.is_some() {
                ConnPhase::SendingFile
            } else {
                ConnPhase::SendingResponse
            };
            conn.write_blocked = false;
            // NOTE: a parked respond already moved the worker to WritingResponse, so the
            // drain-complete hook must not transition twice; static drains still report it.
            progress.entered_response = !responded;
        } else {
            conn.phase = ConnPhase::DrainingBody {
                remaining: left,
                responded,
            };
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
        if conn.phase == ConnPhase::SendingResponse || conn.phase == ConnPhase::SendingFile {
            progress.entered_response = true;
        } else if matches!(conn.phase, ConnPhase::DrainingBody { .. }) {
            progress.entered_body = true;
        }
    }

    // NOTE: a file transfer completes only when its staged offset reaches plan size, so
    // partial flushes keep their WritingResponse deadline via byte progress; the re-arm
    // keeps the poll wait at zero between chunks instead of paying a cadence per slice.
    // Shutdown mid-transfer drains the same way: begin_drain set closing, so completion
    // closes while the force deadline still force-closes through the normal cleanup.
    let response_done = conn.phase == ConnPhase::SendingResponse
        || (conn.phase == ConnPhase::SendingFile && conn.file_complete());
    if response_done && !conn.has_pending_output() {
        if conn.closing {
            return PumpOutcome::Close(conn.close_reason);
        }
        conn.file = None;
        reuse_keep_alive(conn);
        progress.response_flushed = true;
    } else if conn.phase == ConnPhase::SendingFile && !conn.has_pending_output() {
        conn.read_ready = true;
    }
    PumpOutcome::Keep(progress)
}

pub(crate) enum ExchangeOutcome {
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
            let cap = crate::static_routes::route_cap(found.body);
            let method = head.method(&conn.input[..conn.len]).unwrap_or_default();
            let mut snapshot = [0u8; HOST_MAX_METHOD_BYTES];
            let take = method.len().min(HOST_MAX_METHOD_BYTES);
            snapshot[..take].copy_from_slice(&method[..take]);
            let trailers = routes.managed_trailers(&snapshot[..take], found.pattern);
            parked::park_request(
                conn,
                head,
                found.route_id,
                worker_id,
                host,
                draining,
                cap,
                trailers,
            )
        }
        Ok(_) | Err(_) => encode_static(conn, routes, table, head),
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
            file,
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
        Err(_) => ExchangeOutcome::Close(ConnClose::ResponseOverflow),
    }
}

// NOTE: truncation rule: read_chunk clamps to the plan remainder, so over-long reads
// cannot happen; a zero-byte read before plan size means the file shrank underfoot and
// fails the transfer closed, never as a short success. Each stage re-opens the file
// (no fd cache yet); EPIPE and backpressure stay in the dispatch mapping, and Rust
// ignores SIGPIPE process-wide, so a closed receiver closes as PeerClosed, never a
// signal. Partial short reads advance the offset and resume on the next turn.
#[cfg(not(target_os = "linux"))]
fn stage_file_chunk(conn: &mut ConnState) -> Result<(), ConnClose> {
    let Some(transfer) = conn.file.as_mut() else {
        return Err(ConnClose::TransportError);
    };
    if transfer.offset >= transfer.plan.size {
        return Ok(());
    }
    let width = (transfer.plan.size - transfer.offset).min(OUTPUT_CAP as u64) as usize;
    match read_chunk(&transfer.plan, transfer.offset, &mut conn.output[..width]) {
        Ok(0) => Err(ConnClose::TransportError),
        Ok(staged) => {
            transfer.offset += staged as u64;
            conn.out_len = staged;
            conn.out_sent = 0;
            conn.write_blocked = false;
            conn.read_ready = true;
            Ok(())
        }
        Err(_) => Err(ConnClose::TransportError),
    }
}

// NOTE: a shrink racing a send can zero-fill inside the plan bounds (one call straddled
// an ftruncate in testing: zeroed partial-page tail plus a not-yet-discarded page), where
// read_chunk would serve a short read instead. The transfer still fails closed either way:
// the offset stalls below plan size and the next turn reads zero as truncation. Bytes past
// plan.size never serve (count clamp) and only this file's pages ever map (no cross-file
// leak); the race window is one syscall wide and shrinks with nothing here to shrink it.
#[cfg(target_os = "linux")]
fn sendfile_file_chunk(conn: &mut ConnState, byte_budget: u64) -> Result<u64, ConnClose> {
    let Some(transfer) = conn.file.as_ref() else {
        return Err(ConnClose::TransportError);
    };
    if transfer.offset >= transfer.plan.size {
        return Ok(0);
    }
    let width = (transfer.plan.size - transfer.offset)
        .min(OUTPUT_CAP as u64)
        .min(byte_budget) as usize;
    if width == 0 || conn.write_blocked {
        return Ok(0);
    }
    let file = match std::fs::File::open(&transfer.plan.fs_path) {
        Ok(file) => file,
        Err(_) => return Err(ConnClose::TransportError),
    };
    let mut cursor = transfer.offset;
    for _ in 0..2 {
        match rustix::fs::sendfile(&conn.stream, &file, Some(&mut cursor), width) {
            Ok(sent) => {
                if sent == 0 {
                    return Err(ConnClose::TransportError);
                }
                let Some(transfer) = conn.file.as_mut() else {
                    return Err(ConnClose::TransportError);
                };
                transfer.offset = cursor;
                return Ok(sent as u64);
            }
            Err(error) if error == rustix::io::Errno::INTR => continue,
            Err(error) => return sendfile_close(conn, error),
        }
    }
    Ok(0)
}

#[cfg(target_os = "linux")]
fn sendfile_close(conn: &mut ConnState, error: rustix::io::Errno) -> Result<u64, ConnClose> {
    if error == rustix::io::Errno::AGAIN {
        conn.write_blocked = true;
        return Ok(0);
    }
    if error == rustix::io::Errno::PIPE {
        return Err(ConnClose::PeerClosed);
    }
    if error == rustix::io::Errno::CONNRESET {
        return Err(ConnClose::Reset);
    }
    Err(ConnClose::TransportError)
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

// NOTE: the worker-side decoder runs the response trailer policy, which permits keep-alive and
// proxy-connection; RFC 9110 forbids both in request trailers, so the allowlist gate rejects them
// here alongside unlisted and Connection-nominated names. Decoder verdicts stay authoritative for
// syntax and its own prohibited set; anything refused anywhere fails the connection closed.
pub(crate) fn trailer_allowed(allow: &[Vec<u8>], connection: &[u8], name: &[u8]) -> bool {
    if name.eq_ignore_ascii_case(b"keep-alive") || name.eq_ignore_ascii_case(b"proxy-connection") {
        return false;
    }
    if !allow
        .iter()
        .any(|allowed| name.eq_ignore_ascii_case(allowed))
    {
        return false;
    }
    !connection_nominates(connection, name)
}

pub(crate) fn connection_nominates(value: &[u8], name: &[u8]) -> bool {
    if value.is_empty() {
        return false;
    }
    for option in value.split(|byte| *byte == b',') {
        let trimmed = trim_ows(option);
        if trimmed.is_empty() || !trimmed.iter().copied().all(is_token) {
            return true;
        }
        if trimmed.eq_ignore_ascii_case(name) {
            return true;
        }
    }
    false
}

pub(crate) fn connection_value<'a>(
    headers: &'a [Header],
    head: veylor_http1::RequestHead,
    input: &'a [u8],
) -> &'a [u8] {
    for header in headers.iter().take(head.header_count) {
        let (Some(name), Some(value)) = (field(input, header.name), field(input, header.value))
        else {
            continue;
        };
        if name.eq_ignore_ascii_case(b"connection") {
            return value;
        }
    }
    b""
}

fn trim_ows(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| *byte != b' ' && *byte != b'\t');
    let end = value
        .iter()
        .rposition(|byte| *byte != b' ' && *byte != b'\t');
    match (start, end) {
        (Some(start), Some(end)) => &value[start..end + 1],
        _ => b"",
    }
}

pub(crate) fn field(input: &[u8], span: Span) -> Option<&[u8]> {
    input.get(span.offset..span.offset.checked_add(span.length)?)
}
