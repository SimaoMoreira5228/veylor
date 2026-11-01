pub(crate) mod chunked;
pub(crate) mod exchange;
pub(crate) mod file;
pub(crate) mod parked;
pub(crate) mod streams;
pub(crate) mod tls;
pub(crate) mod trailers;
mod tunnel;

pub(crate) use exchange::{ExchangeOutcome, parse_exchange};
#[cfg(target_os = "linux")]
use file::sendfile_file_chunk;
#[cfg(target_os = "linux")]
pub use file::{FD_CACHE_ENTRIES, FdCache};
use file::{FileTransfer, stage_file_chunk};
pub(crate) use parked::{pull_staged_body, push_staged_body};
pub use trailers::{TrailerOutcome, TrailerReport};

use tls::{TLS_PENDING_MIN_BYTES, tls_alpn_is_h2, tls_encrypt_output, tls_turn_active};

use veylor_http1::{BodyDecoder, ConnectionHeader, Header, Parser, RequestVersion};
use veylor_router::RouteTable;
use veylor_runtime::{
    CloseReason, ConnectionToken, IoBatch, ReadStatus, TcpStream, WriteStatus, dispatch,
};
use veylor_tls::TlsConn;

use crate::host::{HOST_MAX_METHOD_BYTES, HostShared};
use crate::static_routes::StaticRoutes;

pub const INPUT_CAP: usize = 8192;
pub const OUTPUT_CAP: usize = 8192;
// NOTE: H1 request-head slots; the H2 turn has its own HEADER_SLOTS (= 64) because decoded
// HPACK entries carry dynamic-table state and cost more per slot than these plain ranges.
// A peer sending 65-100 headers is served over H1 and refused over H2 — protocol cost,
// not an inconsistency; keep both numbers beside their owner, not unified.
pub const HEADER_SLOTS: usize = 100;

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
    // NOTE: the owner-side parked snapshot keeps method and version for response framing,
    // plus the connection verdict, body admission, and event token for staged host dispatch.
    Parked {
        route_id: u64,
        method_len: u8,
        method: [u8; HOST_MAX_METHOD_BYTES],
        request_version: RequestVersion,
        connection: ConnectionHeader,
        body_total: u64,
        body_received: u64,
        event_token: ConnectionToken,
        chunked: bool,
        chunked_done: bool,
    },
    Tunnel,
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
    Abused,
}

pub struct ConnState {
    pub(crate) token: ConnectionToken,
    pub(crate) stream: TcpStream,
    pub(crate) parser: Parser,
    pub(crate) headers: Option<Box<[Header; HEADER_SLOTS]>>,
    pub(crate) input: Option<Box<[u8; INPUT_CAP]>>,
    pub(crate) chunked_decoder: Option<BodyDecoder>,
    pub(crate) trailer_allow: Vec<Vec<u8>>,
    pub(crate) trailer_conn: Vec<u8>,
    pub(crate) trailer_slots: Option<Box<[Header; crate::connection::parked::TRAILER_SLOTS]>>,
    pub(crate) trailer_staged: usize,
    pub(crate) len: usize,
    pub(crate) batched_read: usize,
    pub(crate) head_len: usize,
    pub(crate) output: Option<Box<[u8; OUTPUT_CAP]>>,
    pub(crate) out_len: usize,
    pub(crate) out_sent: usize,
    pub(crate) pending_len: usize,
    pub(crate) phase: ConnPhase,
    pub(crate) file: Option<FileTransfer>,
    #[cfg(target_os = "linux")]
    pub(crate) file_cache: FdCache,
    pub(crate) read_ready: bool,
    pub(crate) write_blocked: bool,
    pub(crate) defer_write: bool,
    pub(crate) write_deferred: bool,
    pub(crate) deferred_sent: usize,
    pub(crate) write_interest: bool,
    pub(crate) closing: bool,
    pub(crate) close_reason: ConnClose,
    pub(crate) error_hint: bool,
    pub(crate) tls: Option<Box<TlsConn>>,
    pub(crate) tunnel_token: ConnectionToken,
    pub(crate) tunnel_eof: bool,
    pub(crate) tunnel_notified: bool,
    pub(crate) tls_pending: Vec<u8>,
    pub(crate) tls_pending_sent: usize,
    pub(crate) tls_plain: Vec<u8>,
}

impl ConnState {
    pub fn new(token: ConnectionToken, stream: TcpStream) -> Self {
        Self {
            token,
            stream,
            parser: Parser::new(),
            headers: None,
            input: None,
            chunked_decoder: None,
            trailer_allow: Vec::new(),
            trailer_conn: Vec::new(),
            trailer_slots: None,
            trailer_staged: 0,
            len: 0,
            batched_read: 0,
            head_len: 0,
            output: None,
            out_len: 0,
            out_sent: 0,
            pending_len: 0,
            phase: ConnPhase::ReadingHead,
            file: None,
            #[cfg(target_os = "linux")]
            file_cache: FdCache::default(),
            read_ready: true,
            write_blocked: false,
            defer_write: false,
            write_deferred: false,
            deferred_sent: 0,
            write_interest: false,
            closing: false,
            close_reason: ConnClose::Completed,
            error_hint: false,
            tls: None,
            tunnel_token: ConnectionToken::default(),
            tunnel_eof: false,
            tunnel_notified: false,
            tls_pending: Vec::new(),
            tls_pending_sent: 0,
            tls_plain: Vec::new(),
        }
    }

    pub(crate) fn trailer_slots(&self) -> &[Header; crate::connection::parked::TRAILER_SLOTS] {
        self.trailer_slots
            .as_deref()
            .expect("trailer slots are materialized before use")
    }

    pub(crate) fn attach_tls(&mut self, tls: TlsConn) {
        self.tls = Some(Box::new(tls));

        if self.tls_pending.capacity() < TLS_PENDING_MIN_BYTES {
            self.tls_pending
                .reserve(TLS_PENDING_MIN_BYTES - self.tls_pending.capacity());
        }

        if self.tls_plain.capacity() < INPUT_CAP {
            self.tls_plain
                .reserve(INPUT_CAP - self.tls_plain.capacity());
        }
    }

    pub(crate) fn has_tls(&self) -> bool {
        self.tls.is_some()
    }

    pub(crate) fn tls_unflushed(&self) -> usize {
        self.tls_pending.len().saturating_sub(self.tls_pending_sent)
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
        self.out_sent < self.out_len || self.tls_pending_sent < self.tls_pending.len()
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

    pub fn into_stream(self) -> TcpStream {
        self.stream
    }

    // NOTE: laziness invariant: a new or fully idle conn holds no slabs; first readable
    // activity materializes input, first parse headers, first encode output. A present slab
    // always covers its scalar window (len/head_len/out_*), so an absent slab is an empty
    // window and every miss path heals to idle or fails closed, never panics.
    pub(crate) fn materialize_input(
        slot: &mut Option<Box<[u8; INPUT_CAP]>>,
    ) -> &mut [u8; INPUT_CAP] {
        slot.get_or_insert_with(|| Box::new([0; INPUT_CAP]))
    }

    pub(crate) fn materialize_headers(
        slot: &mut Option<Box<[Header; HEADER_SLOTS]>>,
    ) -> &mut [Header; HEADER_SLOTS] {
        slot.get_or_insert_with(|| Box::new([Header::default(); HEADER_SLOTS]))
    }

    pub(crate) fn materialize_output(
        slot: &mut Option<Box<[u8; OUTPUT_CAP]>>,
    ) -> &mut [u8; OUTPUT_CAP] {
        slot.get_or_insert_with(|| Box::new([0; OUTPUT_CAP]))
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
    pub tunnel_upgraded: bool,
    pub tunnel_draining: bool,
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
    if conn.phase == ConnPhase::Tunnel {
        return tunnel::pump_tunnel(conn, host, byte_budget);
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
        if conn.has_tls() {
            if let Err(reason) = stage_file_chunk(conn) {
                return PumpOutcome::Close(reason);
            }
        } else {
            match sendfile_file_chunk(conn, byte_budget) {
                Ok(sent) => file_sent = sent,
                Err(reason) => return PumpOutcome::Close(reason),
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    if conn.phase == ConnPhase::SendingFile && !conn.has_pending_output() {
        if let Err(reason) = stage_file_chunk(conn) {
            return PumpOutcome::Close(reason);
        }
    }
    if conn.has_tls() {
        return pump_tls_full(
            conn,
            routes,
            table,
            byte_budget,
            worker_id,
            host,
            draining,
            file_sent,
        );
    }
    let draining_body = matches!(conn.phase, ConnPhase::DrainingBody { .. });
    let reading = (conn.phase == ConnPhase::ReadingHead || draining_body) && conn.len < INPUT_CAP;
    let token = conn.token;
    let batched_read = conn.batched_read;
    conn.batched_read = 0;
    let mut progress = PumpProgress {
        bytes_read: batched_read as u64,
        ..PumpProgress::default()
    };
    let read_ready = conn.read_ready;
    let out_sent = conn.out_sent;
    let out_len = conn.out_len;
    // NOTE: the encoded final response is held while draining and no final byte is written until
    // the declared body is fully skipped, so a drained exchange keeps single-outstanding-response
    // order. An admitted 100-continue interim is staged ahead of the final and stays writable
    // during the drain; dispatch serves the write first and pauses reads while it is pending, so
    // the interim always flushes before further drain bytes are consumed from the wire.
    // NOTE: this flush-before-drain order is the pump's transport half; the verdicts (who gets
    // an interim, the 100 shape) are shared policy in veylor_http1. The host-driven conn crate
    // mirrors the order via its return-to-Idle 100 state, so only the mechanism differs.
    let sending = conn.phase == ConnPhase::SendingResponse || conn.phase == ConnPhase::SendingFile;
    let flushing_interim =
        matches!(conn.phase, ConnPhase::DrainingBody { .. }) && out_sent < out_len;
    let wants_write = (sending || flushing_interim) && out_sent < out_len && !conn.write_blocked;
    let defer = conn.defer_write && conn.phase == ConnPhase::SendingResponse;
    if defer && wants_write {
        conn.write_deferred = true;
    }
    let write_ready = wants_write && !defer;
    // NOTE: slabs materialize on activity, not on phase: a quiet conn (reads wanted but
    // nothing signaled) keeps zero slabs through pump turns; the empty scratch dispatches
    // an Idle read that attempts nothing.
    if reading && read_ready {
        ConnState::materialize_input(&mut conn.input);
    }
    let len = conn.len;
    let read_scratch: &mut [u8] = match conn.input.as_mut() {
        Some(input) => &mut input[len..],
        None => &mut [],
    };
    let write_src: &[u8] = match conn.output.as_ref() {
        Some(output) => &output[out_sent..out_len],
        None => &[],
    };
    let mut batch = [IoBatch::new(
        token,
        &mut conn.stream,
        read_scratch,
        read_ready,
        write_src,
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
            progress.bytes_read += bytes as u64;
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
        if let Some(input) = conn.input.as_mut() {
            input.copy_within(skip..conn.len, 0);
        }
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
            ExchangeOutcome::NeedMore | ExchangeOutcome::Responded => {}
            ExchangeOutcome::Parked => progress.entered_park = true,
            ExchangeOutcome::Close(reason) => return PumpOutcome::Close(reason),
        }
        if conn.phase == ConnPhase::SendingResponse || conn.phase == ConnPhase::SendingFile {
            progress.entered_response = true;
        } else if matches!(conn.phase, ConnPhase::DrainingBody { .. }) {
            progress.entered_body = true;
        }
    }

    // NOTE: the encode above happens after this turn's dispatch, so without this flush the
    // response would wait a whole poll turn for its write. A response just encoded into an
    // empty socket writes immediately; blocked/oversize tails keep the normal next-turn path.
    if progress.entered_response
        && conn.phase == ConnPhase::SendingResponse
        && conn.file.is_none()
        && !conn.write_blocked
        && progress.bytes_written == 0
    {
        if conn.defer_write {
            conn.write_deferred = true;
        } else {
            let out_sent = conn.out_sent;
            let out_len = conn.out_len;
            if out_len > out_sent {
                let write_src = match conn.output.as_ref() {
                    Some(output) => &output[out_sent..out_len],
                    None => &[][..],
                };
                let budget = byte_budget.saturating_sub(progress.bytes_read);
                if !write_src.is_empty() && budget > 0 {
                    let mut batch = [IoBatch::new(
                        token,
                        &mut conn.stream,
                        &mut [],
                        false,
                        write_src,
                        true,
                    )];
                    dispatch(budget, &mut batch);
                    match batch[0].write {
                        WriteStatus::Progress(accepted)
                        | WriteStatus::WouldBlock(accepted)
                        | WriteStatus::BudgetExhausted(accepted)
                        | WriteStatus::Interrupted(accepted) => {
                            if matches!(batch[0].write, WriteStatus::WouldBlock(_)) {
                                conn.write_blocked = true;
                            }
                            conn.out_sent += accepted;
                            progress.bytes_written = accepted as u64;
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
                        WriteStatus::Idle => {}
                    }
                }
            }
        }
    }

    // NOTE: a file transfer completes only when its staged offset reaches plan size, so
    // partial flushes keep their WritingResponse deadline via byte progress; the re-arm
    // keeps the poll wait at zero between chunks instead of paying a cadence per slice.
    // Shutdown mid-transfer drains the same way: begin_drain set closing, so completion
    // closes while the force deadline still force-closes through the normal cleanup.
    let file_done = matches!(&conn.file, Some(transfer) if transfer.offset >= transfer.plan.size);
    let response_done = conn.phase == ConnPhase::SendingResponse
        || (conn.phase == ConnPhase::SendingFile && file_done);
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

#[allow(clippy::too_many_arguments)]
fn pump_tls_full(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    byte_budget: u64,
    worker_id: u32,
    host: &HostShared,
    draining: bool,
    file_sent: u64,
) -> PumpOutcome {
    let mut progress = PumpProgress::default();
    let read_ready_before = conn.read_ready;

    if conn.len < INPUT_CAP {
        ConnState::materialize_input(&mut conn.input);
    }

    if let Err(reason) = tls_turn_active(conn, byte_budget, &mut progress) {
        return PumpOutcome::Close(reason);
    }

    progress.bytes_written += file_sent;

    if conn.phase == ConnPhase::ReadingHead
        && (progress.bytes_read > 0 || (conn.len > 0 && read_ready_before))
    {
        progress.resumed_idle = true;
    }

    if let ConnPhase::DrainingBody {
        remaining,
        responded,
    } = conn.phase
    {
        let skip = (conn.len as u64).min(remaining) as usize;

        if let Some(input) = conn.input.as_mut() {
            input.copy_within(skip..conn.len, 0);
        }

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
            progress.entered_response = !responded;
        } else {
            conn.phase = ConnPhase::DrainingBody {
                remaining: left,
                responded,
            };
        }
    }

    if tls_alpn_is_h2(conn) {
        return PumpOutcome::Keep(progress);
    }

    if conn.phase == ConnPhase::ReadingHead {
        match parse_exchange(conn, routes, table, worker_id, host, draining) {
            ExchangeOutcome::NeedMore | ExchangeOutcome::Responded => {}
            ExchangeOutcome::Parked => progress.entered_park = true,
            ExchangeOutcome::Close(reason) => return PumpOutcome::Close(reason),
        }

        if conn.phase == ConnPhase::SendingResponse || conn.phase == ConnPhase::SendingFile {
            progress.entered_response = true;
        } else if matches!(conn.phase, ConnPhase::DrainingBody { .. }) {
            progress.entered_body = true;
        }
    }

    if !conn.tls.as_ref().is_some_and(|tls| tls.is_handshaking())
        && conn.out_sent < conn.out_len
        && tls_encrypt_output(conn).is_err()
    {
        return PumpOutcome::Close(ConnClose::TransportError);
    }

    let file_done = matches!(&conn.file, Some(transfer) if transfer.offset >= transfer.plan.size);
    let response_done = conn.phase == ConnPhase::SendingResponse
        || (conn.phase == ConnPhase::SendingFile && file_done);

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

pub(crate) fn reuse_keep_alive(conn: &mut ConnState) {
    // NOTE: mechanics only, not the reuse decision: admission is the shared
    // veylor_http1::response_permits_reuse verdict folded into conn.closing at the encode
    // sites (static/parked keep_open) with request-body drain enforced by the DrainingBody
    // phase; this function only shifts the pipelined tail, resets the parser, and re-arms
    // read_ready for the next exchange.
    let tail = conn.len - conn.head_len;
    if tail > 0 {
        if let Some(input) = conn.input.as_mut() {
            input.copy_within(conn.head_len..conn.len, 0);
        }
    }
    conn.len = tail;
    conn.head_len = 0;
    conn.out_len = 0;
    conn.out_sent = 0;
    conn.phase = ConnPhase::ReadingHead;
    conn.parser.reset();
    conn.read_ready = tail > 0;
}

#[cfg(test)]
mod tests {
    use super::*;
    use veylor_router::RouteSlot;

    fn test_token() -> ConnectionToken {
        ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 1,
            generation: 1,
        }
    }

    fn test_conn() -> ConnState {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind rig");
        let addr = listener.local_addr().expect("rig addr");
        let stream = TcpStream::connect(addr).expect("rig connect");
        ConnState::new(test_token(), stream)
    }

    #[test]
    fn new_conn_holds_no_slabs() {
        let conn = test_conn();
        assert!(conn.input.is_none());
        assert!(conn.headers.is_none());
        assert!(conn.output.is_none());
        assert!(conn.is_idle());
    }

    #[test]
    fn first_exchange_materializes_and_reuse_retains_slabs_while_quiet() {
        let mut conn = test_conn();
        let request = b"GET /nope HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
        let input = ConnState::materialize_input(&mut conn.input);
        input[..request.len()].copy_from_slice(request);
        conn.len = request.len();
        let routes = StaticRoutes::new();
        let mut slots = [RouteSlot::EMPTY; 8];
        let table = routes.build_table(&mut slots).expect("empty table");
        let host = HostShared::new(1);
        match parse_exchange(&mut conn, &routes, &table, 0, &host, false) {
            ExchangeOutcome::Responded => {}
            ExchangeOutcome::NeedMore => panic!("full head must parse"),
            ExchangeOutcome::Parked => panic!("no managed routes registered"),
            ExchangeOutcome::Close(_) => panic!("unknown path must encode, not close"),
        }
        assert!(conn.input.is_some());
        assert!(conn.headers.is_some());
        assert!(conn.output.is_some());
        assert!(conn.out_len > 0);
        conn.out_sent = conn.out_len;
        reuse_keep_alive(&mut conn);
        assert!(conn.input.is_some());
        assert!(conn.headers.is_some());
        assert!(conn.output.is_some());
        assert!(conn.is_idle());
        assert!(!conn.read_ready);
        let input_ptr = conn.input.as_ref().map(|slab| slab.as_ptr());
        let headers_ptr = conn.headers.as_ref().map(|slab| slab.as_ptr());
        let output_ptr = conn.output.as_ref().map(|slab| slab.as_ptr());
        let _ = pump_connection(&mut conn, &routes, &table, u64::MAX, 0, &host, false);
        assert_eq!(conn.input.as_ref().map(|slab| slab.as_ptr()), input_ptr);
        assert_eq!(conn.headers.as_ref().map(|slab| slab.as_ptr()), headers_ptr);
        assert_eq!(conn.output.as_ref().map(|slab| slab.as_ptr()), output_ptr);
        assert!(conn.is_idle());
    }

    #[test]
    fn hot_reuse_cycles_never_realloc_slabs() {
        let mut conn = test_conn();
        let request = b"GET /nope HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n";
        let routes = StaticRoutes::new();
        let mut slots = [RouteSlot::EMPTY; 8];
        let table = routes.build_table(&mut slots).expect("empty table");
        let host = HostShared::new(1);
        for _ in 0..8 {
            let input = ConnState::materialize_input(&mut conn.input);
            input[..request.len()].copy_from_slice(request);
            conn.len = request.len();
            conn.read_ready = true;
            match parse_exchange(&mut conn, &routes, &table, 0, &host, false) {
                ExchangeOutcome::Responded => {}
                ExchangeOutcome::NeedMore => panic!("full head must parse"),
                ExchangeOutcome::Parked => panic!("no managed routes registered"),
                ExchangeOutcome::Close(_) => panic!("unknown path must encode, not close"),
            }
            conn.out_sent = conn.out_len;
            reuse_keep_alive(&mut conn);
        }
        let input_ptr = conn.input.as_ref().map(|slab| slab.as_ptr());
        let headers_ptr = conn.headers.as_ref().map(|slab| slab.as_ptr());
        let output_ptr = conn.output.as_ref().map(|slab| slab.as_ptr());
        assert!(input_ptr.is_some());
        assert!(headers_ptr.is_some());
        assert!(output_ptr.is_some());
        for _ in 0..8 {
            let input = ConnState::materialize_input(&mut conn.input);
            input[..request.len()].copy_from_slice(request);
            conn.len = request.len();
            conn.read_ready = true;
            match parse_exchange(&mut conn, &routes, &table, 0, &host, false) {
                ExchangeOutcome::Responded => {}
                ExchangeOutcome::NeedMore => panic!("full head must parse"),
                ExchangeOutcome::Parked => panic!("no managed routes registered"),
                ExchangeOutcome::Close(_) => panic!("unknown path must encode, not close"),
            }
            conn.out_sent = conn.out_len;
            reuse_keep_alive(&mut conn);
            assert_eq!(conn.input.as_ref().map(|slab| slab.as_ptr()), input_ptr);
            assert_eq!(conn.headers.as_ref().map(|slab| slab.as_ptr()), headers_ptr);
            assert_eq!(conn.output.as_ref().map(|slab| slab.as_ptr()), output_ptr);
        }
    }
}

#[cfg(all(test, target_pointer_width = "64"))]
mod size_tests {
    use super::*;

    #[test]
    fn plaintext_idle_record_stays_within_budget() {
        assert!(std::mem::size_of::<ConnState>() <= 1024);
    }
}
