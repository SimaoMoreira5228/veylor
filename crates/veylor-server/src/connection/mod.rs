pub(crate) mod parked;
pub(crate) mod streams;

pub(crate) use parked::{pull_staged_body, push_staged_body};

use veylor_http1::{
    BodyDecoder, ConnectionHeader, Header, Limits, ParseError, Parser, Span, is_token,
};
use veylor_router::{RouteKind, RouteTable};
use veylor_runtime::{
    CloseReason, ConnectionToken, IoBatch, ReadStatus, TcpStream, WriteStatus, dispatch,
};
use veylor_tls::{TlsConn, alpn, drain_plaintext, feed_ciphertext};

use crate::host::{HOST_MAX_METHOD_BYTES, HostShared};
use crate::static_files::FilePlan;
use crate::static_files::read_chunk;
use crate::static_routes::{
    EncodedStaticResponse, RouteBuildError, StaticRoutes, respond_bad_request,
    respond_expectation_failed, respond_request_header_fields_too_large, respond_to_head,
    respond_uri_too_long,
};

pub const INPUT_CAP: usize = 8192;
pub const OUTPUT_CAP: usize = 8192;
pub const HEADER_SLOTS: usize = 100;
// NOTE: TLS scratch floors, not bounds: encrypted records may exceed OUTPUT_CAP, so the
// pending buffer pre-reserves 16 KiB while the plaintext side matches INPUT_CAP. Floors,
// not caps — growth beyond them is normal and unbounded by these values.
pub const TLS_PENDING_MIN_BYTES: usize = 16384;
// NOTE: minimal request-line frame around a target (1-byte method + 2 spaces + 8-byte version);
// a target side longer than the line limit behind this frame is target-caused (RFC 9110 15.5.15).
const MIN_REQUEST_LINE_FRAME: usize = 11;
// NOTE: single-entry fd cache (Linux); one handle per conn bounds fds, replace evicts.
#[cfg(target_os = "linux")]
pub const FD_CACHE_ENTRIES: usize = 1;

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
    // shared connection verdict from the request line alone, fixed-length body admission with
    // the event token so worker turns can stage wire bytes for host pulls; unread bytes drain
    // within the cap.
    Parked {
        route_id: u64,
        method_len: u8,
        method: [u8; HOST_MAX_METHOD_BYTES],
        connection: ConnectionHeader,
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

#[cfg(target_os = "linux")]
#[derive(Debug, Default)]
pub struct FdCache {
    dev: u64,
    ino: u64,
    size: u64,
    mtime: Option<std::time::SystemTime>,
    file: Option<std::fs::File>,
    opens: u64,
}

#[cfg(target_os = "linux")]
impl FdCache {
    // NOTE: TOCTOU honesty: plan-time validation stays the boundary; the fresh fd's dev+ino must match the plan's
    // resolve-time pair, so a stat/open swap fails closed (drop + error) instead of serving unapproved bytes. Hit path
    // serves the held fd on path-stat match alone; same-inode mutation keeps the clamp/short-read rules below.
    pub fn opens(&self) -> u64 {
        self.opens
    }
    pub fn cached(&mut self, plan: &FilePlan) -> std::io::Result<&std::fs::File> {
        let meta = std::fs::metadata(&plan.fs_path)?;
        let hit = self.file.is_some() && (self.dev, self.ino) == file_key(&meta);
        let hit = hit && self.size == meta.len() && self.mtime == meta.modified().ok();
        if !hit {
            let file = std::fs::File::open(&plan.fs_path)?;
            let fresh = file.metadata()?;
            if file_key(&fresh) != plan.identity() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "file replaced underfoot",
                ));
            }
            (self.dev, self.ino) = file_key(&fresh);
            self.size = fresh.len();
            self.mtime = fresh.modified().ok();
            self.file = Some(file);
            self.opens += 1;
        }
        self.file
            .as_ref()
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
    }
}

#[cfg(target_os = "linux")]
fn file_key(meta: &std::fs::Metadata) -> (u64, u64) {
    let dev = std::os::unix::fs::MetadataExt::dev(meta);
    (dev, std::os::unix::fs::MetadataExt::ino(meta))
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
    pub(crate) trailer_slots: [Header; crate::connection::parked::TRAILER_SLOTS],
    pub(crate) trailer_staged: usize,
    pub(crate) len: usize,
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
    pub(crate) closing: bool,
    pub(crate) close_reason: ConnClose,
    pub(crate) error_hint: bool,
    pub(crate) tls: Option<TlsConn>,
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
            trailer_slots: [Header::default(); crate::connection::parked::TRAILER_SLOTS],
            trailer_staged: 0,
            len: 0,
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
            closing: false,
            close_reason: ConnClose::Completed,
            error_hint: false,
            tls: None,
            tls_pending: Vec::new(),
            tls_pending_sent: 0,
            tls_plain: Vec::new(),
        }
    }

    pub(crate) fn attach_tls(&mut self, tls: TlsConn) {
        self.tls = Some(tls);

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

pub(crate) fn tls_flush_pending(conn: &mut ConnState, budget: u64) -> Result<u64, ConnClose> {
    let unflushed = conn.tls_pending.len().saturating_sub(conn.tls_pending_sent);

    if unflushed == 0 {
        return Ok(0);
    }

    if conn.write_blocked {
        return Ok(0);
    }

    if budget == 0 {
        return Ok(0);
    }

    let mut total = 0usize;
    let mut attempts = 0;

    while total < unflushed {
        let remaining_budget = budget.saturating_sub(total as u64);

        if remaining_budget == 0 {
            break;
        }

        let start = conn.tls_pending_sent + total;
        let end = conn.tls_pending.len();
        let available = end - start;
        let limit = (remaining_budget as usize).min(available);

        if limit == 0 {
            break;
        }

        let chunk: &[u8] = &conn.tls_pending[start..start + limit];

        let wrote = tls_write_once(&mut conn.stream, chunk);

        match wrote {
            TlsWriteOutcome::Accepted(accepted) => {
                total += accepted;

                if accepted == 0 {
                    break;
                }

                if accepted < limit {
                    break;
                }
            }
            TlsWriteOutcome::Blocked => {
                conn.write_blocked = true;

                break;
            }
            TlsWriteOutcome::Interrupted => {
                attempts += 1;

                if attempts >= 2 {
                    break;
                }
            }
            TlsWriteOutcome::Closed(reason) => {
                conn.tls_pending_sent += total;
                return Err(reason);
            }
        }
    }

    conn.tls_pending_sent += total;

    if conn.tls_pending_sent >= conn.tls_pending.len() {
        conn.tls_pending.clear();
        conn.tls_pending_sent = 0;
    }

    Ok(total as u64)
}

pub(crate) fn tls_encrypt_output(conn: &mut ConnState) -> Result<(), ConnClose> {
    if conn.tls_pending.len() > conn.tls_pending_sent {
        return Ok(());
    }

    let plain_start = conn.out_sent;
    let plain_end = conn.out_len;

    if plain_start >= plain_end {
        return Ok(());
    }

    let Some(output) = conn.output.as_ref() else {
        return Err(ConnClose::TransportError);
    };

    let width = plain_end - plain_start;

    if width > OUTPUT_CAP || plain_end > output.len() {
        return Err(ConnClose::TransportError);
    }

    let Some(tls) = conn.tls.as_mut() else {
        return Err(ConnClose::TransportError);
    };

    conn.tls_pending.clear();
    conn.tls_pending_sent = 0;

    drain_plaintext(tls, &output[plain_start..plain_end], &mut conn.tls_pending)
        .map_err(|_| ConnClose::TransportError)?;

    conn.out_sent = conn.out_len;
    conn.write_blocked = false;

    Ok(())
}

pub(crate) fn tls_generate_flight(conn: &mut ConnState) -> Result<(), ConnClose> {
    let unflushed = conn.tls_pending.len().saturating_sub(conn.tls_pending_sent);

    if unflushed > 0 {
        return Ok(());
    }

    let handshaking = conn.tls.as_ref().is_some_and(|tls| tls.is_handshaking());

    if !handshaking {
        return Ok(());
    }

    if conn.tls.is_none() {
        return Err(ConnClose::TransportError);
    }

    conn.tls_pending.clear();
    conn.tls_pending_sent = 0;

    let Some(tls) = conn.tls.as_mut() else {
        return Err(ConnClose::TransportError);
    };

    drain_plaintext(tls, &[], &mut conn.tls_pending).map_err(|_| ConnClose::TransportError)?;

    if conn.tls_pending.is_empty() {
        return Ok(());
    }

    conn.write_blocked = false;

    Ok(())
}

pub(crate) fn tls_read_available(
    conn: &mut ConnState,
    budget: u64,
) -> Result<(u64, u64), ConnClose> {
    if !conn.read_ready {
        return Ok((0, 0));
    }

    if conn.len >= INPUT_CAP {
        return Ok((0, 0));
    }

    if budget == 0 {
        return Ok((0, 0));
    }

    ConnState::materialize_input(&mut conn.input);

    let spare = INPUT_CAP - conn.len;
    let want = (spare.min(4096) as u64).min(budget) as usize;

    if want == 0 {
        return Ok((0, 0));
    }

    let mut raw = [0u8; 4096];
    let mut wire = 0usize;
    let mut attempts = 0;

    let got = loop {
        if want <= wire {
            break wire;
        }

        let limit = (want - wire).min(4096 - wire);

        let outcome = tls_read_once(&mut conn.stream, &mut raw[wire..wire + limit]);

        match outcome {
            TlsReadOutcome::Progress(n) => {
                wire += n;

                break wire;
            }
            TlsReadOutcome::Blocked => {
                if wire > 0 {
                    break wire;
                }

                conn.read_ready = false;

                return Ok((0, 0));
            }
            TlsReadOutcome::Interrupted => {
                attempts += 1;

                if attempts >= 2 {
                    break wire;
                }
            }
            TlsReadOutcome::Eof => {
                return Err(ConnClose::PeerClosed);
            }
            TlsReadOutcome::Closed(reason) => {
                return Err(reason);
            }
        }
    };

    if got == 0 {
        return Ok((0, 0));
    }

    let Some(tls) = conn.tls.as_mut() else {
        return Err(ConnClose::TransportError);
    };

    conn.tls_plain.clear();

    let consumed = feed_ciphertext(tls, &raw[..got], &mut conn.tls_plain)
        .map_err(|_| ConnClose::TransportError)?;

    if consumed != got {
        return Err(ConnClose::TransportError);
    }

    let decrypted = conn.tls_plain.len();
    let end = conn.len + decrypted;

    if end > INPUT_CAP {
        return Err(ConnClose::TransportError);
    }

    if decrypted > 0 {
        if let Some(input) = conn.input.as_mut() {
            input[conn.len..end].copy_from_slice(&conn.tls_plain[..]);
        }

        conn.tls_plain.clear();
        conn.len = end;
    }

    conn.read_ready = true;

    Ok((got as u64, decrypted as u64))
}

pub(crate) fn tls_alpn_is_h2(conn: &ConnState) -> bool {
    conn.tls
        .as_ref()
        .is_some_and(|tls| !tls.is_handshaking() && alpn(tls) == Some(b"h2".as_slice()))
}

pub(crate) fn tls_encrypt_bytes(
    tls: &mut TlsConn,
    plain: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), ConnClose> {
    drain_plaintext(tls, plain, out).map_err(|_| ConnClose::TransportError)
}

pub(crate) fn tls_read_decrypted(
    stream: &mut TcpStream,
    tls: &mut TlsConn,
    spare: &mut [u8],
    budget: u64,
    scratch: &mut Vec<u8>,
) -> Result<(u64, usize, bool), ConnClose> {
    if budget == 0 || spare.is_empty() {
        return Ok((0, 0, false));
    }
    let limit = spare.len().min(4096).min(budget as usize);
    if limit == 0 {
        return Ok((0, 0, false));
    }
    let mut raw = [0u8; 4096];
    let mut wire = 0usize;
    let mut attempts = 0;
    let blocked = loop {
        match tls_read_once(stream, &mut raw[wire..limit]) {
            TlsReadOutcome::Progress(n) => {
                wire += n;
                break false;
            }
            TlsReadOutcome::Blocked => {
                if wire > 0 {
                    break false;
                }
                break true;
            }
            TlsReadOutcome::Interrupted => {
                attempts += 1;
                if attempts >= 2 {
                    break false;
                }
            }
            TlsReadOutcome::Eof => return Err(ConnClose::PeerClosed),
            TlsReadOutcome::Closed(reason) => return Err(reason),
        }
    };
    if wire == 0 {
        return Ok((0, 0, blocked));
    }
    scratch.clear();
    let consumed =
        feed_ciphertext(tls, &raw[..wire], scratch).map_err(|_| ConnClose::TransportError)?;
    if consumed != wire {
        return Err(ConnClose::TransportError);
    }
    if scratch.len() > spare.len() {
        return Err(ConnClose::TransportError);
    }
    spare[..scratch.len()].copy_from_slice(scratch);
    let plain = scratch.len();
    scratch.clear();
    Ok((wire as u64, plain, blocked))
}

pub(crate) enum TlsWriteOutcome {
    Accepted(usize),
    Blocked,
    Interrupted,
    Closed(ConnClose),
}

pub(crate) enum TlsReadOutcome {
    Progress(usize),
    Blocked,
    Interrupted,
    Eof,
    Closed(ConnClose),
}

pub(crate) fn tls_write_once(stream: &mut TcpStream, chunk: &[u8]) -> TlsWriteOutcome {
    use std::io::Write as _;

    if chunk.is_empty() {
        return TlsWriteOutcome::Accepted(0);
    }

    match stream.write(chunk) {
        Ok(accepted) => TlsWriteOutcome::Accepted(accepted),
        Err(error) => {
            let kind = error.kind();

            if kind == std::io::ErrorKind::WouldBlock {
                return TlsWriteOutcome::Blocked;
            }

            if kind == std::io::ErrorKind::Interrupted {
                return TlsWriteOutcome::Interrupted;
            }

            if kind == std::io::ErrorKind::ConnectionReset {
                return TlsWriteOutcome::Closed(ConnClose::Reset);
            }

            if kind == std::io::ErrorKind::BrokenPipe {
                return TlsWriteOutcome::Closed(ConnClose::PeerClosed);
            }

            TlsWriteOutcome::Closed(ConnClose::TransportError)
        }
    }
}

pub(crate) fn tls_read_once(stream: &mut TcpStream, scratch: &mut [u8]) -> TlsReadOutcome {
    use std::io::Read as _;

    if scratch.is_empty() {
        return TlsReadOutcome::Progress(0);
    }

    match stream.read(scratch) {
        Ok(0) => TlsReadOutcome::Eof,
        Ok(n) => TlsReadOutcome::Progress(n),
        Err(error) => {
            let kind = error.kind();

            if kind == std::io::ErrorKind::WouldBlock {
                return TlsReadOutcome::Blocked;
            }

            if kind == std::io::ErrorKind::Interrupted {
                return TlsReadOutcome::Interrupted;
            }

            if kind == std::io::ErrorKind::ConnectionReset {
                return TlsReadOutcome::Closed(ConnClose::Reset);
            }

            TlsReadOutcome::Closed(ConnClose::TransportError)
        }
    }
}

pub(crate) fn tls_turn_active(
    conn: &mut ConnState,
    byte_budget: u64,
    progress: &mut PumpProgress,
) -> Result<(), ConnClose> {
    let mut remaining = byte_budget;

    let flushed = tls_flush_pending(conn, remaining)?;

    progress.bytes_written += flushed;
    remaining = remaining.saturating_sub(flushed);

    tls_generate_flight(conn)?;

    let staged = tls_flush_pending(conn, remaining)?;

    progress.bytes_written += staged;
    remaining = remaining.saturating_sub(staged);

    let draining_body = matches!(conn.phase, ConnPhase::DrainingBody { .. });
    let want_read = (conn.phase == ConnPhase::ReadingHead || draining_body) && conn.len < INPUT_CAP;

    if want_read {
        let (wire, _decrypted) = tls_read_available(conn, remaining)?;

        progress.bytes_read += wire;
        remaining = remaining.saturating_sub(wire);
    }

    tls_generate_flight(conn)?;

    let staged = tls_flush_pending(conn, remaining)?;

    progress.bytes_written += staged;
    remaining = remaining.saturating_sub(staged);

    let handshaking = conn.tls.as_ref().is_some_and(|tls| tls.is_handshaking());

    if !handshaking {
        tls_encrypt_output(conn)?;

        let staged = tls_flush_pending(conn, remaining)?;

        progress.bytes_written += staged;
    }

    Ok(())
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
    let mut progress = PumpProgress::default();
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
    let write_ready = (sending || flushing_interim) && out_sent < out_len && !conn.write_blocked;
    // NOTE: slabs materialize on activity, not on phase: a quiet conn (reads wanted but
    // nothing signaled) keeps zero slabs through pump turns; the empty scratch dispatches
    // an Idle read that attempts nothing.
    if reading && read_ready {
        ConnState::materialize_input(&mut conn.input);
    } else if conn.is_idle() {
        // NOTE: retain-vs-reclaim invariant: keep-alive exchanges retain every slab so the
        // hot loop never reallocs; only this idle-gated reclaim drops input+headers+output.
        conn.input = None;
        conn.headers = None;
        conn.output = None;
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

    if conn.is_idle() {
        conn.input = None;
        conn.headers = None;

        if conn.tls_unflushed() == 0 && conn.out_sent >= conn.out_len {
            conn.output = None;
        }
    } else if conn.len < INPUT_CAP {
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
            send_expectation_failed(conn);
            ExchangeOutcome::Responded
        }
        Err(ParseError::HeaderSectionTooLarge) => {
            send_request_header_fields_too_large(conn);
            ExchangeOutcome::Responded
        }
        // NOTE: RFC 9110 15.5.15 scopes 414 to a long target URI specifically, so an over-long
        // request line attributes by component lengths below: target-side overflow answers 414 with
        // Date and close exactly like 400/431, while method/version overflows stay 400. The parser
        // keeps its undifferentiated RequestLineTooLarge (a new variant would break three exhaustive
        // ParseError matches outside this slice), and a full input buffer surfaces as NeedMore, so
        // this arm attributes the buffered line itself; the hot scan is unaffected (error path only).
        Ok(None) | Err(ParseError::RequestLineTooLarge) => {
            if request_target_too_long(conn) {
                send_uri_too_long(conn);
            } else {
                send_bad_request(conn);
            }
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
    let Some(input) = conn.input.as_ref() else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    let input = &input[..conn.len];
    match table.find_request(head, input, &mut captures) {
        Ok(found) if found.kind == RouteKind::Managed => {
            let cap = crate::static_routes::route_cap(found.body);
            let method = head.method(input).unwrap_or_default();
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
    let Some(headers) = conn.headers.as_ref() else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    let Some(input) = conn.input.as_ref() else {
        return ExchangeOutcome::Close(ConnClose::TransportError);
    };
    let output = ConnState::materialize_output(&mut conn.output);
    match respond_to_head(
        routes,
        table,
        head,
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
            conn.close_reason = ConnClose::Completed;
            // NOTE: bytes already buffered past the head are body bytes by position, so
            // skip them in place and drain the rest across pumps; the head is dropped now
            // and keep-alive reuse below treats the remainder as pipelined tail. The
            // encoded response stays unpublished until the drain finishes, so an idle
            // drain waits on poll instead of spinning the worker turn.
            let buffered = (conn.len - conn.head_len) as u64;
            let skip = buffered.min(drain_total) as usize;
            if let Some(input) = conn.input.as_mut() {
                input.copy_within(conn.head_len + skip..conn.len, 0);
            }
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
        // NOTE: handler-reported failure closes as response-overflow (bounded-response family,
        // like static); NativePanic is defense-in-depth for the boundary closure itself —
        // a panic inside the handler aborts nounwind before it can reach here.
        Err(RouteBuildError::NativePanic) => ExchangeOutcome::Close(ConnClose::ProtocolError),
        Err(_) => ExchangeOutcome::Close(ConnClose::ResponseOverflow),
    }
}

// NOTE: truncation rule: read_chunk clamps to the plan remainder, so over-long reads
// cannot happen; a zero-byte read before plan size means the file shrank underfoot and
// fails the transfer closed, never as a short success. Each stage re-opens the file
// (no fd cache yet); EPIPE and backpressure stay in the dispatch mapping, and Rust
// ignores SIGPIPE process-wide, so a closed receiver closes as PeerClosed, never a
// signal. Partial short reads advance the offset and resume on the next turn.
fn stage_file_chunk(conn: &mut ConnState) -> Result<(), ConnClose> {
    let Some(transfer) = conn.file.as_mut() else {
        return Err(ConnClose::TransportError);
    };
    if transfer.offset >= transfer.plan.size {
        return Ok(());
    }
    let width = (transfer.plan.size - transfer.offset).min(OUTPUT_CAP as u64) as usize;
    let Some(output) = conn.output.as_mut() else {
        return Err(ConnClose::TransportError);
    };
    match read_chunk(&transfer.plan, transfer.offset, &mut output[..width]) {
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

// NOTE: a shrink racing a send can zero-fill inside the plan bounds, where read_chunk would
// serve a short read instead. Either way the offset stalls below plan size and fails closed.
// Bytes past plan.size never serve (count clamp) and only this file's pages ever map.
#[cfg(target_os = "linux")]
fn sendfile_file_chunk(conn: &mut ConnState, byte_budget: u64) -> Result<u64, ConnClose> {
    let Some(transfer) = conn.file.as_ref() else {
        return Err(ConnClose::TransportError);
    };
    if transfer.offset >= transfer.plan.size {
        return Ok(0);
    }
    if transfer.plan.multipart {
        return stage_file_chunk(conn).map(|_| 0); // NOTE: framing interleaves file bytes
    }
    let rest = transfer.plan.size - transfer.offset;
    let width = std::cmp::min(std::cmp::min(rest, OUTPUT_CAP as u64), byte_budget) as usize;
    if width == 0 || conn.write_blocked {
        return Ok(0);
    }
    let file = conn
        .file_cache
        .cached(&transfer.plan)
        .map_err(|_| ConnClose::TransportError)?;
    let mut cursor = transfer.plan.start + transfer.offset;
    for _ in 0..2 {
        match rustix::fs::sendfile(&conn.stream, file, Some(&mut cursor), width) {
            Ok(sent) => {
                if sent == 0 {
                    return Err(ConnClose::TransportError);
                }
                let Some(transfer) = conn.file.as_mut() else {
                    return Err(ConnClose::TransportError);
                };
                transfer.offset = cursor - transfer.plan.start;
                return Ok(sent as u64);
            }
            Err(error) if error == rustix::io::Errno::INTR => continue,
            Err(error) if error == rustix::io::Errno::AGAIN => {
                conn.write_blocked = true;
                return Ok(0);
            }
            Err(error) if error == rustix::io::Errno::PIPE => return Err(ConnClose::PeerClosed),
            Err(error) if error == rustix::io::Errno::CONNRESET => return Err(ConnClose::Reset),
            Err(_) => return Err(ConnClose::TransportError),
        }
    }
    Ok(0)
}

fn send_bad_request(conn: &mut ConnState) {
    ConnState::materialize_output(&mut conn.output);
    let staged = match conn.output.as_mut() {
        Some(output) => respond_bad_request(&mut output[..]).ok(),
        None => None,
    };
    finish_simple(conn, ConnClose::ProtocolError, staged);
}

fn send_request_header_fields_too_large(conn: &mut ConnState) {
    ConnState::materialize_output(&mut conn.output);
    let staged = match conn.output.as_mut() {
        Some(output) => respond_request_header_fields_too_large(&mut output[..]).ok(),
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

fn send_uri_too_long(conn: &mut ConnState) {
    ConnState::materialize_output(&mut conn.output);
    let staged = match conn.output.as_mut() {
        Some(output) => respond_uri_too_long(&mut output[..]).ok(),
        None => None,
    };
    finish_simple(conn, ConnClose::ProtocolError, staged);
}

fn send_expectation_failed(conn: &mut ConnState) {
    ConnState::materialize_output(&mut conn.output);
    let staged = match conn.output.as_mut() {
        Some(output) => respond_expectation_failed(&mut output[..]).ok(),
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

fn reuse_keep_alive(conn: &mut ConnState) {
    // NOTE: mechanics only, not the reuse decision: admission is the shared
    // veylor_http1::response_permits_reuse verdict folded into conn.closing at the encode
    // sites (static/parked keep_open) with request-body drain enforced by the DrainingBody
    // phase; this function only shifts the pipelined tail, resets the parser, and re-arms
    // read_ready for the next exchange.
    let tail = conn.len - conn.head_len;
    if let Some(input) = conn.input.as_mut() {
        input.copy_within(conn.head_len..conn.len, 0);
    }
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
    fn first_exchange_materializes_and_reuse_retains_until_quiet_reclaim() {
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
        let _ = pump_connection(&mut conn, &routes, &table, u64::MAX, 0, &host, false);
        assert!(conn.input.is_none());
        assert!(conn.headers.is_none());
        assert!(conn.output.is_none());
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
