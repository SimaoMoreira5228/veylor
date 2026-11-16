use super::tls::TLS_PENDING_MIN_BYTES;

use veylor_http1::{BodyDecoder, ConnectionHeader, Header, Parser, RequestVersion};
use veylor_runtime::{ConnectionToken, TcpStream};
use veylor_tls::TlsConn;

#[cfg(target_os = "linux")]
use super::file::FdCache;
use super::file::FileTransfer;
use super::slabs::{SLAB_HOLD_NS, SLAB_QUIET_TURNS};
use super::{HEADER_SLOTS, INPUT_CAP, OUTPUT_CAP};
use crate::host::HOST_MAX_METHOD_BYTES;

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
    pub(crate) close_drain_remaining: u64,
    pub(crate) error_hint: bool,
    pub(crate) tls: Option<Box<TlsConn>>,
    pub(crate) tunnel_token: ConnectionToken,
    pub(crate) tunnel_echo: Option<veylor_websocket::WebSocket>,
    pub(crate) tunnel_close: bool,
    pub(crate) tunnel_eof: bool,
    pub(crate) tunnel_notified: bool,
    pub(crate) tls_pending: Vec<u8>,
    pub(crate) tls_pending_sent: usize,
    pub(crate) last_active_ns: u64,
    pub(crate) quiet_turns: u8,
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
            close_drain_remaining: 0,
            error_hint: false,
            tls: None,
            tunnel_token: ConnectionToken::default(),
            tunnel_echo: None,
            tunnel_close: false,
            tunnel_eof: false,
            tunnel_notified: false,
            tls_pending: Vec::new(),
            tls_pending_sent: 0,
            last_active_ns: 0,
            quiet_turns: 0,
        }
    }

    pub(crate) fn trailer_slots(&self) -> &[Header; crate::connection::parked::TRAILER_SLOTS] {
        self.trailer_slots
            .as_deref()
            .expect("trailer slots are materialized before use")
    }

    pub(crate) fn attach_tls(&mut self, tls: TlsConn) {
        self.tls = Some(Box::new(tls));
        self.reserve_tls_scratch();
    }

    pub(crate) fn reserve_tls_scratch(&mut self) {
        if self.tls.is_none() {
            return;
        }

        if self.tls_pending.capacity() < TLS_PENDING_MIN_BYTES {
            self.tls_pending
                .reserve(TLS_PENDING_MIN_BYTES - self.tls_pending.capacity());
        }
    }

    pub(crate) fn drop_tls_scratch(&mut self) {
        if self.tls.is_none() {
            return;
        }

        self.tls_pending = Vec::new();
        self.tls_pending_sent = 0;
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

    pub(crate) fn holds_slabs(&self) -> bool {
        self.input.is_some() || self.output.is_some() || self.headers.is_some()
    }

    // NOTE: waiting with slabs and nothing to do: no buffered head, no staged response, no
    // armed read claim to use the input slab. Only such a connection can give slabs back.
    pub(crate) fn slab_idle(&self) -> bool {
        self.is_idle() && !self.read_ready
    }

    // NOTE: gate for the reclaim pass in the turn loop: a worker that has nothing to do stops
    // sweeping, so turns stop accruing and silence is the only clock left.
    pub(crate) fn slab_release_ready(&self, now_ns: u64) -> bool {
        self.slab_idle() && now_ns.saturating_sub(self.last_active_ns) >= SLAB_HOLD_NS
    }

    // NOTE: gate for the sweep, which runs while other connections keep the worker busy: a
    // pipelined request is parsed in the same turn it is read, so two idle sweeps really are
    // silence rather than a gap between exchanges.
    pub(crate) fn slab_release_due(&self, now_ns: u64) -> bool {
        self.quiet_turns >= SLAB_QUIET_TURNS && self.slab_release_ready(now_ns)
    }

    pub(crate) fn slab_release_at(&self) -> u64 {
        self.last_active_ns.saturating_add(SLAB_HOLD_NS)
    }

    pub(crate) fn note_activity(&mut self, moved: u64, now_ns: u64) {
        if moved > 0 || !self.is_idle() {
            self.last_active_ns = now_ns;
            self.quiet_turns = 0;
            return;
        }

        self.quiet_turns = self.quiet_turns.saturating_add(1);
    }

    pub(crate) fn note_slab_adopt(&mut self, now_ns: u64) {
        self.last_active_ns = now_ns;
        self.quiet_turns = 0;
    }

    pub(crate) fn note_slab_release(&mut self) {
        self.quiet_turns = 0;
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

#[cfg(all(test, target_pointer_width = "64"))]
mod size_tests {
    use super::*;

    #[test]
    fn plaintext_idle_record_stays_within_budget() {
        assert!(std::mem::size_of::<ConnState>() <= 1024);
    }
}
