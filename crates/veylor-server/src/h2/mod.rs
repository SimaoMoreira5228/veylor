#![allow(unsafe_code)]

use std::borrow::Cow;

use veylor_http2::{
    ErrorCode, H2C_GOAWAY_LEN, H2FrameDriver, H2ServerDriver, H2cServerHandshake, HeaderRange,
    HpackHeader, HpackTable, ServerError, StreamState, StreamTable,
};
use veylor_runtime::ConnectionToken;

use crate::host::{HostShared, StagedWork};

// NOTE: worker-turn hook (slot state plus accept/pump/close dispatch). The
// step driver above stays transport-neutral; turn.rs owns the sockets,
// listeners, and timers that drive it.
pub(crate) mod turn;
// NOTE: seam: routes owns the static table plus match policy, send owns sync wire
// assembly, serve owns per-event stream serving (host parks, trailers, streaming
// writes), step owns the staged step draining one queued host command, slots owns
// the per-worker slot table, tables owns the slot layout and its pooled slabs, flush
// owns socket flush pacing, and file owns file-mount assembly (range planning,
// HEADERS emit, DATA pump).
mod accept;
mod batch;
mod file;
mod flush;
mod pending;
mod pump;
mod routes;
mod send;
mod serve;
pub(crate) mod slots;
mod step;
mod tables;

pub(crate) use file::{FileTable, H2FileTransfer};
pub(crate) use pump::{fail_goaway, grant_window};
pub use routes::{FrozenNativeRoute, FrozenRoute, FrozenRoutesView};
pub(crate) use step::apply_h2_streams;

use tables::{H2EngineSlab, H2EngineView};

pub const INPUT_STAGING_CAP: usize = 16_384 + 64;
pub const STREAM_SLOTS: usize = 32;
pub(crate) const TUNNEL_STEP: usize = 16_384;
// NOTE: managed H2 connection ceiling: 8 concurrent connections per worker; the 9th
// accept finds no free slot and is dropped with a TransportError close (pinned by the
// 9th-connection test), the same fail-closed shape as the per-stream ceiling.
pub const MAX_H2_CONNS: usize = 64;
// NOTE: H2 decoded-header slots; the H1 pump has its own HEADER_SLOTS (= 100) since plain
// ranges are cheaper than HPACK entries with dynamic-table state. See the H1 site for the
// deliberate split (65-100 headers: served on H1, refused on H2).
pub const HEADER_SLOTS: usize = 64;
pub const HEADER_BLOCK_CAP: usize = 16_384;
pub const H2C_MAX_HEADER_LIST_SIZE: u32 = 16_384;
pub const DECODE_ENTRIES: usize = 64;
pub const DECODE_BYTES: usize = 16_384;
pub const ENCODE_ENTRIES: usize = 64;
pub const ENCODE_BYTES: usize = 4_096;
pub const ACK_SLOTS: usize = 4;
pub const HPACK_BLOCK_CAP: usize = 512;
pub const DECIMAL_CAP: usize = 20;
// NOTE: per-stream admitted-body aggregate: half a connection window, so admitted bodies
// arrive without mid-upload grants. H2 has no chunked coding; END_STREAM frames the body.
pub const H2_BODY_CAP: u64 = 32_768;
// NOTE: per-step frame bound, so at most this many streams complete HEADERS per step; fuller
// batches report OutputFull and their remainder stays staged for the next step via `pending`.
pub const MAX_STREAMS_PER_STEP: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushError {
    Closed,
    Overflow,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StepOutcome {
    pub written: usize,
    pub served: u16,
    pub refused: u16,
    pub parked: u16,
    pub goaway: bool,
    pub closed: bool,
    pub pending: bool,
}

// NOTE: host dispatch context for step_host: base names worker/slot/generation, stream_id is
// filled per stream at park time so stale/duplicate respond/cancel fail closed per stream.
#[derive(Clone, Copy)]
pub struct H2HostCtx<'h> {
    pub host: &'h HostShared,
    pub base: ConnectionToken,
}

#[derive(Clone, Copy)]
pub(crate) enum StreamEvent {
    Headers {
        stream: u32,
        range: HeaderRange,
        end: bool,
    },
    Data(u32, usize, usize, bool),
    PeerReset,
    PeerGoAway,
}

const MIN_EVENT_ROOM: usize = 1024;

pub(crate) enum Phase<'a> {
    Handshake(H2cServerHandshake<'a, 'a, 'a, 'a>),
    Serving(H2ServerDriver<'a, 'a, 'a, 'a>),
    Dead,
}

impl<'a> Phase<'a> {
    fn limits(&self) -> Option<(u32, u32)> {
        let streams = match self {
            Phase::Handshake(handshake) => handshake.server().driver().streams(),
            Phase::Serving(server) => server.driver().streams(),
            Phase::Dead => return None,
        };
        Some((
            streams.highest_remote(),
            streams.remote_settings().max_frame_size,
        ))
    }

    fn serving(
        &mut self,
        terminal: &mut bool,
        outcome: &mut StepOutcome,
    ) -> Option<&mut H2ServerDriver<'a, 'a, 'a, 'a>> {
        match self {
            Phase::Serving(server) => Some(server),
            _ => {
                *terminal = true;
                outcome.closed = true;
                None
            }
        }
    }

    fn handshake(
        &mut self,
        terminal: &mut bool,
        outcome: &mut StepOutcome,
    ) -> Option<&mut H2cServerHandshake<'a, 'a, 'a, 'a>> {
        match self {
            Phase::Handshake(handshake) => Some(handshake),
            _ => {
                *terminal = true;
                outcome.closed = true;
                None
            }
        }
    }
}

pub struct H2Conn<'a> {
    // NOTE: transport slabs are optional so an idle connection can give them back to its
    // worker pool. Present always covers `staged`/the step window: an absent slab is an empty
    // window and every miss path heals to idle or fails closed, never panics.
    pub(crate) staging: Option<Box<[u8; INPUT_STAGING_CAP]>>,
    staged: usize,
    headers: &'a mut [HpackHeader],
    pub(crate) phase: Phase<'a>,
    settings_sent: bool,
    terminal: bool,
    // NOTE: file-mount streaming state beside the driver (projection-adjacent, not a third
    // route table): one entry per servable stream at most, plus a single OUTPUT_CAP chunk
    // scratch reused every turn, so file bytes frame in bounded slices and never stage whole.
    // The transfer table is allocated on the first file response and pooled once it empties.
    pending_responses: pending::PendingResponses,
    pending_events: [Option<StreamEvent>; MAX_STREAMS_PER_STEP],
    pending_event_len: usize,
    file_transfers: Option<Box<FileTable>>,
    pub(crate) file_scratch: Option<Box<[u8; crate::connection::OUTPUT_CAP]>>,
    pub(crate) tunnel_scratch: Option<Box<[u8; TUNNEL_STEP]>>,
    // NOTE: the engine tables borrow the slot's slab; a released connection keeps its counters
    // and settings but leaves the arrays in the worker pool until the next readiness edge.
    engine_attached: bool,
}

impl<'a> H2Conn<'a> {
    pub fn new(
        streams: &'a mut StreamTable<'a>,
        decoder: &'a mut HpackTable<'a>,
        encoder: &'a mut HpackTable<'a>,
        ack_limits: &'a mut [u32],
        headers: &'a mut [HpackHeader],
    ) -> Self {
        let phase = match H2cServerHandshake::new(
            streams,
            decoder,
            encoder,
            ack_limits,
            H2C_MAX_HEADER_LIST_SIZE,
        ) {
            Ok(handshake) => Phase::Handshake(handshake),
            Err(_) => Phase::Dead,
        };
        let terminal = matches!(phase, Phase::Dead);
        Self {
            staging: None,
            staged: 0,
            headers,
            phase,
            settings_sent: false,
            terminal,
            pending_responses: pending::PendingResponses::new(),
            pending_events: [None; MAX_STREAMS_PER_STEP],
            pending_event_len: 0,
            file_transfers: None,
            file_scratch: None,
            tunnel_scratch: None,
            engine_attached: true,
        }
    }

    // NOTE: HPACK scratch is append-only across header blocks; compact only once the
    // turn's decoded headers are consumed, never between blocks (their spans are live).
    pub(crate) fn compact_decoder(&mut self) {
        match &mut self.phase {
            Phase::Handshake(handshake) => handshake.compact_decoder(),
            Phase::Serving(server) => server.compact_decoder(),
            Phase::Dead => {}
        }
    }

    pub fn push_input(&mut self, bytes: &[u8]) -> Result<(), PushError> {
        if self.terminal {
            return Err(PushError::Closed);
        }
        let Some(staging) = self.staging.as_deref_mut() else {
            self.terminal = true;
            return Err(PushError::Overflow);
        };
        if bytes.len() > staging.len() - self.staged {
            self.terminal = true;
            return Err(PushError::Overflow);
        }
        staging[self.staged..self.staged + bytes.len()].copy_from_slice(bytes);
        self.staged += bytes.len();
        Ok(())
    }

    pub const fn is_closed(&self) -> bool {
        self.terminal
    }

    pub const fn handshake_complete(&self) -> bool {
        matches!(self.phase, Phase::Serving(_))
    }

    pub const fn staged_len(&self) -> usize {
        self.staged
    }

    pub(crate) fn holds_slabs(&self) -> bool {
        self.staging.is_some()
            || self.file_scratch.is_some()
            || self.tunnel_scratch.is_some()
            || self.file_transfers.is_some()
            || self.engine_attached
    }

    // NOTE: what an adopt must install, which is everything except the lazily boxed file
    // table: requiring that one would re-adopt on every edge of a connection that never
    // serves a file.
    pub(crate) fn needs_slabs(&self) -> bool {
        self.staging.is_none()
            || self.file_scratch.is_none()
            || self.tunnel_scratch.is_none()
            || !self.engine_attached
    }

    pub(crate) const fn tables_attached(&self) -> bool {
        self.engine_attached
    }

    // NOTE: the stream slot array, header-block assembler and encoder arrays move back into the
    // tables for this connection; the encoder opens its next block with a table size update to
    // zero, so no index from the slab's previous life is ever referenced.
    pub(crate) fn attach_engine(&mut self, engine: &mut H2EngineSlab) -> bool {
        let Some(driver) = self.frame_driver_mut() else {
            return false;
        };
        // SAFETY: the slab is owned by the slot's `bufs` and is not moved while this
        // connection borrows it; the tables are dropped before the slot is retired.
        let H2EngineView {
            slots,
            block,
            headers,
            enc_entries,
            enc_scratch,
        } = unsafe { engine.view() };
        driver.streams_mut().install_storage(slots, block);
        let encoder = driver.hpack_encoder_mut();
        encoder.install_storage(enc_entries, enc_scratch);
        encoder.reset_dynamic_table();
        self.headers = headers;
        self.engine_attached = true;
        true
    }

    // NOTE: refuses while a stream or a partly assembled header block still owns the storage;
    // the caller keeps the slab then and retries on a later pass.
    pub(crate) fn detach_engine(&mut self) -> bool {
        {
            let Some(driver) = self.frame_driver_mut() else {
                return false;
            };
            let Some((_slots, _block)) = driver.streams_mut().take_storage() else {
                return false;
            };
            let (_enc_entries, _enc_scratch) = driver.hpack_encoder_mut().take_storage();
        }
        self.headers = &mut [];
        self.engine_attached = false;
        true
    }

    fn frame_driver_mut(&mut self) -> Option<&mut H2FrameDriver<'a, 'a, 'a, 'a>> {
        match &mut self.phase {
            Phase::Handshake(handshake) => Some(handshake.server_mut().driver_mut()),
            Phase::Serving(server) => Some(server.driver_mut()),
            Phase::Dead => None,
        }
    }

    // NOTE: staged input rides the staging slab, so only an empty staging window may move it.
    pub(crate) fn take_staging(&mut self) -> Option<Box<[u8; INPUT_STAGING_CAP]>> {
        if self.staged > 0 {
            return None;
        }
        self.staging.take()
    }

    pub(crate) fn active_streams(&self) -> usize {
        match &self.phase {
            Phase::Handshake(handshake) => handshake.server().driver().streams().active_count(),
            Phase::Serving(server) => server.driver().streams().active_count(),
            Phase::Dead => 0,
        }
    }

    pub(crate) fn has_file_transfers(&self) -> bool {
        self.file_transfers
            .as_ref()
            .is_some_and(|files| files.iter().any(Option::is_some))
    }

    pub(crate) fn take_file_table(&mut self) -> Option<Box<FileTable>> {
        let mut files = self.file_transfers.take()?;
        for slot in files.iter_mut() {
            *slot = None;
        }
        Some(files)
    }

    #[cfg(test)]
    pub(crate) const fn file_table_allocated(&self) -> bool {
        self.file_transfers.is_some()
    }

    pub fn set_response_write_clock(&mut self, now_ns: u64, timeout_ns: u64) {
        self.pending_responses.set_time(now_ns, timeout_ns);
    }

    pub fn has_pending_responses(&self) -> bool {
        !self.pending_responses.is_empty()
    }

    pub fn flood_counters(&self) -> (u32, u32) {
        match &self.phase {
            Phase::Serving(server) => server.flood_counters(),
            Phase::Handshake(handshake) => handshake.server().flood_counters(),
            Phase::Dead => (0, 0),
        }
    }

    pub fn spare_mut(&mut self) -> &mut [u8] {
        match self.staging.as_deref_mut() {
            Some(staging) => &mut staging[self.staged..],
            None => &mut [],
        }
    }

    pub fn install_staging(&mut self, staging: Box<[u8; INPUT_STAGING_CAP]>) {
        self.staging = Some(staging);
    }

    pub fn commit_staged(&mut self, count: usize) {
        if self.terminal {
            return;
        }
        let capacity = match self.staging.as_deref() {
            Some(staging) => staging.len(),
            None => 0,
        };
        self.staged = (self.staged + count).min(capacity);
    }

    pub fn drain(&mut self, output: &mut [u8]) -> StepOutcome {
        self.drain_with_code(output, ErrorCode::NoError)
    }

    pub fn drain_with_code(&mut self, output: &mut [u8], code: ErrorCode) -> StepOutcome {
        let mut outcome = StepOutcome::default();
        if self.terminal {
            outcome.closed = true;
            return outcome;
        }
        if output.len() < H2C_GOAWAY_LEN {
            outcome.pending = true;
            return outcome;
        }
        if self.phase.limits().is_none() {
            self.terminal = true;
            outcome.closed = true;
            return outcome;
        }
        let mut used = 0;
        self.fail(code, output, &mut used, &mut outcome);
        outcome
    }

    pub fn step(&mut self, output: &mut [u8], routes: &FrozenRoutesView<'_>) -> StepOutcome {
        self.step_inner(output, routes, None)
    }

    pub fn step_host(
        &mut self,
        output: &mut [u8],
        routes: &FrozenRoutesView<'_>,
        ctx: H2HostCtx<'_>,
    ) -> StepOutcome {
        self.step_inner(output, routes, Some(ctx))
    }

    pub fn step_host_timed(
        &mut self,
        output: &mut [u8],
        routes: &FrozenRoutesView<'_>,
        ctx: H2HostCtx<'_>,
        now_ns: u64,
        response_write_ns: u64,
    ) -> StepOutcome {
        self.pending_responses.set_time(now_ns, response_write_ns);
        self.step_inner(output, routes, Some(ctx))
    }

    // NOTE: host-parked streams leave their response to apply_staged; abandoned parks hold no
    // worker timer here (transport-neutral driver owns none) and time out in the turn on the
    // connection-scoped body-progress activity deadline, which GOAWAY-drains like any idle conn.
    pub fn apply_staged(&mut self, output: &mut [u8], staged: StagedWork) -> StepOutcome {
        let mut outcome = StepOutcome::default();
        if self.terminal {
            outcome.closed = true;
            return outcome;
        }
        let Some(server) = self.phase.serving(&mut self.terminal, &mut outcome) else {
            return outcome;
        };
        let stream = staged.token.stream_id;
        let mut used = 0;
        let owed = staged.grant_owed;
        used += grant_window(server, 0, owed, &mut output[used..]);
        if staged.cancel {
            match server.rst_stream(stream, ErrorCode::Cancel, &mut output[used..]) {
                Ok(writer) => {
                    used += writer.pending_bytes().len();
                    outcome.written = used;
                }
                Err(ServerError::StreamClosed | ServerError::UnknownStream) => {}
                Err(_) => self.fail(ErrorCode::InternalError, output, &mut used, &mut outcome),
            }
            return outcome;
        }
        let live = server.driver().streams().find(stream);
        let gone = live.is_none_or(|live| live.state == StreamState::Closed);
        if gone {
            outcome.written = used;
            return outcome;
        }
        used += grant_window(server, stream, owed, &mut output[used..]);
        let mut extra = [(&b""[..], &b""[..]); crate::host::MAX_STAGED_RESPONSE_HEADERS];
        let extra_len = crate::h2::routes::project_extra_headers(&mut extra, &staged.headers);
        let response_end = staged.body.is_empty();
        let response_body = Cow::Owned(staged.body);
        match send::send_full(
            server,
            stream,
            staged.status,
            response_body,
            None,
            false,
            &extra[..extra_len],
            response_end,
            true,
            &mut output[..],
            &mut used,
            &mut self.pending_responses,
        ) {
            Ok(complete) => {
                outcome.served += u16::from(complete);
                outcome.pending = !complete;
                outcome.written = used;
            }
            Err(ErrorCode::RefusedStream) => {
                match server.rst_stream(stream, ErrorCode::RefusedStream, &mut output[used..]) {
                    Ok(writer) => {
                        used += writer.pending_bytes().len();
                        outcome.refused += 1;
                        outcome.written = used;
                    }
                    Err(ServerError::StreamClosed) | Err(ServerError::UnknownStream) => {}
                    Err(_) => self.fail(ErrorCode::InternalError, output, &mut used, &mut outcome),
                }
            }
            Err(code) => self.fail(code, output, &mut used, &mut outcome),
        }
        outcome
    }
}
