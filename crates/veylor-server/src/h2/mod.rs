use core::mem;

use veylor_http2::{
    ControlFrameWriter, ErrorCode, FeedStatus, H2C_GOAWAY_LEN, H2FrameAction, H2ServerDriver,
    H2cServerHandshake, HeaderRange, HpackHeader, HpackTable, ServerError, StreamState,
    StreamTable,
};
use veylor_runtime::ConnectionToken;

use crate::host::{HostShared, StagedWork};

// NOTE: worker-turn hook (slot state plus accept/pump/close dispatch). The
// step driver above stays transport-neutral; turn.rs owns the sockets,
// listeners, and timers that drive it.
pub(crate) mod turn;
// NOTE: seam: routes owns the static table plus sync response assembly, serve owns
// per-event stream serving (host parks, trailers, streaming writes), and file owns
// file-mount assembly (range planning, HEADERS emit, DATA pump).
mod file;
mod routes;
mod serve;

pub(crate) use file::H2FileTransfer;
pub use routes::{FrozenNativeRoute, FrozenRoute, FrozenRoutesView};
pub(crate) use serve::apply_h2_streams;

pub const INPUT_STAGING_CAP: usize = 16_384 + 64;
pub const STREAM_SLOTS: usize = 32;
pub const HEADER_SLOTS: usize = 64;
pub const HEADER_BLOCK_CAP: usize = 16_384;
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
    staging: &'a mut [u8],
    staged: usize,
    headers: &'a mut [HpackHeader],
    pub(crate) phase: Phase<'a>,
    settings_sent: bool,
    terminal: bool,
    // NOTE: file-mount streaming state beside the driver (projection-adjacent, not a third
    // route table): one entry per servable stream at most, plus a single OUTPUT_CAP chunk
    // scratch reused every turn, so file bytes frame in bounded slices and never stage whole.
    file_transfers: Box<[Option<H2FileTransfer>; STREAM_SLOTS]>,
    file_scratch: Box<[u8; crate::connection::OUTPUT_CAP]>,
}

impl<'a> H2Conn<'a> {
    pub fn new(
        streams: &'a mut StreamTable<'a>,
        decoder: &'a mut HpackTable<'a>,
        encoder: &'a mut HpackTable<'a>,
        staging: &'a mut [u8],
        ack_limits: &'a mut [u32],
        headers: &'a mut [HpackHeader],
    ) -> Self {
        let phase = match H2cServerHandshake::new(streams, decoder, encoder, ack_limits) {
            Ok(handshake) => Phase::Handshake(handshake),
            Err(_) => Phase::Dead,
        };
        let terminal = matches!(phase, Phase::Dead);
        Self {
            staging,
            staged: 0,
            headers,
            phase,
            settings_sent: false,
            terminal,
            file_transfers: Box::new(std::array::from_fn(|_| None)),
            file_scratch: Box::new([0; crate::connection::OUTPUT_CAP]),
        }
    }

    pub fn push_input(&mut self, bytes: &[u8]) -> Result<(), PushError> {
        if self.terminal {
            return Err(PushError::Closed);
        }
        if bytes.len() > self.staging.len() - self.staged {
            self.terminal = true;
            return Err(PushError::Overflow);
        }
        self.staging[self.staged..self.staged + bytes.len()].copy_from_slice(bytes);
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

    pub fn spare_mut(&mut self) -> &mut [u8] {
        &mut self.staging[self.staged..]
    }

    pub fn commit_staged(&mut self, count: usize) {
        if self.terminal {
            return;
        }
        self.staged = (self.staged + count).min(self.staging.len());
    }

    pub fn drain(&mut self, output: &mut [u8]) -> StepOutcome {
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
        self.fail(ErrorCode::NoError, output, &mut used, &mut outcome);
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

    // NOTE: host-parked streams leave their response to apply_staged; abandoned parks hold no
    // worker timer here (transport-neutral driver owns none) and time out in the turn on the
    // connection-scoped body-progress activity deadline, which GOAWAY-drains like any idle conn.
    pub fn apply_staged(&mut self, output: &mut [u8], staged: &StagedWork) -> StepOutcome {
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
        match routes::send_full(
            server,
            stream,
            staged.status,
            &staged.body,
            None,
            false,
            staged.body.is_empty(),
            true,
            &mut output[..],
            &mut used,
        ) {
            Ok(()) => {
                outcome.served += 1;
                outcome.written = used;
            }
            Err(code) => self.fail(code, output, &mut used, &mut outcome),
        }
        outcome
    }

    fn step_inner(
        &mut self,
        output: &mut [u8],
        routes: &FrozenRoutesView<'_>,
        host: Option<H2HostCtx<'_>>,
    ) -> StepOutcome {
        let mut outcome = StepOutcome::default();
        if self.terminal {
            outcome.closed = true;
            return outcome;
        }
        let mut used = 0;
        if !self.settings_sent && !self.send_settings(output, &mut used, &mut outcome) {
            return outcome;
        }
        let mut events: [Option<StreamEvent>; MAX_STREAMS_PER_STEP] = [None; MAX_STREAMS_PER_STEP];
        let mut event_len = 0;
        let mut databuf = Vec::new();
        if !self.is_serving() {
            if !self.run_handshake(
                output,
                &mut used,
                &mut outcome,
                &mut events,
                &mut event_len,
                &mut databuf,
            ) {
                return outcome;
            }
            if !self.is_serving() {
                outcome.written = used;
                return outcome;
            }
        }
        if event_len == 0
            && self.staged > 0
            && !self.run_feed(
                output,
                &mut used,
                &mut outcome,
                &mut events,
                &mut event_len,
                &mut databuf,
            )
        {
            return outcome;
        }
        let server = match self.phase.serving(&mut self.terminal, &mut outcome) {
            Some(server) => server,
            None => {
                outcome.written = used;
                return outcome;
            }
        };
        let mut serve = serve::Serve {
            server,
            headers: &*self.headers,
            data: &databuf,
            terminal: &mut self.terminal,
            routes,
            host,
            output,
            used: &mut used,
            outcome: &mut outcome,
            files: &mut self.file_transfers,
            scratch: &mut self.file_scratch[..],
        };
        for event in events.iter().take(event_len).flatten() {
            serve.serve_one(*event);
            if serve.outcome.closed {
                break;
            }
        }
        // NOTE: file DATA resumes here every step (not only on new HEADERS), so a transfer
        // wider than one output buffer streams across steps and turns through the same
        // turn budgets as static DATA, with no new flow-control machinery.
        if !serve.outcome.closed {
            serve.pump_files();
        }
        outcome.written = used;
        if outcome.closed {
            self.terminal = true;
        }
        outcome
    }

    fn is_serving(&self) -> bool {
        matches!(self.phase, Phase::Serving(_))
    }

    fn send_settings(
        &mut self,
        output: &mut [u8],
        used: &mut usize,
        outcome: &mut StepOutcome,
    ) -> bool {
        let Some(handshake) = self.phase.handshake(&mut self.terminal, &mut *outcome) else {
            return false;
        };
        let Ok(writer) = handshake.server_settings(&mut output[*used..]) else {
            outcome.pending = true;
            return false;
        };
        *used += writer.pending_bytes().len();
        self.settings_sent = true;
        true
    }

    fn run_handshake(
        &mut self,
        output: &mut [u8],
        used: &mut usize,
        outcome: &mut StepOutcome,
        events: &mut [Option<StreamEvent>; MAX_STREAMS_PER_STEP],
        event_len: &mut usize,
        staged: &mut Vec<u8>,
    ) -> bool {
        let Some(handshake) = self.phase.handshake(&mut self.terminal, &mut *outcome) else {
            return false;
        };
        let max = handshake
            .server()
            .driver()
            .streams()
            .remote_settings()
            .max_frame_size;
        let mut actions: [Option<H2FrameAction<'_>>; MAX_STREAMS_PER_STEP] =
            [None; MAX_STREAMS_PER_STEP];
        let result = handshake.feed(
            &self.staging[..self.staged],
            &mut output[*used..],
            &mut actions,
            &mut *self.headers,
        );
        if result.status == FeedStatus::ConnectionFailed {
            *used += result.output_used.min(output.len() - *used);
            outcome.written = *used;
            outcome.goaway = result.output_used > 0;
            outcome.closed = true;
            self.terminal = true;
            return false;
        }
        let full = result.status == FeedStatus::OutputFull;
        if !pump_controls(&mut actions, output, used, events, event_len, max, staged) {
            self.fail(ErrorCode::InternalError, output, used, outcome);
            return false;
        }
        consume(&mut *self.staging, &mut self.staged, result.consumed);
        if full || (result.consumed > 0 && self.staged > 0) {
            outcome.pending = true;
        }
        if !handshake.is_complete() {
            if *event_len > 0 {
                self.fail(ErrorCode::ProtocolError, output, used, outcome);
                return false;
            }
        } else {
            let Phase::Handshake(handshake) = mem::replace(&mut self.phase, Phase::Dead) else {
                self.terminal = true;
                outcome.closed = true;
                return false;
            };
            self.phase = Phase::Serving(handshake.into_server());
        }
        true
    }

    fn run_feed(
        &mut self,
        output: &mut [u8],
        used: &mut usize,
        outcome: &mut StepOutcome,
        events: &mut [Option<StreamEvent>; MAX_STREAMS_PER_STEP],
        event_len: &mut usize,
        staged: &mut Vec<u8>,
    ) -> bool {
        let server = match self.phase.serving(&mut self.terminal, &mut *outcome) {
            Some(server) => server,
            None => return false,
        };
        let max = server.driver().streams().remote_settings().max_frame_size;
        let mut actions: [Option<H2FrameAction<'_>>; MAX_STREAMS_PER_STEP] =
            [None; MAX_STREAMS_PER_STEP];
        let result = server.driver_mut().feed(
            &self.staging[..self.staged],
            &mut actions,
            &mut *self.headers,
        );
        if result.status == FeedStatus::ConnectionFailed {
            let code = result.error.unwrap_or(ErrorCode::InternalError);
            consume(&mut *self.staging, &mut self.staged, result.consumed);
            self.fail(code, output, used, outcome);
            return false;
        }
        let full = result.status == FeedStatus::OutputFull;
        if !pump_controls(&mut actions, output, used, events, event_len, max, staged) {
            self.fail(ErrorCode::InternalError, output, used, outcome);
            return false;
        }
        consume(&mut *self.staging, &mut self.staged, result.consumed);
        if full || (result.consumed > 0 && self.staged > 0) {
            outcome.pending = true;
        }
        true
    }

    fn fail(
        &mut self,
        code: ErrorCode,
        output: &mut [u8],
        used: &mut usize,
        outcome: &mut StepOutcome,
    ) {
        let Some((last, max)) = self.phase.limits() else {
            outcome.closed = true;
            self.terminal = true;
            return;
        };
        fail_goaway(last, max, &mut self.terminal, output, used, outcome, code);
    }
}

fn grant_window(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    owed: u32,
    output: &mut [u8],
) -> usize {
    if owed == 0 || output.len() < 13 {
        return 0;
    }
    let max = server.driver().streams().remote_settings().max_frame_size;
    let mut writer = ControlFrameWriter::new(output, max);
    let _ = writer.queue_window_update(stream, owed);
    let credit = if stream == 0 {
        server.driver_mut().add_connection_receive_credit(owed)
    } else {
        server.driver_mut().add_stream_receive_credit(stream, owed)
    };
    if credit.is_err() {
        return 0;
    }
    writer.pending_bytes().len()
}

fn consume(staging: &mut [u8], staged: &mut usize, consumed: usize) {
    let consumed = consumed.min(*staged);
    staging.copy_within(consumed..*staged, 0);
    *staged -= consumed;
}

fn pump_controls(
    actions: &mut [Option<H2FrameAction<'_>>; MAX_STREAMS_PER_STEP],
    output: &mut [u8],
    used: &mut usize,
    events: &mut [Option<StreamEvent>; MAX_STREAMS_PER_STEP],
    event_len: &mut usize,
    max: u32,
    staged: &mut Vec<u8>,
) -> bool {
    let mut stage = |event: StreamEvent| {
        if *event_len < MAX_STREAMS_PER_STEP {
            events[*event_len] = Some(event);
            *event_len += 1;
        }
    };
    for slot in actions.iter_mut() {
        let Some(action) = slot.take() else { continue };
        match action {
            H2FrameAction::SettingsAckRequired
            | H2FrameAction::PingAck { .. }
            | H2FrameAction::SendReset { .. } => {
                let mut writer = ControlFrameWriter::new(&mut output[*used..], max);
                if writer.queue_action(action, 0).is_err() {
                    return false;
                }
                *used += writer.pending_bytes().len();
            }
            H2FrameAction::Headers {
                stream_id,
                headers,
                end_stream,
            } => stage(StreamEvent::Headers {
                stream: stream_id,
                range: headers,
                end: end_stream,
            }),
            H2FrameAction::Data {
                stream_id,
                data,
                end_stream,
            } => {
                let start = staged.len();
                staged.extend_from_slice(data);
                stage(StreamEvent::Data(stream_id, start, data.len(), end_stream));
            }
            H2FrameAction::StreamReset { .. } => stage(StreamEvent::PeerReset),
            H2FrameAction::GoAway { .. } | H2FrameAction::ConnectionError { .. } => {
                stage(StreamEvent::PeerGoAway)
            }
        }
    }
    true
}

pub(crate) fn fail_goaway(
    last: u32,
    max: u32,
    terminal: &mut bool,
    output: &mut [u8],
    used: &mut usize,
    outcome: &mut StepOutcome,
    code: ErrorCode,
) {
    let mut writer = ControlFrameWriter::new(&mut output[*used..], max);
    if let Ok(staged) =
        writer.queue_action(H2FrameAction::ConnectionError { error_code: code }, last)
    {
        *used += staged;
        outcome.goaway = true;
    }
    outcome.written = *used;
    outcome.closed = true;
    *terminal = true;
}
