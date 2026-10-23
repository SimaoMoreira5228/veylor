use core::mem;

use veylor_http1::is_token;
use veylor_http2::{
    ControlFrameWriter, ErrorCode, FeedStatus, H2C_GOAWAY_LEN, H2FrameAction, H2ServerDriver,
    H2cServerHandshake, HeaderRange, HpackFieldToEncode, HpackHeader, HpackRepresentation,
    HpackTable, ServerError, StreamState, StreamTable, ValidatedServerRequest,
};
use veylor_runtime::ConnectionToken;

use crate::host::{HostShared, StagedWork};

use crate::static_routes::{MAX_RESPONSE_BODY, MAX_ROUTES, RouteBuildError};

// NOTE: worker-turn hook (slot state plus accept/pump/close dispatch). The
// step driver above stays transport-neutral; turn.rs owns the sockets,
// listeners, and timers that drive it.
pub(crate) mod turn;

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
const NOT_FOUND_BODY: &[u8] = b"Not Found";

#[derive(Clone, Copy, Debug)]
pub struct FrozenRoute<'a> {
    pub method: &'a [u8],
    pub path: &'a [u8],
    pub status: u16,
    pub body: &'a [u8],
    pub managed: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct FrozenRoutesView<'a> {
    routes: &'a [FrozenRoute<'a>],
}

impl<'a> FrozenRoutesView<'a> {
    pub fn new(routes: &'a [FrozenRoute<'a>]) -> Result<Self, RouteBuildError> {
        if routes.len() > MAX_ROUTES {
            return Err(RouteBuildError::Full);
        }
        for route in routes {
            if route.method.is_empty() || !route.method.iter().copied().all(is_token) {
                return Err(RouteBuildError::Invalid);
            }
            if route.path.is_empty() {
                return Err(RouteBuildError::Invalid);
            }
            if !(200..=599).contains(&route.status) || route.status == 204 {
                return Err(RouteBuildError::Invalid);
            }
            if route.body.len() > MAX_RESPONSE_BODY {
                return Err(RouteBuildError::BodyTooLarge);
            }
        }
        for (index, route) in routes.iter().enumerate() {
            let dupe = routes[..index]
                .iter()
                .any(|prev| prev.method == route.method && prev.path == route.path);
            if dupe {
                return Err(RouteBuildError::Duplicate);
            }
        }
        Ok(Self { routes })
    }

    fn find(&self, method: &[u8], path: &[u8]) -> Option<usize> {
        let exact = self
            .routes
            .iter()
            .position(|route| route.method == method && route.path == path);
        if exact.is_some() {
            return exact;
        }
        if method != b"HEAD" {
            return None;
        }
        self.routes
            .iter()
            .position(|route| route.method == b"GET" && route.path == path)
    }

    fn lookup(&self, method: &[u8], path: &[u8]) -> Option<(u16, &[u8])> {
        self.find(method, path)
            .map(|index| (self.routes[index].status, self.routes[index].body))
    }

    fn is_managed(&self, method: &[u8], path: &[u8]) -> bool {
        self.find(method, path)
            .is_some_and(|index| self.routes[index].managed)
    }
}

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
enum StreamEvent {
    Headers {
        stream: u32,
        range: HeaderRange,
        end: bool,
    },
    Data(u32, usize, usize, bool),
    PeerReset,
    PeerGoAway,
}

enum Phase<'a> {
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
            Phase::Handshake(_) | Phase::Dead => {
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
            Phase::Serving(_) | Phase::Dead => {
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
    phase: Phase<'a>,
    settings_sent: bool,
    terminal: bool,
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
        let mut terminal = false;
        let phase = match H2cServerHandshake::new(streams, decoder, encoder, ack_limits) {
            Ok(handshake) => Phase::Handshake(handshake),
            Err(_) => {
                terminal = true;
                Phase::Dead
            }
        };
        Self {
            staging,
            staged: 0,
            headers,
            phase,
            settings_sent: false,
            terminal,
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
        if owed > 0 && output[used..].len() >= 13 {
            let max = server.driver().streams().remote_settings().max_frame_size;
            let mut writer = ControlFrameWriter::new(&mut output[used..], max);
            let _ = writer.queue_window_update(0, owed);
            let credit = server.driver_mut().add_connection_receive_credit(owed);
            if credit.is_ok() {
                used += writer.pending_bytes().len();
            }
        }
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
        if owed > 0 && output[used..].len() >= 13 {
            let max = server.driver().streams().remote_settings().max_frame_size;
            let mut writer = ControlFrameWriter::new(&mut output[used..], max);
            let _ = writer.queue_window_update(stream, owed);
            let credit = server.driver_mut().add_stream_receive_credit(stream, owed);
            if credit.is_ok() {
                used += writer.pending_bytes().len();
            }
        }
        match send_full(
            server,
            stream,
            staged.status,
            &staged.body,
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
        let mut serve = Serve {
            server,
            headers: &*self.headers,
            data: &databuf,
            terminal: &mut self.terminal,
            routes,
            host,
            output,
            used: &mut used,
            outcome: &mut outcome,
        };
        for event in events.iter().take(event_len).flatten() {
            serve.serve_one(*event);
            if serve.outcome.closed {
                break;
            }
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
        let handshake = match self.phase.handshake(&mut self.terminal, &mut *outcome) {
            Some(handshake) => handshake,
            None => return false,
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
        let handshake = match self.phase.handshake(&mut self.terminal, &mut *outcome) {
            Some(handshake) => handshake,
            None => return false,
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
            let phase = mem::replace(&mut self.phase, Phase::Dead);
            match phase {
                Phase::Handshake(handshake) => self.phase = Phase::Serving(handshake.into_server()),
                Phase::Serving(_) | Phase::Dead => {
                    self.terminal = true;
                    outcome.closed = true;
                    return false;
                }
            }
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

struct Serve<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> {
    server: &'s mut H2ServerDriver<'b, 't, 'd, 'e>,
    headers: &'s [HpackHeader],
    data: &'u [u8],
    terminal: &'s mut bool,
    routes: &'o FrozenRoutesView<'o>,
    host: Option<H2HostCtx<'h>>,
    output: &'o mut [u8],
    used: &'o mut usize,
    outcome: &'o mut StepOutcome,
}

impl<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> Serve<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> {
    fn fail(&mut self, code: ErrorCode) {
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

    fn deny(&mut self, stream: u32, code: ErrorCode, close: bool) {
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

    fn serve_one(&mut self, event: StreamEvent) {
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
                    Err(ServerError::Request(_)) => return self.fail(ErrorCode::ProtocolError),
                    Err(_) => return self.fail(ErrorCode::InternalError),
                };
                let path = match request.path {
                    Some(path) if path.first() == Some(&b'/') => path,
                    _ => return self.deny(stream, ErrorCode::RefusedStream, false),
                };
                let is_head = request.method == b"HEAD";
                if self.routes.is_managed(request.method, path) {
                    if let Some(ctx) = self.host {
                        let declared = request.content_length;
                        if end && declared.unwrap_or(0) > 0 {
                            return self.deny(stream, ErrorCode::ProtocolError, false);
                        }
                        if end || declared.is_some() {
                            let capped = declared.unwrap_or(0) <= H2_BODY_CAP;
                            let parked = capped
                                && park_stream(ctx, self.routes, stream, request, self.outcome);
                            if parked {
                                return;
                            }
                            return self.deny(stream, ErrorCode::RefusedStream, false);
                        }
                    }
                }
                let (status, body) = self
                    .routes
                    .lookup(request.method, path)
                    .unwrap_or((404, NOT_FOUND_BODY));
                if !end {
                    if send_full(
                        self.server,
                        stream,
                        status,
                        body,
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
    ) {
        Ok(_) => {
            outcome.parked += 1;
            true
        }
        Err(_) => false,
    }
}

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
        (H2DataOutcome::Stored, None)
    });
    let Some((result, _)) = outcome else {
        return H2DataOutcome::Gone;
    };
    result
}

#[allow(clippy::too_many_arguments)]
fn send_full(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    status: u16,
    body: &[u8],
    end_stream: bool,
    send_body: bool,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(status));
    let mut length_buf = [0; DECIMAL_CAP];
    let length_len = push_decimal(&mut length_buf, body.len() as u64);
    let response_fields = [
        field(b":status", &status_buf[..status_len]),
        field(b"content-length", &length_buf[..length_len]),
    ];
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields,
            &mut block,
            &mut output[*used..],
            end_stream,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    if end_stream || !send_body {
        return Ok(());
    }
    let mut offset = 0;
    while offset < body.len() {
        let room = output.len() - *used;
        if room < 10 {
            return Err(ErrorCode::EnhanceYourCalm);
        }
        let take = (body.len() - offset).min(room - 9);
        let last = offset + take == body.len();
        let sent = server
            .send_data(
                stream,
                &body[offset..offset + take],
                last,
                &mut output[*used..],
            )
            .map_err(|_| ErrorCode::FlowControlError)?;
        if sent.frame.payload_length == 0 {
            return Err(ErrorCode::FlowControlError);
        }
        *used += sent.writer.pending_bytes().len();
        offset += sent.frame.payload_length;
    }
    Ok(())
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

fn fail_goaway(
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

fn field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::Default,
        huffman_name: false,
        huffman_value: false,
    }
}

fn push_decimal(buf: &mut [u8; DECIMAL_CAP], mut value: u64) -> usize {
    let mut end = DECIMAL_CAP;
    loop {
        end -= 1;
        buf[end] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    let len = DECIMAL_CAP - end;
    buf.copy_within(end.., 0);
    len
}
