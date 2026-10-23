use core::mem;

use veylor_http1::is_token;
use veylor_http2::{
    ControlFrameWriter, ErrorCode, FeedStatus, H2C_GOAWAY_LEN, H2FrameAction, H2ServerDriver,
    H2cServerHandshake, HeaderRange, HpackFieldToEncode, HpackHeader, HpackRepresentation,
    HpackTable, ServerError, StreamTable,
};

use crate::static_routes::{MAX_RESPONSE_BODY, MAX_ROUTES, RouteBuildError};

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

    fn lookup(&self, method: &[u8], path: &[u8]) -> Option<(u16, &[u8])> {
        if let Some(route) = self
            .routes
            .iter()
            .find(|route| route.method == method && route.path == path)
        {
            return Some((route.status, route.body));
        }
        if method == b"HEAD" {
            if let Some(route) = self
                .routes
                .iter()
                .find(|route| route.method == b"GET" && route.path == path)
            {
                return Some((route.status, route.body));
            }
        }
        None
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
    pub goaway: bool,
    pub closed: bool,
    pub pending: bool,
}

#[derive(Clone, Copy)]
enum StreamEvent {
    Headers {
        stream: u32,
        range: HeaderRange,
        end: bool,
    },
    Data {
        stream: u32,
    },
    PeerReset,
    PeerGoAway,
}

enum Phase<'a> {
    Handshake(H2cServerHandshake<'a, 'a, 'a, 'a>),
    Serving(H2ServerDriver<'a, 'a, 'a, 'a>),
    Dead,
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
        let (last, max) = match &self.phase {
            Phase::Handshake(handshake) => (
                handshake.server().driver().streams().highest_remote(),
                handshake
                    .server()
                    .driver()
                    .streams()
                    .remote_settings()
                    .max_frame_size,
            ),
            Phase::Serving(server) => (
                server.driver().streams().highest_remote(),
                server.driver().streams().remote_settings().max_frame_size,
            ),
            Phase::Dead => {
                outcome.closed = true;
                return outcome;
            }
        };
        let mut used = 0;
        fail_goaway(
            last,
            max,
            &mut self.terminal,
            output,
            &mut used,
            &mut outcome,
            ErrorCode::NoError,
        );
        outcome
    }

    pub fn step(&mut self, output: &mut [u8], routes: &FrozenRoutesView<'_>) -> StepOutcome {
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
        if !self.is_serving() {
            if !self.run_handshake(output, &mut used, &mut outcome, &mut events, &mut event_len) {
                return outcome;
            }
            if !self.is_serving() {
                outcome.written = used;
                return outcome;
            }
        }
        if event_len == 0
            && self.staged > 0
            && !self.run_feed(output, &mut used, &mut outcome, &mut events, &mut event_len)
        {
            return outcome;
        }
        let server = match self.phase {
            Phase::Serving(ref mut server) => server,
            Phase::Handshake(_) | Phase::Dead => {
                self.terminal = true;
                outcome.written = used;
                outcome.closed = true;
                return outcome;
            }
        };
        let mut serve = Serve {
            server,
            headers: &*self.headers,
            terminal: &mut self.terminal,
            routes,
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
        let handshake = match self.phase {
            Phase::Handshake(ref mut handshake) => handshake,
            Phase::Serving(_) | Phase::Dead => {
                self.terminal = true;
                outcome.closed = true;
                return false;
            }
        };
        match handshake.server_settings(&mut output[*used..]) {
            Ok(writer) => *used += writer.pending_bytes().len(),
            Err(_) => {
                outcome.pending = true;
                return false;
            }
        }
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
    ) -> bool {
        let (max, handshake) = match self.phase {
            Phase::Handshake(ref mut handshake) => (
                handshake
                    .server()
                    .driver()
                    .streams()
                    .remote_settings()
                    .max_frame_size,
                handshake,
            ),
            Phase::Serving(_) | Phase::Dead => {
                self.terminal = true;
                outcome.closed = true;
                return false;
            }
        };
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
        if !pump_controls(&mut actions, output, used, events, event_len, max) {
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
    ) -> bool {
        let (max, server) = match self.phase {
            Phase::Serving(ref mut server) => (
                server.driver().streams().remote_settings().max_frame_size,
                server,
            ),
            Phase::Handshake(_) | Phase::Dead => {
                self.terminal = true;
                outcome.closed = true;
                return false;
            }
        };
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
        if !pump_controls(&mut actions, output, used, events, event_len, max) {
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
        let (last, max) = match self.phase {
            Phase::Handshake(ref mut handshake) => (
                handshake.server().driver().streams().highest_remote(),
                handshake
                    .server()
                    .driver()
                    .streams()
                    .remote_settings()
                    .max_frame_size,
            ),
            Phase::Serving(ref mut server) => (
                server.driver().streams().highest_remote(),
                server.driver().streams().remote_settings().max_frame_size,
            ),
            Phase::Dead => {
                outcome.closed = true;
                self.terminal = true;
                return;
            }
        };
        fail_goaway(last, max, &mut self.terminal, output, used, outcome, code);
    }
}

struct Serve<'s, 'o, 'b, 't, 'd, 'e> {
    server: &'s mut H2ServerDriver<'b, 't, 'd, 'e>,
    headers: &'s [HpackHeader],
    terminal: &'s mut bool,
    routes: &'o FrozenRoutesView<'o>,
    output: &'o mut [u8],
    used: &'o mut usize,
    outcome: &'o mut StepOutcome,
}

impl<'s, 'o, 'b, 't, 'd, 'e> Serve<'s, 'o, 'b, 't, 'd, 'e> {
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

    fn refuse(&mut self, stream: u32, close: bool) {
        match self.server.refuse(stream, &mut self.output[*self.used..]) {
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
                    Err(ServerError::RefusedStream) => return self.refuse(stream, false),
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
                    _ => return self.refuse(stream, false),
                };
                let is_head = request.method == b"HEAD";
                let refuse_body = !end;
                let (status, body) = self
                    .routes
                    .lookup(request.method, path)
                    .unwrap_or((404, NOT_FOUND_BODY));
                let mut status_buf = [0; DECIMAL_CAP];
                let status_len = push_decimal(&mut status_buf, u64::from(status));
                let mut length_buf = [0; DECIMAL_CAP];
                let length_len = push_decimal(&mut length_buf, body.len() as u64);
                let response_fields = [
                    literal(b":status", &status_buf[..status_len]),
                    literal(b"content-length", &length_buf[..length_len]),
                ];
                let end_stream = (is_head || body.is_empty()) && !refuse_body;
                let mut block = [0; HPACK_BLOCK_CAP];
                let sent = match self.server.send_response(
                    stream,
                    &response_fields,
                    &mut block,
                    &mut self.output[*self.used..],
                    end_stream,
                ) {
                    Ok(sent) => sent,
                    Err(_) => return self.fail(ErrorCode::InternalError),
                };
                *self.used += sent.writer.pending_bytes().len();
                if !end_stream && !refuse_body {
                    let mut offset = 0;
                    while offset < body.len() {
                        let room = self.output.len() - *self.used;
                        if room < 10 {
                            return self.fail(ErrorCode::EnhanceYourCalm);
                        }
                        let take = (body.len() - offset).min(room - 9);
                        let last = offset + take == body.len();
                        let sent = match self.server.send_data(
                            stream,
                            &body[offset..offset + take],
                            last,
                            &mut self.output[*self.used..],
                        ) {
                            Ok(sent) => sent,
                            Err(_) => return self.fail(ErrorCode::FlowControlError),
                        };
                        if sent.frame.payload_length == 0 {
                            return self.fail(ErrorCode::FlowControlError);
                        }
                        *self.used += sent.writer.pending_bytes().len();
                        offset += sent.frame.payload_length;
                    }
                }
                if refuse_body {
                    return self.refuse(stream, true);
                }
                self.outcome.served += 1;
            }
            StreamEvent::Data { stream } => {
                self.refuse(stream, true);
                self.outcome.closed = true;
            }
            StreamEvent::PeerReset => {}
            StreamEvent::PeerGoAway => self.outcome.closed = true,
        }
    }
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
) -> bool {
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
            } => {
                if *event_len < MAX_STREAMS_PER_STEP {
                    events[*event_len] = Some(StreamEvent::Headers {
                        stream: stream_id,
                        range: headers,
                        end: end_stream,
                    });
                    *event_len += 1;
                }
            }
            H2FrameAction::Data { stream_id, .. } => {
                if *event_len < MAX_STREAMS_PER_STEP {
                    events[*event_len] = Some(StreamEvent::Data { stream: stream_id });
                    *event_len += 1;
                }
            }
            H2FrameAction::StreamReset { .. } => {
                if *event_len < MAX_STREAMS_PER_STEP {
                    events[*event_len] = Some(StreamEvent::PeerReset);
                    *event_len += 1;
                }
            }
            H2FrameAction::GoAway { .. } | H2FrameAction::ConnectionError { .. } => {
                if *event_len < MAX_STREAMS_PER_STEP {
                    events[*event_len] = Some(StreamEvent::PeerGoAway);
                    *event_len += 1;
                }
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

fn literal<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

fn push_decimal(buf: &mut [u8; DECIMAL_CAP], mut value: u64) -> usize {
    let mut digits = [0; DECIMAL_CAP];
    let mut len = 0;
    loop {
        digits[len] = b'0' + (value % 10) as u8;
        len += 1;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    for (index, digit) in digits[..len].iter().rev().enumerate() {
        buf[index] = *digit;
    }
    len
}
