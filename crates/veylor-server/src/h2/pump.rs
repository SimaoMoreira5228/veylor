use core::mem;

use veylor_http2::{ControlFrameWriter, ErrorCode, FeedStatus, H2FrameAction, H2ServerDriver};

use super::serve;
use super::{
    FrozenRoutesView, H2Conn, H2HostCtx, MAX_STREAMS_PER_STEP, MIN_EVENT_ROOM, Phase, StepOutcome,
    StreamEvent,
};

impl<'a> H2Conn<'a> {
    pub(super) fn step_inner(
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
        let mut events = self.pending_events;
        let mut event_len = self.pending_event_len;
        self.pending_events = [None; MAX_STREAMS_PER_STEP];
        self.pending_event_len = 0;
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
        let (expired_written, expired_count) =
            self.pending_responses.expire(server, &mut output[used..]);
        used += expired_written;
        outcome.refused += expired_count;

        match self.pending_responses.pump(server, output, &mut used) {
            Ok((completed, pending)) => {
                outcome.served += completed;
                outcome.pending |= pending;
            }
            Err(code) => {
                self.fail(code, output, &mut used, &mut outcome);
                outcome.written = used;
                return outcome;
            }
        }

        let output_len = output.len();
        let scratch: &mut [u8] = match self.file_scratch.as_deref_mut() {
            Some(scratch) => &mut scratch[..],
            None => &mut [],
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
            pending: &mut self.pending_responses,
            files: &mut self.file_transfers,
            scratch,
        };
        let mut served_events = 0;
        for event in events.iter().take(event_len).flatten() {
            // NOTE: a file HEADERS emits its head plus the first DATA slice into the
            // shared pending buffer. Serving the next stream with too little room would
            // fail the connection, so leave the rest of the batch queued for the next
            // step, which starts with a flush.
            if *serve.used + MIN_EVENT_ROOM > output_len {
                break;
            }
            serve.serve_one(*event);
            served_events += 1;
            if serve.outcome.closed {
                break;
            }
        }
        if !serve.outcome.closed && served_events < event_len {
            let mut slot = 0;
            for event in events[served_events..event_len].iter().flatten() {
                self.pending_events[slot] = Some(*event);
                slot += 1;
            }
            self.pending_event_len = slot;
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
        let input: &[u8] = match self.staging.as_deref() {
            Some(staging) => &staging[..self.staged],
            None => &[],
        };
        let result = handshake.feed(
            input,
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
        let controls = Controls {
            max,
            output: &mut *output,
            used: &mut *used,
            events: &mut *events,
            event_len: &mut *event_len,
            staged: &mut *staged,
        };
        if !pump_controls(handshake.server_mut(), &mut actions, controls) {
            self.fail(ErrorCode::InternalError, output, used, outcome);
            return false;
        }
        if let Some(staging) = self.staging.as_deref_mut() {
            consume(staging, &mut self.staged, result.consumed);
        }
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
        let input: &[u8] = match self.staging.as_deref() {
            Some(staging) => &staging[..self.staged],
            None => &[],
        };
        let result = server
            .driver_mut()
            .feed(input, &mut actions, &mut *self.headers);
        if result.status == FeedStatus::ConnectionFailed {
            let code = result.error.unwrap_or(ErrorCode::InternalError);
            if let Some(staging) = self.staging.as_deref_mut() {
                consume(staging, &mut self.staged, result.consumed);
            }
            self.fail(code, output, used, outcome);
            return false;
        }
        let full = result.status == FeedStatus::OutputFull;
        let controls = Controls {
            max,
            output: &mut *output,
            used: &mut *used,
            events: &mut *events,
            event_len: &mut *event_len,
            staged: &mut *staged,
        };
        if !pump_controls(server, &mut actions, controls) {
            self.fail(ErrorCode::InternalError, output, used, outcome);
            return false;
        }
        if let Some(staging) = self.staging.as_deref_mut() {
            consume(staging, &mut self.staged, result.consumed);
        }
        if full || (result.consumed > 0 && self.staged > 0) {
            outcome.pending = true;
        }
        true
    }

    pub(super) fn fail(
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

pub(crate) fn grant_window(
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

struct Controls<'a> {
    max: u32,
    output: &'a mut [u8],
    used: &'a mut usize,
    events: &'a mut [Option<StreamEvent>; MAX_STREAMS_PER_STEP],
    event_len: &'a mut usize,
    staged: &'a mut Vec<u8>,
}

fn pump_controls(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    actions: &mut [Option<H2FrameAction<'_>>; MAX_STREAMS_PER_STEP],
    controls: Controls<'_>,
) -> bool {
    let Controls {
        max,
        output,
        used,
        events,
        event_len,
        staged,
    } = controls;
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
                padding,
                end_stream,
            } => {
                // SECURITY: padding is charged to both receive windows but never reaches the
                // body, so it is credited back here or a padded flood starves the connection.
                if padding > 0 {
                    let Ok(owed) = u32::try_from(padding) else {
                        return false;
                    };
                    let connection = grant_window(server, 0, owed, &mut output[*used..]);
                    if connection == 0 {
                        return false;
                    }
                    *used += connection;
                    *used += grant_window(server, stream_id, owed, &mut output[*used..]);
                }
                let start = staged.len();
                staged.extend_from_slice(data);
                stage(StreamEvent::Data(stream_id, start, data.len(), end_stream));
            }
            H2FrameAction::ConnectionCredit { amount } => {
                let written = grant_window(server, 0, amount, &mut output[*used..]);
                if written == 0 {
                    return false;
                }
                *used += written;
            }
            H2FrameAction::StreamReset { .. } => stage(StreamEvent::PeerReset),
            H2FrameAction::ConnectionError { error_code } => {
                let mut writer = ControlFrameWriter::new(&mut output[*used..], max);
                if writer
                    .queue_action(H2FrameAction::ConnectionError { error_code }, 0)
                    .is_err()
                {
                    return false;
                }
                *used += writer.pending_bytes().len();
                stage(StreamEvent::PeerGoAway);
            }
            H2FrameAction::GoAway { .. } => stage(StreamEvent::PeerGoAway),
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
