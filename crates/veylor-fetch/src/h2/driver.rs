use std::io;
use std::sync::Arc;

use veylor_client::h2::{
    CLIENT_CONNECTION_PREFACE, DEFAULT_MAX_FIELDS, DEFAULT_REQUEST_BLOCK, ErrorCode,
    H2ClientConnection, H2ClientError, H2ClientLimits, H2ClientTables, HPACK_ENTRIES,
    HpackEntrySlot, HpackHeader, MIN_STREAM_SLOTS, SETTINGS_ACKS, SETTINGS_INITIAL_WINDOW_SIZE,
    SettingsParameter, StreamSlot, WriteOut, decoder_scratch_len, encoder_scratch_len,
    header_block_len,
};

use super::fields;
use super::{BODY_BUFFER_CAP, Command, Failure, H2Settings, Shared};
use crate::budget::Budget;
use crate::wire::Wire;

const READ_CHUNK: usize = 16 * 1024 + 512;
const OUT_CAP: usize = 64 * 1024;
const MAX_INBUF: usize = 512 * 1024;
const CONTROL_ROOM: usize = 64;
const DATA_ROOM: usize = 1_024;
const HEAD_SLACK: usize = 64;
const MIN_FREE: usize = 4_096;
const STREAM_WINDOW: u32 = 256 * 1024;
const CONNECTION_WINDOW: u32 = 1024 * 1024;
const PROTOCOL_WINDOW: u32 = 65_535;
const CREDIT_THRESHOLD: usize = 16 * 1024;
const MAX_QUEUED: usize = 8;

#[derive(Default)]
pub(super) struct Outbox {
    pub(super) settings_acks: usize,
    pub(super) pings: Vec<[u8; 8]>,
    pub(super) resets: Vec<(u32, ErrorCode)>,
    pub(super) connection_error: Option<ErrorCode>,
}

impl Outbox {
    pub(super) fn push_ping(&mut self, data: [u8; 8]) {
        if self.pings.len() < MAX_QUEUED {
            self.pings.push(data);
        }
    }

    pub(super) fn push_reset(&mut self, id: u32, code: ErrorCode) {
        if self.resets.len() < MAX_QUEUED {
            self.resets.push((id, code));
        }
    }
}

pub(super) struct Active {
    pub(super) id: u32,
    pub(super) body: Vec<u8>,
    pub(super) sent: usize,
    pub(super) local_ended: bool,
}

pub(super) struct Driver<'c, 'b> {
    pub(super) conn: H2ClientConnection<'c, 'b>,
    pub(super) wire: Wire,
    pub(super) shared: Arc<Shared>,
    pub(super) settings: H2Settings,
    pub(super) header_buf: Box<[HpackHeader]>,
    pub(super) inbuf: Vec<u8>,
    pub(super) out: Vec<u8>,
    pub(super) out_len: usize,
    pub(super) outbox: Outbox,
    pub(super) active: Option<Active>,
    pub(super) credit_conn: usize,
    pub(super) credit_stream: usize,
    pub(super) draining: bool,
    pub(super) terminal: Option<Failure>,
}

pub(super) fn run(wire: Wire, shared: Arc<Shared>, settings: H2Settings) {
    let limits = H2ClientLimits {
        max_header_list_size: settings.max_head.clamp(4 * 1024, 1024 * 1024) as u32,
        max_fields: DEFAULT_MAX_FIELDS,
        request_block: DEFAULT_REQUEST_BLOCK,
    };
    let mut slots = vec![StreamSlot::EMPTY; MIN_STREAM_SLOTS + 1];
    let mut header_block = vec![0u8; header_block_len(limits.max_header_list_size)];
    let mut decoder_entries = vec![HpackEntrySlot::EMPTY; HPACK_ENTRIES];
    let mut decoder_scratch = vec![0u8; decoder_scratch_len(limits.max_header_list_size)];
    let mut encoder_entries = vec![HpackEntrySlot::EMPTY; HPACK_ENTRIES];
    let mut encoder_scratch = vec![0u8; encoder_scratch_len(limits.request_block)];
    let mut ack_limits = [0u32; SETTINGS_ACKS];
    let mut tables = H2ClientTables::new(
        &mut slots,
        &mut header_block,
        &mut decoder_entries,
        &mut decoder_scratch,
        &mut encoder_entries,
        &mut encoder_scratch,
        &mut ack_limits,
    )
    .with_limits(limits);

    let failure = match tables.connect() {
        Ok(conn) => Driver::new(conn, wire, Arc::clone(&shared), settings).serve(),
        Err(_) => Failure::new(io::ErrorKind::Other, "h2 engine setup failed"),
    };

    let mut state = shared.state.lock();

    state.dead = true;
    state.idle = false;
    state.command = None;

    if !state.ended && state.failure.is_none() {
        state.failure = Some(failure);
    }

    drop(state);
    shared.wake.notify_all();
}

impl<'c, 'b> Driver<'c, 'b> {
    fn new(
        conn: H2ClientConnection<'c, 'b>,
        wire: Wire,
        shared: Arc<Shared>,
        settings: H2Settings,
    ) -> Self {
        Self {
            conn,
            wire,
            shared,
            settings,
            header_buf: vec![HpackHeader::EMPTY; DEFAULT_MAX_FIELDS].into_boxed_slice(),
            inbuf: Vec::new(),
            out: vec![0; OUT_CAP],
            out_len: 0,
            outbox: Outbox::default(),
            active: None,
            credit_conn: 0,
            credit_stream: 0,
            draining: false,
            terminal: None,
        }
    }

    fn serve(mut self) -> Failure {
        match self.main_loop() {
            Err(failure) => failure,
            Ok(()) => Failure::new(io::ErrorKind::ConnectionAborted, "h2 connection closed"),
        }
    }

    fn main_loop(&mut self) -> Result<(), Failure> {
        let handshake = Budget {
            read: Some(self.settings.handshake_timeout),
            write: Some(self.settings.handshake_timeout),
            overall: None,
        };

        self.write_preface(&handshake)?;

        loop {
            let Some(command) = self.next_command() else {
                return Ok(());
            };

            self.exchange(command)?;

            if self.draining {
                return Ok(());
            }

            let mut state = self.shared.state.lock();

            state.idle = state.command.is_none();
        }
    }

    fn next_command(&self) -> Option<Command> {
        let mut state = self.shared.state.lock();

        loop {
            if state.shutdown {
                return None;
            }

            if let Some(command) = state.command.take() {
                return Some(command);
            }

            let waited = self
                .shared
                .wake
                .wait_for(&mut state, self.settings.idle_timeout);

            if waited.timed_out() && state.command.is_none() {
                return None;
            }
        }
    }

    fn exchange(&mut self, command: Command) -> Result<(), Failure> {
        let budget = command.budget;

        self.ensure_ready(&budget)?;

        if !self.begin(command, &budget)? {
            return Ok(());
        }

        loop {
            if self.shared.state.lock().shutdown {
                return Err(Failure::new(
                    io::ErrorKind::ConnectionAborted,
                    "request abandoned",
                ));
            }

            self.produce(&budget)?;
            self.flush(&budget)?;

            if self.active.is_none() {
                return Ok(());
            }

            if self.buffered() >= BODY_BUFFER_CAP {
                self.wait_room();

                continue;
            }

            self.read_more(&budget)?;
        }
    }

    fn ensure_ready(&mut self, budget: &Budget) -> Result<(), Failure> {
        while !(self.conn.peer_settings_received() && self.conn.local_settings_acknowledged()) {
            self.produce(budget)?;
            self.flush(budget)?;
            self.read_more(budget)?;
        }

        Ok(())
    }

    fn write_preface(&mut self, budget: &Budget) -> Result<(), Failure> {
        let preface = CLIENT_CONNECTION_PREFACE;

        self.out[..preface.len()].copy_from_slice(preface);
        self.out_len = preface.len();

        let parameters = [SettingsParameter {
            id: SETTINGS_INITIAL_WINDOW_SIZE,
            value: STREAM_WINDOW,
        }];

        if !self.emit(|conn, out| conn.send_settings(&parameters, out)) {
            return Err(Failure::new(io::ErrorKind::Other, "h2 settings failed"));
        }

        let increment = CONNECTION_WINDOW - PROTOCOL_WINDOW;

        if !self.emit(|conn, out| conn.send_window_update(0, increment, out)) {
            return Err(Failure::new(
                io::ErrorKind::Other,
                "h2 window update failed",
            ));
        }

        self.flush(budget)
    }

    fn begin(&mut self, command: Command, budget: &Budget) -> Result<bool, Failure> {
        let end_stream = command.body.is_empty();
        let mut digits = [0u8; 20];

        if self.free() < DEFAULT_REQUEST_BLOCK + HEAD_SLACK {
            self.flush(budget)?;
        }

        let started = match fields::build(&command, &mut digits) {
            Ok((built, used)) => {
                let output = &mut self.out[self.out_len..];

                self.conn.start_request(&built[..used], output, end_stream)
            }
            Err(failure) => {
                self.publish_failure(failure);

                return Ok(false);
            }
        };

        let started = match started {
            Ok(started) => started,
            Err(H2ClientError::BadState) => {
                return Err(Failure::new(
                    io::ErrorKind::ConnectionAborted,
                    "h2 connection closing",
                ));
            }
            Err(error) => {
                let message = format!("h2 request rejected: {error:?}");

                self.publish_failure(Failure::new(io::ErrorKind::InvalidInput, message));

                return Ok(false);
            }
        };

        if self.conn.acknowledge(started.written).is_err() {
            return Err(Failure::new(
                io::ErrorKind::Other,
                "h2 write bookkeeping failed",
            ));
        }

        self.out_len += started.written;
        self.credit_stream = 0;
        self.active = Some(Active {
            id: started.stream_id,
            body: command.body,
            sent: 0,
            local_ended: end_stream,
        });

        Ok(true)
    }

    fn produce(&mut self, budget: &Budget) -> Result<(), Failure> {
        if self.free() < MIN_FREE {
            self.flush(budget)?;
        }

        self.write_control();
        self.write_window_updates();
        self.write_data(budget)
    }

    pub(super) fn buffered(&self) -> usize {
        let state = self.shared.state.lock();

        state.body.len() - state.body_pos
    }

    fn wait_room(&self) {
        let mut state = self.shared.state.lock();

        while !state.shutdown && state.body.len() - state.body_pos >= BODY_BUFFER_CAP {
            self.shared.wake.wait(&mut state);
        }
    }

    pub(super) fn free(&self) -> usize {
        self.out.len() - self.out_len
    }

    fn emit(
        &mut self,
        write: impl FnOnce(
            &mut H2ClientConnection<'_, '_>,
            &mut [u8],
        ) -> Result<WriteOut, H2ClientError>,
    ) -> bool {
        let output = &mut self.out[self.out_len..];
        let Ok(written) = write(&mut self.conn, output) else {
            return false;
        };

        if self.conn.acknowledge(written.written).is_err() {
            return false;
        }

        self.out_len += written.written;

        true
    }

    fn write_control(&mut self) {
        while self.outbox.settings_acks > 0 && self.free() > CONTROL_ROOM {
            if !self.emit(|conn, out| conn.send_settings_ack(out)) {
                break;
            }

            self.outbox.settings_acks -= 1;
        }

        while let Some(data) = self.outbox.pings.last().copied() {
            if self.free() <= CONTROL_ROOM || !self.emit(|conn, out| conn.send_ping_ack(&data, out))
            {
                break;
            }

            self.outbox.pings.pop();
        }

        while let Some(&(id, code)) = self.outbox.resets.last() {
            if self.free() <= CONTROL_ROOM
                || !self.emit(|conn, out| conn.send_queued_reset(id, code, out))
            {
                break;
            }

            self.outbox.resets.pop();
        }

        if let Some(code) = self.outbox.connection_error
            && self.free() > CONTROL_ROOM
            && self.emit(|conn, out| conn.send_connection_error(code, out))
        {
            self.outbox.connection_error = None;
        }
    }

    fn write_window_updates(&mut self) {
        if self.buffered() >= BODY_BUFFER_CAP || self.free() <= CONTROL_ROOM {
            return;
        }

        if self.credit_conn >= CREDIT_THRESHOLD {
            let increment = self.credit_conn as u32;

            if self.emit(|conn, out| conn.send_window_update(0, increment, out)) {
                self.credit_conn = 0;
            }
        }

        let Some(id) = self.active.as_ref().map(|active| active.id) else {
            return;
        };

        if self.credit_stream >= CREDIT_THRESHOLD && self.free() > CONTROL_ROOM {
            let increment = self.credit_stream as u32;

            if self.emit(|conn, out| conn.send_window_update(id, increment, out)) {
                self.credit_stream = 0;
            }
        }
    }

    fn write_data(&mut self, budget: &Budget) -> Result<(), Failure> {
        loop {
            let Some(active) = self.active.as_ref() else {
                return Ok(());
            };

            if active.local_ended {
                return Ok(());
            }

            let id = active.id;
            let remaining = active.body.len() - active.sent;

            if self.free() < DATA_ROOM {
                self.flush(budget)?;
            }

            if remaining > 0 && self.conn.send_credit(id) == 0 {
                return Ok(());
            }

            let Some(active) = self.active.as_mut() else {
                return Ok(());
            };
            let input = &active.body[active.sent..];
            let output = &mut self.out[self.out_len..];
            let outcome = self.conn.send_data(id, input, true, output);

            match outcome {
                Ok(sent) => {
                    if self.conn.acknowledge(sent.written).is_err() {
                        return Err(Failure::new(
                            io::ErrorKind::Other,
                            "h2 write bookkeeping failed",
                        ));
                    }

                    self.out_len += sent.written;
                    active.sent += sent.consumed;
                    active.local_ended |= sent.end_stream;

                    if sent.consumed == 0 && sent.written == 0 {
                        return Ok(());
                    }
                }
                Err(H2ClientError::WouldBlock | H2ClientError::BufferTooSmall { .. }) => {
                    return Ok(());
                }
                Err(_) => {
                    self.fail_stream(
                        Failure::new(io::ErrorKind::Other, "h2 request body rejected"),
                        Some(ErrorCode::Cancel),
                    );

                    return Ok(());
                }
            }
        }
    }

    fn flush(&mut self, budget: &Budget) -> Result<(), Failure> {
        if self.out_len == 0 {
            return Ok(());
        }

        let armed = self.wire.arm_write(budget);
        let written = armed.and_then(|()| self.wire.write_plain(&self.out[..self.out_len]));

        self.out_len = 0;

        written.map_err(failure_from)
    }

    fn read_more(&mut self, budget: &Budget) -> Result<(), Failure> {
        self.wire.arm_read(budget).map_err(failure_from)?;

        let read = self
            .wire
            .read_plain(&mut self.inbuf, READ_CHUNK)
            .map_err(failure_from)?;

        if read == 0 {
            return Err(Failure::new(
                io::ErrorKind::UnexpectedEof,
                "h2 connection closed",
            ));
        }

        if self.inbuf.len() > MAX_INBUF {
            return Err(Failure::new(
                io::ErrorKind::InvalidData,
                "h2 input exceeds the buffer limit",
            ));
        }

        self.process_input();

        if let Some(failure) = self.terminal.take() {
            self.write_control();

            let _ = self.flush(budget);

            return Err(failure);
        }

        Ok(())
    }

    pub(super) fn publish_failure(&self, failure: Failure) {
        let mut state = self.shared.state.lock();

        state.failure = Some(failure);
        drop(state);
        self.shared.wake.notify_all();
    }

    pub(super) fn fail_stream(&mut self, failure: Failure, reset: Option<ErrorCode>) {
        if let Some(active) = self.active.take() {
            self.conn.forget_stream(active.id);

            if let Some(code) = reset {
                self.outbox.push_reset(active.id, code);
            }
        }

        self.publish_failure(failure);
    }
}

fn failure_from(error: io::Error) -> Failure {
    Failure::new(error.kind(), error.to_string())
}
