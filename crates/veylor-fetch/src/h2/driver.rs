use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use veylor_client::h2::{
    DEFAULT_MAX_FIELDS, DEFAULT_REQUEST_BLOCK, ErrorCode, H2ClientConnection, H2ClientLimits,
    H2ClientTables, HPACK_ENTRIES, HpackEntrySlot, HpackHeader, SETTINGS_ACKS, StreamSlot,
    decoder_scratch_len, encoder_scratch_len, header_block_len,
};

use super::shared::{
    Chunk, DriverShared, Failure, STATE_CLOSED, STATE_DRAINING, STATE_OPEN, StreamShared,
};
use super::{H2Settings, MAX_STREAMS};
use crate::wire::Wire;

const OUT_CAP: usize = 64 * 1024;
const MAX_INBUF: usize = 512 * 1024;
const ABSORB_CHUNK: usize = 64 * 1024;
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PINGS: usize = 8;
const RESET_CAP: usize = (MAX_STREAMS + 2) * 2;

#[derive(Default)]
pub(super) struct Outbox {
    pub(super) settings_acks: usize,
    pub(super) pings: Vec<[u8; 8]>,
    pub(super) resets: Vec<(u32, ErrorCode)>,
    pub(super) connection_error: Option<ErrorCode>,
    pub(super) goaway: bool,
}

impl Outbox {
    pub(super) fn push_ping(&mut self, data: [u8; 8]) {
        if self.pings.len() < MAX_PINGS {
            self.pings.push(data);
        }
    }

    pub(super) fn push_reset(&mut self, id: u32, code: ErrorCode) {
        if self.resets.len() < RESET_CAP {
            self.resets.push((id, code));
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.settings_acks == 0
            && self.pings.is_empty()
            && self.resets.is_empty()
            && self.connection_error.is_none()
            && !self.goaway
    }
}

pub(super) enum Uplink {
    Buffered { data: Vec<u8>, sent: usize },
    Stream { chunk: Option<Chunk>, sent: usize },
}

impl Uplink {
    pub(super) fn streaming(&self) -> bool {
        matches!(self, Self::Stream { .. })
    }

    pub(super) fn hold_chunk(&mut self, next: Chunk) {
        if let Self::Stream { chunk, sent } = self {
            *chunk = Some(next);
            *sent = 0;
        }
    }

    pub(super) fn drop_chunk(&mut self) -> Option<Chunk> {
        let Self::Stream { chunk, sent } = self else {
            return None;
        };

        *sent = 0;

        chunk.take()
    }

    pub(super) fn take_sent_chunk(&mut self) -> Option<Chunk> {
        let Self::Stream { chunk, sent } = self else {
            return None;
        };

        if !chunk.as_ref().is_some_and(|chunk| *sent >= chunk.used) {
            return None;
        }

        *sent = 0;

        chunk.take()
    }
}

pub(super) struct Active {
    pub(super) id: u32,
    pub(super) shared: Arc<StreamShared>,
    pub(super) uplink: Uplink,
    pub(super) local_ended: bool,
    pub(super) remote_ended: bool,
    pub(super) deadline: Option<Instant>,
}

pub(super) struct Driver<'c, 'b> {
    pub(super) conn: H2ClientConnection<'c, 'b>,
    pub(super) wire: Wire,
    pub(super) shared: Arc<DriverShared>,
    pub(super) settings: H2Settings,
    pub(super) streams: Vec<Active>,
    pub(super) header_buf: Box<[HpackHeader]>,
    pub(super) inbuf: Vec<u8>,
    pub(super) cipher: Vec<u8>,
    pub(super) out: Vec<u8>,
    pub(super) out_len: usize,
    pub(super) outbox: Outbox,
    pub(super) window_check: bool,
    pub(super) round_robin: usize,
    pub(super) terminal: Option<Failure>,
    dirty_scratch: Vec<Arc<StreamShared>>,
    handshake_deadline: Option<Instant>,
    idle_since: Option<Instant>,
    close_deadline: Option<Instant>,
    closing_seen: bool,
}

pub(super) fn run(wire: Wire, shared: Arc<DriverShared>, settings: H2Settings) {
    let limits = H2ClientLimits {
        max_header_list_size: settings.max_head.clamp(4 * 1024, 1024 * 1024) as u32,
        max_fields: DEFAULT_MAX_FIELDS,
        request_block: DEFAULT_REQUEST_BLOCK,
    };
    let mut slots = vec![StreamSlot::EMPTY; MAX_STREAMS + 2];
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

    let Ok(conn) = tables.connect() else {
        let failure = Failure::new(io::ErrorKind::Other, "h2 engine setup failed");

        close_shared(&shared, &failure);
        wire.shutdown();

        return;
    };

    let mut driver = Driver::new(conn, wire, shared, settings);
    let failure = driver.serve();

    driver.finish(&failure);
}

pub(super) fn close_shared(shared: &DriverShared, failure: &Failure) {
    let mut core = shared.core.lock();

    shared.state.store(STATE_CLOSED, Ordering::Release);

    for pending in core.queue.drain(..) {
        pending.stream.fail(failure.clone().unprocessed());
    }

    core.dirty.clear();
    core.inbound = Vec::new();
    drop(core);

    shared.room.notify_all();
}

impl<'c, 'b> Driver<'c, 'b> {
    fn new(
        conn: H2ClientConnection<'c, 'b>,
        wire: Wire,
        shared: Arc<DriverShared>,
        settings: H2Settings,
    ) -> Self {
        Self {
            conn,
            wire,
            shared,
            settings,
            streams: Vec::with_capacity(MAX_STREAMS),
            header_buf: vec![HpackHeader::EMPTY; DEFAULT_MAX_FIELDS].into_boxed_slice(),
            inbuf: Vec::new(),
            cipher: Vec::new(),
            out: vec![0; OUT_CAP],
            out_len: 0,
            outbox: Outbox::default(),
            window_check: false,
            round_robin: 0,
            terminal: None,
            dirty_scratch: Vec::new(),
            handshake_deadline: Some(Instant::now() + settings.handshake_timeout),
            idle_since: None,
            close_deadline: None,
            closing_seen: false,
        }
    }

    fn serve(&mut self) -> Failure {
        match self.main_loop() {
            Err(failure) => failure,
            Ok(()) => Failure::new(io::ErrorKind::ConnectionAborted, "h2 connection closed"),
        }
    }

    fn main_loop(&mut self) -> Result<(), Failure> {
        self.write_preface()?;

        loop {
            let eof = self.wait_for_work();

            self.absorb_input()?;
            self.housekeeping();
            self.produce()?;
            self.flush()?;

            if let Some(failure) = self.exit_reason(eof) {
                return Err(failure);
            }
        }
    }

    fn wait_for_work(&mut self) -> Option<Failure> {
        let shared = Arc::clone(&self.shared);
        let mut core = shared.core.lock();

        loop {
            let admissible = !core.queue.is_empty() && self.can_admit();

            if !core.inbound.is_empty()
                || !core.dirty.is_empty()
                || core.eof.is_some()
                || admissible
                || self.note_closing(&core)
                || self.terminal.is_some()
            {
                break;
            }

            let Some(deadline) = self.next_deadline(&core.queue) else {
                shared.wake.wait(&mut core);

                continue;
            };

            if Instant::now() >= deadline || shared.wake.wait_until(&mut core, deadline).timed_out()
            {
                break;
            }
        }

        std::mem::swap(&mut core.inbound, &mut self.cipher);
        std::mem::swap(&mut core.dirty, &mut self.dirty_scratch);

        let eof = core.eof.take();

        drop(core);
        shared.room.notify_one();

        eof
    }

    fn note_closing(&mut self, core: &super::shared::Core) -> bool {
        if !core.closing || self.closing_seen {
            return false;
        }

        self.closing_seen = true;

        true
    }

    fn absorb_input(&mut self) -> Result<(), Failure> {
        if self.cipher.is_empty() {
            return Ok(());
        }

        let cipher = std::mem::take(&mut self.cipher);
        let mut result = Ok(());

        for chunk in cipher.chunks(ABSORB_CHUNK) {
            if let Err(error) = self.wire.absorb(chunk, &mut self.inbuf) {
                result = Err(Failure::new(error.kind(), error.to_string()));

                break;
            }

            self.process_input();

            if self.inbuf.len() > MAX_INBUF {
                result = Err(Failure::new(
                    io::ErrorKind::InvalidData,
                    "h2 input exceeds the buffer limit",
                ));

                break;
            }
        }

        self.cipher = cipher;
        self.cipher.clear();

        result
    }

    fn exit_reason(&mut self, eof: Option<Failure>) -> Option<Failure> {
        if self.terminal.is_some() && self.outbox.connection_error.is_none() {
            return self.terminal.take();
        }

        if eof.is_some() {
            return eof;
        }

        let flushed = self.out_len == 0 && self.outbox.is_empty();

        if self.shared.state.load(Ordering::Acquire) != STATE_OPEN
            && flushed
            && self.streams.is_empty()
            && self.shared.core.lock().queue.is_empty()
        {
            return Some(Failure::new(
                io::ErrorKind::ConnectionAborted,
                "h2 connection closed",
            ));
        }

        let deadline = self.close_deadline?;

        if Instant::now() < deadline {
            return None;
        }

        Some(Failure::new(io::ErrorKind::TimedOut, "h2 close timed out"))
    }

    fn housekeeping(&mut self) {
        let now = Instant::now();

        self.drain_dirty();

        if self.handshake_deadline.is_some() {
            if self.conn.peer_settings_received() && self.conn.local_settings_acknowledged() {
                self.handshake_deadline = None;
            } else if self.handshake_deadline.is_some_and(|limit| now >= limit) {
                self.terminal.get_or_insert(Failure::new(
                    io::ErrorKind::TimedOut,
                    "h2 handshake timed out",
                ));
            }
        }

        let mut index = 0;

        while index < self.streams.len() {
            let expired = self.streams[index]
                .deadline
                .is_some_and(|limit| now >= limit);

            if expired {
                let failure = Failure::new(io::ErrorKind::TimedOut, "h2 stream timed out");

                self.kill_stream(index, failure, Some(ErrorCode::Cancel));

                continue;
            }

            index += 1;
        }

        self.expire_queue(now);
        self.check_idle(now);
    }

    fn check_idle(&mut self, now: Instant) {
        let (queue_empty, closing) = {
            let core = self.shared.core.lock();

            (core.queue.is_empty(), core.closing)
        };

        if !self.streams.is_empty() || !queue_empty {
            self.idle_since = None;

            return;
        }

        let since = *self.idle_since.get_or_insert(now);
        let timed_out = now >= since + self.settings.idle_timeout;

        if closing || timed_out {
            self.begin_close();
        }
    }

    fn drain_dirty(&mut self) {
        let mut dropped_queued = false;

        for stream in std::mem::take(&mut self.dirty_scratch) {
            stream.clear_dirty();

            self.window_check = true;

            if !stream.is_cancelled() {
                continue;
            }

            let position = self
                .streams
                .iter()
                .position(|active| Arc::ptr_eq(&active.shared, &stream));

            match position {
                Some(index) => {
                    let failure = Failure::new(io::ErrorKind::Interrupted, "h2 stream cancelled");

                    self.kill_stream(index, failure, Some(ErrorCode::Cancel));
                }
                None => dropped_queued = true,
            }
        }

        if dropped_queued {
            let failure = Failure::new(io::ErrorKind::Interrupted, "h2 stream cancelled");

            self.shared.core.lock().queue.retain(|pending| {
                if pending.stream.is_cancelled() {
                    pending.stream.fail(failure.clone());

                    return false;
                }

                true
            });
        }
    }

    fn expire_queue(&mut self, now: Instant) {
        let mut core = self.shared.core.lock();
        let expired = core
            .queue
            .iter()
            .any(|pending| pending.deadline.is_some_and(|limit| now >= limit));

        if !expired {
            return;
        }

        let queue = std::mem::take(&mut core.queue);

        for pending in queue {
            if pending.deadline.is_some_and(|limit| now >= limit) {
                let failure = Failure::new(io::ErrorKind::TimedOut, "h2 request timed out");

                pending.stream.fail(failure);
            } else {
                core.queue.push_back(pending);
            }
        }
    }

    pub(super) fn begin_close(&mut self) {
        if self.shared.state.load(Ordering::Acquire) == STATE_OPEN {
            let failure = Failure::new(io::ErrorKind::ConnectionAborted, "h2 connection closing");

            self.drain_queue(&failure);
            self.close_deadline = Some(Instant::now() + CLOSE_TIMEOUT);
        }

        if !self.conn.goaway_sent() && self.conn.goaway_received().is_none() {
            self.outbox.goaway = true;
        }
    }

    pub(super) fn mark_draining_from_peer(&mut self) {
        let failure = Failure::new(io::ErrorKind::ConnectionAborted, "h2 goaway");

        self.drain_queue(&failure);
    }

    fn drain_queue(&mut self, failure: &Failure) {
        let mut core = self.shared.core.lock();

        self.shared.state.store(STATE_DRAINING, Ordering::Release);

        for pending in core.queue.drain(..) {
            pending.stream.fail(failure.clone().unprocessed());
        }
    }

    fn next_deadline(
        &self,
        queue: &std::collections::VecDeque<super::shared::Pending>,
    ) -> Option<Instant> {
        let mut earliest = self.handshake_deadline;
        let mut consider = |candidate: Option<Instant>| {
            if let Some(candidate) = candidate {
                earliest = Some(earliest.map_or(candidate, |current| current.min(candidate)));
            }
        };

        for stream in &self.streams {
            consider(stream.deadline);
        }

        for pending in queue {
            consider(pending.deadline);
        }

        consider(self.close_deadline);

        if let Some(since) = self.idle_since {
            consider(Some(since + self.settings.idle_timeout));
        }

        earliest
    }

    pub(super) fn kill_stream(&mut self, index: usize, failure: Failure, reset: Option<ErrorCode>) {
        let stream = self.streams.swap_remove(index);

        self.conn.forget_stream(stream.id);

        if let Some(code) = reset {
            self.outbox.push_reset(stream.id, code);
        }

        if stream.shared.is_cancelled() {
            stream.shared.discard_body();
        }

        stream.shared.fail(failure);
        self.window_check = true;
    }

    pub(super) fn find_stream(&self, id: u32) -> Option<usize> {
        self.streams.iter().position(|stream| stream.id == id)
    }

    fn finish(&mut self, failure: &Failure) {
        for stream in self.streams.drain(..) {
            stream.shared.fail(failure.clone());
        }

        close_shared(&self.shared, failure);
        self.wire.shutdown();
    }
}
