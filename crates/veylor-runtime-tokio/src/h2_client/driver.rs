use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{Instant, sleep_until, timeout};
use veylor_client::h2::{
    ErrorCode, H2ClientConnection, H2ClientLimits, H2ClientTables, HPACK_ENTRIES, HpackEntrySlot,
    HpackHeader, SETTINGS_ACKS, StreamSlot, decoder_scratch_len, encoder_scratch_len,
    header_block_len,
};

use super::config::H2Config;
use super::error::{H2Error, TimeoutKind};
use super::shared::{DriverShared, Pending, STATE_CLOSED, STATE_DRAINING, STATE_OPEN};
use super::streams::{ActiveStream, Outbox, Source};

const FLUSH_GRACE: Duration = Duration::from_secs(1);

pub(super) struct Driver<'c, 'b> {
    pub(super) conn: H2ClientConnection<'c, 'b>,
    pub(super) shared: Arc<DriverShared>,
    pub(super) config: H2Config,
    pub(super) streams: Vec<ActiveStream>,
    pub(super) header_buf: Box<[HpackHeader]>,
    pub(super) inbuf: Vec<u8>,
    pub(super) in_len: usize,
    pub(super) out: Vec<u8>,
    pub(super) out_len: usize,
    pub(super) out_pos: usize,
    pub(super) outbox: Outbox,
    pub(super) preface_sent: bool,
    pub(super) window_check: bool,
    pub(super) round_robin: usize,
    pub(super) terminal: Option<H2Error>,
    dirty_scratch: Vec<Arc<super::shared::StreamShared>>,
    handshake_deadline: Option<Instant>,
    idle_since: Option<Instant>,
    close_deadline: Option<Instant>,
}

pub(super) async fn run<S>(io: S, config: H2Config, shared: Arc<DriverShared>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let limits = H2ClientLimits {
        max_header_list_size: config.max_header_list_size,
        max_fields: config.max_header_fields,
        request_block: config.max_request_header_bytes,
    };
    let mut slots = vec![StreamSlot::EMPTY; config.stream_slots()];
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
        Driver::fail_queue(&shared, H2Error::Closed);
        shared.state.store(STATE_CLOSED, Ordering::Release);
        shared.exited.notify_waiters();
        return;
    };

    let mut driver = Driver::new(conn, config, shared);
    let reason = driver.serve(io).await;
    driver.finish(reason);
}

impl<'c, 'b> Driver<'c, 'b> {
    fn new(conn: H2ClientConnection<'c, 'b>, config: H2Config, shared: Arc<DriverShared>) -> Self {
        let handshake_deadline = Instant::now() + config.handshake_timeout;
        let slots = config.stream_slots();
        Self {
            conn,
            header_buf: vec![HpackHeader::EMPTY; config.max_header_fields].into_boxed_slice(),
            streams: Vec::with_capacity(config.max_concurrent_streams),
            inbuf: vec![0; config.read_buffer],
            in_len: 0,
            out: vec![0; config.write_buffer],
            out_len: 0,
            out_pos: 0,
            outbox: Outbox::new(slots * 2),
            preface_sent: false,
            window_check: false,
            round_robin: 0,
            terminal: None,
            dirty_scratch: Vec::with_capacity(slots),
            handshake_deadline: Some(handshake_deadline),
            idle_since: None,
            close_deadline: None,
            shared,
            config,
        }
    }

    async fn serve<S>(&mut self, io: S) -> H2Error
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (mut reader, mut writer) = tokio::io::split(io);
        let mut eof = false;

        let reason = loop {
            self.housekeeping();
            if self.out_pos == self.out_len {
                self.out_len = 0;
                self.out_pos = 0;
                self.produce();
            }
            if let Some(reason) = self.exit_reason(eof) {
                break reason;
            }

            let reading = !eof && self.in_len < self.inbuf.len();
            let writing = self.out_pos < self.out_len;
            let deadline = self.next_deadline();
            let timer = async {
                match deadline {
                    Some(deadline) => sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };

            tokio::select! {
                read = reader.read(&mut self.inbuf[self.in_len..]), if reading => match read {
                    Ok(0) => eof = true,
                    Ok(count) => {
                        self.in_len += count;
                        self.process_input();
                    }
                    Err(error) => break H2Error::Io(error.kind()),
                },
                written = writer.write(&self.out[self.out_pos..self.out_len]), if writing => {
                    match written {
                        Ok(0) => break H2Error::Io(io::ErrorKind::WriteZero),
                        Ok(count) => self.out_pos += count,
                        Err(error) => break H2Error::Io(error.kind()),
                    }
                }
                _ = self.shared.notify.notified() => {}
                _ = timer => {}
            }
        };

        let pending = &self.out[self.out_pos..self.out_len];
        let _ = timeout(FLUSH_GRACE, async {
            writer.write_all(pending).await?;
            writer.shutdown().await
        })
        .await;
        reason
    }

    fn exit_reason(&self, eof: bool) -> Option<H2Error> {
        let flushed = self.out_pos == self.out_len && self.outbox.is_empty();
        if let Some(reason) = self.terminal {
            if flushed {
                return Some(reason);
            }
            return None;
        }
        if eof {
            return Some(H2Error::Io(io::ErrorKind::UnexpectedEof));
        }
        let draining = self.shared.state.load(Ordering::Acquire) != STATE_OPEN;
        if draining && flushed && self.streams.is_empty() && self.queue_is_empty() {
            return Some(H2Error::Closed);
        }
        match self.close_deadline {
            Some(deadline) if Instant::now() >= deadline => {
                Some(H2Error::Timeout(TimeoutKind::Close))
            }
            _ => None,
        }
    }

    fn queue_is_empty(&self) -> bool {
        self.shared.queue.lock().is_empty()
    }

    fn housekeeping(&mut self) {
        let now = Instant::now();

        self.drain_dirty();

        if self.shared.close_requested.load(Ordering::Acquire) {
            self.begin_close(H2Error::Closed);
        }
        if self.handshake_deadline.is_some() {
            if self.conn.peer_settings_received() && self.conn.local_settings_acknowledged() {
                self.handshake_deadline = None;
            } else if self
                .handshake_deadline
                .is_some_and(|deadline| now >= deadline)
            {
                self.terminal
                    .get_or_insert(H2Error::Timeout(TimeoutKind::Handshake));
            }
        }

        let mut index = 0;
        while index < self.streams.len() {
            let expired = self.streams[index]
                .deadline
                .is_some_and(|deadline| now >= deadline);
            if expired {
                self.kill_stream(
                    index,
                    H2Error::Timeout(TimeoutKind::Stream),
                    Some(ErrorCode::Cancel),
                );
                continue;
            }
            index += 1;
        }
        self.expire_queue(now);

        let idle = self.streams.is_empty() && self.queue_is_empty();
        if !idle {
            self.idle_since = None;
            return;
        }
        let since = *self.idle_since.get_or_insert(now);
        let abandoned = self.shared.handles.load(Ordering::Acquire) == 0;
        let timed_out = self
            .config
            .idle_timeout
            .is_some_and(|limit| now >= since + limit);
        if abandoned || timed_out {
            self.begin_close(H2Error::Closed);
        }
    }

    fn drain_dirty(&mut self) {
        {
            let mut list = self.shared.dirty.lock();
            std::mem::swap(&mut *list, &mut self.dirty_scratch);
        }
        let mut dropped_queued = false;
        for stream in std::mem::take(&mut self.dirty_scratch) {
            stream.clear_dirty();
            self.window_check = true;
            if !stream.is_cancelled() {
                continue;
            }
            match self
                .streams
                .iter()
                .position(|active| Arc::ptr_eq(&active.shared, &stream))
            {
                Some(index) => {
                    self.kill_stream(index, H2Error::Cancelled, Some(ErrorCode::Cancel));
                }
                None => dropped_queued = true,
            }
        }
        if dropped_queued {
            let mut queue = self.shared.queue.lock();
            queue.retain(|pending| !pending.stream.is_cancelled());
        }
    }

    fn expire_queue(&mut self, now: Instant) {
        let mut queue = self.shared.queue.lock();
        if !queue
            .iter()
            .any(|pending| pending.deadline.is_some_and(|deadline| now >= deadline))
        {
            return;
        }
        let mut kept = std::collections::VecDeque::with_capacity(queue.len());
        for pending in queue.drain(..) {
            if pending.deadline.is_some_and(|deadline| now >= deadline) {
                Self::reject(pending, H2Error::Timeout(TimeoutKind::Stream));
            } else {
                kept.push_back(pending);
            }
        }
        *queue = kept;
    }

    pub(super) fn reject(pending: Pending, error: H2Error) {
        if let super::request::RequestBody::Stream(upload) = &pending.request.body {
            upload.abort();
        }
        pending.stream.fail(error);
    }

    pub(super) fn fail_queue(shared: &DriverShared, error: H2Error) {
        let mut queue = shared.queue.lock();
        for pending in queue.drain(..) {
            Self::reject(pending, error);
        }
    }

    pub(super) fn begin_close(&mut self, reason: H2Error) {
        if self.shared.state.load(Ordering::Acquire) == STATE_OPEN {
            let mut queue = self.shared.queue.lock();
            self.shared.state.store(STATE_DRAINING, Ordering::Release);
            for pending in queue.drain(..) {
                Self::reject(pending, reason);
            }
            drop(queue);
            self.close_deadline = Some(Instant::now() + self.config.close_timeout);
        }
        if !self.conn.goaway_sent() && self.conn.goaway_received().is_none() {
            self.outbox.goaway = true;
        }
    }

    pub(super) fn next_deadline(&self) -> Option<Instant> {
        let mut earliest = self.handshake_deadline;
        let mut consider = |candidate: Option<Instant>| {
            if let Some(candidate) = candidate {
                earliest = Some(earliest.map_or(candidate, |current| current.min(candidate)));
            }
        };
        for stream in &self.streams {
            consider(stream.deadline);
        }
        for pending in self.shared.queue.lock().iter() {
            consider(pending.deadline);
        }
        consider(self.close_deadline);
        if let (Some(since), Some(limit)) = (self.idle_since, self.config.idle_timeout) {
            consider(Some(since + limit));
        }
        earliest
    }

    pub(super) fn kill_stream(&mut self, index: usize, error: H2Error, reset: Option<ErrorCode>) {
        let stream = self.streams.swap_remove(index);
        self.conn.forget_stream(stream.id);
        if let Some(code) = reset {
            self.outbox.resets.push((stream.id, code));
        }
        if let Source::Stream(upload) = &stream.source {
            upload.abort();
        }
        if error == H2Error::Cancelled {
            stream.shared.discard_body();
        }
        stream.shared.fail(error);
        self.window_check = true;
    }

    pub(super) fn find_stream(&self, id: u32) -> Option<usize> {
        self.streams.iter().position(|stream| stream.id == id)
    }

    fn finish(&mut self, reason: H2Error) {
        let stream_error = reason;
        for stream in self.streams.drain(..) {
            if let Source::Stream(upload) = &stream.source {
                upload.abort();
            }
            stream.shared.fail(stream_error);
        }
        {
            let mut queue = self.shared.queue.lock();
            self.shared.state.store(STATE_CLOSED, Ordering::Release);
            for pending in queue.drain(..) {
                Self::reject(pending, stream_error);
            }
        }
        self.shared.exited.notify_waiters();
    }

    pub(super) fn mark_draining_from_peer(&mut self) {
        let mut queue = self.shared.queue.lock();
        self.shared.state.store(STATE_DRAINING, Ordering::Release);
        let error = match self.conn.goaway_received() {
            Some(info) => H2Error::GoAway {
                error_code: info.error_code,
            },
            None => H2Error::Closed,
        };
        for pending in queue.drain(..) {
            Self::reject(pending, error);
        }
        drop(queue);
        self.close_deadline
            .get_or_insert(Instant::now() + self.config.close_timeout);
    }
}
