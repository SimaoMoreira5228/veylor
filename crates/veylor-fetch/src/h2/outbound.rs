use std::io;
use std::sync::atomic::Ordering;

use veylor_client::h2::{
    CLIENT_CONNECTION_PREFACE, DEFAULT_REQUEST_BLOCK, ErrorCode, H2ClientConnection, H2ClientError,
    SETTINGS_INITIAL_WINDOW_SIZE, SettingsParameter, WriteOut,
};

use super::Body;
use super::MAX_STREAMS;
use super::driver::{Active, Driver, Uplink};
use super::fields;
use super::shared::{Failure, Pending, STATE_OPEN, UploadPhase};
use crate::budget::Budget;

pub(super) const STREAM_WINDOW: u32 = 128 * 1024;
const CONNECTION_WINDOW: u32 = 1024 * 1024;
const PROTOCOL_WINDOW: i64 = 65_535;
const CONTROL_ROOM: usize = 64;
const DATA_ROOM: usize = 1_024;
const HEAD_SLACK: usize = 64;
const MIN_FREE: usize = 4_096;

impl Driver<'_, '_> {
    pub(super) fn free(&self) -> usize {
        self.out.len() - self.out_len
    }

    pub(super) fn produce(&mut self) -> Result<(), Failure> {
        if self.free() < MIN_FREE {
            self.flush()?;
        }

        self.write_control();

        if self.window_check {
            self.write_window_updates();
        }

        if self.terminal.is_none() {
            self.admit_requests()?;
            self.write_data()?;
        }

        Ok(())
    }

    pub(super) fn flush(&mut self) -> Result<(), Failure> {
        if self.out_len == 0 {
            return Ok(());
        }

        let budget = Budget {
            read: None,
            write: self.settings.write_timeout,
            overall: None,
        };
        let armed = self.wire.arm_write(&budget);
        let written = armed.and_then(|()| self.wire.write_plain(&self.out[..self.out_len]));

        self.out_len = 0;

        written.map_err(|error| Failure::new(error.kind(), error.to_string()))
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

    pub(super) fn write_preface(&mut self) -> Result<(), Failure> {
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

        self.window_check = true;

        Ok(())
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

        if self.free() <= CONTROL_ROOM {
            return;
        }

        if let Some(code) = self.outbox.connection_error {
            if self.emit(|conn, out| conn.send_connection_error(code, out)) {
                self.outbox.connection_error = None;
            }

            return;
        }

        if self.outbox.goaway && self.emit(|conn, out| conn.goaway(out)) {
            self.outbox.goaway = false;
        }
    }

    // NOTE: connection credit goes back as soon as bytes arrive, whether the stream buffered
    // them, discarded them or the frame was padding, because the engine already deducts the full
    // frame length. Only the per-stream window waits for the reader, so one slow body cannot
    // starve its siblings of connection window.
    fn write_window_updates(&mut self) {
        self.window_check = false;

        let target = i64::from(CONNECTION_WINDOW);
        let deficit = target - self.conn.connection_receive_window();
        let threshold = (target / 4).min(target - PROTOCOL_WINDOW).max(1);

        if deficit >= threshold && self.free() > CONTROL_ROOM {
            let increment = deficit as u32;

            if !self.emit(|conn, out| conn.send_window_update(0, increment, out)) {
                self.window_check = true;
            }
        }

        let window = i64::from(STREAM_WINDOW);

        for index in 0..self.streams.len() {
            if self.streams[index].remote_ended || self.free() <= CONTROL_ROOM {
                continue;
            }

            let id = self.streams[index].id;
            let Some(receive) = self.conn.stream_receive_window(id) else {
                continue;
            };
            let held = self.streams[index].shared.held() as i64;
            let deficit = window - (receive + held);

            if deficit < window / 2 {
                continue;
            }

            let increment = deficit as u32;

            if !self.emit(|conn, out| conn.send_window_update(id, increment, out)) {
                self.window_check = true;
            }
        }
    }

    pub(super) fn can_admit(&self) -> bool {
        if self.shared.state.load(Ordering::Acquire) != STATE_OPEN
            || self.terminal.is_some()
            || !self.conn.peer_settings_received()
            || !self.conn.local_settings_acknowledged()
        {
            return false;
        }

        let peer = self.conn.remote_max_concurrent_streams() as usize;
        let limit = MAX_STREAMS.min(peer);

        self.shared
            .stream_limit
            .store(limit.max(1), Ordering::Release);

        self.streams.len() < limit
    }

    fn admit_requests(&mut self) -> Result<(), Failure> {
        loop {
            if !self.can_admit() {
                return Ok(());
            }

            if self.free() < DEFAULT_REQUEST_BLOCK + HEAD_SLACK {
                self.flush()?;
            }

            let Some(pending) = self.shared.core.lock().queue.pop_front() else {
                return Ok(());
            };

            if pending.stream.is_cancelled() {
                let failure = Failure::new(io::ErrorKind::Interrupted, "h2 stream cancelled");

                pending.stream.fail(failure);

                continue;
            }

            if let Some(pending) = self.start(pending)? {
                self.shared.core.lock().queue.push_front(pending);

                return Ok(());
            }
        }
    }

    fn start(&mut self, pending: Pending) -> Result<Option<Pending>, Failure> {
        let Pending {
            stream,
            command,
            deadline,
        } = pending;
        let end_stream = command.body.ends_at_head();
        let mut digits = [0u8; 20];
        let started = match fields::build(&command, &mut digits) {
            Ok((built, used)) => {
                let output = &mut self.out[self.out_len..];

                self.conn.start_request(&built[..used], output, end_stream)
            }
            Err(failure) => {
                stream.fail(failure);

                return Ok(None);
            }
        };

        let started = match started {
            Ok(started) => started,
            Err(
                H2ClientError::Capacity
                | H2ClientError::WouldBlock
                | H2ClientError::BufferTooSmall { .. },
            ) => {
                return Ok(Some(Pending {
                    stream,
                    command,
                    deadline,
                }));
            }
            Err(H2ClientError::BadState) => {
                let failure =
                    Failure::new(io::ErrorKind::ConnectionAborted, "h2 connection closing");

                stream.fail(failure.unprocessed());

                return Ok(None);
            }
            Err(error) => {
                let message = format!("h2 request rejected: {error:?}");

                stream.fail(Failure::new(io::ErrorKind::InvalidInput, message));

                return Ok(None);
            }
        };

        if self.conn.acknowledge(started.written).is_err() {
            return Err(Failure::new(
                io::ErrorKind::Other,
                "h2 write bookkeeping failed",
            ));
        }

        let uplink = match command.body {
            Body::Buffered(data) => Uplink::Buffered { data, sent: 0 },
            Body::Stream { .. } => Uplink::Stream {
                chunk: None,
                sent: 0,
            },
        };

        self.out_len += started.written;
        self.streams.push(Active {
            id: started.stream_id,
            shared: stream,
            uplink,
            local_ended: end_stream,
            remote_ended: false,
            deadline,
        });

        Ok(None)
    }

    fn write_data(&mut self) -> Result<(), Failure> {
        let count = self.streams.len();
        let mut progressed = true;

        while progressed && count > 0 {
            progressed = false;

            let first = self.round_robin % count;

            for offset in 0..count {
                if self.free() < DATA_ROOM {
                    self.flush()?;
                }

                let index = (first + offset) % count;

                if index >= self.streams.len() {
                    continue;
                }

                progressed |= self.write_stream_data(index)?;
            }
        }

        self.round_robin = self.round_robin.wrapping_add(1);

        Ok(())
    }

    fn write_stream_data(&mut self, index: usize) -> Result<bool, Failure> {
        if self.streams[index].local_ended {
            return Ok(false);
        }

        if self.streams[index].uplink.streaming() {
            self.write_upload(index)
        } else {
            self.write_buffered(index)
        }
    }

    fn write_buffered(&mut self, index: usize) -> Result<bool, Failure> {
        let active = &self.streams[index];
        let id = active.id;
        let Uplink::Buffered { data, sent } = &active.uplink else {
            return Ok(false);
        };
        let input = &data[*sent..];
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

                let Uplink::Buffered { sent: used, .. } = &mut self.streams[index].uplink else {
                    return Ok(false);
                };

                *used += sent.consumed;
                self.streams[index].local_ended |= sent.end_stream;

                Ok(sent.consumed > 0 || sent.written > 0)
            }
            Err(H2ClientError::WouldBlock | H2ClientError::BufferTooSmall { .. }) => Ok(false),
            Err(_) => {
                let failure = Failure::new(io::ErrorKind::Other, "h2 request body rejected");

                self.kill_stream(index, failure, Some(ErrorCode::Cancel));

                Ok(true)
            }
        }
    }

    // NOTE: the uploader owns the buffers, so the driver only pulls a chunk once there is credit
    // to send it. Holding at most one chunk plus the uploader's slots bounds the upload memory.
    fn write_upload(&mut self, index: usize) -> Result<bool, Failure> {
        let id = self.streams[index].id;

        if self.streams[index].shared.upload_phase() == Some(UploadPhase::Aborted) {
            if let Some(chunk) = self.streams[index].uplink.drop_chunk() {
                self.streams[index].shared.recycle(chunk);
            }

            return self.end_upload(index);
        }

        if matches!(
            self.streams[index].uplink,
            Uplink::Stream { chunk: None, .. }
        ) {
            if self.conn.send_credit(id) == 0 {
                return Ok(false);
            }

            match self.streams[index].shared.take_ready() {
                Some(chunk) => self.streams[index].uplink.hold_chunk(chunk),
                None => return self.end_upload(index),
            }
        }

        let outcome = {
            let active = &mut self.streams[index];
            let Uplink::Stream {
                chunk: Some(chunk),
                sent,
            } = &mut active.uplink
            else {
                return Ok(false);
            };
            let output = &mut self.out[self.out_len..];
            let input = &chunk.data[*sent..chunk.used];

            self.conn.send_data(id, input, false, output)
        };

        match outcome {
            Ok(sent) => {
                if self.conn.acknowledge(sent.written).is_err() {
                    return Err(Failure::new(
                        io::ErrorKind::Other,
                        "h2 write bookkeeping failed",
                    ));
                }

                self.out_len += sent.written;

                if let Uplink::Stream { sent: used, .. } = &mut self.streams[index].uplink {
                    *used += sent.consumed;
                }

                if let Some(chunk) = self.streams[index].uplink.take_sent_chunk() {
                    self.streams[index].shared.recycle(chunk);
                }

                Ok(sent.consumed > 0 || sent.written > 0)
            }
            Err(H2ClientError::WouldBlock | H2ClientError::BufferTooSmall { .. }) => Ok(false),
            Err(_) => {
                let failure = Failure::new(io::ErrorKind::Other, "h2 request body rejected");

                self.kill_stream(index, failure, Some(ErrorCode::Cancel));

                Ok(true)
            }
        }
    }

    fn end_upload(&mut self, index: usize) -> Result<bool, Failure> {
        let phase = self.streams[index].shared.upload_phase();
        let ending = matches!(phase, Some(UploadPhase::Ended) | Some(UploadPhase::Aborted));

        if !ending {
            return Ok(false);
        }

        let id = self.streams[index].id;
        let output = &mut self.out[self.out_len..];

        match self.conn.send_data(id, &[], true, output) {
            Ok(sent) => {
                if self.conn.acknowledge(sent.written).is_err() {
                    return Err(Failure::new(
                        io::ErrorKind::Other,
                        "h2 write bookkeeping failed",
                    ));
                }

                self.out_len += sent.written;
                self.streams[index].local_ended = true;

                Ok(true)
            }
            Err(H2ClientError::WouldBlock | H2ClientError::BufferTooSmall { .. }) => Ok(false),
            Err(_) => {
                let failure = Failure::new(io::ErrorKind::Other, "h2 request body could not end");

                self.kill_stream(index, failure, Some(ErrorCode::Cancel));

                Ok(true)
            }
        }
    }
}
