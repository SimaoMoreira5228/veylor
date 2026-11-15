use std::sync::atomic::Ordering;

use veylor_client::h2::{
    CLIENT_CONNECTION_PREFACE, DataOut, ErrorCode, H2ClientError, HpackFieldToEncode,
    HpackRepresentation, MAX_FIELDS, SETTINGS_INITIAL_WINDOW_SIZE, SettingsParameter, WriteOut,
};

use super::driver::Driver;
use super::error::H2Error;
use super::request::{H2Request, RequestBody};
use super::shared::{Pending, STATE_OPEN};
use super::streams::{ActiveStream, Source};
use super::upload::UploadState;

const CONTROL_ROOM: usize = 64;
const HEAD_ROOM: usize = 4_096 + 64;
const DATA_ROOM: usize = 1_024;
const PROTOCOL_WINDOW: i64 = 65_535;

impl Driver<'_, '_> {
    pub(super) fn produce(&mut self) {
        self.write_preface();
        self.write_control();
        if self.window_check {
            self.write_window_updates();
        }
        if self.terminal.is_none() {
            self.admit_requests();
            self.write_data();
        }
    }

    fn free(&self) -> usize {
        self.out.len() - self.out_len
    }

    fn emit(
        &mut self,
        write: impl FnOnce(
            &mut veylor_client::h2::H2ClientConnection<'_, '_>,
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

    fn write_preface(&mut self) {
        if self.preface_sent {
            return;
        }
        self.preface_sent = true;
        let preface = CLIENT_CONNECTION_PREFACE;
        self.out[self.out_len..self.out_len + preface.len()].copy_from_slice(preface);
        self.out_len += preface.len();

        let parameters = [SettingsParameter {
            id: SETTINGS_INITIAL_WINDOW_SIZE,
            value: self.config.stream_window,
        }];
        if !self.emit(|conn, out| conn.send_settings(&parameters, out)) {
            self.terminal
                .get_or_insert(H2Error::Connection(ErrorCode::InternalError));
            return;
        }
        self.window_check = true;
    }

    fn write_control(&mut self) {
        while self.outbox.settings_acks > 0 && self.free() > CONTROL_ROOM {
            if !self.emit(|conn, out| conn.send_settings_ack(out)) {
                break;
            }
            self.outbox.settings_acks -= 1;
        }

        while self.outbox.ping_count > 0 && self.free() > CONTROL_ROOM {
            let data = self.outbox.pings[self.outbox.ping_count - 1];
            if !self.emit(|conn, out| conn.send_ping_ack(&data, out)) {
                break;
            }
            self.outbox.ping_count -= 1;
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

    fn write_window_updates(&mut self) {
        self.window_check = false;
        let target = i64::from(self.config.connection_window);
        let held = self.shared.held_total.load(Ordering::Acquire) as i64;
        let deficit = target - (self.conn.connection_receive_window() + held);
        let threshold = (target / 4).min(target - PROTOCOL_WINDOW).max(1);
        if deficit >= threshold && self.free() > CONTROL_ROOM {
            let increment = deficit as u32;
            if !self.emit(|conn, out| conn.send_window_update(0, increment, out)) {
                self.window_check = true;
            }
        }

        let window = i64::from(self.config.stream_window);
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

    fn admit_requests(&mut self) {
        loop {
            if self.shared.state.load(Ordering::Acquire) != STATE_OPEN
                || !self.conn.peer_settings_received()
                || !self.conn.local_settings_acknowledged()
                || self.free() < HEAD_ROOM
            {
                return;
            }
            let peer_limit = self.conn.remote_max_concurrent_streams() as usize;
            let limit = self.config.max_concurrent_streams.min(peer_limit);

            self.shared.stream_limit.store(limit, Ordering::Release);

            if self.streams.len() >= limit {
                return;
            }
            let Some(pending) = self.shared.queue.lock().pop_front() else {
                return;
            };
            if pending.stream.is_cancelled() {
                Self::reject(pending, H2Error::Cancelled);
                continue;
            }
            if let Err(pending) = self.start(pending) {
                self.shared.queue.lock().push_front(*pending);
                return;
            }
        }
    }

    fn start(&mut self, pending: Pending) -> Result<(), Box<Pending>> {
        let Pending {
            stream,
            request,
            deadline,
        } = pending;
        let mut length_digits = [0u8; 20];
        let fields = match build_fields(&request, &mut length_digits) {
            Ok(fields) => fields,
            Err(error) => {
                Self::reject(
                    Pending {
                        stream,
                        request,
                        deadline,
                    },
                    error,
                );
                return Ok(());
            }
        };
        let used = fields.1;
        let end_stream = matches!(request.body, RequestBody::Empty);
        let started = {
            let output = &mut self.out[self.out_len..];
            self.conn
                .start_request(&fields.0[..used], output, end_stream)
        };
        let started = match started {
            Ok(started) => started,
            Err(
                H2ClientError::Capacity
                | H2ClientError::WouldBlock
                | H2ClientError::BufferTooSmall { .. },
            ) => {
                return Err(Box::new(Pending {
                    stream,
                    request,
                    deadline,
                }));
            }
            Err(H2ClientError::BadState) => {
                Self::reject(
                    Pending {
                        stream,
                        request,
                        deadline,
                    },
                    H2Error::Closed,
                );
                return Ok(());
            }
            Err(_) => {
                Self::reject(
                    Pending {
                        stream,
                        request,
                        deadline,
                    },
                    H2Error::InvalidRequest,
                );
                return Ok(());
            }
        };
        if self.conn.acknowledge(started.written).is_err() {
            self.terminal
                .get_or_insert(H2Error::Connection(ErrorCode::InternalError));
            return Ok(());
        }
        self.out_len += started.written;

        let source = match request.body {
            RequestBody::Empty => Source::Done,
            RequestBody::Full(data) => Source::Full { data, sent: 0 },
            RequestBody::Stream(upload) => Source::Stream(upload),
        };
        self.streams.push(ActiveStream {
            id: started.stream_id,
            shared: stream,
            source,
            deadline,
            remote_ended: false,
        });
        Ok(())
    }

    fn write_data(&mut self) {
        let count = self.streams.len();
        let mut progressed = true;
        while progressed && count > 0 {
            progressed = false;
            let first = self.round_robin % count;
            for offset in 0..count {
                if self.free() < DATA_ROOM {
                    return;
                }
                let index = (first + offset) % count;
                if index >= self.streams.len() {
                    continue;
                }
                progressed |= self.write_stream_data(index);
            }
        }
        self.round_robin = self.round_robin.wrapping_add(1);
    }

    fn write_stream_data(&mut self, index: usize) -> bool {
        let id = self.streams[index].id;
        let credit = self.conn.send_credit(id);
        let mut result: Option<Result<DataOut, H2ClientError>> = None;

        let output_start = self.out_len;
        let stream = &mut self.streams[index];
        match &mut stream.source {
            Source::Done => return false,
            Source::Full { data, sent } => {
                let input = &data[*sent..];
                if !input.is_empty() && credit == 0 {
                    return false;
                }
                let output = &mut self.out[output_start..];
                let outcome = self.conn.send_data(id, input, true, output);
                if let Ok(out) = &outcome {
                    *sent += out.consumed;
                }
                result = Some(outcome);
            }
            Source::Stream(upload) => {
                let upload = upload.clone();
                if !upload.has_data() {
                    return false;
                }
                let output = &mut self.out[output_start..];
                let conn = &mut self.conn;
                let state = upload.drain(|input, finished| {
                    if input.is_empty() && !finished {
                        return 0;
                    }
                    if !input.is_empty() && credit == 0 {
                        return 0;
                    }
                    let outcome = conn.send_data(id, input, finished, output);
                    let consumed = outcome.as_ref().map_or(0, |out| out.consumed);
                    result = Some(outcome);
                    consumed
                });
                if matches!(state, UploadState::Aborted) {
                    self.kill_stream(index, H2Error::Cancelled, Some(ErrorCode::Cancel));
                    return true;
                }
            }
        }

        let Some(outcome) = result else {
            return false;
        };
        match outcome {
            Ok(out) => {
                if self.conn.acknowledge(out.written).is_err() {
                    self.terminal
                        .get_or_insert(H2Error::Connection(ErrorCode::InternalError));
                    return false;
                }
                self.out_len += out.written;
                if out.end_stream {
                    self.streams[index].source = Source::Done;
                }
                true
            }
            Err(H2ClientError::WouldBlock | H2ClientError::BufferTooSmall { .. }) => false,
            Err(_) => {
                self.kill_stream(
                    index,
                    H2Error::Reset(ErrorCode::InternalError),
                    Some(ErrorCode::Cancel),
                );
                true
            }
        }
    }
}

type Fields<'a> = ([HpackFieldToEncode<'a>; MAX_FIELDS], usize);

fn field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::Default,
        huffman_name: false,
        huffman_value: true,
    }
}

fn build_fields<'a>(
    request: &'a H2Request,
    digits: &'a mut [u8; 20],
) -> Result<Fields<'a>, H2Error> {
    let mut fields: [HpackFieldToEncode<'a>; MAX_FIELDS] = [field(b"", b""); MAX_FIELDS];
    let mut used = 0;
    let mut push = |entry: HpackFieldToEncode<'a>| -> Result<(), H2Error> {
        if used == MAX_FIELDS {
            return Err(H2Error::InvalidRequest);
        }
        fields[used] = entry;
        used += 1;
        Ok(())
    };

    push(field(b":method", &request.method))?;
    push(field(b":scheme", &request.scheme))?;
    push(field(b":authority", &request.authority))?;
    push(field(b":path", &request.path))?;
    for (name, value) in request.headers.iter() {
        push(field(name, value))?;
    }
    if let RequestBody::Full(data) = &request.body
        && request.headers.get(b"content-length").is_none()
    {
        let text = format_length(data.len(), digits);
        push(field(b"content-length", text))?;
    }
    Ok((fields, used))
}

fn format_length(mut value: usize, digits: &mut [u8; 20]) -> &[u8] {
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    &digits[start..]
}
