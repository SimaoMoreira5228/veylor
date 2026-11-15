use veylor_client::h2::{
    BatchStatus, ErrorCode, FeedBatch, FeedScratch, H2FrameAction, HpackHeader, ResponseSectionKind,
};

use super::driver::Driver;
use super::error::H2Error;
use super::headers::HeaderList;
use super::streams::Source;

impl Driver<'_, '_> {
    pub(super) fn process_input(&mut self) {
        let input = std::mem::take(&mut self.inbuf);
        let length = self.in_len;
        let data = &input[..length];
        let mut scratch = FeedScratch::new();
        let mut position = 0;

        while position < length {
            let Ok(batch) = self.conn.poll_feed(&data[position..], &mut scratch) else {
                self.terminal
                    .get_or_insert(H2Error::Connection(ErrorCode::InternalError));
                break;
            };
            let consumed = batch.consumed;
            let progressed = consumed > 0 || batch.action_count > 0;
            let failed = batch.connection_failed;
            let status = batch.status;
            self.handle_batch(&batch);
            self.conn.finish_batch();
            position += consumed;
            if failed || !progressed {
                break;
            }
            if status == BatchStatus::NeedMore && position == length {
                break;
            }
        }

        let mut input = input;
        input.copy_within(position..length, 0);
        self.in_len = length - position;
        self.inbuf = input;
        self.window_check = true;
    }

    fn handle_batch(&mut self, batch: &FeedBatch<'_, '_>) {
        for action in batch.actions[..batch.action_count].iter().flatten() {
            match *action {
                H2FrameAction::Headers {
                    stream_id,
                    headers,
                    end_stream,
                } => {
                    let fields = &batch.headers[headers.start..headers.start + headers.count];
                    self.on_headers(stream_id, fields, end_stream);
                }
                H2FrameAction::Data {
                    stream_id,
                    data,
                    end_stream,
                    ..
                } => self.on_data(stream_id, data, end_stream),
                H2FrameAction::StreamReset {
                    stream_id,
                    error_code,
                } => {
                    if let Some(index) = self.find_stream(stream_id) {
                        self.kill_stream(index, H2Error::Reset(error_code), None);
                    }
                }
                H2FrameAction::SendReset {
                    stream_id,
                    error_code,
                } => match self.find_stream(stream_id) {
                    Some(index) => {
                        self.kill_stream(index, H2Error::Reset(error_code), Some(error_code));
                    }
                    None => self.outbox.resets.push((stream_id, error_code)),
                },
                H2FrameAction::GoAway { last_stream_id, .. } => self.on_goaway(last_stream_id),
                H2FrameAction::SettingsAckRequired => self.outbox.settings_acks += 1,
                H2FrameAction::PingAck { opaque_data } => self.outbox.push_ping(opaque_data),
                H2FrameAction::ConnectionError { error_code } => {
                    self.outbox.connection_error = Some(error_code);
                    self.terminal.get_or_insert(H2Error::Connection(error_code));
                }
            }
        }
    }

    fn on_headers(&mut self, stream_id: u32, fields: &[HpackHeader], end_stream: bool) {
        let Some(index) = self.find_stream(stream_id) else {
            return;
        };
        let class = match self.conn.classify_headers(stream_id, fields, end_stream) {
            Ok(class) => class,
            Err(_) => {
                let code = ErrorCode::ProtocolError;
                self.kill_stream(index, H2Error::Reset(code), Some(code));
                return;
            }
        };
        if class.kind == ResponseSectionKind::Informational {
            return;
        }

        let mut list = HeaderList::default();
        for field in fields {
            let Some((name, value)) = self.conn.resolve_header(*field) else {
                continue;
            };
            if name.first() != Some(&b':') {
                list.push(name, value);
            }
        }

        let stream = &mut self.streams[index];
        match class.kind {
            ResponseSectionKind::Final => stream.shared.set_head(class.status, list),
            _ => stream.shared.set_trailers(list),
        }
        if end_stream {
            self.on_remote_end(index);
        }
    }

    fn on_data(&mut self, stream_id: u32, data: &[u8], end_stream: bool) {
        let Some(index) = self.find_stream(stream_id) else {
            return;
        };
        if self
            .conn
            .account_data(stream_id, data.len(), end_stream)
            .is_err()
        {
            let code = ErrorCode::ProtocolError;
            self.kill_stream(index, H2Error::Reset(code), Some(code));
            return;
        }
        let cap = self.config.stream_window as usize;
        if !self.streams[index].shared.push_body(data, cap) {
            let code = ErrorCode::FlowControlError;
            self.kill_stream(index, H2Error::BodyOverflow, Some(code));
            return;
        }
        if end_stream {
            self.on_remote_end(index);
        }
    }

    fn on_remote_end(&mut self, index: usize) {
        let stream = &mut self.streams[index];
        stream.remote_ended = true;
        stream.shared.complete();
        if !stream.local_ended() {
            // NOTE: the response finished before the upload did; RFC 9113 8.1 lets the
            // client stop sending with RST_STREAM(NO_ERROR) without failing the response.
            let id = stream.id;
            let finished = self.streams.swap_remove(index);
            if let Source::Stream(upload) = &finished.source {
                upload.abort();
            }
            self.conn.forget_stream(id);
            self.outbox.resets.push((id, ErrorCode::NoError));
            return;
        }
        let finished = self.streams.swap_remove(index);
        self.conn.forget_stream(finished.id);
    }

    fn on_goaway(&mut self, last_stream_id: u32) {
        self.mark_draining_from_peer();
        let error_code = self
            .conn
            .goaway_received()
            .map_or(ErrorCode::NoError, |info| info.error_code);
        let mut index = 0;
        while index < self.streams.len() {
            if self.streams[index].id > last_stream_id {
                self.kill_stream(index, H2Error::GoAway { error_code }, None);
                continue;
            }
            index += 1;
        }
    }
}
