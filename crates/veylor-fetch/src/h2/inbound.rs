use std::io;

use veylor_client::h2::{
    BatchStatus, ErrorCode, FeedBatch, FeedScratch, H2FrameAction, HpackHeader, ResponseSectionKind,
};

use super::driver::Driver;
use super::outbound::STREAM_WINDOW;
use super::shared::Failure;
use crate::headers::Headers;

impl Driver<'_, '_> {
    pub(super) fn process_input(&mut self) {
        let input = std::mem::take(&mut self.inbuf);
        let length = input.len();
        let mut scratch = FeedScratch::from_headers(std::mem::take(&mut self.header_buf));
        let mut position = 0;

        while position < length {
            let Ok(batch) = self.conn.poll_feed(&input[position..], &mut scratch) else {
                self.terminal.get_or_insert(Failure::new(
                    io::ErrorKind::InvalidData,
                    "h2 engine failure",
                ));

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

        self.header_buf = scratch.into_headers();
        self.inbuf = input;
        self.inbuf.drain(..position);
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
                        let message = format!("stream reset by peer: {error_code:?}");
                        let mut failure = Failure::new(io::ErrorKind::ConnectionReset, message);

                        if error_code == ErrorCode::RefusedStream {
                            failure = failure.unprocessed();
                        }

                        self.kill_stream(index, failure, None);
                    }
                }
                H2FrameAction::SendReset {
                    stream_id,
                    error_code,
                } => match self.find_stream(stream_id) {
                    Some(index) => {
                        let message = format!("stream failed: {error_code:?}");
                        let failure = Failure::new(io::ErrorKind::InvalidData, message);

                        self.kill_stream(index, failure, Some(error_code));
                    }
                    None => self.outbox.push_reset(stream_id, error_code),
                },
                H2FrameAction::GoAway { last_stream_id, .. } => self.on_goaway(last_stream_id),
                H2FrameAction::ConnectionCredit { .. } => {}
                H2FrameAction::SettingsAckRequired => self.outbox.settings_acks += 1,
                H2FrameAction::PingAck { opaque_data } => self.outbox.push_ping(opaque_data),
                H2FrameAction::ConnectionError { error_code } => {
                    let message = format!("h2 connection error: {error_code:?}");

                    self.outbox.connection_error = Some(error_code);
                    self.terminal
                        .get_or_insert(Failure::new(io::ErrorKind::InvalidData, message));
                }
            }
        }
    }

    fn on_headers(&mut self, stream_id: u32, fields: &[HpackHeader], end_stream: bool) {
        let Some(index) = self.find_stream(stream_id) else {
            return;
        };

        let Ok(class) = self.conn.classify_headers(stream_id, fields, end_stream) else {
            let failure = Failure::new(io::ErrorKind::InvalidData, "malformed h2 response headers");

            self.kill_stream(index, failure, Some(ErrorCode::ProtocolError));

            return;
        };

        if class.kind == ResponseSectionKind::Informational {
            return;
        }

        // NOTE: response trailers are validated by the engine and then dropped, the public
        // response type has nowhere to expose them yet.
        if class.kind == ResponseSectionKind::Final {
            let mut headers = Headers::default();

            for field in fields {
                let Some((name, value)) = self.conn.resolve_header(*field) else {
                    continue;
                };

                if name.first() != Some(&b':') {
                    headers.push(name, value);
                }
            }

            self.streams[index].shared.set_head(class.status, headers);
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
            let failure = Failure::new(io::ErrorKind::InvalidData, "h2 data accounting failed");

            self.kill_stream(index, failure, Some(ErrorCode::ProtocolError));

            return;
        }

        if !self.streams[index]
            .shared
            .push_body(data, STREAM_WINDOW as usize)
        {
            let failure = Failure::new(io::ErrorKind::InvalidData, "h2 body overflow");

            self.kill_stream(index, failure, Some(ErrorCode::FlowControlError));

            return;
        }

        if end_stream {
            self.on_remote_end(index);
        }
    }

    fn on_remote_end(&mut self, index: usize) {
        let stream = self.streams.swap_remove(index);

        stream.shared.complete();

        // NOTE: RFC 9113 8.1 lets a client stop sending with RST_STREAM(NO_ERROR) once the
        // response is complete, so an unfinished upload does not fail the exchange.
        if !stream.local_ended {
            self.outbox.push_reset(stream.id, ErrorCode::NoError);
        }

        self.conn.forget_stream(stream.id);
        self.window_check = true;
    }

    fn on_goaway(&mut self, last_stream_id: u32) {
        self.mark_draining_from_peer();

        let mut index = 0;

        while index < self.streams.len() {
            if self.streams[index].id > last_stream_id {
                let failure = Failure::new(io::ErrorKind::ConnectionAborted, "h2 goaway");

                self.kill_stream(index, failure.unprocessed(), None);

                continue;
            }

            index += 1;
        }
    }
}
