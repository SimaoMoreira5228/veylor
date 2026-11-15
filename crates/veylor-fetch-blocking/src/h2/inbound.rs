use std::io;

use veylor_client::h2::{
    BatchStatus, ErrorCode, FeedBatch, FeedScratch, H2FrameAction, HpackHeader, ResponseSectionKind,
};

use super::driver::Driver;
use super::{BODY_BUFFER_CAP, Failure};
use crate::headers::Headers;

const STREAM_OVERFLOW: usize = 512 * 1024;

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
                    if self.is_active(stream_id) {
                        let message = format!("stream reset by peer: {error_code:?}");

                        self.fail_stream(
                            Failure::new(io::ErrorKind::ConnectionReset, message),
                            None,
                        );
                    }
                }
                H2FrameAction::SendReset {
                    stream_id,
                    error_code,
                } => {
                    if self.is_active(stream_id) {
                        let message = format!("stream failed: {error_code:?}");

                        self.fail_stream(
                            Failure::new(io::ErrorKind::InvalidData, message),
                            Some(error_code),
                        );
                    } else {
                        self.outbox.push_reset(stream_id, error_code);
                    }
                }
                H2FrameAction::GoAway { last_stream_id, .. } => {
                    self.draining = true;

                    if self
                        .active
                        .as_ref()
                        .is_some_and(|active| active.id > last_stream_id)
                    {
                        self.fail_stream(
                            Failure::new(io::ErrorKind::ConnectionAborted, "h2 goaway"),
                            None,
                        );
                    }
                }
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

    fn is_active(&self, stream_id: u32) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| active.id == stream_id)
    }

    fn on_headers(&mut self, stream_id: u32, fields: &[HpackHeader], end_stream: bool) {
        if !self.is_active(stream_id) {
            return;
        }

        let Ok(class) = self.conn.classify_headers(stream_id, fields, end_stream) else {
            let failure = Failure::new(io::ErrorKind::InvalidData, "malformed h2 response headers");

            self.fail_stream(failure, Some(ErrorCode::ProtocolError));

            return;
        };

        if class.kind == ResponseSectionKind::Informational {
            return;
        }

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

            let mut state = self.shared.state.lock();

            state.head = Some((class.status, headers));
            state.head_seen = true;
            drop(state);
            self.shared.wake.notify_all();
        }

        if end_stream {
            self.on_remote_end();
        }
    }

    fn on_data(&mut self, stream_id: u32, data: &[u8], end_stream: bool) {
        self.credit_conn += data.len();

        if !self.is_active(stream_id) {
            return;
        }

        if self
            .conn
            .account_data(stream_id, data.len(), end_stream)
            .is_err()
        {
            let failure = Failure::new(io::ErrorKind::InvalidData, "h2 data accounting failed");

            self.fail_stream(failure, Some(ErrorCode::ProtocolError));

            return;
        }

        self.credit_stream += data.len();

        let mut state = self.shared.state.lock();

        if state.body.len() - state.body_pos + data.len() > BODY_BUFFER_CAP + STREAM_OVERFLOW {
            drop(state);

            let failure = Failure::new(io::ErrorKind::InvalidData, "h2 body overflow");

            self.fail_stream(failure, Some(ErrorCode::FlowControlError));

            return;
        }

        state.body.extend_from_slice(data);
        drop(state);
        self.shared.wake.notify_all();

        if end_stream {
            self.on_remote_end();
        }
    }

    fn on_remote_end(&mut self) {
        let Some(active) = self.active.take() else {
            return;
        };

        // NOTE: RFC 9113 8.1 lets a client stop sending with RST_STREAM(NO_ERROR) once the
        // response is complete, so an unfinished upload does not fail the exchange.
        if !active.local_ended {
            self.outbox.push_reset(active.id, ErrorCode::NoError);
        }

        self.conn.forget_stream(active.id);

        self.shared.state.lock().ended = true;
        self.shared.wake.notify_all();
    }
}
