use veylor_http2::{ControlFrameWriter, ErrorCode, H2FrameAction, StreamState};

use super::api::map_control_error;
use super::{
    H2ClientConnection, H2ClientError, PendingWrite, PendingWriteKind, ResponseSlot, WriteOut,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GoAwayInfo {
    pub last_stream_id: u32,
    pub error_code: ErrorCode,
}

impl<'c, 'b> H2ClientConnection<'c, 'b> {
    pub fn peer_settings_received(&self) -> bool {
        self.driver.driver().streams().first_settings_received()
    }

    pub fn local_settings_acknowledged(&self) -> bool {
        let streams = self.driver.driver().streams();
        streams.first_settings_sent() && streams.unacked_settings() == 0
    }

    pub fn remote_max_concurrent_streams(&self) -> u32 {
        self.driver
            .driver()
            .streams()
            .remote_settings()
            .max_concurrent_streams
    }

    pub fn remote_max_frame_size(&self) -> u32 {
        self.driver
            .driver()
            .streams()
            .remote_settings()
            .max_frame_size
    }

    pub fn active_streams(&self) -> usize {
        self.driver.driver().streams().active_count()
    }

    pub fn connection_failure(&self) -> Option<ErrorCode> {
        self.driver.driver().failed()
    }

    pub fn goaway_received(&self) -> Option<GoAwayInfo> {
        let streams = self.driver.driver().streams();
        if !streams.goaway_received() {
            return None;
        }
        Some(GoAwayInfo {
            last_stream_id: streams.goaway_recv_last(),
            error_code: streams.goaway_recv_code(),
        })
    }

    pub fn goaway_sent(&self) -> bool {
        self.driver.driver().streams().goaway_sent()
    }

    pub fn connection_receive_window(&self) -> i64 {
        self.driver.driver().connection_receive_window().value()
    }

    pub fn stream_receive_window(&self, stream_id: u32) -> Option<i64> {
        let stream = self.driver.driver().streams().find(stream_id)?;
        Some(stream.receive_window.value())
    }

    pub fn send_credit(&self, stream_id: u32) -> usize {
        let driver = self.driver.driver();
        let Some(stream) = driver.streams().find(stream_id) else {
            return 0;
        };
        let credit = driver
            .connection_send_window()
            .value()
            .min(stream.send_window.value());
        usize::try_from(credit).unwrap_or(0)
    }

    pub fn stream_is_open(&self, stream_id: u32) -> bool {
        self.driver
            .driver()
            .streams()
            .find(stream_id)
            .is_some_and(|stream| stream.state != StreamState::Closed)
    }

    pub fn forget_stream(&mut self, stream_id: u32) {
        self.driver
            .driver_mut()
            .streams_mut()
            .close_stream(stream_id);
        self.remove_response(stream_id);
    }

    // NOTE: feed already closed the stream when it queued a `SendReset` action, so the
    // state-checked `send_reset` would refuse; this only frames the RST_STREAM.
    pub fn send_queued_reset(
        &mut self,
        stream_id: u32,
        error_code: ErrorCode,
        output: &mut [u8],
    ) -> Result<WriteOut, H2ClientError> {
        self.ensure_live()?;
        if self.pending.kind != PendingWriteKind::None {
            return Err(H2ClientError::WouldBlock);
        }
        let max_frame_size = self.remote_max_frame_size();
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        let mut required = 0;
        let written = writer
            .queue_action(
                H2FrameAction::SendReset {
                    stream_id,
                    error_code,
                },
                0,
            )
            .map_err(|error| map_control_error(error, &mut required))?;
        self.pending = PendingWrite {
            kind: PendingWriteKind::Control,
            remaining: written,
            ..PendingWrite::default()
        };
        Ok(WriteOut {
            written,
            required: written,
        })
    }

    pub fn send_connection_error(
        &mut self,
        error_code: ErrorCode,
        output: &mut [u8],
    ) -> Result<WriteOut, H2ClientError> {
        self.ensure_live()?;
        if self.pending.kind != PendingWriteKind::None {
            return Err(H2ClientError::WouldBlock);
        }
        let last_stream_id = self.driver.driver().streams().highest_remote();
        let max_frame_size = self.remote_max_frame_size();
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        let mut required = 0;
        let written = writer
            .queue_action(
                H2FrameAction::ConnectionError { error_code },
                last_stream_id,
            )
            .map_err(|error| map_control_error(error, &mut required))?;
        self.pending = PendingWrite {
            kind: PendingWriteKind::Control,
            remaining: written,
            ..PendingWrite::default()
        };
        Ok(WriteOut {
            written,
            required: written,
        })
    }

    pub(crate) fn remove_response(&mut self, stream_id: u32) {
        let length = self.response_len;
        let Some(position) = self.responses[..length]
            .iter()
            .position(|slot| matches!(slot, Some((id, _)) if *id == stream_id))
        else {
            return;
        };
        let last: ResponseSlot = self.responses[length - 1].take();
        self.responses[position] = last;
        self.response_len = length - 1;
    }
}
