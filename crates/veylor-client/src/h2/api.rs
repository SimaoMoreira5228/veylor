use veylor_http2::{
    ControlFrameWriter, FrameWriteError, H2FrameAction, MAX_WINDOW_INCREMENT, SettingsParameter,
    settings_parameter_valid,
};

use super::{H2ClientConnection, H2ClientError, MAX_SETTINGS, PendingWrite, PendingWriteKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteOut {
    pub written: usize,
    pub required: usize,
}

fn map_control_error(error: FrameWriteError, required: &mut usize) -> H2ClientError {
    match error {
        FrameWriteError::OutputTooSmall { required: need } => {
            *required = need;
            H2ClientError::BufferTooSmall { required: need }
        }
        FrameWriteError::PendingFrame => H2ClientError::WouldBlock,
        FrameWriteError::Terminal => H2ClientError::BadState,
        FrameWriteError::UnsupportedAction => H2ClientError::InternalError,
        FrameWriteError::InvalidHeader(_) | FrameWriteError::InvalidPayload(_) => {
            H2ClientError::InvalidArgument
        }
        FrameWriteError::OverAcknowledged => H2ClientError::WriteOverrun,
    }
}

impl<'c, 'b> H2ClientConnection<'c, 'b> {
    fn control_max_frame(&self) -> u32 {
        self.driver
            .driver()
            .streams()
            .remote_settings()
            .max_frame_size
    }

    pub fn send_settings(
        &mut self,
        parameters: &[SettingsParameter],
        output: &mut [u8],
    ) -> Result<WriteOut, H2ClientError> {
        if parameters.len() > MAX_SETTINGS {
            return Err(H2ClientError::Capacity);
        }
        self.ensure_writable()?;
        for parameter in parameters {
            // NOTE: the `SettingsParameter` type bounds ids to `u16`; only the unset id is
            // reachable here, and hosts must stage wider raw ids to zero before calling.
            if parameter.id == 0 {
                return Err(H2ClientError::InvalidArgument);
            }
            if !settings_parameter_valid(parameter.id, parameter.value) {
                return Err(H2ClientError::InvalidArgument);
            }
        }
        let max_frame_size = self.control_max_frame();
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        let mut required = 0;
        let written = writer
            .queue_settings(parameters)
            .map_err(|error| map_control_error(error, &mut required))?;
        let mut stored = [(0, 0); MAX_SETTINGS];
        for (destination, parameter) in stored.iter_mut().zip(parameters.iter()) {
            *destination = (parameter.id, parameter.value);
        }
        self.pending = PendingWrite {
            kind: PendingWriteKind::Settings,
            remaining: written,
            setting_count: parameters.len(),
            settings: stored,
        };
        Ok(WriteOut {
            written,
            required: written,
        })
    }

    pub fn acknowledge(&mut self, transferred: usize) -> Result<usize, H2ClientError> {
        self.ensure_live()?;
        if self.pending.kind == PendingWriteKind::None {
            return Err(H2ClientError::BadState);
        }
        if transferred > self.pending.remaining {
            return Err(H2ClientError::WriteOverrun);
        }
        self.pending.remaining -= transferred;
        if self.pending.remaining != 0 {
            return Ok(self.pending.remaining);
        }
        if self.pending.kind == PendingWriteKind::Settings {
            let mut parameters = [SettingsParameter { id: 0, value: 0 }; MAX_SETTINGS];
            for (destination, stored) in parameters
                .iter_mut()
                .zip(self.pending.settings.iter())
                .take(self.pending.setting_count)
            {
                *destination = SettingsParameter {
                    id: stored.0,
                    value: stored.1,
                };
            }
            self.driver
                .driver_mut()
                .register_fully_written_local_settings(&parameters[..self.pending.setting_count])
                .map_err(|_| H2ClientError::InternalError)?;
        }
        self.pending = PendingWrite::default();
        Ok(0)
    }

    pub fn send_settings_ack(&mut self, output: &mut [u8]) -> Result<WriteOut, H2ClientError> {
        self.send_control(H2FrameAction::SettingsAckRequired, output)
    }

    pub fn send_ping_ack(
        &mut self,
        opaque_data: &[u8; 8],
        output: &mut [u8],
    ) -> Result<WriteOut, H2ClientError> {
        self.send_control(
            H2FrameAction::PingAck {
                opaque_data: *opaque_data,
            },
            output,
        )
    }

    fn send_control(
        &mut self,
        action: H2FrameAction<'_>,
        output: &mut [u8],
    ) -> Result<WriteOut, H2ClientError> {
        self.ensure_writable()?;
        let max_frame_size = self.control_max_frame();
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        let mut required = 0;
        let written = writer
            .queue_action(action, 0)
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

    pub fn send_window_update(
        &mut self,
        stream_id: u32,
        increment: u32,
        output: &mut [u8],
    ) -> Result<WriteOut, H2ClientError> {
        if increment == 0 || increment > MAX_WINDOW_INCREMENT {
            return Err(H2ClientError::InvalidArgument);
        }
        if stream_id > veylor_http2::MAX_STREAM_ID {
            return Err(H2ClientError::InvalidArgument);
        }
        self.ensure_writable()?;
        let available = if stream_id == 0 {
            veylor_http2::MAX_FLOW_WINDOW_SIZE
                - self.driver.driver().connection_receive_window().value()
        } else {
            match self.driver.driver().streams().find(stream_id) {
                Some(stream) => veylor_http2::MAX_FLOW_WINDOW_SIZE - stream.receive_window.value(),
                None => return Err(H2ClientError::InvalidArgument),
            }
        };
        if i64::from(increment) > available {
            return Err(H2ClientError::InvalidArgument);
        }
        let max_frame_size = self.control_max_frame();
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        let mut required = 0;
        let written = writer
            .queue_window_update(stream_id, increment)
            .map_err(|error| map_control_error(error, &mut required))?;
        let credited = if stream_id == 0 {
            self.driver
                .driver_mut()
                .add_connection_receive_credit(increment)
                .is_ok()
        } else {
            self.driver
                .driver_mut()
                .add_stream_receive_credit(stream_id, increment)
                .is_ok()
        };
        if !credited {
            return Err(H2ClientError::InternalError);
        }
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
}
