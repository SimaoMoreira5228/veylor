use veylor_http2::{
    ControlFrameWriter, FrameWriteError, H2FrameAction, MAX_WINDOW_INCREMENT,
    SETTINGS_MAX_HEADER_LIST_SIZE, SettingsParameter, settings_parameter_valid,
};

use super::{
    H2ServerConnection, H2ServerError, MAX_HEADER_LIST_SIZE, MAX_PENDING_SETTINGS, MAX_SETTINGS,
    PendingWrite, PendingWriteKind,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteOut {
    pub written: usize,
    pub required: usize,
}

fn map_control_error(error: FrameWriteError, required: &mut usize) -> H2ServerError {
    match error {
        FrameWriteError::OutputTooSmall { required: need } => {
            *required = need;
            H2ServerError::BufferTooSmall { required: need }
        }
        FrameWriteError::PendingFrame => H2ServerError::WouldBlock,
        FrameWriteError::Terminal => H2ServerError::BadState,
        FrameWriteError::UnsupportedAction => H2ServerError::InternalError,
        FrameWriteError::InvalidHeader(_) | FrameWriteError::InvalidPayload(_) => {
            H2ServerError::InvalidArgument
        }
        FrameWriteError::OverAcknowledged => H2ServerError::WriteOverrun,
    }
}

impl<'c, 'b> H2ServerConnection<'c, 'b> {
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
    ) -> Result<WriteOut, H2ServerError> {
        if parameters.len() > MAX_SETTINGS {
            return Err(H2ServerError::Capacity);
        }
        self.ensure_writable()?;

        let mut staged = [SettingsParameter { id: 0, value: 0 }; MAX_PENDING_SETTINGS];
        let mut setting_count = 0;
        let mut max_header_list_size_seen = false;

        for parameter in parameters {
            // NOTE: the `SettingsParameter` type bounds ids to `u16`; only the unset id is
            // reachable here, and hosts must stage wider raw ids to zero before calling.
            if parameter.id == 0 {
                return Err(H2ServerError::InvalidArgument);
            }
            if !settings_parameter_valid(parameter.id, parameter.value) {
                return Err(H2ServerError::InvalidArgument);
            }
            if parameter.id == SETTINGS_MAX_HEADER_LIST_SIZE {
                if parameter.value > MAX_HEADER_LIST_SIZE {
                    return Err(H2ServerError::InvalidArgument);
                }
                max_header_list_size_seen = true;
            }
            staged[setting_count] = *parameter;
            setting_count += 1;
        }

        if !max_header_list_size_seen {
            staged[setting_count] = SettingsParameter {
                id: SETTINGS_MAX_HEADER_LIST_SIZE,
                value: MAX_HEADER_LIST_SIZE,
            };
            setting_count += 1;
        }

        let max_frame_size = self.control_max_frame();
        let mut writer = ControlFrameWriter::new(output, max_frame_size);
        let mut required = 0;
        let written = writer
            .queue_settings(&staged[..setting_count])
            .map_err(|error| map_control_error(error, &mut required))?;

        let mut stored = [(0, 0); MAX_PENDING_SETTINGS];
        for (destination, parameter) in stored.iter_mut().zip(staged[..setting_count].iter()) {
            *destination = (parameter.id, parameter.value);
        }
        self.pending = PendingWrite {
            kind: PendingWriteKind::Settings,
            remaining: written,
            setting_count,
            settings: stored,
        };
        Ok(WriteOut {
            written,
            required: written,
        })
    }

    pub fn acknowledge(&mut self, transferred: usize) -> Result<usize, H2ServerError> {
        self.ensure_live()?;
        if self.pending.kind == PendingWriteKind::None {
            return Err(H2ServerError::BadState);
        }
        if transferred > self.pending.remaining {
            return Err(H2ServerError::WriteOverrun);
        }
        self.pending.remaining -= transferred;
        if self.pending.remaining != 0 {
            return Ok(self.pending.remaining);
        }
        if self.pending.kind == PendingWriteKind::Settings {
            let mut parameters = [SettingsParameter { id: 0, value: 0 }; MAX_PENDING_SETTINGS];
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
                .map_err(|_| H2ServerError::InternalError)?;
        }
        self.pending = PendingWrite::default();
        Ok(0)
    }

    pub fn send_settings_ack(&mut self, output: &mut [u8]) -> Result<WriteOut, H2ServerError> {
        self.send_control(H2FrameAction::SettingsAckRequired, output)
    }

    pub fn send_ping_ack(
        &mut self,
        opaque_data: &[u8; 8],
        output: &mut [u8],
    ) -> Result<WriteOut, H2ServerError> {
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
    ) -> Result<WriteOut, H2ServerError> {
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
    ) -> Result<WriteOut, H2ServerError> {
        if increment == 0 || increment > MAX_WINDOW_INCREMENT {
            return Err(H2ServerError::InvalidArgument);
        }
        if stream_id > veylor_http2::MAX_STREAM_ID {
            return Err(H2ServerError::InvalidArgument);
        }
        self.ensure_writable()?;
        let available = if stream_id == 0 {
            veylor_http2::MAX_FLOW_WINDOW_SIZE
                - self.driver.driver().connection_receive_window().value()
        } else {
            match self.driver.driver().streams().find(stream_id) {
                Some(stream) => veylor_http2::MAX_FLOW_WINDOW_SIZE - stream.receive_window.value(),
                None => return Err(H2ServerError::InvalidArgument),
            }
        };
        if i64::from(increment) > available {
            return Err(H2ServerError::InvalidArgument);
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
            return Err(H2ServerError::InternalError);
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
