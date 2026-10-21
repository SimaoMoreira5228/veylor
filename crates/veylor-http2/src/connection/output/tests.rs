use super::*;
use crate::frame::{
    DEFAULT_MAX_FRAME_SIZE, ErrorCode, SETTINGS_MAX_CONCURRENT_STREAMS, SETTINGS_MAX_FRAME_SIZE_MIN,
};

#[test]
fn encodes_control_action_golden_frames() {
    let mut storage = [0; 32];
    let mut writer = ControlFrameWriter::new(&mut storage, DEFAULT_MAX_FRAME_SIZE);

    assert_eq!(
        writer.queue_action(H2FrameAction::SettingsAckRequired, 0),
        Ok(FRAME_HEADER_LEN)
    );
    assert_eq!(writer.pending_bytes(), &[0, 0, 0, 4, 1, 0, 0, 0, 0]);
    assert_eq!(
        writer.acknowledge(FRAME_HEADER_LEN),
        Ok(OutputProgress::Complete)
    );

    assert_eq!(
        writer.queue_action(
            H2FrameAction::PingAck {
                opaque_data: *b"pingpong",
            },
            0,
        ),
        Ok(FRAME_HEADER_LEN + PING_PAYLOAD_LEN)
    );
    assert_eq!(
        writer.pending_bytes(),
        &[
            0, 0, 8, 6, 1, 0, 0, 0, 0, b'p', b'i', b'n', b'g', b'p', b'o', b'n', b'g',
        ]
    );
    assert_eq!(
        writer.acknowledge(FRAME_HEADER_LEN + PING_PAYLOAD_LEN),
        Ok(OutputProgress::Complete)
    );

    assert_eq!(
        writer.queue_action(
            H2FrameAction::SendReset {
                stream_id: 1,
                error_code: ErrorCode::Cancel,
            },
            0,
        ),
        Ok(FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN)
    );
    assert_eq!(
        writer.pending_bytes(),
        &[0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 8]
    );
    assert_eq!(
        writer.acknowledge(FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN),
        Ok(OutputProgress::Complete)
    );

    assert_eq!(
        writer.queue_action(
            H2FrameAction::ConnectionError {
                error_code: ErrorCode::FlowControlError,
            },
            1,
        ),
        Ok(FRAME_HEADER_LEN + GOAWAY_FIXED_LEN)
    );
    assert_eq!(
        writer.pending_bytes(),
        &[0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3]
    );
}

#[test]
fn window_update_and_settings_helpers_encode_control_payloads() {
    let mut storage = [0; 64];
    let mut writer = ControlFrameWriter::new(&mut storage, DEFAULT_MAX_FRAME_SIZE);

    assert_eq!(writer.queue_window_update(3, 10), Ok(FRAME_HEADER_LEN + 4));
    assert_eq!(
        writer.pending_bytes(),
        &[0, 0, 4, 8, 0, 0, 0, 0, 3, 0, 0, 0, 10]
    );
    assert_eq!(writer.acknowledge(13), Ok(OutputProgress::Complete));

    let setting = SettingsParameter {
        id: SETTINGS_MAX_CONCURRENT_STREAMS,
        value: 7,
    };
    assert_eq!(
        writer.queue_settings(&[setting]),
        Ok(FRAME_HEADER_LEN + SETTINGS_ENTRY_LEN)
    );
    assert_eq!(
        writer.pending_bytes(),
        &[0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 7]
    );
}

#[test]
fn short_outputs_and_invalid_payloads_leave_storage_untouched() {
    let mut storage = [0xa5; 14];
    let mut writer = ControlFrameWriter::new(&mut storage, DEFAULT_MAX_FRAME_SIZE);

    assert_eq!(
        writer.queue_action(
            H2FrameAction::PingAck {
                opaque_data: *b"12345678",
            },
            0,
        ),
        Err(FrameWriteError::OutputTooSmall { required: 17 })
    );
    assert_eq!(writer.buffer, [0xa5; 14]);
    assert!(writer.pending_bytes().is_empty());

    let mut short_window = [0xa5; 12];
    let mut window_writer = ControlFrameWriter::new(&mut short_window, DEFAULT_MAX_FRAME_SIZE);
    assert_eq!(
        window_writer.queue_window_update(0, 1),
        Err(FrameWriteError::OutputTooSmall { required: 13 })
    );
    assert_eq!(window_writer.buffer, [0xa5; 12]);

    let mut short_settings = [0xa5; FRAME_HEADER_LEN + SETTINGS_ENTRY_LEN - 1];
    let mut settings_writer = ControlFrameWriter::new(&mut short_settings, DEFAULT_MAX_FRAME_SIZE);
    let parameter = SettingsParameter {
        id: SETTINGS_MAX_CONCURRENT_STREAMS,
        value: 3,
    };
    assert_eq!(
        settings_writer.queue_settings(&[parameter]),
        Err(FrameWriteError::OutputTooSmall {
            required: FRAME_HEADER_LEN + SETTINGS_ENTRY_LEN,
        })
    );
    assert_eq!(
        settings_writer.buffer,
        [0xa5; FRAME_HEADER_LEN + SETTINGS_ENTRY_LEN - 1]
    );

    let mut invalid_output = [0xa5; 32];
    let mut invalid_writer = ControlFrameWriter::new(&mut invalid_output, DEFAULT_MAX_FRAME_SIZE);
    assert_eq!(
        invalid_writer.queue_window_update(0, 0),
        Err(FrameWriteError::InvalidPayload(
            crate::frame::FramePayloadError::InvalidWindowIncrement,
        ))
    );
    assert_eq!(invalid_writer.buffer, [0xa5; 32]);

    let invalid_setting = SettingsParameter {
        id: crate::frame::SETTINGS_MAX_FRAME_SIZE,
        value: SETTINGS_MAX_FRAME_SIZE_MIN - 1,
    };
    assert_eq!(
        invalid_writer.queue_settings(&[invalid_setting]),
        Err(FrameWriteError::InvalidPayload(
            crate::frame::FramePayloadError::InvalidSetting,
        ))
    );
    assert_eq!(invalid_writer.buffer, [0xa5; 32]);
}

#[test]
fn pending_bytes_are_not_overwritten_and_partial_acknowledgments_advance_exactly() {
    let mut storage = [0; 32];
    let mut writer = ControlFrameWriter::new(&mut storage, DEFAULT_MAX_FRAME_SIZE);
    writer
        .queue_action(
            H2FrameAction::PingAck {
                opaque_data: *b"pingpong",
            },
            0,
        )
        .unwrap();
    let mut original = [0; 17];
    original.copy_from_slice(writer.pending_bytes());

    assert_eq!(
        writer.queue_action(H2FrameAction::SettingsAckRequired, 0),
        Err(FrameWriteError::PendingFrame)
    );
    assert_eq!(writer.pending_bytes(), &original);
    assert_eq!(
        writer.acknowledge(3),
        Ok(OutputProgress::Pending { remaining: 14 })
    );
    assert_eq!(writer.pending_bytes(), &original[3..]);
    assert_eq!(
        writer.acknowledge(4),
        Ok(OutputProgress::Pending { remaining: 10 })
    );
    assert_eq!(writer.pending_bytes(), &original[7..]);
    assert_eq!(writer.acknowledge(10), Ok(OutputProgress::Complete));
    assert!(writer.pending_bytes().is_empty());
    assert_eq!(writer.acknowledge(0), Ok(OutputProgress::Idle));
}

#[test]
fn over_acknowledgment_is_terminal() {
    let mut storage = [0; 16];
    let mut writer = ControlFrameWriter::new(&mut storage, DEFAULT_MAX_FRAME_SIZE);
    writer
        .queue_action(
            H2FrameAction::SendReset {
                stream_id: 1,
                error_code: ErrorCode::Cancel,
            },
            0,
        )
        .unwrap();

    assert_eq!(
        writer.acknowledge(14),
        Err(FrameWriteError::OverAcknowledged)
    );
    assert!(writer.is_terminal());
    assert_eq!(writer.pending_bytes(), &[]);
    assert_eq!(
        writer.queue_action(H2FrameAction::SettingsAckRequired, 0),
        Err(FrameWriteError::Terminal)
    );
    assert_eq!(writer.acknowledge(0), Err(FrameWriteError::Terminal));
}

#[test]
fn unsupported_events_do_not_change_the_output_buffer() {
    let mut storage = [0xa5; 16];
    let mut writer = ControlFrameWriter::new(&mut storage, DEFAULT_MAX_FRAME_SIZE);
    assert_eq!(
        writer.queue_action(
            H2FrameAction::StreamReset {
                stream_id: 1,
                error_code: ErrorCode::Cancel,
            },
            0,
        ),
        Err(FrameWriteError::UnsupportedAction)
    );
    assert_eq!(storage, [0xa5; 16]);
}
