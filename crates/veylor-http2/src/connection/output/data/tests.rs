use super::*;
use crate::flow_control::DEFAULT_FLOW_WINDOW_SIZE;
use crate::frame::{
    DATA_FLAG_END_STREAM, DEFAULT_MAX_FRAME_SIZE, SETTINGS_INITIAL_WINDOW_SIZE, SettingsParameter,
    encode_frame_header,
};
use crate::hpack::{HpackEntrySlot, HpackHeader, HpackTable};
use crate::stream::{FrameDirection, H2Role, StreamSlot, StreamState, StreamTable};

fn with_open_stream(run: impl FnOnce(&mut H2FrameDriver<'_, '_, '_, '_>)) {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut header_block_storage = [0; 64];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block_storage, 64);
    streams
        .receive_client_preface_fragment(crate::stream::CLIENT_CONNECTION_PREFACE)
        .unwrap();

    let settings = FrameHeader {
        payload_length: 0,
        frame_type: FrameType::Settings,
        flags: 0,
        stream_id: 0,
    };
    assert_eq!(
        streams.on_frame(settings, FrameDirection::Sent, &[]).kind,
        StreamActionKind::Accept
    );
    assert_eq!(
        streams
            .on_frame(settings, FrameDirection::Received, &[])
            .kind,
        StreamActionKind::Accept
    );

    let mut hpack_entries = [HpackEntrySlot::EMPTY; 8];
    let mut hpack_storage = [0; 256];
    let mut decoder = HpackTable::new(&mut hpack_entries, &mut hpack_storage, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_storage = [0; 256];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_storage, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();
    open_remote_stream(&mut driver, 1);
    run(&mut driver);
}

fn open_remote_stream(driver: &mut H2FrameDriver<'_, '_, '_, '_>, stream_id: u32) {
    let mut input = [0; FRAME_HEADER_LEN + 3];
    let header = FrameHeader {
        payload_length: 3,
        frame_type: FrameType::Headers,
        flags: crate::frame::HEADERS_FLAG_END_HEADERS,
        stream_id,
    };
    encode_frame_header(
        header,
        &mut input[..FRAME_HEADER_LEN],
        DEFAULT_MAX_FRAME_SIZE,
    )
    .unwrap();
    input[FRAME_HEADER_LEN..].copy_from_slice(&[0x82, 0x86, 0x84]);

    let mut actions = [None; 1];
    let mut headers = [HpackHeader::EMPTY; 4];
    let result = driver.feed(&input, &mut actions, &mut headers);

    assert_eq!(result.consumed, input.len());
    assert_eq!(result.actions, 1);
    assert_eq!(result.headers, 3);
}

#[test]
fn bounds_data_by_output_capacity_and_resumes_after_partial_acknowledgments() {
    with_open_stream(|driver| {
        let mut output = [0; 12];
        let mut writer = DataFrameWriter::new(&mut output);
        let queued = writer.queue_data(driver, 1, b"hello", true).unwrap();

        assert_eq!(
            queued,
            QueuedDataFrame {
                frame_length: 12,
                payload_length: 3,
                end_stream: false,
            }
        );
        assert_eq!(
            writer.pending_bytes(),
            &[0, 0, 3, 0, 0, 0, 0, 0, 1, b'h', b'e', b'l']
        );
        assert_eq!(
            driver.streams().find(1).unwrap().send_window.value(),
            DEFAULT_FLOW_WINDOW_SIZE - 3
        );
        assert_eq!(
            driver.connection_send_window().value(),
            DEFAULT_FLOW_WINDOW_SIZE - 3
        );
        assert_eq!(
            writer.acknowledge(4),
            Ok(OutputProgress::Pending { remaining: 8 })
        );
        assert_eq!(writer.pending_bytes(), &[0, 0, 0, 0, 1, b'h', b'e', b'l']);
        assert_eq!(
            writer.queue_data(driver, 1, b"lo", true),
            Err(DataWriteError::PendingFrame)
        );
        assert_eq!(writer.acknowledge(8), Ok(OutputProgress::Complete));

        let queued = writer.queue_data(driver, 1, b"lo", true).unwrap();
        assert_eq!(queued.payload_length, 2);
        assert!(queued.end_stream);
        assert_eq!(writer.pending_bytes()[4], DATA_FLAG_END_STREAM);
        assert_eq!(
            driver.streams().find(1).unwrap().state,
            StreamState::HalfClosedLocal
        );
        assert_eq!(
            writer.acknowledge(queued.frame_length),
            Ok(OutputProgress::Complete)
        );
        let mut output_before_rejection = [0; 12];
        output_before_rejection.copy_from_slice(writer.buffer);
        let connection_credit_before_rejection = driver.connection_send_window().value();
        let stream_credit_before_rejection = driver.streams().find(1).unwrap().send_window.value();
        assert_eq!(
            writer.queue_data(driver, 1, b"late", false),
            Err(DataWriteError::StreamClosed)
        );
        assert_eq!(writer.buffer, output_before_rejection);
        assert_eq!(
            driver.connection_send_window().value(),
            connection_credit_before_rejection
        );
        assert_eq!(
            driver.streams().find(1).unwrap().send_window.value(),
            stream_credit_before_rejection
        );
    });
}

#[test]
fn enforces_peer_max_frame_size_and_resumes_the_unframed_tail() {
    with_open_stream(|driver| {
        let input = [b'x'; crate::frame::MIN_MAX_FRAME_SIZE as usize + 1];
        let mut output = [0; crate::frame::MIN_MAX_FRAME_SIZE as usize + FRAME_HEADER_LEN];
        let mut writer = DataFrameWriter::new(&mut output);
        let queued = writer.queue_data(driver, 1, &input, true).unwrap();

        assert_eq!(
            queued.payload_length,
            crate::frame::MIN_MAX_FRAME_SIZE as usize
        );
        assert!(!queued.end_stream);
        assert_eq!(writer.pending_bytes()[..3], [0, 0x40, 0]);
        assert_eq!(writer.pending_bytes()[3], 0);
        assert_eq!(
            writer.acknowledge(queued.frame_length),
            Ok(OutputProgress::Complete)
        );

        let queued = writer
            .queue_data(
                driver,
                1,
                &input[crate::frame::MIN_MAX_FRAME_SIZE as usize..],
                true,
            )
            .unwrap();
        assert_eq!(queued.payload_length, 1);
        assert!(queued.end_stream);
    });
}

#[test]
fn output_capacity_and_negative_stream_credit_leave_state_unchanged() {
    with_open_stream(|driver| {
        let mut short_output = [0xa5; FRAME_HEADER_LEN];
        let mut writer = DataFrameWriter::new(&mut short_output);

        assert_eq!(
            writer.queue_data(driver, 1, b"x", false),
            Err(DataWriteError::OutputTooSmall {
                required: FRAME_HEADER_LEN + 1,
            })
        );
        assert_eq!(writer.buffer, [0xa5; FRAME_HEADER_LEN]);
        assert_eq!(
            driver.connection_send_window().value(),
            DEFAULT_FLOW_WINDOW_SIZE
        );
        assert_eq!(
            driver.streams().find(1).unwrap().send_window.value(),
            DEFAULT_FLOW_WINDOW_SIZE
        );

        let parameter = SettingsParameter {
            id: SETTINGS_INITIAL_WINDOW_SIZE,
            value: 0,
        };
        driver.take_send_credit(1, 1).unwrap();
        assert_eq!(
            driver
                .streams_mut()
                .apply_remote_settings(&[parameter])
                .kind,
            StreamActionKind::Accept
        );
        assert_eq!(driver.streams().find(1).unwrap().send_window.value(), -1);
        let mut credit_output = [0xa5; FRAME_HEADER_LEN + 1];
        let mut credit_writer = DataFrameWriter::new(&mut credit_output);
        assert_eq!(
            credit_writer.queue_data(driver, 1, b"x", false),
            Err(DataWriteError::NoSendCredit)
        );
        assert_eq!(credit_writer.buffer, [0xa5; FRAME_HEADER_LEN + 1]);
        assert_eq!(
            driver.connection_send_window().value(),
            DEFAULT_FLOW_WINDOW_SIZE - 1
        );
    });
}

#[test]
fn empty_data_frames_do_not_consume_window_credit() {
    with_open_stream(|driver| {
        open_remote_stream(driver, 3);
        driver
            .take_send_credit(1, DEFAULT_FLOW_WINDOW_SIZE)
            .unwrap();
        assert_eq!(driver.connection_send_window().value(), 0);
        assert_eq!(driver.streams().find(1).unwrap().send_window.value(), 0);

        let mut output = [0; FRAME_HEADER_LEN];
        let mut writer = DataFrameWriter::new(&mut output);
        let queued = writer.queue_data(driver, 1, &[], false).unwrap();

        assert_eq!(queued.payload_length, 0);
        assert!(!queued.end_stream);
        assert_eq!(writer.pending_bytes()[4], 0);
        assert_eq!(
            writer.acknowledge(queued.frame_length),
            Ok(OutputProgress::Complete)
        );
        assert_eq!(driver.streams().find(1).unwrap().state, StreamState::Open);

        let queued = writer.queue_data(driver, 1, &[], true).unwrap();

        assert_eq!(
            queued,
            QueuedDataFrame {
                frame_length: FRAME_HEADER_LEN,
                payload_length: 0,
                end_stream: true,
            }
        );
        assert_eq!(
            writer.pending_bytes(),
            &[0, 0, 0, 0, DATA_FLAG_END_STREAM, 0, 0, 0, 1]
        );
        assert_eq!(
            writer.acknowledge(queued.frame_length + 1),
            Err(DataWriteError::OverAcknowledged)
        );
        assert!(writer.is_terminal());
        assert_eq!(driver.connection_send_window().value(), 0);
        assert_eq!(driver.streams().find(1).unwrap().send_window.value(), 0);
        assert_eq!(
            driver.streams().find(3).unwrap().send_window.value(),
            DEFAULT_FLOW_WINDOW_SIZE
        );

        let mut data_output = [0xa5; FRAME_HEADER_LEN + 1];
        let mut data_writer = DataFrameWriter::new(&mut data_output);
        assert_eq!(
            data_writer.queue_data(driver, 3, b"x", false),
            Err(DataWriteError::NoSendCredit)
        );
        assert_eq!(data_writer.buffer, [0xa5; FRAME_HEADER_LEN + 1]);
        assert_eq!(
            driver.streams().find(3).unwrap().send_window.value(),
            DEFAULT_FLOW_WINDOW_SIZE
        );
    });
}
