use super::*;
use crate::frame::{
    DEFAULT_MAX_FRAME_SIZE, FRAME_HEADER_LEN, HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM,
    MIN_MAX_FRAME_SIZE, decode_frame_header, encode_frame_header,
};
use crate::hpack::{HpackEntrySlot, HpackHeader, HpackTable};
use crate::stream::{
    FrameDirection, H2Role, StreamActionKind, StreamSlot, StreamState, StreamTable,
};

fn with_server_stream(run: impl FnOnce(&mut H2FrameDriver<'_, '_, '_, '_>)) {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut header_block_storage = [0; 64];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block_storage, 64);
    streams
        .receive_client_preface_fragment(crate::stream::CLIENT_CONNECTION_PREFACE)
        .unwrap();
    establish(&mut streams);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_storage = [0; 256];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
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

fn with_client_driver(run: impl FnOnce(&mut H2FrameDriver<'_, '_, '_, '_>)) {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut header_block_storage = [0; 64];
    let mut streams = StreamTable::new(&mut slots, H2Role::Client, &mut header_block_storage, 64);
    establish(&mut streams);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_storage = [0; 256];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
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

    run(&mut driver);
}

fn establish(streams: &mut StreamTable<'_>) {
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
}

fn open_remote_stream(driver: &mut H2FrameDriver<'_, '_, '_, '_>, stream_id: u32) {
    let mut input = [0; FRAME_HEADER_LEN + 3];
    let header = FrameHeader {
        payload_length: 3,
        frame_type: FrameType::Headers,
        flags: HEADERS_FLAG_END_HEADERS,
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
fn splits_headers_into_continuations_and_resumes_after_partial_acknowledgments() {
    with_client_driver(|driver| {
        let block = [b'x'; MIN_MAX_FRAME_SIZE as usize + 1];
        let required = block.len() + 2 * FRAME_HEADER_LEN;
        let mut short_output = [0xa5; MIN_MAX_FRAME_SIZE as usize + 1 + FRAME_HEADER_LEN * 2 - 1];
        let mut short_writer = HeadersFrameWriter::new(&mut short_output);

        assert_eq!(
            short_writer.queue_headers(driver, 1, &block, true),
            Err(HeadersWriteError::OutputTooSmall { required })
        );
        assert_eq!(
            short_writer.buffer,
            [0xa5; MIN_MAX_FRAME_SIZE as usize + 1 + FRAME_HEADER_LEN * 2 - 1]
        );
        assert_eq!(driver.streams().find(1), None);

        let mut output = [0; MIN_MAX_FRAME_SIZE as usize + 1 + FRAME_HEADER_LEN * 2];
        let mut writer = HeadersFrameWriter::new(&mut output);
        let queued = writer.queue_headers(driver, 1, &block, true).unwrap();

        assert_eq!(
            queued,
            QueuedHeadersBlock {
                frame_count: 2,
                block_length: block.len(),
                output_length: required,
                end_stream: true,
            }
        );
        let first = decode_frame_header(writer.pending_bytes(), DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(first.frame_type, FrameType::Headers);
        assert_eq!(first.payload_length, MIN_MAX_FRAME_SIZE);
        assert_eq!(first.flags, HEADERS_FLAG_END_STREAM);
        assert_eq!(first.stream_id, 1);
        assert_eq!(
            &writer.pending_bytes()
                [FRAME_HEADER_LEN..FRAME_HEADER_LEN + MIN_MAX_FRAME_SIZE as usize],
            &block[..MIN_MAX_FRAME_SIZE as usize]
        );

        let second_offset = FRAME_HEADER_LEN + MIN_MAX_FRAME_SIZE as usize;
        let second = decode_frame_header(
            &writer.pending_bytes()[second_offset..],
            DEFAULT_MAX_FRAME_SIZE,
        )
        .unwrap();
        assert_eq!(second.frame_type, FrameType::Continuation);
        assert_eq!(second.payload_length, 1);
        assert_eq!(second.flags, CONTINUATION_FLAG_END_HEADERS);
        assert_eq!(second.stream_id, 1);
        assert_eq!(
            writer.pending_bytes()[second_offset + FRAME_HEADER_LEN],
            block[MIN_MAX_FRAME_SIZE as usize]
        );
        assert_eq!(
            driver.streams().find(1).unwrap().state,
            StreamState::HalfClosedLocal
        );

        assert_eq!(
            writer.acknowledge(7),
            Ok(OutputProgress::Pending {
                remaining: required - 7,
            })
        );
        assert_eq!(writer.pending_bytes(), &writer.buffer[7..required]);
        assert_eq!(
            writer.queue_headers(driver, 3, b"block", false),
            Err(HeadersWriteError::PendingFrame)
        );
        assert_eq!(
            writer.acknowledge(required - 7),
            Ok(OutputProgress::Complete)
        );
        assert!(matches!(
            writer.queue_headers(driver, 1, b"late", false),
            Err(HeadersWriteError::StreamRejected(action))
                if action.kind == StreamActionKind::SendRst
                    && action.code == crate::frame::ErrorCode::StreamClosed
        ));
    });
}

#[test]
fn empty_headers_block_sets_end_headers_and_transitions_remote_stream() {
    with_server_stream(|driver| {
        let mut output = [0; FRAME_HEADER_LEN];
        let mut writer = HeadersFrameWriter::new(&mut output);
        let queued = writer.queue_headers(driver, 1, &[], true).unwrap();

        assert_eq!(queued.frame_count, 1);
        assert_eq!(queued.output_length, FRAME_HEADER_LEN);
        let header = decode_frame_header(writer.pending_bytes(), DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(header.frame_type, FrameType::Headers);
        assert_eq!(header.payload_length, 0);
        assert_eq!(
            header.flags,
            HEADERS_FLAG_END_STREAM | HEADERS_FLAG_END_HEADERS
        );
        assert_eq!(header.stream_id, 1);
        assert_eq!(
            driver.streams().find(1).unwrap().state,
            StreamState::HalfClosedLocal
        );
    });
}

#[test]
fn rejected_local_stream_headers_leave_output_and_stream_table_unchanged() {
    with_client_driver(|driver| {
        driver.streams_mut().set_remote_max_concurrent(0);
        let mut output = [0xa5; FRAME_HEADER_LEN + 5];
        let mut writer = HeadersFrameWriter::new(&mut output);

        let rejected = writer.queue_headers(driver, 1, b"block", false);
        assert!(matches!(
            rejected,
            Err(HeadersWriteError::StreamRejected(action))
                if action.kind == StreamActionKind::RejectProtocolError
        ));
        assert_eq!(writer.buffer, [0xa5; FRAME_HEADER_LEN + 5]);
        assert_eq!(driver.streams().find(1), None);
        assert_eq!(driver.streams().active_count(), 0);

        driver.streams_mut().set_remote_max_concurrent(1);
        let queued = writer.queue_headers(driver, 1, b"block", false).unwrap();
        assert_eq!(queued.frame_count, 1);
        assert_eq!(driver.streams().find(1).unwrap().state, StreamState::Open);
    });
}
