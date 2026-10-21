use super::ClientError;
use super::H2ClientDriver;
use crate::frame::DATA_FLAG_END_STREAM;
use crate::frame::DEFAULT_MAX_FRAME_SIZE;
use crate::frame::ErrorCode;
use crate::frame::FRAME_HEADER_LEN;
use crate::frame::FrameHeader;
use crate::frame::FrameType;
use crate::frame::GOAWAY_FIXED_LEN;
use crate::frame::HEADERS_FLAG_END_HEADERS;
use crate::frame::HEADERS_FLAG_END_STREAM;
use crate::frame::MIN_MAX_FRAME_SIZE;
use crate::frame::RST_STREAM_PAYLOAD_LEN;
use crate::frame::decode_frame_header;
use crate::frame::decode_goaway_payload;
use crate::frame::decode_rst_stream_payload;
use crate::frame::encode_frame_header;
use crate::frame::encode_goaway_payload;
use crate::hpack::HpackEntrySlot;
use crate::hpack::HpackFieldToEncode;
use crate::hpack::HpackHeader;
use crate::hpack::HpackRepresentation;
use crate::hpack::HpackTable;
use crate::hpack::decode_header_block;
use crate::stream::FrameDirection;
use crate::stream::H2Role;
use crate::stream::StreamActionKind;
use crate::stream::StreamSlot;
use crate::stream::StreamState;
use crate::stream::StreamTable;

fn with_client(run: impl FnOnce(&mut H2ClientDriver<'_, '_, '_, '_>)) {
    let mut slots = [StreamSlot::EMPTY; 4];
    let mut header_block_storage = [0; 256];
    let mut streams = StreamTable::new(&mut slots, H2Role::Client, &mut header_block_storage, 256);
    establish(&mut streams);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_storage = [0; 256];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_storage = [0; 256];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_storage, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut client = H2ClientDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    run(&mut client);
}

fn establish(streams: &mut StreamTable<'_>) {
    let settings = FrameHeader {
        payload_length: 0,
        frame_type: FrameType::Settings,
        flags: 0,
        stream_id: 0,
    };
    let sent = streams.on_frame(settings, FrameDirection::Sent, &[]);
    assert_eq!(sent.kind, StreamActionKind::Accept);
    let received = streams.on_frame(settings, FrameDirection::Received, &[]);
    assert_eq!(received.kind, StreamActionKind::Accept);
}

fn field(name: &'static [u8], value: &'static [u8]) -> HpackFieldToEncode<'static> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

fn request_fields() -> [HpackFieldToEncode<'static>; 4] {
    [
        field(b":method", b"GET"),
        field(b":scheme", b"https"),
        field(b":path", b"/"),
        field(b":authority", b"example.com"),
    ]
}

#[test]
fn sequential_requests_use_odd_stream_ids() {
    with_client(|client| {
        let fields = request_fields();
        let mut block_a = [0; 256];
        let mut output_a = [0; 1_024];
        let first = client
            .start_request(&fields, &mut block_a, &mut output_a, true)
            .unwrap();
        assert_eq!(first.stream_id, 1);

        let mut block_b = [0; 256];
        let mut output_b = [0; 1_024];
        let second = client
            .start_request(&fields, &mut block_b, &mut output_b, true)
            .unwrap();
        assert_eq!(second.stream_id, 3);
        assert_eq!(client.driver().streams().highest_local(), 3);
    });
}

#[test]
fn headers_bytes_decode_to_request_fields() {
    with_client(|client| {
        let fields = request_fields();
        let mut block = [0; 256];
        let mut output = [0; 1_024];
        let started = client
            .start_request(&fields, &mut block, &mut output, true)
            .unwrap();
        assert_eq!(started.stream_id, 1);

        let bytes = started.writer.pending_bytes();
        assert_eq!(bytes.len(), started.frame.output_length);
        let header = decode_frame_header(bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(header.frame_type, FrameType::Headers);
        assert_eq!(header.stream_id, 1);
        assert_eq!(
            header.flags,
            HEADERS_FLAG_END_STREAM | HEADERS_FLAG_END_HEADERS
        );

        let fragment = &bytes[FRAME_HEADER_LEN..];
        assert_eq!(fragment.len(), started.block_length);
        let mut entries = [HpackEntrySlot::EMPTY; 8];
        let mut storage = [0; 256];
        let mut decoder = HpackTable::new(&mut entries, &mut storage, 4_096);
        let mut headers = [HpackHeader::EMPTY; 8];
        let decoded = decode_header_block(fragment, &mut decoder, &mut headers).unwrap();
        assert_eq!(decoded.0, 4);
        assert_eq!(decoded.1, fragment.len());

        let expected: [(&[u8], &[u8]); 4] = [
            (&b":method"[..], &b"GET"[..]),
            (&b":scheme"[..], &b"https"[..]),
            (&b":path"[..], &b"/"[..]),
            (&b":authority"[..], &b"example.com"[..]),
        ];
        for (index, (name, value)) in expected.iter().enumerate() {
            let resolved_name = decoder.resolve(headers[index].field.name).unwrap();
            assert_eq!(resolved_name, *name);
            let resolved_value = decoder.resolve(headers[index].field.value).unwrap();
            assert_eq!(resolved_value, *value);
        }
        assert_eq!(
            client.driver().streams().find(1).unwrap().state,
            StreamState::HalfClosedLocal
        );
    });
}

#[test]
fn data_splits_across_max_frame_size_and_resumes() {
    with_client(|client| {
        let fields = request_fields();
        let mut block = [0; 256];
        let mut output = [0; 1_024];
        let started = client
            .start_request(&fields, &mut block, &mut output, false)
            .unwrap();
        assert_eq!(started.stream_id, 1);

        let payload = [0xAB; MIN_MAX_FRAME_SIZE as usize + 116];
        let mut first_output = [0; MIN_MAX_FRAME_SIZE as usize + FRAME_HEADER_LEN + 16];
        let first = client
            .send_data(1, &payload, true, &mut first_output)
            .unwrap();
        assert_eq!(first.frame.payload_length, MIN_MAX_FRAME_SIZE as usize);
        assert!(!first.frame.end_stream);

        let first_bytes = first.writer.pending_bytes();
        let first_header = decode_frame_header(first_bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(first_header.frame_type, FrameType::Data);
        assert_eq!(first_header.payload_length, MIN_MAX_FRAME_SIZE);
        assert_eq!(first_header.flags & DATA_FLAG_END_STREAM, 0);
        assert_eq!(
            &first_bytes[FRAME_HEADER_LEN..],
            &payload[..MIN_MAX_FRAME_SIZE as usize]
        );

        let consumed = first.frame.payload_length;
        let mut second_output = [0; 256];
        let second = client
            .send_data(1, &payload[consumed..], true, &mut second_output)
            .unwrap();
        assert_eq!(second.frame.payload_length, 116);
        assert!(second.frame.end_stream);

        let second_bytes = second.writer.pending_bytes();
        let second_header = decode_frame_header(second_bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(second_header.frame_type, FrameType::Data);
        assert_eq!(second_header.payload_length, 116);
        assert_ne!(second_header.flags & DATA_FLAG_END_STREAM, 0);
        assert_eq!(&second_bytes[FRAME_HEADER_LEN..], &payload[consumed..]);
    });
}

#[test]
fn cancel_closes_stream_and_emits_rst() {
    with_client(|client| {
        let fields = request_fields();
        let mut block = [0; 256];
        let mut output = [0; 1_024];
        client
            .start_request(&fields, &mut block, &mut output, false)
            .unwrap();

        let mut rst = [0; FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN];
        {
            let writer = client.cancel(1, &mut rst).unwrap();
            let bytes = writer.pending_bytes();
            assert_eq!(bytes.len(), FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN);
            let header = decode_frame_header(bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();
            assert_eq!(header.frame_type, FrameType::RstStream);
            assert_eq!(header.stream_id, 1);
            let code = decode_rst_stream_payload(header, &bytes[FRAME_HEADER_LEN..]).unwrap();
            assert_eq!(code, ErrorCode::Cancel);
        }
        assert_eq!(
            client.driver().streams().find(1).unwrap().state,
            StreamState::Closed
        );
        assert!(matches!(
            client.cancel(1, &mut rst),
            Err(ClientError::StreamClosed)
        ));
        assert!(matches!(
            client.cancel(9, &mut rst),
            Err(ClientError::UnknownStream)
        ));
    });
}

#[test]
fn goaway_blocks_new_streams_but_keeps_inflight_data() {
    with_client(|client| {
        let fields = request_fields();
        let mut block = [0; 256];
        let mut output = [0; 1_024];
        client
            .start_request(&fields, &mut block, &mut output, false)
            .unwrap();

        let mut goaway = [0; FRAME_HEADER_LEN + GOAWAY_FIXED_LEN];
        {
            let writer = client.goaway(&mut goaway).unwrap();
            let bytes = writer.pending_bytes();
            let header = decode_frame_header(bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();
            assert_eq!(header.frame_type, FrameType::GoAway);
            assert_eq!(header.stream_id, 0);
            let decoded = decode_goaway_payload(header, &bytes[FRAME_HEADER_LEN..]).unwrap();
            assert_eq!(decoded.last_stream_id, 0);
            assert_eq!(decoded.error_code, ErrorCode::NoError);
        }
        assert!(client.is_closed_for_new_streams());

        let mut late_block = [0; 256];
        let mut late_output = [0; 1_024];
        assert!(matches!(
            client.start_request(&fields, &mut late_block, &mut late_output, true),
            Err(ClientError::NewStreamsClosed)
        ));

        let mut data_output = [0; 64];
        let sent = client
            .send_data(1, b"hello", true, &mut data_output)
            .unwrap();
        assert_eq!(sent.frame.payload_length, 5);
        assert!(sent.frame.end_stream);
    });
}

#[test]
fn received_goaway_blocks_new_streams() {
    with_client(|client| {
        let header = FrameHeader {
            payload_length: GOAWAY_FIXED_LEN as u32,
            frame_type: FrameType::GoAway,
            flags: 0,
            stream_id: 0,
        };
        let mut frame = [0; FRAME_HEADER_LEN + GOAWAY_FIXED_LEN];
        encode_frame_header(
            header,
            &mut frame[..FRAME_HEADER_LEN],
            DEFAULT_MAX_FRAME_SIZE,
        )
        .unwrap();
        encode_goaway_payload(0, ErrorCode::NoError, &[], &mut frame[FRAME_HEADER_LEN..]).unwrap();

        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 1];
        let result = client.driver_mut().feed(&frame, &mut actions, &mut headers);
        assert_eq!(result.actions, 1);
        assert!(client.is_closed_for_new_streams());

        let fields = request_fields();
        let mut block = [0; 256];
        let mut output = [0; 1_024];
        assert!(matches!(
            client.start_request(&fields, &mut block, &mut output, true),
            Err(ClientError::NewStreamsClosed)
        ));
    });
}
