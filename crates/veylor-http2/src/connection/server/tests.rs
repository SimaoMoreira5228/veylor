use super::H2ServerDriver;
use super::ServerError;
use crate::connection::FeedResult;
use crate::connection::H2FrameAction;
use crate::frame::DEFAULT_MAX_FRAME_SIZE;
use crate::frame::ErrorCode;
use crate::frame::FRAME_HEADER_LEN;
use crate::frame::FrameHeader;
use crate::frame::FrameType;
use crate::frame::GOAWAY_FIXED_LEN;
use crate::frame::HEADERS_FLAG_END_HEADERS;
use crate::frame::HEADERS_FLAG_END_STREAM;
use crate::frame::RST_STREAM_PAYLOAD_LEN;
use crate::frame::decode_frame_header;
use crate::frame::decode_goaway_payload;
use crate::frame::decode_rst_stream_payload;
use crate::frame::encode_frame_header;
use crate::hpack::HpackEntrySlot;
use crate::hpack::HpackFieldToEncode;
use crate::hpack::HpackHeader;
use crate::hpack::HpackRepresentation;
use crate::hpack::HpackTable;
use crate::hpack::decode_header_block;
use crate::hpack::encode_header_block;
use crate::stream::CLIENT_CONNECTION_PREFACE;
use crate::stream::FrameDirection;
use crate::stream::H2Role;
use crate::stream::StreamActionKind;
use crate::stream::StreamSlot;
use crate::stream::StreamState;
use crate::stream::StreamTable;

fn with_server(run: impl FnOnce(&mut H2ServerDriver<'_, '_, '_, '_>)) {
    let mut slots = [StreamSlot::EMPTY; 4];
    let mut header_block_storage = [0; 256];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block_storage, 256);
    establish(&mut streams);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_storage = [0; 512];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_storage = [0; 512];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_storage, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut server = H2ServerDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    run(&mut server);
}

fn establish(streams: &mut StreamTable<'_>) {
    streams
        .receive_client_preface_fragment(CLIENT_CONNECTION_PREFACE)
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

fn encode_block(fields: &[HpackFieldToEncode<'_>], output: &mut [u8]) -> usize {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut storage = [0; 256];
    let mut encoder = HpackTable::new(&mut entries, &mut storage, 4_096);

    encode_header_block(fields, &mut encoder, output).unwrap()
}

fn feed_headers<'input>(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream_id: u32,
    block: &[u8],
    flags: u8,
    actions: &mut [Option<H2FrameAction<'input>>],
    headers: &mut [HpackHeader],
    input: &'input mut [u8],
) -> FeedResult {
    let header = FrameHeader {
        payload_length: block.len() as u32,
        frame_type: FrameType::Headers,
        flags,
        stream_id,
    };
    encode_frame_header(
        header,
        &mut input[..FRAME_HEADER_LEN],
        DEFAULT_MAX_FRAME_SIZE,
    )
    .unwrap();
    input[FRAME_HEADER_LEN..FRAME_HEADER_LEN + block.len()].copy_from_slice(block);

    server
        .driver_mut()
        .feed(&input[..FRAME_HEADER_LEN + block.len()], actions, headers)
}

fn get_block(block: &mut [u8]) -> usize {
    let fields = [
        field(b":method", b"GET"),
        field(b":scheme", b"https"),
        field(b":path", b"/"),
        field(b":authority", b"example.com"),
    ];

    encode_block(&fields, block)
}

#[test]
fn fed_get_decodes_and_validates() {
    with_server(|server| {
        let mut block = [0; 256];
        let block_length = get_block(&mut block);

        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 8];
        let mut input = [0; 512];
        let result = feed_headers(
            server,
            1,
            &block[..block_length],
            HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
            &mut actions,
            &mut headers,
            &mut input,
        );

        assert_eq!(result.actions, 1);
        assert_eq!(result.headers, 4);

        let range = match actions[0] {
            Some(H2FrameAction::Headers {
                stream_id,
                headers,
                end_stream,
            }) => {
                assert_eq!(stream_id, 1);
                assert!(end_stream);
                headers
            }
            _ => panic!("expected headers action"),
        };

        let request = server
            .accept_request(1, &headers[range.start..range.start + range.count])
            .unwrap();

        assert_eq!(request.method, b"GET");
        assert_eq!(request.scheme, Some(&b"https"[..]));
        assert_eq!(request.authority, Some(&b"example.com"[..]));
        assert_eq!(request.path, Some(&b"/"[..]));
        assert_eq!(request.content_length, None);
    });
}

#[test]
fn connect_authority_only_accepts() {
    with_server(|server| {
        let fields = [
            field(b":method", b"CONNECT"),
            field(b":authority", b"example.com:443"),
        ];
        let mut block = [0; 256];
        let block_length = encode_block(&fields, &mut block);

        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 8];
        let mut input = [0; 512];
        feed_headers(
            server,
            1,
            &block[..block_length],
            HEADERS_FLAG_END_HEADERS,
            &mut actions,
            &mut headers,
            &mut input,
        );

        let range = match actions[0] {
            Some(H2FrameAction::Headers { headers, .. }) => headers,
            _ => panic!("expected headers action"),
        };
        let request = server
            .accept_request(1, &headers[range.start..range.start + range.count])
            .unwrap();

        assert_eq!(request.method, b"CONNECT");
        assert_eq!(request.scheme, None);
        assert_eq!(request.authority, Some(&b"example.com:443"[..]));
        assert_eq!(request.path, None);
    });
}

#[test]
fn options_asterisk_accepts() {
    with_server(|server| {
        let fields = [
            field(b":method", b"OPTIONS"),
            field(b":scheme", b"https"),
            field(b":path", b"*"),
        ];
        let mut block = [0; 256];
        let block_length = encode_block(&fields, &mut block);

        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 8];
        let mut input = [0; 512];
        feed_headers(
            server,
            1,
            &block[..block_length],
            HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
            &mut actions,
            &mut headers,
            &mut input,
        );

        let range = match actions[0] {
            Some(H2FrameAction::Headers { headers, .. }) => headers,
            _ => panic!("expected headers action"),
        };
        let request = server
            .accept_request(1, &headers[range.start..range.start + range.count])
            .unwrap();

        assert_eq!(request.method, b"OPTIONS");
        assert_eq!(request.path, Some(&b"*"[..]));
        assert_eq!(request.authority, None);
    });
}

#[test]
fn duplicate_method_rejected_then_refused() {
    with_server(|server| {
        let fields = [
            field(b":method", b"GET"),
            field(b":method", b"POST"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
            field(b":authority", b"example.com"),
        ];
        let mut block = [0; 256];
        let block_length = encode_block(&fields, &mut block);

        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 8];
        let mut input = [0; 512];
        feed_headers(
            server,
            1,
            &block[..block_length],
            HEADERS_FLAG_END_HEADERS,
            &mut actions,
            &mut headers,
            &mut input,
        );

        let range = match actions[0] {
            Some(H2FrameAction::Headers { headers, .. }) => headers,
            _ => panic!("expected headers action"),
        };
        assert!(matches!(
            server.accept_request(1, &headers[range.start..range.start + range.count]),
            Err(ServerError::Request(
                crate::connection::ServerRequestError::DuplicatePseudoHeader
            ))
        ));

        let mut rst = [0; FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN];
        {
            let writer = server.refuse(1, &mut rst).unwrap();
            let bytes = writer.pending_bytes();

            assert_eq!(bytes.len(), FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN);

            let header = decode_frame_header(bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();

            assert_eq!(header.frame_type, FrameType::RstStream);
            assert_eq!(header.stream_id, 1);

            let code = decode_rst_stream_payload(header, &bytes[FRAME_HEADER_LEN..]).unwrap();

            assert_eq!(code, ErrorCode::RefusedStream);
        }

        assert_eq!(
            server.driver().streams().find(1).unwrap().state,
            StreamState::Closed
        );
        assert!(matches!(
            server.refuse(1, &mut rst),
            Err(ServerError::StreamClosed)
        ));
        assert!(matches!(
            server.refuse(9, &mut rst),
            Err(ServerError::UnknownStream)
        ));
    });
}

#[test]
fn connection_specific_field_rejected() {
    with_server(|server| {
        let fields = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
            field(b":authority", b"example.com"),
            field(b"connection", b"close"),
        ];
        let mut block = [0; 256];
        let block_length = encode_block(&fields, &mut block);

        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 8];
        let mut input = [0; 512];
        feed_headers(
            server,
            1,
            &block[..block_length],
            HEADERS_FLAG_END_HEADERS,
            &mut actions,
            &mut headers,
            &mut input,
        );

        let range = match actions[0] {
            Some(H2FrameAction::Headers { headers, .. }) => headers,
            _ => panic!("expected headers action"),
        };
        assert!(matches!(
            server.accept_request(1, &headers[range.start..range.start + range.count]),
            Err(ServerError::Request(
                crate::connection::ServerRequestError::ConnectionSpecificField
            ))
        ));
    });
}

#[test]
fn response_headers_bytes_round_trip_decode_to_identical_fields() {
    with_server(|server| {
        let mut block = [0; 256];
        let block_length = get_block(&mut block);

        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 8];
        let mut input = [0; 512];
        feed_headers(
            server,
            1,
            &block[..block_length],
            HEADERS_FLAG_END_HEADERS,
            &mut actions,
            &mut headers,
            &mut input,
        );

        let fields = [
            field(b":status", b"200"),
            field(b"content-type", b"text/plain"),
            field(b"content-length", b"5"),
        ];
        let mut response_block = [0; 256];
        let mut output = [0; 1_024];
        let sent = server
            .send_response(1, &fields, &mut response_block, &mut output, false)
            .unwrap();

        assert_eq!(sent.block_length, response_block[..sent.block_length].len());

        let bytes = sent.writer.pending_bytes();
        let header = decode_frame_header(bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();

        assert_eq!(header.frame_type, FrameType::Headers);
        assert_eq!(header.stream_id, 1);
        assert_eq!(header.flags & HEADERS_FLAG_END_STREAM, 0);

        let fragment = &bytes[FRAME_HEADER_LEN..];

        assert_eq!(fragment.len(), sent.block_length);

        let mut entries = [HpackEntrySlot::EMPTY; 8];
        let mut storage = [0; 256];
        let mut decoder = HpackTable::new(&mut entries, &mut storage, 4_096);
        let mut decoded = [HpackHeader::EMPTY; 8];
        let (count, consumed) = decode_header_block(fragment, &mut decoder, &mut decoded).unwrap();

        assert_eq!(count, 3);
        assert_eq!(consumed, fragment.len());

        let expected: [(&[u8], &[u8]); 3] = [
            (&b":status"[..], &b"200"[..]),
            (&b"content-type"[..], &b"text/plain"[..]),
            (&b"content-length"[..], &b"5"[..]),
        ];

        for (index, (name, value)) in expected.iter().enumerate() {
            assert_eq!(decoder.resolve(decoded[index].field.name), Some(*name));
            assert_eq!(decoder.resolve(decoded[index].field.value), Some(*value));
        }

        assert_eq!(
            server.driver().streams().find(1).unwrap().state,
            StreamState::Open
        );

        let mut data_output = [0; 64];
        let data = server
            .send_data(1, b"hello", true, &mut data_output)
            .unwrap();

        assert_eq!(data.frame.payload_length, 5);
        assert!(data.frame.end_stream);
        assert_eq!(
            server.driver().streams().find(1).unwrap().state,
            StreamState::HalfClosedLocal
        );
    });
}

#[test]
fn response_validation_rejects_bad_status_and_fields() {
    with_server(|server| {
        let mut block = [0; 256];
        let block_length = get_block(&mut block);

        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 8];
        let mut input = [0; 512];
        feed_headers(
            server,
            1,
            &block[..block_length],
            HEADERS_FLAG_END_HEADERS,
            &mut actions,
            &mut headers,
            &mut input,
        );

        let mut response_block = [0; 256];
        let mut output = [0; 1_024];

        let missing_status = [field(b"content-type", b"text/plain")];
        assert!(matches!(
            server.send_response(1, &missing_status, &mut response_block, &mut output, true),
            Err(ServerError::Response(
                crate::connection::ServerResponseError::MissingStatus
            ))
        ));

        let switching = [field(b":status", b"101")];
        assert!(matches!(
            server.send_response(1, &switching, &mut response_block, &mut output, true),
            Err(ServerError::Response(
                crate::connection::ServerResponseError::SwitchingProtocols
            ))
        ));

        let informational_end = [field(b":status", b"103")];
        assert!(matches!(
            server.send_response(
                1,
                &informational_end,
                &mut response_block,
                &mut output,
                true
            ),
            Err(ServerError::Response(
                crate::connection::ServerResponseError::InformationalEndedStream
            ))
        ));

        let length_on_no_content = [field(b":status", b"204"), field(b"content-length", b"0")];
        assert!(matches!(
            server.send_response(
                1,
                &length_on_no_content,
                &mut response_block,
                &mut output,
                true
            ),
            Err(ServerError::Response(
                crate::connection::ServerResponseError::ContentLengthNotAllowed
            ))
        ));

        let prohibited = [field(b":status", b"200"), field(b"connection", b"close")];
        assert!(matches!(
            server.send_response(1, &prohibited, &mut response_block, &mut output, true),
            Err(ServerError::Response(
                crate::connection::ServerResponseError::ProhibitedField
            ))
        ));

        let request_pseudo = [field(b":status", b"200"), field(b":method", b"GET")];
        assert!(matches!(
            server.send_response(1, &request_pseudo, &mut response_block, &mut output, true),
            Err(ServerError::Response(
                crate::connection::ServerResponseError::UnexpectedPseudoHeader
            ))
        ));
    });
}

#[test]
fn refuse_emits_rst_refused_stream_and_closes() {
    with_server(|server| {
        let mut block = [0; 256];
        let block_length = get_block(&mut block);

        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 8];
        let mut input = [0; 512];
        feed_headers(
            server,
            1,
            &block[..block_length],
            HEADERS_FLAG_END_HEADERS,
            &mut actions,
            &mut headers,
            &mut input,
        );

        let mut rst = [0; FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN];
        {
            let writer = server.refuse(1, &mut rst).unwrap();
            let bytes = writer.pending_bytes();
            let header = decode_frame_header(bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();

            assert_eq!(header.frame_type, FrameType::RstStream);
            assert_eq!(header.stream_id, 1);

            let code = decode_rst_stream_payload(header, &bytes[FRAME_HEADER_LEN..]).unwrap();

            assert_eq!(code, ErrorCode::RefusedStream);
        }

        assert_eq!(
            server.driver().streams().find(1).unwrap().state,
            StreamState::Closed
        );

        let mut cancel = [0; FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN];
        assert!(matches!(
            server.rst_stream(1, ErrorCode::Cancel, &mut cancel),
            Err(ServerError::StreamClosed)
        ));
    });
}

#[test]
fn goaway_drain_marks_closing_and_bars_new_streams() {
    with_server(|server| {
        let mut block = [0; 256];
        let block_length = get_block(&mut block);

        let mut actions = [None; 1];
        let mut headers = [HpackHeader::EMPTY; 8];
        let mut input = [0; 512];
        feed_headers(
            server,
            1,
            &block[..block_length],
            HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
            &mut actions,
            &mut headers,
            &mut input,
        );

        assert!(!server.is_closing());

        let mut goaway = [0; FRAME_HEADER_LEN + GOAWAY_FIXED_LEN];
        {
            let writer = server.goaway_drain(1, &mut goaway).unwrap();
            let bytes = writer.pending_bytes();
            let header = decode_frame_header(bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();

            assert_eq!(header.frame_type, FrameType::GoAway);
            assert_eq!(header.stream_id, 0);

            let decoded = decode_goaway_payload(header, &bytes[FRAME_HEADER_LEN..]).unwrap();

            assert_eq!(decoded.last_stream_id, 1);
            assert_eq!(decoded.error_code, ErrorCode::NoError);
        }

        assert!(server.is_closing());
        assert!(server.is_closed_for_new_streams());
        assert!(matches!(
            server.accept_request(3, &[]),
            Err(ServerError::NewStreamsClosed)
        ));

        let fields = [field(b":status", b"200")];
        let mut response_block = [0; 256];
        let mut output = [0; 1_024];
        server
            .send_response(1, &fields, &mut response_block, &mut output, true)
            .unwrap();

        assert_eq!(
            server.driver().streams().find(1).unwrap().state,
            StreamState::Closed
        );
    });
}
