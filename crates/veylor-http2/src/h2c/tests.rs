use super::{
    H2C_GOAWAY_LEN, H2C_HEADER_TABLE_SIZE, H2C_MAX_CONCURRENT_STREAMS, H2C_MAX_FRAME_SIZE,
    H2C_SERVER_SETTINGS_LEN, H2cError, H2cServerHandshake,
};
use crate::connection::{ControlFrameWriter, FeedStatus, H2FrameAction};
use crate::frame::{
    DEFAULT_MAX_FRAME_SIZE, ErrorCode, FRAME_HEADER_LEN, FrameHeader, FrameType,
    HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, PING_PAYLOAD_LEN,
    SETTINGS_HEADER_TABLE_SIZE, SettingsParameter, decode_frame_header, decode_goaway_payload,
    decode_settings_payload, encode_frame_header,
};
use crate::hpack::{
    HpackEntrySlot, HpackFieldToEncode, HpackHeader, HpackRepresentation, HpackTable,
    encode_header_block,
};
use crate::stream::{CLIENT_CONNECTION_PREFACE, H2Role, StreamSlot, StreamTable};

fn with_handshake(run: impl FnOnce(H2cServerHandshake<'_, '_, '_, '_>)) {
    let mut slots = [StreamSlot::EMPTY; 4];
    let mut header_block_storage = [0; 256];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block_storage, 256);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_storage = [0; 512];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_storage = [0; 512];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_storage, 4_096);
    let mut settings_ack_limits = [0; 4];
    let handshake = H2cServerHandshake::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();
    run(handshake);
}

fn send_server_settings(handshake: &mut H2cServerHandshake<'_, '_, '_, '_>) -> [u8; 64] {
    let mut output = [0; 64];
    let writer = handshake.server_settings(&mut output).unwrap();
    assert_eq!(writer.pending_bytes().len(), H2C_SERVER_SETTINGS_LEN);
    output
}

fn frame_into(
    output: &mut [u8],
    frame_type: FrameType,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> usize {
    let header = FrameHeader {
        payload_length: payload.len() as u32,
        frame_type,
        flags,
        stream_id,
    };
    encode_frame_header(
        header,
        &mut output[..FRAME_HEADER_LEN],
        DEFAULT_MAX_FRAME_SIZE,
    )
    .unwrap();
    output[FRAME_HEADER_LEN..FRAME_HEADER_LEN + payload.len()].copy_from_slice(payload);
    FRAME_HEADER_LEN + payload.len()
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

fn get_block(block: &mut [u8]) -> usize {
    let fields = [
        field(b":method", b"GET"),
        field(b":scheme", b"http"),
        field(b":path", b"/"),
        field(b":authority", b"example.com"),
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut storage = [0; 256];
    let mut encoder = HpackTable::new(&mut entries, &mut storage, 4_096);
    encode_header_block(&fields, &mut encoder, block).unwrap()
}

fn assert_protocol_goaway(output: &[u8]) {
    assert_eq!(output.len(), H2C_GOAWAY_LEN);
    let header = decode_frame_header(output, DEFAULT_MAX_FRAME_SIZE).unwrap();
    assert_eq!(header.frame_type, FrameType::GoAway);
    assert_eq!(header.stream_id, 0);
    let goaway = decode_goaway_payload(header, &output[FRAME_HEADER_LEN..]).unwrap();
    assert_eq!(goaway.last_stream_id, 0);
    assert_eq!(goaway.error_code, ErrorCode::ProtocolError);
}

#[test]
fn exact_magic_plus_empty_settings_completes() {
    with_handshake(|mut handshake| {
        let settings_bytes = send_server_settings(&mut handshake);
        let header = decode_frame_header(&settings_bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(header.frame_type, FrameType::Settings);
        assert_eq!(header.stream_id, 0);
        let mut parameters = [SettingsParameter { id: 0, value: 0 }; 4];
        let count = decode_settings_payload(
            header,
            &settings_bytes[FRAME_HEADER_LEN..H2C_SERVER_SETTINGS_LEN],
            &mut parameters,
        )
        .unwrap();
        assert_eq!(count, 4);
        assert_eq!(
            parameters,
            [
                SettingsParameter {
                    id: SETTINGS_HEADER_TABLE_SIZE,
                    value: H2C_HEADER_TABLE_SIZE
                },
                SettingsParameter {
                    id: crate::frame::SETTINGS_MAX_FRAME_SIZE,
                    value: H2C_MAX_FRAME_SIZE
                },
                SettingsParameter {
                    id: crate::frame::SETTINGS_MAX_CONCURRENT_STREAMS,
                    value: H2C_MAX_CONCURRENT_STREAMS.min(4)
                },
                // NOTE: the handshake advertises the table's effective assembly cap
                // (this harness wires 256-byte storage with a 256 limit), not a fixed
                // const, so the wire promise always matches what fits.
                SettingsParameter {
                    id: crate::frame::SETTINGS_MAX_HEADER_LIST_SIZE,
                    value: 256,
                },
            ]
        );
        assert!(!handshake.is_complete());

        let mut input = [0; 64];
        input[..CLIENT_CONNECTION_PREFACE.len()].copy_from_slice(CLIENT_CONNECTION_PREFACE);
        let settings_len = frame_into(
            &mut input[CLIENT_CONNECTION_PREFACE.len()..],
            FrameType::Settings,
            0,
            0,
            &[],
        );
        let total = CLIENT_CONNECTION_PREFACE.len() + settings_len;
        let mut output = [0; 64];
        let mut actions = [None; 4];
        let mut headers = [HpackHeader::EMPTY; 8];
        let result = handshake.feed(&input[..total], &mut output, &mut actions, &mut headers);

        assert_eq!(result.consumed, total);
        assert_eq!(result.actions, 1);
        assert_eq!(result.headers, 0);
        assert_eq!(result.output_used, 0);
        assert_eq!(result.status, FeedStatus::NeedMoreInput);
        assert_eq!(result.error, None);
        assert!(matches!(
            actions[0],
            Some(H2FrameAction::SettingsAckRequired)
        ));
        assert!(handshake.is_complete());
    });
}

#[test]
fn split_magic_byte_at_a_time_completes() {
    with_handshake(|mut handshake| {
        send_server_settings(&mut handshake);
        let mut output = [0; 64];
        for byte in CLIENT_CONNECTION_PREFACE.iter() {
            let mut actions = [None; 4];
            let mut headers = [HpackHeader::EMPTY; 8];
            let result = handshake.feed(
                std::slice::from_ref(byte),
                &mut output,
                &mut actions,
                &mut headers,
            );
            assert_eq!(result.consumed, 1);
            assert_eq!(result.actions, 0);
            assert_eq!(result.status, FeedStatus::NeedMoreInput);
            assert_eq!(result.error, None);
            assert!(!handshake.is_complete());
        }
        assert!(handshake.client_preface_complete());

        let mut input = [0; FRAME_HEADER_LEN];
        let length = frame_into(&mut input, FrameType::Settings, 0, 0, &[]);
        let mut actions = [None; 4];
        let mut headers = [HpackHeader::EMPTY; 8];
        let result = handshake.feed(&input[..length], &mut output, &mut actions, &mut headers);
        assert_eq!(result.actions, 1);
        assert!(handshake.is_complete());
    });
}

#[test]
fn bad_magic_emits_goaway_and_stays_dead() {
    with_handshake(|mut handshake| {
        send_server_settings(&mut handshake);
        let mut input = [0; 32];
        input[..CLIENT_CONNECTION_PREFACE.len()].copy_from_slice(CLIENT_CONNECTION_PREFACE);
        input[0] = b'X';
        let mut output = [0; 64];
        let mut actions = [None; 4];
        let mut headers = [HpackHeader::EMPTY; 8];
        let result = handshake.feed(
            &input[..CLIENT_CONNECTION_PREFACE.len()],
            &mut output,
            &mut actions,
            &mut headers,
        );

        assert_eq!(result.status, FeedStatus::ConnectionFailed);
        assert_eq!(result.error, Some(ErrorCode::ProtocolError));
        assert_eq!(handshake.failed(), Some(ErrorCode::ProtocolError));
        assert!(!handshake.is_complete());
        assert_protocol_goaway(&output[..result.output_used]);

        let mut retry_actions = [None; 4];
        let mut retry_headers = [HpackHeader::EMPTY; 8];
        let retry = handshake.feed(
            &input[..CLIENT_CONNECTION_PREFACE.len()],
            &mut output,
            &mut retry_actions,
            &mut retry_headers,
        );
        assert_eq!(retry.status, FeedStatus::ConnectionFailed);
        assert_eq!(retry.consumed, 0);
        assert_eq!(retry.actions, 0);
    });
}

#[test]
fn ping_before_client_settings_is_protocol_error() {
    with_handshake(|mut handshake| {
        send_server_settings(&mut handshake);
        let mut input = [0; 64];
        input[..CLIENT_CONNECTION_PREFACE.len()].copy_from_slice(CLIENT_CONNECTION_PREFACE);
        let ping_len = frame_into(
            &mut input[CLIENT_CONNECTION_PREFACE.len()..],
            FrameType::Ping,
            0,
            0,
            &[1, 2, 3, 4, 5, 6, 7, 8],
        );
        let total = CLIENT_CONNECTION_PREFACE.len() + ping_len;
        let mut output = [0; 64];
        let mut actions = [None; 4];
        let mut headers = [HpackHeader::EMPTY; 8];
        let result = handshake.feed(&input[..total], &mut output, &mut actions, &mut headers);

        assert_eq!(result.status, FeedStatus::ConnectionFailed);
        assert_eq!(result.error, Some(ErrorCode::ProtocolError));
        assert_protocol_goaway(&output[..result.output_used]);
        assert!(!handshake.is_complete());
    });
}

#[test]
fn non_empty_client_settings_applied_and_acknowledged() {
    with_handshake(|mut handshake| {
        send_server_settings(&mut handshake);
        let mut input = [0; 64];
        input[..CLIENT_CONNECTION_PREFACE.len()].copy_from_slice(CLIENT_CONNECTION_PREFACE);
        let entry = [0, SETTINGS_HEADER_TABLE_SIZE as u8, 0, 0, 0, 128];
        let settings_len = frame_into(
            &mut input[CLIENT_CONNECTION_PREFACE.len()..],
            FrameType::Settings,
            0,
            0,
            &entry,
        );
        let total = CLIENT_CONNECTION_PREFACE.len() + settings_len;
        let mut output = [0; 64];
        let mut actions = [None; 4];
        let mut headers = [HpackHeader::EMPTY; 8];
        let result = handshake.feed(&input[..total], &mut output, &mut actions, &mut headers);

        assert_eq!(result.error, None);
        assert_eq!(result.actions, 1);
        assert_eq!(
            handshake
                .server()
                .driver()
                .streams()
                .remote_settings()
                .header_table_size,
            128
        );
        assert!(handshake.is_complete());

        let action = actions[0].expect("settings ack action");
        assert!(matches!(action, H2FrameAction::SettingsAckRequired));
        let max_frame_size = handshake
            .server()
            .driver()
            .streams()
            .remote_settings()
            .max_frame_size;
        let mut ack = [0; FRAME_HEADER_LEN + PING_PAYLOAD_LEN];
        let mut writer = ControlFrameWriter::new(&mut ack, max_frame_size);
        let written = writer.queue_action(action, 0).unwrap();
        let header = decode_frame_header(&ack[..written], DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(header.frame_type, FrameType::Settings);
        assert_eq!(
            header.flags & crate::frame::SETTINGS_ACK,
            crate::frame::SETTINGS_ACK
        );
    });
}

#[test]
fn post_handshake_get_reaches_request_path() {
    with_handshake(|mut handshake| {
        send_server_settings(&mut handshake);
        let mut input = [0; 1_024];
        input[..CLIENT_CONNECTION_PREFACE.len()].copy_from_slice(CLIENT_CONNECTION_PREFACE);
        let settings_len = frame_into(
            &mut input[CLIENT_CONNECTION_PREFACE.len()..],
            FrameType::Settings,
            0,
            0,
            &[],
        );
        let mut block = [0; 256];
        let block_length = get_block(&mut block);
        let headers_len = frame_into(
            &mut input[CLIENT_CONNECTION_PREFACE.len() + settings_len..],
            FrameType::Headers,
            HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
            1,
            &block[..block_length],
        );
        let total = CLIENT_CONNECTION_PREFACE.len() + settings_len + headers_len;
        let mut output = [0; 64];
        let mut actions = [None; 4];
        let mut headers = [HpackHeader::EMPTY; 8];
        let result = handshake.feed(&input[..total], &mut output, &mut actions, &mut headers);

        assert_eq!(result.error, None);
        assert_eq!(result.actions, 2);
        assert!(handshake.is_complete());
        let range = match actions[1] {
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
        let server = handshake.into_server();
        let request = server
            .accept_request(1, &headers[range.start..range.start + range.count])
            .unwrap();
        assert_eq!(request.method, b"GET");
        assert_eq!(request.path, Some(&b"/"[..]));
    });
}

#[test]
fn handshake_rejects_client_role_and_double_settings() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block_storage = [0; 8];
    let mut streams = StreamTable::new(&mut slots, H2Role::Client, &mut header_block_storage, 8);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 1];
    let mut decoder_storage = [0; 64];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 1];
    let mut encoder_storage = [0; 64];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_storage, 4_096);
    let mut settings_ack_limits = [0; 1];
    assert!(matches!(
        H2cServerHandshake::new(
            &mut streams,
            &mut decoder,
            &mut encoder,
            &mut settings_ack_limits
        ),
        Err(H2cError::WrongRole)
    ));

    with_handshake(|mut handshake| {
        let mut output = [0; 64];
        handshake.server_settings(&mut output).unwrap();
        assert!(matches!(
            handshake.server_settings(&mut output),
            Err(H2cError::SettingsAlreadySent)
        ));
    });

    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block_storage = [0; 8];
    let mut server_streams =
        StreamTable::new(&mut slots, H2Role::Server, &mut header_block_storage, 8);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 1];
    let mut decoder_storage = [0; 64];
    let mut server_decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 1];
    let mut encoder_storage = [0; 64];
    let mut server_encoder = HpackTable::new(&mut encoder_entries, &mut encoder_storage, 4_096);
    let mut empty_limits: [u32; 0] = [];
    assert!(matches!(
        H2cServerHandshake::new(
            &mut server_streams,
            &mut server_decoder,
            &mut server_encoder,
            &mut empty_limits
        ),
        Err(H2cError::EmptySettingsAckStorage)
    ));

    with_handshake(|mut handshake| {
        let mut small = [0; 8];
        assert!(matches!(
            handshake.server_settings(&mut small),
            Err(H2cError::OutputTooSmall { .. })
        ));
    });
}
