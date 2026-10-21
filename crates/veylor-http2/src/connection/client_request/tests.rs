use super::*;
use crate::frame::{FrameHeader, FrameType};
use crate::hpack::{HpackEntrySlot, HpackFieldToEncode, HpackRepresentation, HpackTable};
use crate::stream::{FrameDirection, H2Role, StreamActionKind, StreamSlot, StreamTable};

fn with_client_driver(run: impl FnOnce(&mut H2FrameDriver<'_, '_, '_, '_>)) {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block_storage = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Client, &mut header_block_storage, 32);
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

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut decoder_storage = [0; 64];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut encoder_storage = [0; 256];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_storage, 4_096);
    let mut settings_ack_limits = [0; 2];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    run(&mut driver);
}

fn with_server_driver(run: impl FnOnce(&mut H2FrameDriver<'_, '_, '_, '_>)) {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_block_storage = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block_storage, 32);
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

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut decoder_storage = [0; 64];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut encoder_storage = [0; 64];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_storage, 4_096);
    let mut settings_ack_limits = [0; 2];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    run(&mut driver);
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

fn get_fields() -> [HpackFieldToEncode<'static>; 4] {
    [
        field(b":method", b"GET"),
        field(b":scheme", b"https"),
        field(b":authority", b"example.org"),
        field(b":path", b"/items%20here?q=1/next?again"),
    ]
}

#[test]
fn encodes_ordinary_requests_classic_connect_and_options_asterisk() {
    with_client_driver(|driver| {
        let mut output = [0; 256];
        let ordinary = get_fields();
        let written = encode_request_field_section(driver, &ordinary, &mut output).unwrap();
        assert!(written > 0);

        let connect = [
            field(b":method", b"CONNECT"),
            field(b":authority", b"example.org:443"),
        ];
        let written = encode_request_field_section(driver, &connect, &mut output).unwrap();
        assert!(written > 0);

        let ipv6_connect = [
            field(b":method", b"CONNECT"),
            field(b":authority", b"[2001:db8::1]:443"),
        ];
        let written = encode_request_field_section(driver, &ipv6_connect, &mut output).unwrap();
        assert!(written > 0);

        let options = [
            field(b":method", b"OPTIONS"),
            field(b":scheme", b"https"),
            field(b":path", b"*"),
        ];
        let written = encode_request_field_section(driver, &options, &mut output).unwrap();
        assert!(written > 0);

        let trailers_te = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":authority", b"example.org"),
            field(b":path", b"/"),
            field(b"te", b"Trailers , trailers"),
        ];
        let written = encode_request_field_section(driver, &trailers_te, &mut output).unwrap();
        assert!(written > 0);
    });
}

#[test]
fn rejects_missing_duplicate_out_of_order_and_wrong_context_pseudo_headers() {
    with_client_driver(|driver| {
        let mut output = [0xa5; 256];
        let missing_method = [field(b":scheme", b"https"), field(b":path", b"/")];
        assert_eq!(
            encode_request_field_section(driver, &missing_method, &mut output),
            Err(RequestFieldSectionError::MissingMethod)
        );

        let missing_scheme = [field(b":method", b"GET"), field(b":path", b"/")];
        assert_eq!(
            encode_request_field_section(driver, &missing_scheme, &mut output),
            Err(RequestFieldSectionError::MissingScheme)
        );

        let missing_path = [field(b":method", b"GET"), field(b":scheme", b"https")];
        assert_eq!(
            encode_request_field_section(driver, &missing_path, &mut output),
            Err(RequestFieldSectionError::MissingPath)
        );

        let missing_authority = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &missing_authority, &mut output),
            Err(RequestFieldSectionError::MissingAuthority)
        );

        let duplicate_method = [
            field(b":method", b"GET"),
            field(b":method", b"POST"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &duplicate_method, &mut output),
            Err(RequestFieldSectionError::DuplicatePseudoHeader)
        );

        let pseudo_after_regular = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b"accept", b"*/*"),
            field(b":path", b"/"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &pseudo_after_regular, &mut output),
            Err(RequestFieldSectionError::PseudoHeaderAfterRegular)
        );

        let invalid_method = [
            field(b":method", b"GET?"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &invalid_method, &mut output),
            Err(RequestFieldSectionError::InvalidMethod)
        );

        let response_pseudo = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
            field(b":status", b"200"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &response_pseudo, &mut output),
            Err(RequestFieldSectionError::UnsupportedPseudoHeader)
        );
        assert_eq!(output, [0xa5; 256]);
        assert_eq!(driver.hpack_encoder_mut().size(), 0);
    });
}

#[test]
fn validates_classic_connect_and_options_asterisk_forms() {
    with_client_driver(|driver| {
        let mut output = [0; 256];
        let missing_connect_authority = [field(b":method", b"CONNECT")];
        assert_eq!(
            encode_request_field_section(driver, &missing_connect_authority, &mut output),
            Err(RequestFieldSectionError::MissingAuthority)
        );

        let connect_with_scheme = [
            field(b":method", b"CONNECT"),
            field(b":authority", b"example.org:443"),
            field(b":scheme", b"https"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &connect_with_scheme, &mut output),
            Err(RequestFieldSectionError::InvalidConnectPseudoHeaders)
        );

        let connect_without_port = [
            field(b":method", b"CONNECT"),
            field(b":authority", b"example.org"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &connect_without_port, &mut output),
            Err(RequestFieldSectionError::InvalidAuthority)
        );
        let connect_overflow_port = [
            field(b":method", b"CONNECT"),
            field(b":authority", b"example.org:65536"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &connect_overflow_port, &mut output),
            Err(RequestFieldSectionError::InvalidAuthority)
        );

        let wrong_asterisk_method = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"*"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &wrong_asterisk_method, &mut output),
            Err(RequestFieldSectionError::OptionsAsteriskOnly)
        );
        let authority_with_asterisk = [
            field(b":method", b"OPTIONS"),
            field(b":scheme", b"https"),
            field(b":authority", b"example.org"),
            field(b":path", b"*"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &authority_with_asterisk, &mut output),
            Err(RequestFieldSectionError::AuthorityNotAllowed)
        );

        let invalid_scheme = [
            field(b":method", b"GET"),
            field(b":scheme", b"ht^tp"),
            field(b":path", b"/"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &invalid_scheme, &mut output),
            Err(RequestFieldSectionError::InvalidScheme)
        );

        let empty_http_path = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b""),
        ];
        assert_eq!(
            encode_request_field_section(driver, &empty_http_path, &mut output),
            Err(RequestFieldSectionError::InvalidPath)
        );

        let fragment_path = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/a#fragment"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &fragment_path, &mut output),
            Err(RequestFieldSectionError::InvalidPath)
        );

        let relative_path = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":authority", b"example.org"),
            field(b":path", b"items"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &relative_path, &mut output),
            Err(RequestFieldSectionError::InvalidPath)
        );

        let query_only_path = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":authority", b"example.org"),
            field(b":path", b"?q=1"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &query_only_path, &mut output),
            Err(RequestFieldSectionError::InvalidPath)
        );

        let whitespace_path = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":authority", b"example.org"),
            field(b":path", b"/a b"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &whitespace_path, &mut output),
            Err(RequestFieldSectionError::InvalidPath)
        );

        let invalid_escape_path = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":authority", b"example.org"),
            field(b":path", b"/%xx"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &invalid_escape_path, &mut output),
            Err(RequestFieldSectionError::InvalidPath)
        );

        let authority_userinfo = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":authority", b"user@example.org"),
            field(b":path", b"/"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &authority_userinfo, &mut output),
            Err(RequestFieldSectionError::InvalidAuthority)
        );

        for invalid_authority in [
            b"[not-ip]:443".as_slice(),
            b"2001:db8::1",
            b"example%.org",
            b"example.org:abc",
        ] {
            let fields = [
                field(b":method", b"GET"),
                field(b":scheme", b"http"),
                field(b":authority", invalid_authority),
                field(b":path", b"/"),
            ];
            assert_eq!(
                encode_request_field_section(driver, &fields, &mut output),
                Err(RequestFieldSectionError::InvalidAuthority)
            );
        }
    });
}

#[test]
fn validates_connection_fields_te_field_names_and_values() {
    with_client_driver(|driver| {
        let mut output = [0xa5; 256];
        let ordinary = get_fields();
        for name in [
            b"connection".as_slice(),
            b"proxy-connection",
            b"keep-alive",
            b"transfer-encoding",
            b"upgrade",
        ] {
            let fields = [
                ordinary[0],
                ordinary[1],
                ordinary[2],
                ordinary[3],
                field(name, b"x"),
            ];
            assert_eq!(
                encode_request_field_section(driver, &fields, &mut output),
                Err(RequestFieldSectionError::ConnectionSpecificField)
            );
        }

        let invalid_te = [
            ordinary[0],
            ordinary[1],
            ordinary[2],
            ordinary[3],
            field(b"te", b"trailers, gzip"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &invalid_te, &mut output),
            Err(RequestFieldSectionError::InvalidTe)
        );

        let uppercase_name = [
            ordinary[0],
            ordinary[1],
            ordinary[2],
            ordinary[3],
            field(b"Content-Type", b"text/plain"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &uppercase_name, &mut output),
            Err(RequestFieldSectionError::InvalidFieldName)
        );

        let colon_name = [
            ordinary[0],
            ordinary[1],
            ordinary[2],
            ordinary[3],
            field(b"x:name", b"value"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &colon_name, &mut output),
            Err(RequestFieldSectionError::InvalidFieldName)
        );

        let invalid_value = [
            ordinary[0],
            ordinary[1],
            ordinary[2],
            ordinary[3],
            field(b"x-check", b" bad"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &invalid_value, &mut output),
            Err(RequestFieldSectionError::InvalidFieldValue)
        );

        let host_field = [
            ordinary[0],
            ordinary[1],
            ordinary[2],
            ordinary[3],
            field(b"host", b"different.example"),
        ];
        assert_eq!(
            encode_request_field_section(driver, &host_field, &mut output),
            Err(RequestFieldSectionError::HostFieldUnsupported)
        );
        assert_eq!(output, [0xa5; 256]);
    });
}

#[test]
fn rejects_server_role_driver_before_encoding() {
    with_server_driver(|driver| {
        let mut output = [0xa5; 64];
        let fields = get_fields();

        assert_eq!(
            encode_request_field_section(driver, &fields, &mut output),
            Err(RequestFieldSectionError::WrongRole)
        );
        assert_eq!(output, [0xa5; 64]);
        assert_eq!(driver.hpack_encoder_mut().size(), 0);
    });
}

#[test]
fn reports_encoder_capacity_without_partial_output_or_table_changes() {
    with_client_driver(|driver| {
        let fields = [
            HpackFieldToEncode {
                name: b":method",
                value: b"GET",
                representation: HpackRepresentation::WithoutIndexing,
                huffman_name: false,
                huffman_value: false,
            },
            HpackFieldToEncode {
                name: b":scheme",
                value: b"https",
                representation: HpackRepresentation::WithoutIndexing,
                huffman_name: false,
                huffman_value: false,
            },
            HpackFieldToEncode {
                name: b":authority",
                value: b"example.org",
                representation: HpackRepresentation::WithoutIndexing,
                huffman_name: false,
                huffman_value: false,
            },
            HpackFieldToEncode {
                name: b":path",
                value: b"/",
                representation: HpackRepresentation::WithoutIndexing,
                huffman_name: false,
                huffman_value: false,
            },
        ];
        let mut output = [0xa5; 1];

        let result = encode_request_field_section(driver, &fields, &mut output);
        assert!(
            matches!(
                result,
                Err(RequestFieldSectionError::Hpack(
                    crate::hpack::HpackError::BufferTooSmall { .. }
                ))
            ),
            "{result:?}"
        );
        assert_eq!(output, [0xa5; 1]);
        assert_eq!(driver.hpack_encoder_mut().size(), 0);
    });
}
