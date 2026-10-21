use super::*;
use crate::frame::{FrameHeader, FrameType};
use crate::hpack::{HpackBytes, HpackEntrySlot, HpackField, HpackRepresentation, HpackTable};
use crate::stream::{FrameDirection, StreamActionKind, StreamSlot, StreamTable};

fn with_server_driver(run: impl FnOnce(&H2FrameDriver<'_, '_, '_, '_>)) {
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
    let driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    run(&driver);
}

fn with_client_driver(run: impl FnOnce(&H2FrameDriver<'_, '_, '_, '_>)) {
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
    let mut encoder_storage = [0; 64];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_storage, 4_096);
    let mut settings_ack_limits = [0; 2];
    let driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    run(&driver);
}

fn field(name: &'static [u8], value: &'static [u8]) -> HpackHeader {
    HpackHeader {
        field: HpackField {
            name: HpackBytes::Static(name),
            value: HpackBytes::Static(value),
        },
        representation: HpackRepresentation::WithoutIndexing,
    }
}

fn get_fields() -> [HpackHeader; 5] {
    [
        field(b":method", GET_METHOD),
        field(b":scheme", b"https"),
        field(b":path", b"/index?x=1"),
        field(b":authority", b"example.com"),
        field(b"content-length", b"17"),
    ]
}

static GET_METHOD: &[u8] = b"GET";

#[test]
fn valid_get_returns_zero_copy_views_and_content_length() {
    with_server_driver(|driver| {
        let fields = get_fields();
        let request = validate_server_request_fields(driver, &fields).unwrap();

        assert_eq!(request.method, b"GET");
        assert_eq!(request.scheme, Some(&b"https"[..]));
        assert_eq!(request.authority, Some(&b"example.com"[..]));
        assert_eq!(request.path, Some(&b"/index?x=1"[..]));
        assert_eq!(request.content_length, Some(17));
        assert!(std::ptr::eq(request.method, GET_METHOD));
    });
}

#[test]
fn connect_requires_authority_only() {
    with_server_driver(|driver| {
        let fields = [
            field(b":method", b"CONNECT"),
            field(b":authority", b"example.com:443"),
        ];
        let request = validate_server_request_fields(driver, &fields).unwrap();

        assert_eq!(request.method, b"CONNECT");
        assert_eq!(request.scheme, None);
        assert_eq!(request.authority, Some(&b"example.com:443"[..]));
        assert_eq!(request.path, None);

        let with_scheme = [
            field(b":method", b"CONNECT"),
            field(b":scheme", b"https"),
            field(b":authority", b"example.com:443"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &with_scheme),
            Err(ServerRequestError::InvalidConnectPseudoHeaders)
        );

        let missing_port = [field(b":method", b"CONNECT"), field(b":path", b"/")];
        assert_eq!(
            validate_server_request_fields(driver, &missing_port),
            Err(ServerRequestError::InvalidConnectPseudoHeaders)
        );

        let userinfo = [
            field(b":method", b"CONNECT"),
            field(b":authority", b"user@example.com:443"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &userinfo),
            Err(ServerRequestError::InvalidAuthority)
        );
    });
}

#[test]
fn options_asterisk_rejects_authority_and_other_methods() {
    with_server_driver(|driver| {
        let fields = [
            field(b":method", b"OPTIONS"),
            field(b":scheme", b"https"),
            field(b":path", b"*"),
        ];
        let request = validate_server_request_fields(driver, &fields).unwrap();

        assert_eq!(request.path, Some(&b"*"[..]));
        assert_eq!(request.authority, None);

        let with_authority = [
            field(b":method", b"OPTIONS"),
            field(b":scheme", b"https"),
            field(b":path", b"*"),
            field(b":authority", b"example.com"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &with_authority),
            Err(ServerRequestError::AuthorityNotAllowed)
        );

        let get_asterisk = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"*"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &get_asterisk),
            Err(ServerRequestError::OptionsAsteriskOnly)
        );
    });
}

#[test]
fn rejects_missing_duplicate_and_misplaced_pseudo_headers() {
    with_server_driver(|driver| {
        let missing_method = [field(b":scheme", b"https"), field(b":path", b"/")];
        assert_eq!(
            validate_server_request_fields(driver, &missing_method),
            Err(ServerRequestError::MissingMethod)
        );

        let missing_scheme = [field(b":method", b"GET"), field(b":path", b"/")];
        assert_eq!(
            validate_server_request_fields(driver, &missing_scheme),
            Err(ServerRequestError::MissingScheme)
        );

        let missing_path = [field(b":method", b"GET"), field(b":scheme", b"https")];
        assert_eq!(
            validate_server_request_fields(driver, &missing_path),
            Err(ServerRequestError::MissingPath)
        );

        let missing_authority = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &missing_authority),
            Err(ServerRequestError::MissingAuthority)
        );

        let duplicate_method = [
            field(b":method", b"GET"),
            field(b":method", b"POST"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &duplicate_method),
            Err(ServerRequestError::DuplicatePseudoHeader)
        );

        let pseudo_after_regular = [
            field(b":method", b"GET"),
            field(b"accept", b"text/html"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &pseudo_after_regular),
            Err(ServerRequestError::PseudoHeaderAfterRegular)
        );

        for name in [b":status".as_slice(), b":protocol", b":unknown"] {
            let unknown = [
                field(b":method", b"GET"),
                field(b":scheme", b"https"),
                field(b":path", b"/"),
                field(name, b"x"),
            ];
            assert_eq!(
                validate_server_request_fields(driver, &unknown),
                Err(ServerRequestError::UnsupportedPseudoHeader)
            );
        }
    });
}

#[test]
fn rejects_connection_specific_host_and_te_fields() {
    with_server_driver(|driver| {
        for name in [
            b"connection".as_slice(),
            b"proxy-connection",
            b"keep-alive",
            b"transfer-encoding",
            b"upgrade",
        ] {
            let fields = [
                field(b":method", b"GET"),
                field(b":scheme", b"https"),
                field(b":path", b"/"),
                field(b":authority", b"example.com"),
                field(name, b"close"),
            ];
            assert_eq!(
                validate_server_request_fields(driver, &fields),
                Err(ServerRequestError::ConnectionSpecificField)
            );
        }

        let host = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
            field(b"host", b"example.com"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &host),
            Err(ServerRequestError::HostFieldUnsupported)
        );

        let bad_te = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
            field(b"te", b"gzip"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &bad_te),
            Err(ServerRequestError::InvalidTe)
        );

        let trailers_te = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
            field(b":authority", b"example.com"),
            field(b"te", b"trailers"),
        ];
        validate_server_request_fields(driver, &trailers_te).unwrap();
    });
}

#[test]
fn rejects_malformed_and_mismatched_content_length() {
    with_server_driver(|driver| {
        for value in [b"".as_slice(), b"abc", b"12x", b"-1", b"1,2"] {
            let fields = [
                field(b":method", b"POST"),
                field(b":scheme", b"https"),
                field(b":path", b"/upload"),
                field(b":authority", b"example.com"),
                field(b"content-length", value),
            ];
            assert_eq!(
                validate_server_request_fields(driver, &fields),
                Err(ServerRequestError::InvalidContentLength)
            );
        }

        let overflow = [
            field(b":method", b"POST"),
            field(b":scheme", b"https"),
            field(b":path", b"/upload"),
            field(b":authority", b"example.com"),
            field(b"content-length", b"18446744073709551616"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &overflow),
            Err(ServerRequestError::InvalidContentLength)
        );

        let mismatched = [
            field(b":method", b"POST"),
            field(b":scheme", b"https"),
            field(b":path", b"/upload"),
            field(b":authority", b"example.com"),
            field(b"content-length", b"5"),
            field(b"content-length", b"6"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &mismatched),
            Err(ServerRequestError::InvalidContentLength)
        );

        let repeated = [
            field(b":method", b"POST"),
            field(b":scheme", b"https"),
            field(b":path", b"/upload"),
            field(b":authority", b"example.com"),
            field(b"content-length", b"5"),
            field(b"content-length", b"5"),
        ];
        let request = validate_server_request_fields(driver, &repeated).unwrap();

        assert_eq!(request.content_length, Some(5));
    });
}

#[test]
fn rejects_invalid_method_scheme_path_and_authority() {
    with_server_driver(|driver| {
        let bad_method = [
            field(b":method", b"GE T"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
            field(b":authority", b"example.com"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &bad_method),
            Err(ServerRequestError::InvalidMethod)
        );

        let bad_scheme = [
            field(b":method", b"GET"),
            field(b":scheme", b"1http"),
            field(b":path", b"/"),
            field(b":authority", b"example.com"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &bad_scheme),
            Err(ServerRequestError::InvalidScheme)
        );

        for path in [b"/frag#ment".as_slice(), b"/bad%zz", b"relative", b""] {
            let fields = [
                field(b":method", b"GET"),
                field(b":scheme", b"https"),
                field(b":path", path),
                field(b":authority", b"example.com"),
            ];
            assert_eq!(
                validate_server_request_fields(driver, &fields),
                Err(ServerRequestError::InvalidPath)
            );
        }

        let bad_authority = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
            field(b":authority", b"exa mple.com"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &bad_authority),
            Err(ServerRequestError::InvalidAuthority)
        );

        let uppercase_name = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
            field(b":authority", b"example.com"),
            field(b"X-Custom", b"value"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &uppercase_name),
            Err(ServerRequestError::InvalidFieldName)
        );

        let control_value = [
            field(b":method", b"GET"),
            field(b":scheme", b"https"),
            field(b":path", b"/"),
            field(b":authority", b"example.com"),
            field(b"x-custom", b"bad\rvalue"),
        ];
        assert_eq!(
            validate_server_request_fields(driver, &control_value),
            Err(ServerRequestError::InvalidFieldValue)
        );
    });
}

#[test]
fn refuses_client_role_drivers() {
    with_client_driver(|driver| {
        let fields = get_fields();

        assert_eq!(
            validate_server_request_fields(driver, &fields),
            Err(ServerRequestError::WrongRole)
        );
    });
}
