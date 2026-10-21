use super::*;
use crate::frame::{FrameHeader, FrameType};
use crate::hpack::{HpackBytes, HpackEntrySlot, HpackField, HpackRepresentation, HpackTable};
use crate::stream::{FrameDirection, H2Role, StreamActionKind, StreamSlot, StreamTable};

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

#[test]
fn classifies_informational_and_final_sections_and_leaves_data_progress_to_caller() {
    with_client_driver(|driver| {
        let informational = [
            field(b":status", b"103"),
            field(b"link", b"</a>; rel=preload"),
        ];
        let mut response = ClientResponseHeaders::new();
        let section = response.classify(driver, &informational, false).unwrap();

        assert_eq!(section.kind, ResponseSectionKind::Informational);
        assert_eq!(section.status, Some(103));
        assert_eq!(section.fields.as_ptr(), informational.as_ptr());
        assert_eq!(response.phase(), ClientResponsePhase::AwaitingFinalHeaders);
        assert_eq!(response.final_status(), None);

        let continue_response = [field(b":status", b"100")];
        let section = response
            .classify(driver, &continue_response, false)
            .unwrap();
        assert_eq!(section.status, Some(100));
        assert_eq!(response.phase(), ClientResponsePhase::AwaitingFinalHeaders);

        let final_headers = [
            field(b":status", b"299"),
            field(b"content-type", b"text/plain"),
        ];
        let section = response.classify(driver, &final_headers, false).unwrap();
        assert_eq!(section.kind, ResponseSectionKind::Final);
        assert_eq!(section.status, Some(299));
        assert_eq!(response.final_status(), Some(299));
        assert_eq!(response.phase(), ClientResponsePhase::Content);

        assert_eq!(
            response.data_received(0, false),
            Ok(ClientResponsePhase::Content)
        );
        assert_eq!(
            response.data_received(2, true),
            Ok(ClientResponsePhase::Complete)
        );
        assert_eq!(
            response.classify(driver, &final_headers, true),
            Err(ResponseSectionError::UnexpectedHeaders)
        );
    });
}

#[test]
fn requires_single_ordered_status_and_rejects_invalid_statuses() {
    with_client_driver(|driver| {
        let mut response = ClientResponseHeaders::new();
        let missing_status = [field(b"content-type", b"text/plain")];
        assert_eq!(
            response.classify(driver, &missing_status, false),
            Err(ResponseSectionError::MissingStatus)
        );

        let out_of_order = [field(b"server", b"example"), field(b":status", b"200")];
        assert_eq!(
            response.classify(driver, &out_of_order, false),
            Err(ResponseSectionError::PseudoHeaderAfterRegular)
        );

        let duplicate_status = [field(b":status", b"200"), field(b":status", b"204")];
        assert_eq!(
            response.classify(driver, &duplicate_status, false),
            Err(ResponseSectionError::DuplicateStatus)
        );

        let request_pseudo = [field(b":method", b"GET"), field(b":status", b"200")];
        assert_eq!(
            response.classify(driver, &request_pseudo, false),
            Err(ResponseSectionError::UnexpectedPseudoHeader)
        );

        for status in [b"099".as_slice(), b"600", b"20", b"abc"] {
            let invalid = [field(b":status", status)];
            assert_eq!(
                response.classify(driver, &invalid, false),
                Err(ResponseSectionError::InvalidStatus)
            );
        }
        assert_eq!(response.phase(), ClientResponsePhase::AwaitingFinalHeaders);
        let unknown_status = [field(b":status", b"599")];
        assert_eq!(
            response
                .classify(driver, &unknown_status, false)
                .unwrap()
                .status,
            Some(599)
        );
        assert_eq!(response.phase(), ClientResponsePhase::Content);
    });
}

#[test]
fn enforces_http2_status_and_response_field_restrictions() {
    with_client_driver(|driver| {
        let mut response = ClientResponseHeaders::new();
        let switching_protocols = [field(b":status", b"101")];
        assert_eq!(
            response.classify(driver, &switching_protocols, false),
            Err(ResponseSectionError::SwitchingProtocols)
        );

        for name in [
            b"connection".as_slice(),
            b"proxy-connection",
            b"keep-alive",
            b"transfer-encoding",
            b"upgrade",
            b"te",
        ] {
            let invalid = [field(b":status", b"200"), field(name, b"x")];
            assert_eq!(
                response.classify(driver, &invalid, false),
                Err(ResponseSectionError::ProhibitedField)
            );
        }

        for status in [b"100".as_slice(), b"204"] {
            let invalid = [field(b":status", status), field(b"content-length", b"0")];
            assert_eq!(
                response.classify(driver, &invalid, false),
                Err(ResponseSectionError::ContentLengthNotAllowed)
            );
        }
        assert_eq!(response.phase(), ClientResponsePhase::AwaitingFinalHeaders);
    });
}

#[test]
fn classifies_final_trailers_and_requires_end_stream() {
    with_client_driver(|driver| {
        let mut response = ClientResponseHeaders::new();
        response
            .classify(driver, &[field(b":status", b"200")], false)
            .unwrap();

        let trailers = [
            field(b"content-digest", b"sha-256=:abc:"),
            field(b"date", b"Sun, 26 Oct 2026 12:00:00 GMT"),
            field(b"set-cookie", b"session=abc"),
        ];
        assert_eq!(
            response.classify(driver, &trailers, false),
            Err(ResponseSectionError::TrailerNotFinal)
        );
        assert_eq!(response.phase(), ClientResponsePhase::Content);

        let section = response.classify(driver, &trailers, true).unwrap();
        assert_eq!(section.kind, ResponseSectionKind::Trailers);
        assert_eq!(section.status, None);
        assert_eq!(section.fields.as_ptr(), trailers.as_ptr());
        assert_eq!(response.phase(), ClientResponsePhase::Complete);

        let pseudo_trailer = [field(b":status", b"200")];
        assert_eq!(
            response.classify(driver, &pseudo_trailer, true),
            Err(ResponseSectionError::UnexpectedHeaders)
        );
    });
}

#[test]
fn rejects_malformed_fields_and_forbidden_trailer_names() {
    with_client_driver(|driver| {
        let mut response = ClientResponseHeaders::new();
        let uppercase = [
            field(b":status", b"200"),
            field(b"Content-Type", b"text/plain"),
        ];
        assert_eq!(
            response.classify(driver, &uppercase, false),
            Err(ResponseSectionError::InvalidFieldName)
        );
        let control_value = [field(b":status", b"200"), field(b"x-test", b"bad\rvalue")];
        assert_eq!(
            response.classify(driver, &control_value, false),
            Err(ResponseSectionError::InvalidFieldValue)
        );

        response
            .classify(driver, &[field(b":status", b"200")], false)
            .unwrap();
        for name in [
            b"content-length".as_slice(),
            b"content-type",
            b"authorization",
            b"trailer",
        ] {
            let forbidden = [field(name, b"x")];
            assert_eq!(
                response.classify(driver, &forbidden, true),
                Err(ResponseSectionError::ProhibitedField)
            );
        }
        assert_eq!(response.phase(), ClientResponsePhase::Content);
    });
}

#[test]
fn data_events_require_a_final_response_and_reject_post_end_stream_data() {
    with_client_driver(|driver| {
        let mut response = ClientResponseHeaders::new();
        assert_eq!(
            response.data_received(1, false),
            Err(ResponseSectionError::DataBeforeFinalHeaders)
        );
        response
            .classify(driver, &[field(b":status", b"200")], true)
            .unwrap();
        assert_eq!(
            response.data_received(0, false),
            Err(ResponseSectionError::DataAfterComplete)
        );
    });
}

#[test]
fn rejects_content_for_204_and_304_but_accepts_empty_end_stream_data() {
    with_client_driver(|driver| {
        for status in [b"204".as_slice(), b"304"] {
            let mut response = ClientResponseHeaders::new();
            let headers = [field(b":status", status)];
            response.classify(driver, &headers, false).unwrap();

            assert_eq!(
                response.data_received(1, false),
                Err(ResponseSectionError::ContentNotAllowed)
            );
            assert_eq!(response.phase(), ClientResponsePhase::Content);
            assert_eq!(
                response.data_received(0, true),
                Ok(ClientResponsePhase::Complete)
            );
        }

        let mut not_modified = ClientResponseHeaders::new();
        let headers = [field(b":status", b"304"), field(b"content-length", b"17")];
        not_modified.classify(driver, &headers, false).unwrap();
        assert_eq!(
            not_modified.data_received(1, false),
            Err(ResponseSectionError::ContentNotAllowed)
        );
        assert_eq!(
            not_modified.data_received(0, true),
            Ok(ClientResponsePhase::Complete)
        );
    });
}
