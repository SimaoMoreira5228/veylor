use super::*;

fn header(frame_type: FrameType, stream_id: u32, flags: u8, length: usize) -> FrameHeader {
    FrameHeader {
        payload_length: u32::try_from(length).unwrap(),
        frame_type,
        flags,
        stream_id,
    }
}

#[test]
fn round_trips_data_with_optional_padding() {
    let data = b"hi";
    for padding_length in [None, Some(0), Some(3), Some(u8::MAX)] {
        let mut output = [0; 260];
        let written = encode_data_payload(data, padding_length, &mut output).unwrap();
        let flags = if padding_length.is_some() {
            DATA_FLAG_PADDED
        } else {
            0
        };
        let decoded = decode_data_payload(
            header(FrameType::Data, 3, flags, written),
            &output[..written],
        )
        .unwrap();

        assert_eq!(decoded.data, data);
        assert_eq!(decoded.padding_length, padding_length);
        assert!(output[written..].iter().all(|byte| *byte == 0));
    }
}

#[test]
fn rejects_bad_data_padding_and_stream_ids() {
    let payload = [3, b'a', b'b'];
    assert_eq!(
        decode_data_payload(
            header(FrameType::Data, 1, DATA_FLAG_PADDED, payload.len()),
            &payload
        ),
        Err(FramePayloadError::InvalidPadding)
    );
    assert_eq!(
        decode_data_payload(header(FrameType::Data, 0, 0, 1), b"a"),
        Err(FramePayloadError::InvalidStreamId)
    );
    assert_eq!(
        decode_data_payload(header(FrameType::Headers, 1, 0, 1), b"a"),
        Err(FramePayloadError::WrongFrameType)
    );
}

#[test]
fn round_trips_headers_priority_and_padding() {
    let fragment = [0x88, 0x61];
    let priority = HeadersPriority {
        exclusive: true,
        dependency: 1,
        weight: 15,
    };
    let mut output = [0; 16];
    let written = encode_headers_payload(&fragment, Some(priority), Some(2), &mut output).unwrap();
    let payload = &output[..written];
    let decoded = decode_headers_payload(
        header(
            FrameType::Headers,
            3,
            HEADERS_FLAG_PRIORITY | HEADERS_FLAG_PADDED,
            written,
        ),
        payload,
    )
    .unwrap();

    assert_eq!(decoded.fragment, fragment);
    assert_eq!(decoded.priority, Some(priority));
    assert_eq!(decoded.padding_length, Some(2));
    assert_eq!(payload[0], 2);
    assert_eq!(&payload[1..6], &[0x80, 0, 0, 1, 15]);
    assert_eq!(&payload[8..], &[0, 0]);
}

#[test]
fn validates_headers_padding_priority_and_stream() {
    let too_short_priority = [0, 0, 0];
    assert_eq!(
        decode_headers_payload(
            header(
                FrameType::Headers,
                1,
                HEADERS_FLAG_PRIORITY,
                too_short_priority.len(),
            ),
            &too_short_priority,
        ),
        Err(FramePayloadError::InvalidPayloadLength)
    );

    let bad_padding = [4, 0x88, 0x61];
    assert_eq!(
        decode_headers_payload(
            header(
                FrameType::Headers,
                1,
                HEADERS_FLAG_PADDED,
                bad_padding.len()
            ),
            &bad_padding,
        ),
        Err(FramePayloadError::InvalidPadding)
    );
    assert_eq!(
        decode_headers_payload(header(FrameType::Headers, 0, 0, 0), &[]),
        Err(FramePayloadError::InvalidStreamId)
    );
}

#[test]
fn priority_fields_round_trip_and_validate_dependencies() {
    let priority = HeadersPriority {
        exclusive: true,
        dependency: MAX_STREAM_ID,
        weight: u8::MAX,
    };
    let mut output = [0; PRIORITY_PAYLOAD_LEN];
    encode_priority_fields(priority, &mut output).unwrap();
    assert_eq!(decode_priority_fields(&output), Ok(priority));

    let invalid = HeadersPriority {
        dependency: MAX_STREAM_ID + 1,
        ..priority
    };
    assert_eq!(
        encode_priority_fields(invalid, &mut output),
        Err(FramePayloadError::InvalidStreamId)
    );
    assert_eq!(
        decode_priority_payload(
            header(FrameType::Priority, 1, 0, PRIORITY_PAYLOAD_LEN),
            &output,
        ),
        Ok(priority)
    );
}

#[test]
fn rst_stream_preserves_known_and_unknown_error_codes() {
    let mut output = [0; RST_STREAM_PAYLOAD_LEN];
    for raw in 0..=13 {
        let code = ErrorCode::from(raw);
        encode_rst_stream_payload(code, &mut output).unwrap();
        assert_eq!(
            decode_rst_stream_payload(header(FrameType::RstStream, 1, 0, output.len()), &output,),
            Ok(code)
        );
    }

    let unknown = ErrorCode::Unknown(0xdead_beef);
    encode_rst_stream_payload(unknown, &mut output).unwrap();
    assert_eq!(
        decode_rst_stream_payload(header(FrameType::RstStream, 1, 0, output.len()), &output),
        Ok(unknown)
    );
    assert_eq!(
        decode_rst_stream_payload(header(FrameType::RstStream, 0, 0, output.len()), &output),
        Err(FramePayloadError::InvalidStreamId)
    );
}

#[test]
fn rejects_unknown_error_code_values_that_are_registered() {
    let mut output = [0xa5; RST_STREAM_PAYLOAD_LEN];
    assert_eq!(
        encode_rst_stream_payload(ErrorCode::Unknown(3), &mut output),
        Err(FramePayloadError::InvalidErrorCode)
    );
    assert_eq!(output, [0xa5; RST_STREAM_PAYLOAD_LEN]);
}

#[test]
fn settings_round_trip_borrowed_storage_and_ack() {
    let parameters = [
        SettingsParameter {
            id: SETTINGS_INITIAL_WINDOW_SIZE,
            value: 65_535,
        },
        SettingsParameter {
            id: 0xfafa,
            value: u32::MAX,
        },
    ];
    let mut output = [0; SETTINGS_ENTRY_LEN * 2];
    let written = encode_settings_payload(&parameters, &mut output).unwrap();
    let mut decoded = [SettingsParameter { id: 0, value: 0 }; 2];
    let count = decode_settings_payload(
        header(FrameType::Settings, 0, 0, written),
        &output,
        &mut decoded,
    )
    .unwrap();

    assert_eq!(count, parameters.len());
    assert_eq!(decoded, parameters);
    assert_eq!(
        decode_settings_payload(
            header(FrameType::Settings, 0, SETTINGS_ACK, 0),
            &[],
            &mut []
        ),
        Ok(0)
    );
}

#[test]
fn validates_settings_bounds_and_does_not_partially_write() {
    for (id, value) in [
        (SETTINGS_ENABLE_PUSH, 2),
        (SETTINGS_INITIAL_WINDOW_SIZE, u32::MAX),
        (SETTINGS_MAX_FRAME_SIZE, SETTINGS_MAX_FRAME_SIZE_MIN - 1),
        (SETTINGS_MAX_FRAME_SIZE, SETTINGS_MAX_FRAME_SIZE_MAX + 1),
    ] {
        assert!(!settings_parameter_valid(id, value));
        let parameter = [SettingsParameter { id, value }];
        let mut output = [0xa5; SETTINGS_ENTRY_LEN];
        assert_eq!(
            encode_settings_payload(&parameter, &mut output),
            Err(FramePayloadError::InvalidSetting)
        );
        assert_eq!(output, [0xa5; SETTINGS_ENTRY_LEN]);
    }

    let payload = [0, 4, 0, 0, 0, 1, 0, 2, 0, 0, 0, 2];
    let mut small_output = [SettingsParameter { id: 99, value: 99 }; 1];
    assert_eq!(
        decode_settings_payload(
            header(FrameType::Settings, 0, 0, payload.len()),
            &payload,
            &mut small_output,
        ),
        Err(FramePayloadError::BufferTooSmall { required: 2 })
    );
    assert_eq!(small_output[0].id, 99);

    let mut enough_output = [SettingsParameter { id: 99, value: 99 }; 2];
    assert_eq!(
        decode_settings_payload(
            header(FrameType::Settings, 0, 0, payload.len()),
            &payload,
            &mut enough_output,
        ),
        Err(FramePayloadError::InvalidSetting)
    );
    assert_eq!(enough_output[0].id, 99);
}

#[test]
fn settings_reject_bad_frame_length_ack_payload_and_stream() {
    let mut output = [SettingsParameter { id: 0, value: 0 }; 1];
    assert_eq!(
        decode_settings_payload(header(FrameType::Settings, 0, 0, 1), &[0], &mut output),
        Err(FramePayloadError::InvalidPayloadLength)
    );
    assert_eq!(
        decode_settings_payload(header(FrameType::Settings, 1, 0, 0), &[], &mut output,),
        Err(FramePayloadError::InvalidStreamId)
    );
    assert_eq!(
        decode_settings_payload(
            header(FrameType::Settings, 0, SETTINGS_ACK, 1),
            &[0],
            &mut output
        ),
        Err(FramePayloadError::InvalidPayloadLength)
    );
}

#[test]
fn ping_requires_connection_scope_and_eight_bytes() {
    let opaque = [1, 2, 3, 4, 5, 6, 7, 8];
    let mut output = [0; PING_PAYLOAD_LEN];
    encode_ping_payload(opaque, &mut output).unwrap();
    assert_eq!(
        decode_ping_payload(
            header(FrameType::Ping, 0, PING_FLAG_ACK, output.len()),
            &output
        ),
        Ok(opaque)
    );
    assert_eq!(
        decode_ping_payload(header(FrameType::Ping, 1, 0, output.len()), &output),
        Err(FramePayloadError::InvalidStreamId)
    );
    assert_eq!(
        decode_ping_payload(header(FrameType::Ping, 0, 0, 7), &output[..7]),
        Err(FramePayloadError::InvalidPayloadLength)
    );
}

#[test]
fn goaway_borrows_debug_bytes_and_masks_reserved_bit() {
    let payload = [0x80, 0, 0, 2, 0, 0, 0, 11, b'o', b'k'];
    let decoded =
        decode_goaway_payload(header(FrameType::GoAway, 0, 0, payload.len()), &payload).unwrap();
    assert_eq!(decoded.last_stream_id, 2);
    assert_eq!(decoded.error_code, ErrorCode::EnhanceYourCalm);
    assert_eq!(decoded.debug_data, b"ok");

    let mut output = [0; 10];
    let written =
        encode_goaway_payload(2, decoded.error_code, decoded.debug_data, &mut output).unwrap();
    assert_eq!(&output[..written], &[0, 0, 0, 2, 0, 0, 0, 11, b'o', b'k']);
    assert_eq!(
        decode_goaway_payload(header(FrameType::GoAway, 1, 0, payload.len()), &payload),
        Err(FramePayloadError::InvalidStreamId)
    );
    assert_eq!(
        decode_goaway_payload(header(FrameType::GoAway, 0, 0, 7), &payload[..7]),
        Err(FramePayloadError::InvalidPayloadLength)
    );
}

#[test]
fn window_update_masks_reserved_bit_and_rejects_zero() {
    let payload = [0x80, 0, 0, 1];
    for stream_id in [0, 7] {
        assert_eq!(
            decode_window_update_payload(
                header(FrameType::WindowUpdate, stream_id, 0, payload.len()),
                &payload,
            ),
            Ok(WindowUpdatePayload { increment: 1 })
        );
    }

    let mut output = [0; WINDOW_UPDATE_PAYLOAD_LEN];
    encode_window_update_payload(MAX_WINDOW_INCREMENT, &mut output).unwrap();
    assert_eq!(output[0] & 0x80, 0);
    assert_eq!(
        encode_window_update_payload(0, &mut output),
        Err(FramePayloadError::InvalidWindowIncrement)
    );
    assert_eq!(
        decode_window_update_payload(header(FrameType::WindowUpdate, 0, 0, 4), &[0x80, 0, 0, 0],),
        Err(FramePayloadError::InvalidWindowIncrement)
    );
}

#[test]
fn push_promise_round_trips_padding_and_ignores_reserved_stream_bit() {
    let fragment = [0x82, 0x86];
    let mut output = [0; 16];
    let written = encode_push_promise_payload(5, &fragment, Some(2), &mut output).unwrap();
    assert_eq!(output[0], 2);
    assert_eq!(u32::from(output[1] & 0x7f), 0);
    assert_eq!(&output[5..7], &fragment);
    assert_eq!(&output[7..written], &[0, 0]);

    let mut reserved = [0; 16];
    reserved[..written].copy_from_slice(&output[..written]);
    reserved[1] |= 0x80;
    let decoded = decode_push_promise_payload(
        header(
            FrameType::PushPromise,
            3,
            PUSH_PROMISE_FLAG_PADDED | PUSH_PROMISE_FLAG_END_HEADERS,
            written,
        ),
        &reserved[..written],
    )
    .unwrap();
    assert_eq!(decoded.promised_stream_id, 5);
    assert_eq!(decoded.fragment, fragment);
    assert_eq!(decoded.padding_length, Some(2));

    assert_eq!(
        encode_push_promise_payload(0, &fragment, None, &mut output),
        Err(FramePayloadError::InvalidStreamId)
    );
}

#[test]
fn continuation_accepts_empty_fragments_and_requires_nonzero_stream() {
    let mut output = [];
    assert_eq!(encode_continuation_payload(&[], &mut output), Ok(0));
    assert_eq!(
        decode_continuation_payload(
            header(FrameType::Continuation, 1, CONTINUATION_FLAG_END_HEADERS, 0),
            &[],
        ),
        Ok(&[][..])
    );
    assert_eq!(
        decode_continuation_payload(header(FrameType::Continuation, 0, 0, 0), &[]),
        Err(FramePayloadError::InvalidStreamId)
    );
}
