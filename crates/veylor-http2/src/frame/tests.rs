use super::{
    DEFAULT_MAX_FRAME_SIZE, FRAME_HEADER_LEN, FrameHeader, FrameHeaderError, FrameType,
    MAX_FRAME_SIZE, MAX_STREAM_ID, MIN_MAX_FRAME_SIZE, decode_frame_header, encode_frame_header,
};

#[test]
fn decodes_and_encodes_golden_frame_header() {
    let bytes = [0, 0, 0, 0, 0x1, 0, 0, 0, 1];
    let expected = FrameHeader {
        payload_length: 0,
        frame_type: FrameType::Data,
        flags: 1,
        stream_id: 1,
    };

    assert_eq!(
        decode_frame_header(&bytes, DEFAULT_MAX_FRAME_SIZE),
        Ok(expected)
    );

    let mut output = [0; FRAME_HEADER_LEN];
    encode_frame_header(expected, &mut output, DEFAULT_MAX_FRAME_SIZE).unwrap();
    assert_eq!(output, bytes);
}

#[test]
fn preserves_unknown_frame_types_and_reads_only_the_header() {
    let bytes = [0, 0, 5, 0xff, 0xa5, 0, 0, 0, 7, 9, 9, 9];
    let header = decode_frame_header(&bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();

    assert_eq!(header.payload_length, 5);
    assert_eq!(header.frame_type, FrameType::Unknown(0xff));
    assert_eq!(header.flags, 0xa5);
    assert_eq!(header.stream_id, 7);

    let mut output = [0; FRAME_HEADER_LEN];
    encode_frame_header(header, &mut output, DEFAULT_MAX_FRAME_SIZE).unwrap();
    assert_eq!(output, bytes[..FRAME_HEADER_LEN]);
}

#[test]
fn rejects_unknown_frame_type_values_that_name_registered_types() {
    let header = FrameHeader {
        payload_length: 0,
        frame_type: FrameType::Unknown(1),
        flags: 0,
        stream_id: 0,
    };
    let mut output = [0xa5; FRAME_HEADER_LEN];
    assert_eq!(
        encode_frame_header(header, &mut output, DEFAULT_MAX_FRAME_SIZE),
        Err(super::FrameHeaderError::InvalidFrameType)
    );
    assert_eq!(output, [0xa5; FRAME_HEADER_LEN]);
}

#[test]
fn rejects_truncated_headers_and_short_output() {
    let bytes = [0; FRAME_HEADER_LEN];
    for length in 0..FRAME_HEADER_LEN {
        assert_eq!(
            decode_frame_header(&bytes[..length], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameHeaderError::InputTooShort)
        );
    }

    let header = FrameHeader {
        payload_length: 0,
        frame_type: FrameType::Settings,
        flags: 0,
        stream_id: 0,
    };
    let mut output = [0xa5; FRAME_HEADER_LEN];
    for length in 0..FRAME_HEADER_LEN {
        assert_eq!(
            encode_frame_header(header, &mut output[..length], DEFAULT_MAX_FRAME_SIZE),
            Err(FrameHeaderError::OutputTooShort)
        );
        assert_eq!(output, [0xa5; FRAME_HEADER_LEN]);
    }
}

#[test]
fn ignores_the_reserved_stream_bit_when_decoding() {
    let bytes = [0, 0, 0, 6, 0, 0x80, 0, 0, 1];
    let header = decode_frame_header(&bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();

    assert_eq!(header.stream_id, 1);
}

#[test]
fn enforces_configured_frame_size_and_stream_id_bounds() {
    let header = FrameHeader {
        payload_length: MIN_MAX_FRAME_SIZE + 1,
        frame_type: FrameType::Data,
        flags: 0,
        stream_id: MAX_STREAM_ID,
    };
    let mut output = [0xa5; FRAME_HEADER_LEN];
    assert_eq!(
        encode_frame_header(header, &mut output, MIN_MAX_FRAME_SIZE),
        Err(FrameHeaderError::FrameTooLarge)
    );
    assert_eq!(output, [0xa5; FRAME_HEADER_LEN]);

    let bytes = [0, 0x40, 1, 0, 0, 0, 0, 0, 0];
    assert_eq!(
        decode_frame_header(&bytes, MIN_MAX_FRAME_SIZE),
        Err(FrameHeaderError::FrameTooLarge)
    );
    assert_eq!(
        decode_frame_header(&bytes, MAX_FRAME_SIZE),
        Ok(FrameHeader {
            payload_length: MIN_MAX_FRAME_SIZE + 1,
            frame_type: FrameType::Data,
            flags: 0,
            stream_id: 0,
        })
    );

    let reserved_stream = FrameHeader {
        payload_length: 0,
        frame_type: FrameType::Data,
        flags: 0,
        stream_id: MAX_STREAM_ID + 1,
    };
    assert_eq!(
        encode_frame_header(reserved_stream, &mut output, MAX_FRAME_SIZE),
        Err(FrameHeaderError::StreamIdOutOfRange)
    );
    assert_eq!(output, [0xa5; FRAME_HEADER_LEN]);
}

#[test]
fn rejects_invalid_frame_size_limits() {
    let bytes = [0; FRAME_HEADER_LEN];
    let mut output = [0; FRAME_HEADER_LEN];
    for max_frame_size in [0, MIN_MAX_FRAME_SIZE - 1, MAX_FRAME_SIZE + 1] {
        assert_eq!(
            decode_frame_header(&bytes, max_frame_size),
            Err(FrameHeaderError::InvalidMaxFrameSize)
        );
        assert_eq!(
            encode_frame_header(
                FrameHeader {
                    payload_length: 0,
                    frame_type: FrameType::Ping,
                    flags: 0,
                    stream_id: 0,
                },
                &mut output,
                max_frame_size,
            ),
            Err(FrameHeaderError::InvalidMaxFrameSize)
        );
    }
}
