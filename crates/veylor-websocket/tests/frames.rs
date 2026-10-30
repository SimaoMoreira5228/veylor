use veylor_websocket::{FrameError, OpCode, encode_frame, encode_frame_header, parse_frame_header};

const MASK: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];
const MASKED_HELLO: &[u8] = &[
    0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58,
];

#[test]
fn parses_unmasked_text_header() {
    let header = parse_frame_header(&[0x81, 0x05, b'H']).unwrap().unwrap();
    assert!(header.fin);
    assert_eq!(header.opcode, OpCode::Text);
    assert_eq!(header.mask, None);
    assert_eq!(header.payload_len, 5);
    assert_eq!(header.header_len, 2);
}

#[test]
fn parses_masked_text_header() {
    let header = parse_frame_header(MASKED_HELLO).unwrap().unwrap();
    assert!(header.fin);
    assert_eq!(header.opcode, OpCode::Text);
    assert_eq!(header.mask, Some(MASK));
    assert_eq!(header.payload_len, 5);
    assert_eq!(header.header_len, 6);
}

#[test]
fn encodes_server_and_client_frames() {
    let mut out = [0u8; 32];
    let written = encode_frame(true, OpCode::Text, None, b"Hello", &mut out).unwrap();
    assert_eq!(&out[..written], &[0x81, 0x05, b'H', b'e', b'l', b'l', b'o']);

    let written = encode_frame(true, OpCode::Text, Some(MASK), b"Hello", &mut out).unwrap();
    assert_eq!(&out[..written], MASKED_HELLO);
}

#[test]
fn header_lengths_use_minimal_encoding() {
    let mut out = [0u8; 16];
    let written = encode_frame_header(false, OpCode::Binary, None, 126, &mut out).unwrap();
    assert_eq!(&out[..written], &[0x02, 126, 0x00, 126]);

    let written = encode_frame_header(true, OpCode::Binary, None, 65_536, &mut out).unwrap();
    assert_eq!(&out[..written], &[0x82, 127, 0, 0, 0, 0, 0, 1, 0, 0]);
}

#[test]
fn rejects_non_minimal_and_high_bit_lengths() {
    assert_eq!(
        parse_frame_header(&[0x81, 126, 0x00, 125]).unwrap_err(),
        FrameError::NonMinimalLength
    );
    assert_eq!(
        parse_frame_header(&[0x81, 127, 0, 0, 0, 0, 0, 0, 0xff, 0xff]).unwrap_err(),
        FrameError::NonMinimalLength
    );
    assert_eq!(
        parse_frame_header(&[0x81, 127, 0x80, 0, 0, 0, 0, 0, 0, 0]).unwrap_err(),
        FrameError::LengthHighBit
    );
}

#[test]
fn rejects_reserved_bits_opcodes_and_bad_control_frames() {
    assert_eq!(
        parse_frame_header(&[0xc1, 0]).unwrap_err(),
        FrameError::ReservedBits
    );
    assert_eq!(
        parse_frame_header(&[0x83, 0]).unwrap_err(),
        FrameError::ReservedOpcode(3)
    );
    assert_eq!(
        parse_frame_header(&[0x89, 126, 0x00, 126]).unwrap_err(),
        FrameError::ControlTooLong
    );
    assert_eq!(
        parse_frame_header(&[0x09, 0]).unwrap_err(),
        FrameError::FragmentedControl
    );
}

#[test]
fn incomplete_input_returns_none() {
    assert_eq!(parse_frame_header(&[]).unwrap(), None);
    assert_eq!(parse_frame_header(&[0x81]).unwrap(), None);
    assert_eq!(parse_frame_header(&[0x81, 126, 0]).unwrap(), None);
    assert_eq!(parse_frame_header(&[0x81, 0x85, 1, 2, 3]).unwrap(), None);
}

#[test]
fn encode_reports_required_capacity() {
    let mut out = [0u8; 4];
    let error = encode_frame(true, OpCode::Text, None, b"hello", &mut out).unwrap_err();
    assert!(matches!(
        error,
        veylor_websocket::EncodeError::BufferTooSmall { required } if required == 7
    ));
}
