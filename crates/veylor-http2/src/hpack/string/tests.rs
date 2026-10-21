use super::{DecodedHpackString, HpackStringError, decode_hpack_string, encode_hpack_string};

#[test]
fn encodes_rfc_huffman_string_literal_vector() {
    let expected = [
        0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
    ];
    let mut output = [0; 32];

    let written = encode_hpack_string(b"www.example.com", true, 7, &mut output).unwrap();
    assert_eq!(&output[..written], &expected);
}

#[test]
fn encodes_and_decodes_raw_strings_without_copying() {
    let mut output = [0; 16];
    let written = encode_hpack_string(b"abc", false, 5, &mut output).unwrap();
    assert_eq!(&output[..written], &[3, b'a', b'b', b'c']);

    let input = [3, b'a', b'b', b'c', 0xaa];
    let (decoded, consumed) = decode_hpack_string(&input, 7, &mut []).unwrap();
    let DecodedHpackString::Raw(raw) = decoded else {
        panic!("raw encoding was decoded as Huffman");
    };
    assert_eq!(raw, b"abc");
    assert_eq!(raw.as_ptr(), input[1..].as_ptr());
    assert_eq!(consumed, 4);
}

#[test]
fn decodes_huffman_string_into_caller_scratch() {
    let input = [
        0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff, 0xaa,
    ];
    let mut output = [0; 32];
    let (decoded, consumed) = decode_hpack_string(&input, 7, &mut output).unwrap();
    let DecodedHpackString::Huffman(value) = decoded else {
        panic!("Huffman encoding was decoded as raw");
    };

    assert_eq!(value, b"www.example.com");
    assert_eq!(consumed, 13);
}

#[test]
fn supports_empty_raw_and_huffman_literals() {
    let mut output = [0; 4];
    for huffman in [false, true] {
        let written = encode_hpack_string(&[], huffman, 7, &mut output).unwrap();
        assert_eq!(written, 1);
        assert_eq!(output[0], if huffman { 0x80 } else { 0 });
        let (decoded, consumed) = decode_hpack_string(&output[..written], 7, &mut []).unwrap();
        assert_eq!(consumed, 1);
        match decoded {
            DecodedHpackString::Raw(value) | DecodedHpackString::Huffman(value) => {
                assert!(value.is_empty());
            }
        }
    }
}

#[test]
fn rejects_bad_prefix_bits_and_truncated_lengths_or_payloads() {
    let mut output = [0; 16];
    for prefix_bits in [0, 8, u8::MAX] {
        assert_eq!(
            decode_hpack_string(&[1], prefix_bits, &mut output),
            Err(HpackStringError::InvalidPrefixWidth)
        );
        assert_eq!(
            encode_hpack_string(b"a", false, prefix_bits, &mut output),
            Err(HpackStringError::InvalidPrefixWidth)
        );
    }
    assert_eq!(
        decode_hpack_string(&[0x40, 0], 5, &mut output),
        Err(HpackStringError::InvalidPrefixBits)
    );

    let encoded = [
        0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
    ];
    for end in 0..encoded.len() {
        assert!(decode_hpack_string(&encoded[..end], 7, &mut output).is_err());
    }
    assert_eq!(
        decode_hpack_string(&[0x7f, 0x80], 7, &mut output),
        Err(HpackStringError::InputTooShort)
    );
    assert_eq!(
        decode_hpack_string(
            &[
                0x7f, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02
            ],
            7,
            &mut output,
        ),
        Err(HpackStringError::LengthTooLarge)
    );
}

#[test]
fn reports_string_output_capacity_without_modifying_buffers() {
    let mut encoded = [0xa5; 12];
    assert_eq!(
        encode_hpack_string(b"www.example.com", true, 7, &mut encoded),
        Err(HpackStringError::OutputTooSmall { required: 13 })
    );
    assert_eq!(encoded, [0xa5; 12]);

    let input = [
        0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
    ];
    let mut decoded = [0xa5; 3];
    assert_eq!(
        decode_hpack_string(&input, 7, &mut decoded),
        Err(HpackStringError::OutputTooSmall { required: 15 })
    );
    assert_eq!(decoded, [0xa5; 3]);
}

#[test]
fn rejects_eos_in_huffman_string_literal() {
    let input = [0x84, 0xff, 0xff, 0xff, 0xfc];
    let mut output = [0xa5; 16];
    assert_eq!(
        decode_hpack_string(&input, 7, &mut output),
        Err(HpackStringError::Huffman(
            super::huffman::HuffmanError::EndOfStreamSymbol
        ))
    );
    assert_eq!(output, [0xa5; 16]);
}
