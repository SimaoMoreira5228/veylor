use super::{HpackIntegerError, MAX_HPACK_INTEGER, decode_hpack_integer, encode_hpack_integer};

#[test]
fn matches_rfc_7541_appendix_c_integer_vectors() {
    let mut output = [0; 16];
    let written = encode_hpack_integer(10, 5, &mut output).unwrap();
    assert_eq!(&output[..written], &[10]);
    assert_eq!(decode_hpack_integer(&output[..written], 5), Ok((10, 1)));

    let written = encode_hpack_integer(1_337, 5, &mut output).unwrap();
    assert_eq!(&output[..written], &[31, 154, 10]);
    assert_eq!(decode_hpack_integer(&output[..written], 5), Ok((1_337, 3)));

    let written = encode_hpack_integer(42, 8, &mut output).unwrap();
    assert_eq!(&output[..written], &[42]);
    assert_eq!(decode_hpack_integer(&output[..written], 8), Ok((42, 1)));
}

#[test]
fn round_trips_prefix_edges_and_u64_boundaries() {
    let wide_values = [
        127,
        128,
        255,
        256,
        16_383,
        16_384,
        u64::from(u32::MAX),
        u64::MAX,
    ];

    for prefix_bits in 1..=8 {
        let prefix_max = (1_u64 << prefix_bits) - 1;
        for value in [0, prefix_max - 1, prefix_max, prefix_max + 1]
            .into_iter()
            .chain(wide_values)
        {
            round_trip(value, prefix_bits);
        }
    }
}

#[test]
fn ignores_bits_outside_the_integer_prefix_and_reports_consumption() {
    let input = [0b1110_1010, 0x01, 0xff];
    assert_eq!(decode_hpack_integer(&input, 5), Ok((10, 1)));

    let input = [0xff, 0x9a, 0x0a, 0x55];
    assert_eq!(decode_hpack_integer(&input, 5), Ok((1_337, 3)));
}

#[test]
fn rejects_truncated_prefix_and_continuation_sequences() {
    assert_eq!(
        decode_hpack_integer(&[], 5),
        Err(HpackIntegerError::InputTooShort)
    );
    assert_eq!(
        decode_hpack_integer(&[31], 5),
        Err(HpackIntegerError::InputTooShort)
    );
    assert_eq!(
        decode_hpack_integer(&[31, 154], 5),
        Err(HpackIntegerError::InputTooShort)
    );
}

#[test]
fn rejects_invalid_prefix_widths() {
    let mut output = [0xa5; 16];
    for prefix_bits in [0, 9, u8::MAX] {
        assert_eq!(
            encode_hpack_integer(10, prefix_bits, &mut output),
            Err(HpackIntegerError::InvalidPrefixWidth)
        );
        assert_eq!(
            decode_hpack_integer(&[10], prefix_bits),
            Err(HpackIntegerError::InvalidPrefixWidth)
        );
        assert_eq!(output, [0xa5; 16]);
    }
}

#[test]
fn rejects_u64_overflow_and_shift_overflow() {
    let payload_overflow = [
        31, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x82,
    ];
    assert_eq!(
        decode_hpack_integer(&payload_overflow, 5),
        Err(HpackIntegerError::IntegerOverflow)
    );

    let shift_overflow = [
        31, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
    ];
    assert_eq!(
        decode_hpack_integer(&shift_overflow, 5),
        Err(HpackIntegerError::IntegerOverflow)
    );

    let mut high_value = [0; 16];
    let high_value_length = encode_hpack_integer(MAX_HPACK_INTEGER, 8, &mut high_value).unwrap();
    assert_eq!(
        decode_hpack_integer(&high_value[..high_value_length], 8),
        Ok((MAX_HPACK_INTEGER, high_value_length))
    );
}

#[test]
fn reports_short_output_without_modifying_it() {
    let mut output = [0xa5; 2];
    assert_eq!(
        encode_hpack_integer(1_337, 5, &mut output),
        Err(HpackIntegerError::OutputTooSmall { required: 3 })
    );
    assert_eq!(output, [0xa5; 2]);

    let mut empty = [];
    assert_eq!(
        encode_hpack_integer(10, 5, &mut empty),
        Err(HpackIntegerError::OutputTooSmall { required: 1 })
    );
}

fn round_trip(value: u64, prefix_bits: u8) {
    let mut output = [0; 16];
    let written = encode_hpack_integer(value, prefix_bits, &mut output).unwrap();
    assert_eq!(
        decode_hpack_integer(&output[..written], prefix_bits),
        Ok((value, written))
    );
}
