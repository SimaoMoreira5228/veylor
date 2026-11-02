use super::{
    EOS_SYMBOL, HUFFMAN_CODES, HuffmanError, contains_eos, contains_eos_scalar, decode_huffman,
    decode_huffman_inner, decoded_bound, encode_huffman, encoded_length,
};

#[test]
fn contains_all_rfc_symbols_and_the_eos_code() {
    assert_eq!(HUFFMAN_CODES.len(), 257);
    assert_eq!(HUFFMAN_CODES[EOS_SYMBOL].code(), 0x3fff_ffff);
    assert_eq!(HUFFMAN_CODES[EOS_SYMBOL].bit_length(), 30);
}

#[test]
fn matches_rfc_huffman_string_vectors() {
    let vectors = [
        (
            b"www.example.com".as_slice(),
            [
                0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
            ]
            .as_slice(),
        ),
        (
            b"no-cache".as_slice(),
            [0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf].as_slice(),
        ),
        (b"302".as_slice(), [0x64, 0x02].as_slice()),
    ];
    let mut output = [0; 64];
    let mut decoded = [0; 64];

    for (plain, encoded) in vectors {
        let written = encode_huffman(plain, &mut output).unwrap();
        assert_eq!(&output[..written], encoded);
        let decoded_length = decode_huffman(encoded, &mut decoded).unwrap();
        assert_eq!(&decoded[..decoded_length], plain);
    }
}

#[test]
fn round_trips_every_byte_symbol() {
    let input = core::array::from_fn::<_, 256, _>(|index| index as u8);
    let mut encoded = [0; 1024];
    let mut decoded = [0; 256];

    let encoded_length = encode_huffman(&input, &mut encoded).unwrap();
    let decoded_length = decode_huffman(&encoded[..encoded_length], &mut decoded).unwrap();

    assert_eq!(&decoded[..decoded_length], &input);
}

#[test]
fn rejects_eos_symbol_and_invalid_terminal_padding() {
    let mut output = [0xa5; 8];
    assert_eq!(
        decode_huffman(&[0xff, 0xff, 0xff, 0xfc], &mut output),
        Err(HuffmanError::EndOfStreamSymbol)
    );

    let mut encoded = [0; 8];
    let written = encode_huffman(b"a", &mut encoded).unwrap();
    assert_eq!(encoded[written - 1] & 0x07, 0x07);

    let mut invalid = encoded;
    invalid[written - 1] &= 0xfe;
    assert_eq!(
        decode_huffman(&invalid[..written], &mut output),
        Err(HuffmanError::InvalidPadding)
    );

    let mut overlong = encoded;
    overlong[written] = 0xff;
    assert_eq!(
        decode_huffman(&overlong[..written + 1], &mut output),
        Err(HuffmanError::InvalidPadding)
    );
}

#[test]
fn accepts_seven_eos_prefix_padding_bits() {
    let symbol = HUFFMAN_CODES[..256]
        .iter()
        .position(|entry| entry.bit_length() % 8 == 1)
        .unwrap() as u8;
    let mut encoded = [0; 8];
    let written = encode_huffman(&[symbol], &mut encoded).unwrap();
    assert_eq!(encoded[written - 1] & 0x7f, 0x7f);

    let mut decoded = [0; 1];
    assert_eq!(decode_huffman(&encoded[..written], &mut decoded), Ok(1));
    assert_eq!(decoded[0], symbol);
}

#[test]
fn checks_output_capacity_before_writing() {
    let required = encoded_length(b"www.example.com").unwrap();
    let mut short_encoded = [0xa5; 11];
    assert_eq!(
        encode_huffman(b"www.example.com", &mut short_encoded),
        Err(HuffmanError::OutputTooSmall { required })
    );
    assert_eq!(short_encoded, [0xa5; 11]);

    let mut short_decoded = [0xa5; 3];
    assert_eq!(
        decode_huffman(
            &[
                0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff
            ],
            &mut short_decoded,
        ),
        Err(HuffmanError::OutputTooSmall { required: 15 })
    );
    assert_eq!(short_decoded, [0xa5; 3]);
}

#[test]
fn one_pass_matches_tight_buffer_for_every_symbol() {
    let mut encoded = [0u8; 4096];
    let mut tight = [0u8; 256];
    let mut bound = [0u8; 4096];
    for symbol in 0..=255u8 {
        let plain = [symbol; 7];
        let encoded_length = encode_huffman(&plain, &mut encoded).unwrap();
        let required = decode_huffman(&encoded[..encoded_length], &mut tight).unwrap();
        assert_eq!(&tight[..required], &plain, "symbol {symbol}");
        let capacity = decoded_bound(encoded_length);
        assert!(capacity <= bound.len());
        let written = decode_huffman(&encoded[..encoded_length], &mut bound[..capacity]).unwrap();
        assert_eq!(written, required, "symbol {symbol}");
        assert_eq!(&bound[..written], &plain, "symbol {symbol}");
    }
}

#[test]
fn eos_scan_separates_valid_streams_from_the_eos_code() {
    let mut encoded = [0u8; 4096];
    for symbol in 0..=255u8 {
        let plain = [symbol; 9];
        let encoded_length = encode_huffman(&plain, &mut encoded).unwrap();
        assert!(
            !contains_eos_scalar(&encoded[..encoded_length]),
            "symbol {symbol}"
        );
        if contains_eos_scalar(&encoded[..encoded_length]) {
            assert!(contains_eos(&encoded[..encoded_length]), "symbol {symbol}");
        }
    }
    for eos in [&[0xff, 0xff, 0xff, 0xfc][..], &[0xff, 0xff, 0xff, 0xfe][..]] {
        assert!(contains_eos_scalar(eos));
        assert!(contains_eos(eos));
    }
    for plain in [
        &[0xff, 0xff, 0xff][..],
        &[0xff, 0xff, 0xff, 0x80][..],
        &[0x00, 0xff, 0x00][..],
    ] {
        assert!(!contains_eos_scalar(plain));
        assert!(!contains_eos(plain));
    }
}

#[test]
fn fast_path_agrees_with_the_tree_walk() {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    let mut encoded = [0u8; 512];
    let mut fast = [0u8; 2048];
    let mut slow = [0u8; 2048];
    let mut plain = [0u8; 64];

    let check = |bytes: &[u8], fast: &mut [u8; 2048], slow: &mut [u8; 2048]| {
        let quick = decode_huffman(bytes, fast).map(|length| &fast[..length]);
        let walked = decode_huffman_inner(bytes, Some(slow)).map(|length| &slow[..length]);
        assert_eq!(quick, walked, "bytes {bytes:02x?}");
    };

    for _ in 0..2000 {
        let length = (next() % 64) as usize + 1;
        for slot in plain.iter_mut().take(length) {
            *slot = (next() % 251) as u8;
        }
        let encoded_length = encode_huffman(&plain[..length], &mut encoded).unwrap();
        check(&encoded[..encoded_length], &mut fast, &mut slow);
    }
    for _ in 0..2000 {
        let length = (next() % 96) as usize + 1;
        for slot in encoded.iter_mut().take(length) {
            *slot = (next() % 256) as u8;
        }
        check(&encoded[..length], &mut fast, &mut slow);
    }
}

#[test]
fn fast_path_handles_typical_header_values() {
    use super::decode_huffman_fast;

    let values: [&[u8]; 8] = [
        b"h2load nghttp2/1.70.0",
        b"127.0.0.1:8443",
        b"gzip, deflate",
        b"*/*",
        b"application/json",
        b"Tue, 03 Nov 2026 10:00:00 GMT",
        b"no-cache",
        b"www.example.com",
    ];
    let mut encoded = [0u8; 256];
    let mut decoded = [0u8; 256];
    for value in values {
        let length = encode_huffman(value, &mut encoded).unwrap();
        let fast = decode_huffman_fast(&encoded[..length], &mut decoded);
        assert_eq!(
            fast.map(|written| &decoded[..written]),
            Some(value),
            "value {}",
            String::from_utf8_lossy(value)
        );
    }
}

#[test]
#[ignore]
fn eos_scan_kernel_rate() {
    use std::time::Instant;

    let short = b"h2load nghttp2/1.70.0".as_slice();
    let long: Vec<u8> = std::iter::repeat_n(
        b"session=8f14e45fceea167a5a36dedd4bea2543; Path=/; HttpOnly; ",
        6,
    )
    .flatten()
    .copied()
    .collect();

    for (name, input) in [("short", short), ("long", long.as_slice())] {
        let iterations = 2_000_000u32;
        let start = Instant::now();
        let mut hits = 0usize;
        for _ in 0..iterations {
            hits += usize::from(super::contains_eos_scalar(input));
        }
        let scalar = start.elapsed();

        let start = Instant::now();
        for _ in 0..iterations {
            hits += usize::from(super::contains_eos(input));
        }
        let vector = start.elapsed();

        println!(
            "{name} {}B: scalar {:.2} ns/scan, vector {:.2} ns/scan, hits {hits}",
            input.len(),
            scalar.as_nanos() as f64 / f64::from(iterations),
            vector.as_nanos() as f64 / f64::from(iterations)
        );
    }
}
