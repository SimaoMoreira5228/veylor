use std::time::Instant;

use veylor_http2::{decode_huffman, encode_huffman};

#[test]
#[ignore]
fn huffman_decode_rate() {
    let values: [&[u8]; 7] = [
        b"h2load nghttp2/1.70.0",
        b"127.0.0.1:8443",
        b"gzip, deflate",
        b"*/*",
        b"application/json",
        b"Tue, 03 Nov 2026 10:00:00 GMT",
        b"no-cache",
    ];
    let mut encoded = [[0u8; 128]; 7];
    let mut lengths = [0usize; 7];
    for (slot, value) in values.iter().enumerate() {
        lengths[slot] = encode_huffman(value, &mut encoded[slot]).unwrap();
    }
    let long: Vec<u8> = std::iter::repeat_n(
        b"session=8f14e45fceea167a5a36dedd4bea2543; Path=/; HttpOnly; ",
        6,
    )
    .flatten()
    .copied()
    .collect();
    let mut long_encoded = vec![0u8; long.len() * 2];
    let long_length = encode_huffman(&long, &mut long_encoded).unwrap();
    let mut long_decoded = vec![0u8; long.len()];
    let long_iterations = 1_000_000u32;
    let long_start = Instant::now();
    for _ in 0..long_iterations {
        let written = decode_huffman(&long_encoded[..long_length], &mut long_decoded).unwrap();
        assert_eq!(written, long.len());
    }
    let long_elapsed = long_start.elapsed();
    println!(
        "long value {} bytes: {:.2} ns/decode",
        long.len(),
        long_elapsed.as_nanos() as f64 / f64::from(long_iterations)
    );

    let mut decoded = [0u8; 256];
    let iterations = 3_000_000u32;
    let start = Instant::now();
    let mut total = 0usize;
    for _ in 0..iterations {
        for (slot, value) in values.iter().enumerate() {
            total += decode_huffman(&encoded[slot][..lengths[slot]], &mut decoded).unwrap();
            assert_eq!(&decoded[..value.len()], *value);
        }
    }
    let elapsed = start.elapsed();
    let ops = u64::from(iterations) * values.len() as u64;
    println!(
        "{} decodes in {:?}: {:.2} ns/decode, {} bytes",
        ops,
        elapsed,
        elapsed.as_nanos() as f64 / ops as f64,
        total
    );
}
