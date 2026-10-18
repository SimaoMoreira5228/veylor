#+test
package http2

import "core:testing"

@(test)
test_hpack_string_rfc_huffman_vectors :: proc(t: ^testing.T) {
	enc: [128]u8

	dec: [128]u8

	www_plain := "www.example.com"

	www_huffman := [?]u8{0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff}

	written, ok := hpack_huffman_encode(bytes_of(www_plain), enc[:])

	testing.expect_value(t, ok, true)
	testing.expect_value(t, written, len(www_huffman))
	testing.expect_value(t, string(enc[:written]), string(www_huffman[:]))

	decoded, huffman_ok := hpack_huffman_decode(www_huffman[:], dec[:])

	testing.expect_value(t, huffman_ok, true)
	testing.expect_value(t, string(dec[:decoded]), www_plain)

	str_written, str_ok := encode_hpack_string(bytes_of(www_plain), true, enc[:])

	testing.expect_value(t, str_ok, true)
	testing.expect_value(t, str_written, len(www_huffman) + 1)
	testing.expect_value(t, enc[0], u8(0x8c))
	testing.expect_value(t, string(enc[1:str_written]), string(www_huffman[:]))

	value, consumed, dec_ok := decode_hpack_string(enc[:str_written], 7, dec[:])

	testing.expect_value(t, dec_ok, true)
	testing.expect_value(t, consumed, str_written)
	testing.expect_value(t, string(value), www_plain)

	expect_huffman_pair(t, "no-cache", []u8{0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf})
	expect_huffman_pair(t, "custom-key", []u8{0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xa9, 0x7d, 0x7f})
	expect_huffman_pair(t, "custom-value", []u8{0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xb8, 0xe8, 0xb4, 0xbf})
	expect_huffman_pair(t, "302", []u8{0x64, 0x02})
	expect_huffman_pair(t, "307", []u8{0x64, 0x0e, 0xff})
	expect_huffman_pair(t, "private", []u8{0xae, 0xc3, 0x77, 0x1a, 0x4b})
	expect_huffman_pair(
		t,
		"Mon, 21 Oct 2013 20:13:21 GMT",
		[]u8 {
			0xd0, 0x7a, 0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20, 0x05, 0x95, 0x04, 0x0b,
			0x81, 0x66, 0xe0, 0x82, 0xa6, 0x2d, 0x1b, 0xff,
		},
	)
	expect_huffman_pair(
		t,
		"https://www.example.com",
		[]u8 {
			0x9d, 0x29, 0xad, 0x17, 0x18, 0x63, 0xc7, 0x8f, 0x0b, 0x97, 0xc8, 0xe9, 0xae, 0x82,
			0xae, 0x43, 0xd3,
		},
	)
	expect_huffman_pair(t, "gzip", []u8{0x9b, 0xd9, 0xab})
	expect_huffman_pair(
		t,
		"foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1",
		[]u8 {
			0x94, 0xe7, 0x82, 0x1d, 0xd7, 0xf2, 0xe6, 0xc7, 0xb3, 0x35, 0xdf, 0xdf, 0xcd, 0x5b,
			0x39, 0x60, 0xd5, 0xaf, 0x27, 0x08, 0x7f, 0x36, 0x72, 0xc1, 0xab, 0x27, 0x0f, 0xb5,
			0x29, 0x1f, 0x95, 0x87, 0x31, 0x60, 0x65, 0xc0, 0x03, 0xed, 0x4e, 0xe5, 0xb1, 0x06,
			0x3d, 0x50, 0x07,
		},
	)
}

@(test)
test_hpack_string_round_trip_prefixes :: proc(t: ^testing.T) {
	all_bytes: [256]u8

	for i in 0 ..< 256 {
		all_bytes[i] = u8(i)
	}

	date := "Mon, 21 Oct 2013 20:13:21 GMT"

	cookie := "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1"

	corpus := [?]string{
		"",
		"a",
		"GET",
		"302",
		":authority",
		"custom-key",
		"custom-header",
		"custom-value",
		"www.example.com",
		"https://www.example.com",
		"no-cache",
		"private",
		date,
		cookie,
	}

	for plain in corpus {
		for prefix_bits in 5 ..= 7 {
			expect_string_round_trip(t, bytes_of(plain), false, prefix_bits)
			expect_string_round_trip(t, bytes_of(plain), true, prefix_bits)
		}
	}

	for prefix_bits in 5 ..= 7 {
		expect_string_round_trip(t, all_bytes[:], false, prefix_bits)
		expect_string_round_trip(t, all_bytes[:], true, prefix_bits)
	}
}

@(test)
test_hpack_string_huffman_all_bytes :: proc(t: ^testing.T) {
	enc: [8]u8

	dec: [8]u8

	for sym in 0 ..< 256 {
		single := [?]u8{u8(sym)}

		written, enc_ok := hpack_huffman_encode(single[:], enc[:])

		testing.expect_value(t, enc_ok, true)

		decoded, dec_ok := hpack_huffman_decode(enc[:written], dec[:])

		testing.expect_value(t, dec_ok, true)
		testing.expect_value(t, decoded, 1)
		testing.expect_value(t, dec[0], u8(sym))
	}
}

@(test)
test_hpack_string_rejects_truncation :: proc(t: ^testing.T) {
	enc: [128]u8

	dec: [128]u8

	raw_written, raw_ok := encode_hpack_string(bytes_of("custom-header"), false, enc[:])

	testing.expect_value(t, raw_ok, true)

	for cut in 0 ..< raw_written {
		_, _, ok := decode_hpack_string(enc[:cut], 7, dec[:])

		testing.expect_value(t, ok, false)
	}

	huffman_written, huffman_ok := encode_hpack_string(bytes_of("www.example.com"), true, enc[:])

	testing.expect_value(t, huffman_ok, true)

	for cut in 0 ..< huffman_written {
		_, _, ok := decode_hpack_string(enc[:cut], 7, dec[:])

		testing.expect_value(t, ok, false)
	}

	_, _, empty_ok := decode_hpack_string(nil, 7, dec[:])

	testing.expect_value(t, empty_ok, false)
}

@(test)
test_hpack_string_rejects_oversize_length :: proc(t: ^testing.T) {
	dec: [64]u8

	declared_big := [?]u8{0x7f, 0x9d, 0x03, 0x41, 0x42, 0x43}

	_, _, ok := decode_hpack_string(declared_big[:], 7, dec[:])

	testing.expect_value(t, ok, false)

	length_overflow := [?]u8{0x7f, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}

	_, _, ok = decode_hpack_string(length_overflow[:], 7, dec[:])

	testing.expect_value(t, ok, false)

	enc: [16]u8

	written, enc_ok := encode_hpack_string(bytes_of("hello"), false, enc[:])

	testing.expect_value(t, enc_ok, true)

	small: [4]u8

	_, _, ok = decode_hpack_string(enc[:written], 7, small[:])

	testing.expect_value(t, ok, false)

	huffman_enc: [16]u8

	huffman_written, huffman_enc_ok := encode_hpack_string(bytes_of("www.example.com"), true, huffman_enc[:])

	testing.expect_value(t, huffman_enc_ok, true)

	_, _, ok = decode_hpack_string(huffman_enc[:huffman_written], 7, small[:])

	testing.expect_value(t, ok, false)
}

@(test)
test_hpack_string_rejects_bad_padding :: proc(t: ^testing.T) {
	enc: [128]u8

	dec: [128]u8

	written, enc_ok := encode_hpack_string(bytes_of("www.example.com"), true, enc[:])

	testing.expect_value(t, enc_ok, true)

	corrupt: [128]u8

	for i in 0 ..< written {
		corrupt[i] = enc[i]
	}

	corrupt[written - 1] &= 0xfe

	_, _, ok := decode_hpack_string(corrupt[:written], 7, dec[:])

	testing.expect_value(t, ok, false)

	padded := [?]u8{0x81, 0xff}

	_, _, ok = decode_hpack_string(padded[:], 7, dec[:])

	testing.expect_value(t, ok, false)
}

@(test)
test_hpack_string_rejects_eos :: proc(t: ^testing.T) {
	dec: [16]u8

	eos := [?]u8{0x84, 0xff, 0xff, 0xff, 0xfc}

	_, _, ok := decode_hpack_string(eos[:], 7, dec[:])

	testing.expect_value(t, ok, false)
}

@(test)
test_hpack_string_rejects_bad_prefix_bits :: proc(t: ^testing.T) {
	enc: [16]u8

	dec: [16]u8

	bad := [?]int{-3, 0, 8, 16}

	for prefix_bits in bad {
		_, ok := encode_hpack_string(bytes_of("a"), false, enc[:], prefix_bits)

		testing.expect_value(t, ok, false)

		raw := [?]u8{0x01, 0x61}

		_, _, dec_ok := decode_hpack_string(raw[:], prefix_bits, dec[:])

		testing.expect_value(t, dec_ok, false)
	}
}

@(test)
test_hpack_string_rejects_short_output :: proc(t: ^testing.T) {
	enc: [16]u8

	written, ok := encode_hpack_string(bytes_of("hello"), false, enc[:3])

	testing.expect_value(t, ok, false)
	testing.expect_value(t, written, 0)

	written, ok = encode_hpack_string(bytes_of("hello"), true, enc[:2])

	testing.expect_value(t, ok, false)
	testing.expect_value(t, written, 0)

	written, ok = encode_hpack_string(bytes_of("hello"), false, enc[:0])

	testing.expect_value(t, ok, false)
	testing.expect_value(t, written, 0)
}

@(test)
test_hpack_string_empty :: proc(t: ^testing.T) {
	enc: [16]u8

	dec: [16]u8

	written, ok := encode_hpack_string(nil, false, enc[:])

	testing.expect_value(t, ok, true)
	testing.expect_value(t, written, 1)
	testing.expect_value(t, enc[0], u8(0x00))

	value, consumed, dec_ok := decode_hpack_string(enc[:written], 7, dec[:])

	testing.expect_value(t, dec_ok, true)
	testing.expect_value(t, consumed, 1)
	testing.expect_value(t, len(value), 0)

	written, ok = encode_hpack_string(nil, true, enc[:])

	testing.expect_value(t, ok, true)
	testing.expect_value(t, written, 1)
	testing.expect_value(t, enc[0], u8(0x80))

	value, consumed, dec_ok = decode_hpack_string(enc[:written], 7, dec[:])

	testing.expect_value(t, dec_ok, true)
	testing.expect_value(t, consumed, 1)
	testing.expect_value(t, len(value), 0)
}

expect_huffman_pair :: proc(t: ^testing.T, plain: string, want: []u8) {
	enc: [128]u8

	dec: [128]u8

	written, enc_ok := hpack_huffman_encode(bytes_of(plain), enc[:])

	testing.expect_value(t, enc_ok, true)
	testing.expect_value(t, written, len(want))
	testing.expect_value(t, string(enc[:written]), string(want))

	decoded, dec_ok := hpack_huffman_decode(want, dec[:])

	testing.expect_value(t, dec_ok, true)
	testing.expect_value(t, string(dec[:decoded]), plain)
}

expect_string_round_trip :: proc(t: ^testing.T, plain: []u8, huffman: bool, prefix_bits: int) {
	enc: [2048]u8

	dec: [2048]u8

	written, enc_ok := encode_hpack_string(plain, huffman, enc[:], prefix_bits)

	testing.expect_value(t, enc_ok, true)

	value, consumed, dec_ok := decode_hpack_string(enc[:written], prefix_bits, dec[:])

	testing.expect_value(t, dec_ok, true)
	testing.expect_value(t, consumed, written)
	testing.expect_value(t, string(value), string(plain))

	flag := u8(1) << uint(prefix_bits)

	if huffman {
		testing.expect_value(t, enc[0] & flag != 0, true)
	} else {
		testing.expect_value(t, enc[0] & flag, u8(0))
	}
}

bytes_of :: proc(s: string) -> []u8 {
	return transmute([]u8)(s)
}
