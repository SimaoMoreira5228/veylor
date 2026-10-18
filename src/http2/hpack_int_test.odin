#+test
package http2

import "core:testing"

@(test)
test_hpack_int_rfc_golden_vectors :: proc(t: ^testing.T) {
	out: [16]u8

	written, ok := encode_hpack_int(10, 5, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, written, 1)
	testing.expect_value(t, out[0], u8(10))

	value, consumed, decode_ok := decode_hpack_int(out[:written], 5)
	testing.expect_value(t, decode_ok, true)
	testing.expect_value(t, value, u64(10))
	testing.expect_value(t, consumed, 1)

	written, ok = encode_hpack_int(1337, 5, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, written, 3)
	testing.expect_value(t, out[0], u8(31))
	testing.expect_value(t, out[1], u8(154))
	testing.expect_value(t, out[2], u8(10))

	value, consumed, decode_ok = decode_hpack_int(out[:written], 5)
	testing.expect_value(t, decode_ok, true)
	testing.expect_value(t, value, u64(1337))
	testing.expect_value(t, consumed, 3)

	written, ok = encode_hpack_int(42, 8, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, written, 1)
	testing.expect_value(t, out[0], u8(42))

	value, consumed, decode_ok = decode_hpack_int(out[:written], 8)
	testing.expect_value(t, decode_ok, true)
	testing.expect_value(t, value, u64(42))
	testing.expect_value(t, consumed, 1)
}

@(test)
test_hpack_int_round_trip_boundaries :: proc(t: ^testing.T) {
	big := [?]u64{127, 128, 255, 256, 16383, 16384, 0xffffffff, 0xffffffffffffffff}

	for prefix_bits in 1 ..= 8 {
		max_prefix := (u64(1) << uint(prefix_bits)) - 1

		edges := [?]u64{0, max_prefix - 1, max_prefix, max_prefix + 1}

		for edge in edges {
			expect_round_trip(t, edge, prefix_bits)
		}

		for wide in big {
			expect_round_trip(t, wide, prefix_bits)
		}
	}
}

@(test)
test_hpack_int_rejects_truncated_input :: proc(t: ^testing.T) {
	_, _, ok := decode_hpack_int(nil, 5)
	testing.expect_value(t, ok, false)

	needs_continuation := [?]u8{31}
	_, _, ok = decode_hpack_int(needs_continuation[:], 5)
	testing.expect_value(t, ok, false)

	cut_short := [?]u8{31, 154}
	_, _, ok = decode_hpack_int(cut_short[:], 5)
	testing.expect_value(t, ok, false)
}

@(test)
test_hpack_int_rejects_overflow :: proc(t: ^testing.T) {
	payload_overflow := [?]u8{31, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x82}
	_, _, ok := decode_hpack_int(payload_overflow[:], 5)
	testing.expect_value(t, ok, false)

	shift_overflow := [?]u8{31, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}
	_, _, ok = decode_hpack_int(shift_overflow[:], 5)
	testing.expect_value(t, ok, false)

	endless := [?]u8 {
		255,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x80,
		0x01,
	}
	_, _, ok = decode_hpack_int(endless[:], 8)
	testing.expect_value(t, ok, false)
}

@(test)
test_hpack_int_rejects_bad_prefix :: proc(t: ^testing.T) {
	out: [16]u8

	bad_prefixes := [?]int{-3, 0, 9, 16}

	for prefix_bits in bad_prefixes {
		_, ok := encode_hpack_int(10, prefix_bits, out[:])
		testing.expect_value(t, ok, false)

		raw := [?]u8{10}
		_, _, decode_ok := decode_hpack_int(raw[:], prefix_bits)
		testing.expect_value(t, decode_ok, false)
	}
}

@(test)
test_hpack_int_rejects_short_output :: proc(t: ^testing.T) {
	out: [16]u8

	written, ok := encode_hpack_int(1337, 5, out[:2])
	testing.expect_value(t, ok, false)
	testing.expect_value(t, written, 0)

	written, ok = encode_hpack_int(10, 5, out[:0])
	testing.expect_value(t, ok, false)
	testing.expect_value(t, written, 0)
}

expect_round_trip :: proc(t: ^testing.T, value: u64, prefix_bits: int) {
	out: [16]u8

	written, ok := encode_hpack_int(value, prefix_bits, out[:])
	testing.expect_value(t, ok, true)

	decoded, consumed, decode_ok := decode_hpack_int(out[:written], prefix_bits)
	testing.expect_value(t, decode_ok, true)
	testing.expect_value(t, decoded, value)
	testing.expect_value(t, consumed, written)
}
