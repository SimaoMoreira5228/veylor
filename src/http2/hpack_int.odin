package http2

MAX_HPACK_INT :: u64(0xffffffffffffffff)

encode_hpack_int :: proc(
	value: u64,
	prefix_bits: int,
	out: []u8,
) -> (
	bytes_written: int,
	ok: bool,
) {
	if prefix_bits < 1 || prefix_bits > 8 {
		return 0, false
	}

	max_prefix := (u64(1) << uint(prefix_bits)) - 1

	if value < max_prefix {
		if len(out) < 1 {
			return 0, false
		}

		out[0] = u8(value)

		return 1, true
	}

	remaining := value - max_prefix

	count := 0
	scratch := remaining

	for {
		count += 1
		scratch >>= 7

		if scratch == 0 {
			break
		}
	}

	if len(out) < 1 + count {
		return 0, false
	}

	out[0] = u8(max_prefix)

	index := 1

	for {
		next := remaining >> 7

		if next == 0 {
			out[index] = u8(remaining & 0x7f)
			index += 1

			break
		}

		out[index] = u8(remaining & 0x7f) | 0x80
		index += 1
		remaining = next
	}

	return index, true
}

decode_hpack_int :: proc(input: []u8, prefix_bits: int) -> (value: u64, consumed: int, ok: bool) {
	if prefix_bits < 1 || prefix_bits > 8 {
		return 0, 0, false
	}

	if len(input) == 0 {
		return 0, 0, false
	}

	max_prefix := (u64(1) << uint(prefix_bits)) - 1

	value = u64(input[0]) & max_prefix

	if value < max_prefix {
		return value, 1, true
	}

	shift := uint(0)
	consumed = 1

	for {
		if consumed >= len(input) {
			return 0, 0, false
		}

		// NOTE: once the shift reaches 64, every further payload bit lands outside u64,
		// so the encoding is rejected even when the trailing bits happen to be zero.
		if shift >= 64 {
			return 0, 0, false
		}

		b := input[consumed]
		consumed += 1

		payload := u64(b & 0x7f)

		// NOTE: the bound is checked before shifting so payload bits that would wrap
		// past bit 63 are rejected instead of silently overflowing.
		if payload > (MAX_HPACK_INT - value) >> shift {
			return 0, 0, false
		}

		value += payload << shift

		if b & 0x80 == 0 {
			break
		}

		shift += 7
	}

	return value, consumed, true
}
