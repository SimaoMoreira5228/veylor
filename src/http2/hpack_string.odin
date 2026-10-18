package http2

Hpack_Huffman_Code :: struct {
	code: u32,
	nbits: u8,
}

HPACK_HUFFMAN_CODES := [256]Hpack_Huffman_Code{
	{0x1ff8, 13},
	{0x7fffd8, 23},
	{0xfffffe2, 28},
	{0xfffffe3, 28},
	{0xfffffe4, 28},
	{0xfffffe5, 28},
	{0xfffffe6, 28},
	{0xfffffe7, 28},
	{0xfffffe8, 28},
	{0xffffea, 24},
	{0x3ffffffc, 30},
	{0xfffffe9, 28},
	{0xfffffea, 28},
	{0x3ffffffd, 30},
	{0xfffffeb, 28},
	{0xfffffec, 28},
	{0xfffffed, 28},
	{0xfffffee, 28},
	{0xfffffef, 28},
	{0xffffff0, 28},
	{0xffffff1, 28},
	{0xffffff2, 28},
	{0x3ffffffe, 30},
	{0xffffff3, 28},
	{0xffffff4, 28},
	{0xffffff5, 28},
	{0xffffff6, 28},
	{0xffffff7, 28},
	{0xffffff8, 28},
	{0xffffff9, 28},
	{0xffffffa, 28},
	{0xffffffb, 28},
	{0x14, 6},
	{0x3f8, 10},
	{0x3f9, 10},
	{0xffa, 12},
	{0x1ff9, 13},
	{0x15, 6},
	{0xf8, 8},
	{0x7fa, 11},
	{0x3fa, 10},
	{0x3fb, 10},
	{0xf9, 8},
	{0x7fb, 11},
	{0xfa, 8},
	{0x16, 6},
	{0x17, 6},
	{0x18, 6},
	{0x0, 5},
	{0x1, 5},
	{0x2, 5},
	{0x19, 6},
	{0x1a, 6},
	{0x1b, 6},
	{0x1c, 6},
	{0x1d, 6},
	{0x1e, 6},
	{0x1f, 6},
	{0x5c, 7},
	{0xfb, 8},
	{0x7ffc, 15},
	{0x20, 6},
	{0xffb, 12},
	{0x3fc, 10},
	{0x1ffa, 13},
	{0x21, 6},
	{0x5d, 7},
	{0x5e, 7},
	{0x5f, 7},
	{0x60, 7},
	{0x61, 7},
	{0x62, 7},
	{0x63, 7},
	{0x64, 7},
	{0x65, 7},
	{0x66, 7},
	{0x67, 7},
	{0x68, 7},
	{0x69, 7},
	{0x6a, 7},
	{0x6b, 7},
	{0x6c, 7},
	{0x6d, 7},
	{0x6e, 7},
	{0x6f, 7},
	{0x70, 7},
	{0x71, 7},
	{0x72, 7},
	{0xfc, 8},
	{0x73, 7},
	{0xfd, 8},
	{0x1ffb, 13},
	{0x7fff0, 19},
	{0x1ffc, 13},
	{0x3ffc, 14},
	{0x22, 6},
	{0x7ffd, 15},
	{0x3, 5},
	{0x23, 6},
	{0x4, 5},
	{0x24, 6},
	{0x5, 5},
	{0x25, 6},
	{0x26, 6},
	{0x27, 6},
	{0x6, 5},
	{0x74, 7},
	{0x75, 7},
	{0x28, 6},
	{0x29, 6},
	{0x2a, 6},
	{0x7, 5},
	{0x2b, 6},
	{0x76, 7},
	{0x2c, 6},
	{0x8, 5},
	{0x9, 5},
	{0x2d, 6},
	{0x77, 7},
	{0x78, 7},
	{0x79, 7},
	{0x7a, 7},
	{0x7b, 7},
	{0x7ffe, 15},
	{0x7fc, 11},
	{0x3ffd, 14},
	{0x1ffd, 13},
	{0xffffffc, 28},
	{0xfffe6, 20},
	{0x3fffd2, 22},
	{0xfffe7, 20},
	{0xfffe8, 20},
	{0x3fffd3, 22},
	{0x3fffd4, 22},
	{0x3fffd5, 22},
	{0x7fffd9, 23},
	{0x3fffd6, 22},
	{0x7fffda, 23},
	{0x7fffdb, 23},
	{0x7fffdc, 23},
	{0x7fffdd, 23},
	{0x7fffde, 23},
	{0xffffeb, 24},
	{0x7fffdf, 23},
	{0xffffec, 24},
	{0xffffed, 24},
	{0x3fffd7, 22},
	{0x7fffe0, 23},
	{0xffffee, 24},
	{0x7fffe1, 23},
	{0x7fffe2, 23},
	{0x7fffe3, 23},
	{0x7fffe4, 23},
	{0x1fffdc, 21},
	{0x3fffd8, 22},
	{0x7fffe5, 23},
	{0x3fffd9, 22},
	{0x7fffe6, 23},
	{0x7fffe7, 23},
	{0xffffef, 24},
	{0x3fffda, 22},
	{0x1fffdd, 21},
	{0xfffe9, 20},
	{0x3fffdb, 22},
	{0x3fffdc, 22},
	{0x7fffe8, 23},
	{0x7fffe9, 23},
	{0x1fffde, 21},
	{0x7fffea, 23},
	{0x3fffdd, 22},
	{0x3fffde, 22},
	{0xfffff0, 24},
	{0x1fffdf, 21},
	{0x3fffdf, 22},
	{0x7fffeb, 23},
	{0x7fffec, 23},
	{0x1fffe0, 21},
	{0x1fffe1, 21},
	{0x3fffe0, 22},
	{0x1fffe2, 21},
	{0x7fffed, 23},
	{0x3fffe1, 22},
	{0x7fffee, 23},
	{0x7fffef, 23},
	{0xfffea, 20},
	{0x3fffe2, 22},
	{0x3fffe3, 22},
	{0x3fffe4, 22},
	{0x7ffff0, 23},
	{0x3fffe5, 22},
	{0x3fffe6, 22},
	{0x7ffff1, 23},
	{0x3ffffe0, 26},
	{0x3ffffe1, 26},
	{0xfffeb, 20},
	{0x7fff1, 19},
	{0x3fffe7, 22},
	{0x7ffff2, 23},
	{0x3fffe8, 22},
	{0x1ffffec, 25},
	{0x3ffffe2, 26},
	{0x3ffffe3, 26},
	{0x3ffffe4, 26},
	{0x7ffffde, 27},
	{0x7ffffdf, 27},
	{0x3ffffe5, 26},
	{0xfffff1, 24},
	{0x1ffffed, 25},
	{0x7fff2, 19},
	{0x1fffe3, 21},
	{0x3ffffe6, 26},
	{0x7ffffe0, 27},
	{0x7ffffe1, 27},
	{0x3ffffe7, 26},
	{0x7ffffe2, 27},
	{0xfffff2, 24},
	{0x1fffe4, 21},
	{0x1fffe5, 21},
	{0x3ffffe8, 26},
	{0x3ffffe9, 26},
	{0xffffffd, 28},
	{0x7ffffe3, 27},
	{0x7ffffe4, 27},
	{0x7ffffe5, 27},
	{0xfffec, 20},
	{0xfffff3, 24},
	{0xfffed, 20},
	{0x1fffe6, 21},
	{0x3fffe9, 22},
	{0x1fffe7, 21},
	{0x1fffe8, 21},
	{0x7ffff3, 23},
	{0x3fffea, 22},
	{0x3fffeb, 22},
	{0x1ffffee, 25},
	{0x1ffffef, 25},
	{0xfffff4, 24},
	{0xfffff5, 24},
	{0x3ffffea, 26},
	{0x7ffff4, 23},
	{0x3ffffeb, 26},
	{0x7ffffe6, 27},
	{0x3ffffec, 26},
	{0x3ffffed, 26},
	{0x7ffffe7, 27},
	{0x7ffffe8, 27},
	{0x7ffffe9, 27},
	{0x7ffffea, 27},
	{0x7ffffeb, 27},
	{0xffffffe, 28},
	{0x7ffffec, 27},
	{0x7ffffed, 27},
	{0x7ffffee, 27},
	{0x7ffffef, 27},
	{0x7fffff0, 27},
	{0x3ffffee, 26},
}

HPACK_HUFFMAN_EOS_CODE :: u32(0x3fffffff)
HPACK_HUFFMAN_EOS_NBITS :: u8(30)

HPACK_HUFFMAN_NODES := [256][2]i32{
	{66, 1}, {93, 2},
	{104, 3}, {119, 4},
	{144, 5}, {75, 6},
	{123, 7}, {71, 8},
	{77, 9}, {73, 10},
	{11, 13}, {12, 102},
	{-1, -37}, {127, 14},
	{128, 15}, {98, 16},
	{-124, 17}, {124, 18},
	{150, 19}, {20, 25},
	{199, 21}, {216, 22},
	{23, 162}, {24, 161},
	{-2, -136}, {167, 26},
	{41, 27}, {191, 28},
	{211, 29}, {229, 30},
	{31, 45}, {32, 38},
	{33, 35}, {-255, 34},
	{-3, -4}, {36, 37},
	{-5, -6}, {-7, -8},
	{39, 52}, {40, 51},
	{-9, -12}, {208, 42},
	{43, 165}, {-240, 44},
	{-10, -143}, {55, 46},
	{63, 47}, {147, 48},
	{-250, 49}, {50, 59},
	{-11, -14}, {-13, -15},
	{53, 54}, {-16, -17},
	{-18, -19}, {56, 60},
	{57, 58}, {-20, -21},
	{-22, -24}, {-23, -257},
	{61, 62}, {-25, -26},
	{-27, -28}, {64, 65},
	{-29, -30}, {-31, -32},
	{85, 67}, {68, 82},
	{143, 69}, {70, 81},
	{-33, -38}, {72, 79},
	{-34, -35}, {-125, 74},
	{-36, -63}, {76, 80},
	{-39, -43}, {-64, 78},
	{-40, -44}, {-41, -42},
	{-45, -60}, {-46, -47},
	{83, 90}, {84, 89},
	{-48, -52}, {86, 130},
	{87, 88}, {-49, -50},
	{-51, -98}, {-53, -54},
	{91, 92}, {-55, -56},
	{-57, -58}, {99, 94},
	{138, 95}, {142, 96},
	{97, 103}, {-59, -67},
	{-61, -97}, {100, 132},
	{101, 129}, {-62, -66},
	{-65, -92}, {-68, -69},
	{105, 112}, {106, 109},
	{107, 108}, {-70, -71},
	{-72, -73}, {110, 111},
	{-74, -75}, {-76, -77},
	{113, 116}, {114, 115},
	{-78, -79}, {-80, -81},
	{117, 118}, {-82, -83},
	{-84, -85}, {120, 136},
	{121, 122}, {-86, -87},
	{-88, -90}, {-89, -91},
	{125, 155}, {126, 148},
	{-93, -196}, {-94, -127},
	{-95, -126}, {-96, -99},
	{131, 135}, {-100, -102},
	{133, 134}, {-101, -103},
	{-104, -105}, {-106, -112},
	{137, 141}, {-107, -108},
	{139, 140}, {-109, -110},
	{-111, -113}, {-114, -119},
	{-115, -118}, {-116, -117},
	{145, 146}, {-120, -121},
	{-122, -123}, {-128, -221},
	{-209, 149}, {-129, -131},
	{196, 151}, {152, 178},
	{153, 158}, {-231, 154},
	{-130, -133}, {156, 175},
	{157, 204}, {-132, -163},
	{159, 160}, {-134, -135},
	{-137, -147}, {-138, -139},
	{163, 164}, {-140, -141},
	{-142, -144}, {166, 171},
	{-145, -146}, {168, 185},
	{169, 173}, {170, 172},
	{-148, -150}, {-149, -160},
	{-151, -152}, {174, 181},
	{-153, -156}, {241, 176},
	{177, 188}, {-154, -162},
	{179, 183}, {180, 182},
	{-155, -157}, {-158, -159},
	{-161, -164}, {184, 190},
	{-165, -170}, {186, 194},
	{187, 189}, {-166, -167},
	{-168, -173}, {-169, -175},
	{-171, -174}, {192, 218},
	{193, 234}, {-172, -207},
	{195, 203}, {-176, -181},
	{197, 235}, {198, 202},
	{-177, -178}, {200, 206},
	{201, 205}, {-179, -182},
	{-180, -210}, {-183, -184},
	{-185, -195}, {-186, -187},
	{207, 210}, {-188, -190},
	{209, 215}, {-189, -192},
	{-191, -197}, {212, 224},
	{213, 222}, {214, 221},
	{-193, -194}, {-198, -232},
	{217, 243}, {-199, -229},
	{245, 219}, {220, 244},
	{-200, -208}, {-201, -202},
	{223, 228}, {-203, -206},
	{237, 225}, {248, 226},
	{-256, 227}, {-204, -205},
	{-211, -214}, {230, 249},
	{231, 239}, {232, 233},
	{-212, -213}, {-215, -222},
	{-216, -226}, {236, 242},
	{-217, -218}, {238, 246},
	{-219, -220}, {240, 247},
	{-223, -224}, {-225, -227},
	{-228, -230}, {-233, -234},
	{-235, -236}, {-237, -238},
	{-239, -241}, {-242, -245},
	{-243, -244}, {250, 253},
	{251, 252}, {-246, -247},
	{-248, -249}, {254, 255},
	{-251, -252}, {-253, -254},
}

hpack_huffman_encoded_len :: proc(value: []u8) -> (nbytes: int, ok: bool) {
	total := u64(0)

	for b in value {
		nbits := u64(HPACK_HUFFMAN_CODES[b].nbits)

		if nbits > max(u64) - total {
			return 0, false
		}

		total += nbits
	}

	if total > max(u64) - 7 {
		return 0, false
	}

	return int((total + 7) / 8), true
}

hpack_huffman_encode :: proc(value: []u8, out: []u8) -> (written: int, ok: bool) {
	encoded_len, len_ok := hpack_huffman_encoded_len(value)

	if !len_ok {
		return 0, false
	}

	if len(out) < encoded_len {
		return 0, false
	}

	acc := u64(0)

	pending := uint(0)

	index := 0

	for b in value {
		entry := HPACK_HUFFMAN_CODES[b]

		acc = (acc << uint(entry.nbits)) | u64(entry.code)
		pending += uint(entry.nbits)

		for pending >= 8 {
			pending -= 8

			out[index] = u8(acc >> pending)

			index += 1
		}

		acc &= (u64(1) << pending) - 1
	}

	if pending > 0 {
		pad := 8 - pending

		out[index] = u8(acc << pad) | u8((u64(1) << pad) - 1)

		index += 1
	}

	return index, true
}

hpack_huffman_decode :: proc(input: []u8, out: []u8) -> (written: int, ok: bool) {
	node := i32(0)

	bit_pos := 0

	symbol_end := 0

	for b in input {
		for k in 0 ..< 8 {
			bit := (b >> uint(7 - k)) & 1

			next := HPACK_HUFFMAN_NODES[node][bit]

			bit_pos += 1

			if next >= 0 {
				node = next

				continue
			}

			symbol := int(-next) - 1

			if symbol == 256 {
				return 0, false
			}

			if written >= len(out) {
				return 0, false
			}

			out[written] = u8(symbol)

			written += 1

			node = 0

			symbol_end = bit_pos
		}
	}

	if node != 0 {
		trailing := bit_pos - symbol_end

		if trailing > 7 {
			return 0, false
		}

		mask := u8((u64(1) << uint(trailing)) - 1)

		if input[len(input) - 1] & mask != mask {
			return 0, false
		}
	}

	return written, true
}

encode_hpack_string :: proc(
	value: []u8,
	huffman: bool,
	out: []u8,
	prefix_bits: int = 7,
) -> (
	written: int,
	ok: bool,
) {
	if prefix_bits < 1 || prefix_bits > 7 {
		return 0, false
	}

	flag := u8(0)

	if huffman {
		flag = u8(1) << uint(prefix_bits)
	}

	payload_len := len(value)

	if huffman {
		encoded_len, len_ok := hpack_huffman_encoded_len(value)

		if !len_ok {
			return 0, false
		}

		payload_len = encoded_len
	}

	if payload_len > max(int) - 10 {
		return 0, false
	}

	prefix_scratch: [10]u8

	prefix_len, prefix_ok := encode_hpack_int(u64(payload_len), prefix_bits, prefix_scratch[:])

	if !prefix_ok {
		return 0, false
	}

	if len(out) < prefix_len + payload_len {
		return 0, false
	}

	for i in 0 ..< prefix_len {
		out[i] = prefix_scratch[i]
	}

	out[0] |= flag

	offset := prefix_len

	if huffman {
		payload_written, payload_ok := hpack_huffman_encode(value, out[offset:])

		if !payload_ok {
			return 0, false
		}

		if payload_written != payload_len {
			return 0, false
		}

		offset += payload_written

		return offset, true
	}

	copied := copy(out[offset:], value)

	return offset + copied, true
}

decode_hpack_string :: proc(
	input: []u8,
	prefix_bits: int,
	out: []u8,
) -> (
	value: []u8,
	consumed: int,
	ok: bool,
) {
	if prefix_bits < 1 || prefix_bits > 7 {
		return nil, 0, false
	}

	if len(input) == 0 {
		return nil, 0, false
	}

	high := input[0] >> uint(prefix_bits)

	if high >> 1 != 0 {
		return nil, 0, false
	}

	is_huffman := input[0] & (u8(1) << uint(prefix_bits)) != 0

	strlen, prefix_len, prefix_ok := decode_hpack_int(input, prefix_bits)

	if !prefix_ok {
		return nil, 0, false
	}

	if u64(prefix_len) + strlen > u64(len(input)) {
		return nil, 0, false
	}

	data := input[prefix_len:prefix_len + int(strlen)]

	if is_huffman {
		written, huffman_ok := hpack_huffman_decode(data, out)

		if !huffman_ok {
			return nil, 0, false
		}

		return out[:written], prefix_len + len(data), true
	}

	if len(data) > len(out) {
		return nil, 0, false
	}

	copied := copy(out, data)

	return out[:copied], prefix_len + len(data), true
}
