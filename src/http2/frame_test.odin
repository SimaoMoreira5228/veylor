#+test
package http2

import "core:testing"

@(test)
test_frame_header_golden_bytes :: proc(t: ^testing.T) {
	raw := [?]u8{0, 0, 0, 0, 0x1, 0, 0, 0, 1}

	header, ok := parse_frame_header(raw[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, header.length, u32(0))
	testing.expect_value(t, header.kind, Frame_Type.Data)
	testing.expect_value(t, header.raw_type, u8(0))
	testing.expect_value(t, header.flags, u8(0x1))
	testing.expect_value(t, header.stream_id, u32(1))

	out: [FRAME_HEADER_LEN]u8
	testing.expect_value(t, encode_frame_header(header, out[:]), true)
	testing.expect_value(t, out, raw)
}

@(test)
test_frame_header_round_trip :: proc(t: ^testing.T) {
	kinds := [?]Frame_Type {
		.Data,
		.Headers,
		.Priority,
		.Rst_Stream,
		.Settings,
		.Push_Promise,
		.Ping,
		.GoAway,
		.Window_Update,
		.Continuation,
	}

	lengths := [?]u32{0, 1, 255, 256, 16383, 16384}
	flags := [?]u8{0, 1, 0xff}
	stream_ids := [?]u32{0, 1, 100, 0x7fffffff}

	for kind in kinds {
		for length in lengths {
			for flag in flags {
				for stream_id in stream_ids {
					want := Frame_Header {
						length    = length,
						kind      = kind,
						raw_type  = u8(kind),
						flags     = flag,
						stream_id = stream_id,
					}

					out: [FRAME_HEADER_LEN]u8
					testing.expect_value(t, encode_frame_header(want, out[:]), true)

					got, ok := parse_frame_header(out[:])
					testing.expect_value(t, ok, true)
					testing.expect_value(t, got, want)
				}
			}
		}
	}
}

@(test)
test_frame_header_reads_first_nine_bytes :: proc(t: ^testing.T) {
	raw := [?]u8{0, 0, 5, 1, 0x4, 0, 0, 0, 7, 9, 9, 9}

	header, ok := parse_frame_header(raw[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, header.length, u32(5))
	testing.expect_value(t, header.kind, Frame_Type.Headers)
	testing.expect_value(t, header.flags, u8(0x4))
	testing.expect_value(t, header.stream_id, u32(7))
}

@(test)
test_frame_header_rejects_short_input :: proc(t: ^testing.T) {
	raw := [?]u8{0, 0, 0, 0, 0, 0, 0, 0, 1}

	for width in 0 ..< FRAME_HEADER_LEN {
		_, ok := parse_frame_header(raw[:width])
		testing.expect_value(t, ok, false)
	}
}

@(test)
test_frame_header_rejects_reserved_bit :: proc(t: ^testing.T) {
	raw := [?]u8{0, 0, 0, 0, 0, 0x80, 0, 0, 1}

	_, ok := parse_frame_header(raw[:])
	testing.expect_value(t, ok, false)

	reserved := Frame_Header {
		kind      = .Ping,
		stream_id = 0x80000000,
	}
	out: [FRAME_HEADER_LEN]u8
	testing.expect_value(t, encode_frame_header(reserved, out[:]), false)

	reserved_max := Frame_Header {
		kind      = .Ping,
		stream_id = 0xffffffff,
	}
	testing.expect_value(t, encode_frame_header(reserved_max, out[:]), false)
}

@(test)
test_frame_header_rejects_oversize_length :: proc(t: ^testing.T) {
	over := [?]u8{0, 64, 1, 0, 0, 0, 0, 0, 1}
	_, ok := parse_frame_header(over[:])
	testing.expect_value(t, ok, false)

	huge := [?]u8{0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 1}
	_, ok = parse_frame_header(huge[:])
	testing.expect_value(t, ok, false)

	too_big := Frame_Header {
		kind   = .Data,
		length = 0x1000000,
	}
	out: [FRAME_HEADER_LEN]u8
	testing.expect_value(t, encode_frame_header(too_big, out[:]), false)
}

@(test)
test_frame_header_preserves_unknown_type :: proc(t: ^testing.T) {
	raw := [?]u8{0, 0, 0, 10, 0, 0, 0, 0, 0}

	header, ok := parse_frame_header(raw[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, header.kind, Frame_Type.Unknown)
	testing.expect_value(t, header.raw_type, u8(10))

	out: [FRAME_HEADER_LEN]u8
	testing.expect_value(t, encode_frame_header(header, out[:]), true)
	testing.expect_value(t, out, raw)

	raw_ff := [?]u8{0, 0, 0, 0xff, 0, 0, 0, 0, 0}

	header_ff, ok_ff := parse_frame_header(raw_ff[:])
	testing.expect_value(t, ok_ff, true)
	testing.expect_value(t, header_ff.kind, Frame_Type.Unknown)
	testing.expect_value(t, header_ff.raw_type, u8(0xff))

	testing.expect_value(t, encode_frame_header(header_ff, out[:]), true)
	testing.expect_value(t, out, raw_ff)
}

@(test)
test_frame_header_rejects_short_output :: proc(t: ^testing.T) {
	header := Frame_Header {
		kind      = .Settings,
		stream_id = 0,
	}

	out: [FRAME_HEADER_LEN]u8

	for width in 0 ..< FRAME_HEADER_LEN {
		testing.expect_value(t, encode_frame_header(header, out[:width]), false)
	}
}
