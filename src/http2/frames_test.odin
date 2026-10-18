#+test
package http2

import "core:testing"

make_header :: proc(kind: Frame_Type, flags: u8, stream_id: u32, length: u32) -> Frame_Header {
	return Frame_Header {
		length    = length,
		kind      = kind,
		raw_type  = u8(kind),
		flags     = flags,
		stream_id = stream_id,
	}
}

@(test)
test_data_golden_unpadded :: proc(t: ^testing.T) {
	payload := [?]u8{'h', 'e', 'l', 'l', 'o'}

	header := make_header(.Data, FLAG_END_STREAM, 1, u32(len(payload)))

	data, pad, ok := decode_data_payload(header, payload[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, pad, u8(0))
	testing.expect_value(t, len(data), 5)
	testing.expect_value(t, data[0], u8('h'))
	testing.expect_value(t, data[4], u8('o'))

	out: [5]u8
	written, enc_ok := encode_data_payload(payload[:], 0, out[:])
	testing.expect_value(t, enc_ok, true)
	testing.expect_value(t, written, 5)
	testing.expect_value(t, out, payload)
}

@(test)
test_data_golden_padded :: proc(t: ^testing.T) {
	payload := [?]u8{3, 'h', 'i', 0, 0, 0}

	header := make_header(.Data, FLAG_END_STREAM | FLAG_PADDED, 3, u32(len(payload)))

	data, pad, ok := decode_data_payload(header, payload[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, pad, u8(3))
	testing.expect_value(t, len(data), 2)
	testing.expect_value(t, data[0], u8('h'))
	testing.expect_value(t, data[1], u8('i'))

	out: [6]u8
	written, enc_ok := encode_data_payload(data, 3, out[:])
	testing.expect_value(t, enc_ok, true)
	testing.expect_value(t, written, 6)
	testing.expect_value(t, out, payload)
}

@(test)
test_data_round_trip :: proc(t: ^testing.T) {
	fragment := [?]u8{0x88, 0x61, 0x00, 0xff}

	pads := [?]u8{0, 1, 7, 255}

	for pad in pads {
		out: [270]u8

		written, enc_ok := encode_data_payload(fragment[:], pad, out[:])
		testing.expect_value(t, enc_ok, true)

		flags := u8(0)

		if pad > 0 {
			flags = FLAG_PADDED
		}

		header := make_header(.Data, flags, 5, u32(written))

		data, got_pad, ok := decode_data_payload(header, out[:written])
		testing.expect_value(t, ok, true)
		testing.expect_value(t, got_pad, pad)
		testing.expect_value(t, len(data), len(fragment))

		for i in 0 ..< len(fragment) {
			testing.expect_value(t, data[i], fragment[i])
		}
	}
}

@(test)
test_data_truncation :: proc(t: ^testing.T) {
	full := [?]u8{2, 'a', 'b', 0, 0}

	header := make_header(.Data, FLAG_PADDED, 1, u32(len(full)))

	for n in 0 ..< len(full) {
		_, _, ok := decode_data_payload(header, full[:n])
		testing.expect_value(t, ok, false)
	}
}

@(test)
test_data_rejects_bad_padding :: proc(t: ^testing.T) {
	header := make_header(.Data, FLAG_PADDED, 1, 3)

	over := [?]u8{3, 'a', 'b'}
	_, _, ok := decode_data_payload(header, over[:])
	testing.expect_value(t, ok, false)

	empty_header := make_header(.Data, FLAG_PADDED, 1, 0)
	_, _, empty_ok := decode_data_payload(empty_header, nil)
	testing.expect_value(t, empty_ok, false)
}

@(test)
test_headers_golden_with_priority :: proc(t: ^testing.T) {
	payload := [?]u8{0x80, 0x00, 0x00, 0x01, 0x0f, 0x88}

	header := make_header(.Headers, FLAG_END_HEADERS | FLAG_PRIORITY, 3, u32(len(payload)))

	fragment, priority, present, pad, ok := decode_headers_payload(header, payload[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, present, true)
	testing.expect_value(t, pad, u8(0))
	testing.expect_value(t, priority.exclusive, true)
	testing.expect_value(t, priority.dependency, u32(1))
	testing.expect_value(t, priority.weight, u8(0x0f))
	testing.expect_value(t, len(fragment), 1)
	testing.expect_value(t, fragment[0], u8(0x88))

	want_priority := Headers_Priority {
		exclusive  = true,
		dependency = 1,
		weight     = 0x0f,
	}

	fragment_in := [?]u8{0x88}

	out: [6]u8
	written, enc_ok := encode_headers_payload(fragment_in[:], &want_priority, 0, out[:])
	testing.expect_value(t, enc_ok, true)
	testing.expect_value(t, written, 6)
	testing.expect_value(t, out, payload)
}

@(test)
test_headers_golden_padded_no_priority :: proc(t: ^testing.T) {
	payload := [?]u8{2, 0x88, 0x61, 0, 0}

	header := make_header(.Headers, FLAG_END_HEADERS | FLAG_PADDED, 7, u32(len(payload)))

	fragment, _, present, pad, ok := decode_headers_payload(header, payload[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, present, false)
	testing.expect_value(t, pad, u8(2))
	testing.expect_value(t, len(fragment), 2)
	testing.expect_value(t, fragment[0], u8(0x88))
	testing.expect_value(t, fragment[1], u8(0x61))

	out: [5]u8
	written, enc_ok := encode_headers_payload(fragment, nil, 2, out[:])
	testing.expect_value(t, enc_ok, true)
	testing.expect_value(t, written, 5)
	testing.expect_value(t, out, payload)
}

@(test)
test_headers_truncation :: proc(t: ^testing.T) {
	full := [?]u8{0x80, 0x00, 0x00, 0x01, 0x0f, 0x88}

	header := make_header(.Headers, FLAG_END_HEADERS | FLAG_PRIORITY, 3, u32(len(full)))

	for n in 0 ..< len(full) {
		_, _, _, _, ok := decode_headers_payload(header, full[:n])
		testing.expect_value(t, ok, false)
	}
}

@(test)
test_headers_rejects_priority_short :: proc(t: ^testing.T) {
	payload := [?]u8{0x80, 0x00, 0x00}

	header := make_header(.Headers, FLAG_PRIORITY, 1, u32(len(payload)))

	_, _, _, _, ok := decode_headers_payload(header, payload[:])
	testing.expect_value(t, ok, false)
}

@(test)
test_priority_golden :: proc(t: ^testing.T) {
	payload := [?]u8{0x80, 0x00, 0x00, 0x00, 0x0f}

	header := make_header(.Priority, 0, 9, u32(len(payload)))

	fields, ok := decode_priority_payload(header, payload[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, fields.exclusive, true)
	testing.expect_value(t, fields.dependency, u32(0))
	testing.expect_value(t, fields.weight, u8(0x0f))

	out: [PRIORITY_PAYLOAD_LEN]u8
	testing.expect_value(t, encode_priority_fields(fields, out[:]), true)
	testing.expect_value(t, out, payload)
}

@(test)
test_priority_truncation_and_length :: proc(t: ^testing.T) {
	full := [?]u8{0x00, 0x00, 0x00, 0x01, 0x10}

	for n in 0 ..< len(full) {
		header := make_header(.Priority, 0, 1, u32(n))

		_, ok := decode_priority_payload(header, full[:n])
		testing.expect_value(t, ok, false)
	}

	long := [?]u8{0, 0, 0, 1, 0x10, 0x00}
	long_header := make_header(.Priority, 0, 1, u32(len(long)))
	_, long_ok := decode_priority_payload(long_header, long[:])
	testing.expect_value(t, long_ok, false)
}

@(test)
test_priority_rejects_bad_dependency :: proc(t: ^testing.T) {
	out: [PRIORITY_PAYLOAD_LEN]u8

	bad := Headers_Priority {
		dependency = MAX_STREAM_ID + 1,
	}

	testing.expect_value(t, encode_priority_fields(bad, out[:]), false)
	testing.expect_value(t, encode_priority_fields(Headers_Priority{}, out[:4]), false)
}

@(test)
test_rst_stream_golden :: proc(t: ^testing.T) {
	payload := [?]u8{0, 0, 0, 8}

	header := make_header(.Rst_Stream, 0, 5, u32(len(payload)))

	code, ok := decode_rst_stream_payload(header, payload[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, code, Error_Code.Cancel)

	out: [RST_STREAM_PAYLOAD_LEN]u8
	testing.expect_value(t, encode_rst_stream_payload(.Cancel, out[:]), true)
	testing.expect_value(t, out, payload)
}

@(test)
test_rst_stream_unknown_code_round_trip :: proc(t: ^testing.T) {
	out: [RST_STREAM_PAYLOAD_LEN]u8
	testing.expect_value(t, encode_rst_stream_payload(Error_Code(0xdeadbeef), out[:]), true)

	header := make_header(.Rst_Stream, 0, 1, u32(len(out)))

	code, ok := decode_rst_stream_payload(header, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, code, Error_Code(0xdeadbeef))
}

@(test)
test_rst_stream_truncation :: proc(t: ^testing.T) {
	full := [?]u8{0, 0, 0, 8}

	for n in 0 ..< len(full) {
		header := make_header(.Rst_Stream, 0, 1, u32(n))

		_, ok := decode_rst_stream_payload(header, full[:n])
		testing.expect_value(t, ok, false)
	}
}

@(test)
test_settings_golden_single_entry :: proc(t: ^testing.T) {
	payload := [?]u8{0, 4, 0, 0, 0xff, 0xff}

	header := make_header(.Settings, 0, 0, u32(len(payload)))

	storage: [4]Settings_Parameter
	count, ok := decode_settings_payload(header, payload[:], storage[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, count, 1)
	testing.expect_value(t, storage[0].id, SETTINGS_INITIAL_WINDOW_SIZE)
	testing.expect_value(t, storage[0].value, u32(65535))

	out: [6]u8
	params := [?]Settings_Parameter{{SETTINGS_INITIAL_WINDOW_SIZE, 65535}}
	written, enc_ok := encode_settings_payload(params[:], out[:])
	testing.expect_value(t, enc_ok, true)
	testing.expect_value(t, written, 6)
	testing.expect_value(t, out, payload)
}

@(test)
test_settings_empty_ack_golden :: proc(t: ^testing.T) {
	header := make_header(.Settings, SETTINGS_ACK, 0, 0)

	storage: [1]Settings_Parameter
	count, ok := decode_settings_payload(header, nil, storage[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, count, 0)

	out: [1]u8
	written, enc_ok := encode_settings_payload(nil, out[:])
	testing.expect_value(t, enc_ok, true)
	testing.expect_value(t, written, 0)
}

@(test)
test_settings_ack_with_payload_rejected :: proc(t: ^testing.T) {
	payload := [?]u8{0, 4, 0, 0, 0xff, 0xff}

	header := make_header(.Settings, SETTINGS_ACK, 0, u32(len(payload)))

	storage: [4]Settings_Parameter
	_, ok := decode_settings_payload(header, payload[:], storage[:])
	testing.expect_value(t, ok, false)
}

@(test)
test_settings_rejects_odd_length :: proc(t: ^testing.T) {
	payload := [?]u8{0, 4, 0, 0, 0xff}

	header := make_header(.Settings, 0, 0, u32(len(payload)))

	storage: [4]Settings_Parameter
	_, ok := decode_settings_payload(header, payload[:], storage[:])
	testing.expect_value(t, ok, false)
}

@(test)
test_settings_rejects_small_output :: proc(t: ^testing.T) {
	payload := [?]u8{0, 4, 0, 0, 0, 1, 0, 3, 0, 0, 0, 100}

	header := make_header(.Settings, 0, 0, u32(len(payload)))

	small: [1]Settings_Parameter
	_, ok := decode_settings_payload(header, payload[:], small[:])
	testing.expect_value(t, ok, false)

	tiny: [5]u8
	params := [?]Settings_Parameter{{SETTINGS_INITIAL_WINDOW_SIZE, 1}}
	_, enc_ok := encode_settings_payload(params[:], tiny[:])
	testing.expect_value(t, enc_ok, false)
}

@(test)
test_settings_value_bounds :: proc(t: ^testing.T) {
	testing.expect_value(t, settings_parameter_valid(SETTINGS_ENABLE_PUSH, 0), true)
	testing.expect_value(t, settings_parameter_valid(SETTINGS_ENABLE_PUSH, 1), true)
	testing.expect_value(t, settings_parameter_valid(SETTINGS_ENABLE_PUSH, 2), false)

	testing.expect_value(t, settings_parameter_valid(SETTINGS_INITIAL_WINDOW_SIZE, 0), true)
	testing.expect_value(t, settings_parameter_valid(SETTINGS_INITIAL_WINDOW_SIZE, 0x7fffffff), true)
	testing.expect_value(t, settings_parameter_valid(SETTINGS_INITIAL_WINDOW_SIZE, 0x80000000), false)
	testing.expect_value(t, settings_parameter_valid(SETTINGS_INITIAL_WINDOW_SIZE, 0xffffffff), false)

	testing.expect_value(t, settings_parameter_valid(SETTINGS_MAX_FRAME_SIZE, 16383), false)
	testing.expect_value(t, settings_parameter_valid(SETTINGS_MAX_FRAME_SIZE, 16384), true)
	testing.expect_value(t, settings_parameter_valid(SETTINGS_MAX_FRAME_SIZE, 16777215), true)
	testing.expect_value(t, settings_parameter_valid(SETTINGS_MAX_FRAME_SIZE, 16777216), false)

	testing.expect_value(t, settings_parameter_valid(SETTINGS_HEADER_TABLE_SIZE, 0xffffffff), true)
	testing.expect_value(t, settings_parameter_valid(SETTINGS_MAX_CONCURRENT_STREAMS, 0xffffffff), true)
	testing.expect_value(t, settings_parameter_valid(SETTINGS_MAX_HEADER_LIST_SIZE, 0xffffffff), true)
	testing.expect_value(t, settings_parameter_valid(0x9, 0xffffffff), true)
}

@(test)
test_settings_encode_rejects_out_of_range :: proc(t: ^testing.T) {
	out: [12]u8

	push := [?]Settings_Parameter{{SETTINGS_ENABLE_PUSH, 2}}
	_, ok := encode_settings_payload(push[:], out[:])
	testing.expect_value(t, ok, false)

	initial := [?]Settings_Parameter{{SETTINGS_INITIAL_WINDOW_SIZE, 0x80000000}}
	_, initial_ok := encode_settings_payload(initial[:], out[:])
	testing.expect_value(t, initial_ok, false)

	frame_size := [?]Settings_Parameter{{SETTINGS_MAX_FRAME_SIZE, 16383}}
	_, frame_ok := encode_settings_payload(frame_size[:], out[:])
	testing.expect_value(t, frame_ok, false)
}

@(test)
test_settings_decode_rejects_out_of_range :: proc(t: ^testing.T) {
	payload := [?]u8{0, 2, 0, 0, 0, 5}

	header := make_header(.Settings, 0, 0, u32(len(payload)))

	storage: [2]Settings_Parameter
	_, ok := decode_settings_payload(header, payload[:], storage[:])
	testing.expect_value(t, ok, false)
}

@(test)
test_settings_truncation :: proc(t: ^testing.T) {
	full := [?]u8{0, 4, 0, 0, 0xff, 0xff, 0, 3, 0, 0, 0, 100}

	storage: [4]Settings_Parameter

	for n in 0 ..< len(full) {
		header := make_header(.Settings, 0, 0, u32(n))

		count, ok := decode_settings_payload(header, full[:n], storage[:])

		if n % SETTINGS_ENTRY_LEN == 0 {
			testing.expect_value(t, ok, true)
			testing.expect_value(t, count, n / SETTINGS_ENTRY_LEN)
		} else {
			testing.expect_value(t, ok, false)
		}
	}
}

@(test)
test_ping_golden :: proc(t: ^testing.T) {
	opaque := [8]u8{1, 2, 3, 4, 5, 6, 7, 8}

	out: [PING_PAYLOAD_LEN]u8
	testing.expect_value(t, encode_ping_payload(opaque, out[:]), true)
	testing.expect_value(t, out, opaque)

	header := make_header(.Ping, 0, 0, PING_PAYLOAD_LEN)

	got, ok := decode_ping_payload(header, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, got, opaque)
}

@(test)
test_ping_truncation_and_length :: proc(t: ^testing.T) {
	full := [?]u8{9, 9, 9, 9, 9, 9, 9, 9}

	for n in 0 ..< len(full) {
		header := make_header(.Ping, 0, 0, u32(n))

		_, ok := decode_ping_payload(header, full[:n])
		testing.expect_value(t, ok, false)
	}

	short_out: [7]u8
	testing.expect_value(t, encode_ping_payload(full, short_out[:]), false)

	long := [?]u8{9, 9, 9, 9, 9, 9, 9, 9, 9}
	long_header := make_header(.Ping, 0, 0, u32(len(long)))
	_, long_ok := decode_ping_payload(long_header, long[:])
	testing.expect_value(t, long_ok, false)
}

@(test)
test_goaway_golden :: proc(t: ^testing.T) {
	payload := [?]u8{0, 0, 0, 2, 0, 0, 0, 11, 't', 'o', 'o'}

	header := make_header(.GoAway, 0, 0, u32(len(payload)))

	last, code, debug, ok := decode_goaway_payload(header, payload[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, last, u32(2))
	testing.expect_value(t, code, Error_Code.Enhance_Your_Calm)
	testing.expect_value(t, len(debug), 3)
	testing.expect_value(t, debug[0], u8('t'))

	out: [11]u8
	debug_in := [?]u8{'t', 'o', 'o'}
	written, enc_ok := encode_goaway_payload(2, .Enhance_Your_Calm, debug_in[:], out[:])
	testing.expect_value(t, enc_ok, true)
	testing.expect_value(t, written, 11)
	testing.expect_value(t, out, payload)
}

@(test)
test_goaway_empty_debug_golden :: proc(t: ^testing.T) {
	payload := [?]u8{0, 0, 0, 0, 0, 0, 0, 0}

	header := make_header(.GoAway, 0, 0, u32(len(payload)))

	last, code, debug, ok := decode_goaway_payload(header, payload[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, last, u32(0))
	testing.expect_value(t, code, Error_Code.No_Error)
	testing.expect_value(t, len(debug), 0)
}

@(test)
test_goaway_truncation :: proc(t: ^testing.T) {
	full := [?]u8{0, 0, 0, 2, 0, 0, 0, 11, 'x'}

	for n in 0 ..< len(full) {
		header := make_header(.GoAway, 0, 0, u32(n))

		_, _, _, ok := decode_goaway_payload(header, full[:n])

		if n < GOAWAY_FIXED_LEN {
			testing.expect_value(t, ok, false)
		} else {
			testing.expect_value(t, ok, true)
		}
	}
}

@(test)
test_goaway_masks_reserved_bit :: proc(t: ^testing.T) {
	payload := [?]u8{0x80, 0x00, 0x00, 0x02, 0, 0, 0, 0}

	header := make_header(.GoAway, 0, 0, u32(len(payload)))

	last, _, _, ok := decode_goaway_payload(header, payload[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, last, u32(2))
}

@(test)
test_goaway_rejects_bad_last_stream :: proc(t: ^testing.T) {
	out: [8]u8

	_, ok := encode_goaway_payload(MAX_STREAM_ID + 1, .No_Error, nil, out[:])
	testing.expect_value(t, ok, false)

	_, short_ok := encode_goaway_payload(0, .No_Error, nil, out[:7])
	testing.expect_value(t, short_ok, false)
}

@(test)
test_window_update_golden :: proc(t: ^testing.T) {
	payload := [?]u8{0, 0, 4, 0}

	header := make_header(.Window_Update, 0, 0, u32(len(payload)))

	increment, ok := decode_window_update_payload(header, payload[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, increment, u32(1024))

	out: [WINDOW_UPDATE_PAYLOAD_LEN]u8
	testing.expect_value(t, encode_window_update_payload(1024, out[:]), true)
	testing.expect_value(t, out, payload)
}

@(test)
test_window_update_stream_and_connection :: proc(t: ^testing.T) {
	payload := [?]u8{0, 0, 0, 1}

	conn_header := make_header(.Window_Update, 0, 0, u32(len(payload)))
	_, conn_ok := decode_window_update_payload(conn_header, payload[:])
	testing.expect_value(t, conn_ok, true)

	stream_header := make_header(.Window_Update, 0, 3, u32(len(payload)))
	_, stream_ok := decode_window_update_payload(stream_header, payload[:])
	testing.expect_value(t, stream_ok, true)
}

@(test)
test_window_update_rejects_zero :: proc(t: ^testing.T) {
	out: [WINDOW_UPDATE_PAYLOAD_LEN]u8
	testing.expect_value(t, encode_window_update_payload(0, out[:]), false)
	testing.expect_value(t, encode_window_update_payload(MAX_WINDOW_INCREMENT + 1, out[:]), false)
	testing.expect_value(t, encode_window_update_payload(MAX_WINDOW_INCREMENT, out[:]), true)

	zero := [?]u8{0, 0, 0, 0}
	zero_header := make_header(.Window_Update, 0, 0, u32(len(zero)))
	_, zero_ok := decode_window_update_payload(zero_header, zero[:])
	testing.expect_value(t, zero_ok, false)

	reserved := [?]u8{0x80, 0, 0, 0}
	reserved_header := make_header(.Window_Update, 0, 0, u32(len(reserved)))
	_, reserved_ok := decode_window_update_payload(reserved_header, reserved[:])
	testing.expect_value(t, reserved_ok, false)
}

@(test)
test_window_update_truncation_and_length :: proc(t: ^testing.T) {
	full := [?]u8{0, 0, 4, 0}

	for n in 0 ..< len(full) {
		header := make_header(.Window_Update, 0, 0, u32(n))

		_, ok := decode_window_update_payload(header, full[:n])
		testing.expect_value(t, ok, false)
	}

	long := [?]u8{0, 0, 4, 0, 0}
	long_header := make_header(.Window_Update, 0, 0, u32(len(long)))
	_, long_ok := decode_window_update_payload(long_header, long[:])
	testing.expect_value(t, long_ok, false)

	short_out: [3]u8
	testing.expect_value(t, encode_window_update_payload(1, short_out[:]), false)
}

@(test)
test_continuation_round_trip :: proc(t: ^testing.T) {
	fragment := [?]u8{0x40, 0x61, 0x62}

	header := make_header(.Continuation, FLAG_END_HEADERS, 1, u32(len(fragment)))

	got, ok := decode_continuation_payload(header, fragment[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, len(got), 3)

	out: [3]u8
	written, enc_ok := encode_continuation_payload(fragment[:], out[:])
	testing.expect_value(t, enc_ok, true)
	testing.expect_value(t, written, 3)
	testing.expect_value(t, out, fragment)
}

@(test)
test_continuation_empty_is_legal :: proc(t: ^testing.T) {
	header := make_header(.Continuation, FLAG_END_HEADERS, 1, 0)

	got, ok := decode_continuation_payload(header, nil)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, len(got), 0)
}

@(test)
test_stream_zero_rules :: proc(t: ^testing.T) {
	data_payload := [?]u8{'x'}
	data_header := make_header(.Data, 0, 0, 1)
	_, _, data_ok := decode_data_payload(data_header, data_payload[:])
	testing.expect_value(t, data_ok, false)

	headers_header := make_header(.Headers, FLAG_END_HEADERS, 0, 1)
	fragment := [?]u8{0x88}
	_, _, _, _, headers_ok := decode_headers_payload(headers_header, fragment[:])
	testing.expect_value(t, headers_ok, false)

	priority_payload := [?]u8{0, 0, 0, 1, 1}
	priority_header := make_header(.Priority, 0, 0, 5)
	_, priority_ok := decode_priority_payload(priority_header, priority_payload[:])
	testing.expect_value(t, priority_ok, false)

	rst_payload := [?]u8{0, 0, 0, 8}
	rst_header := make_header(.Rst_Stream, 0, 0, 4)
	_, rst_ok := decode_rst_stream_payload(rst_header, rst_payload[:])
	testing.expect_value(t, rst_ok, false)

	cont_header := make_header(.Continuation, FLAG_END_HEADERS, 0, 1)
	_, cont_ok := decode_continuation_payload(cont_header, data_payload[:])
	testing.expect_value(t, cont_ok, false)

	settings_payload := [?]u8{0, 4, 0, 0, 0, 1}
	settings_header := make_header(.Settings, 0, 1, 6)
	storage: [2]Settings_Parameter
	_, settings_ok := decode_settings_payload(settings_header, settings_payload[:], storage[:])
	testing.expect_value(t, settings_ok, false)

	ping_payload := [?]u8{1, 2, 3, 4, 5, 6, 7, 8}
	ping_header := make_header(.Ping, 0, 1, 8)
	_, ping_ok := decode_ping_payload(ping_header, ping_payload[:])
	testing.expect_value(t, ping_ok, false)

	goaway_payload := [?]u8{0, 0, 0, 0, 0, 0, 0, 0}
	goaway_header := make_header(.GoAway, 0, 1, 8)
	_, _, _, goaway_ok := decode_goaway_payload(goaway_header, goaway_payload[:])
	testing.expect_value(t, goaway_ok, false)
}

@(test)
test_wrong_kind_rejected :: proc(t: ^testing.T) {
	payload := [?]u8{0, 0, 0, 8}

	ping_as_data := make_header(.Ping, 0, 1, 4)
	_, _, ok := decode_data_payload(ping_as_data, payload[:])
	testing.expect_value(t, ok, false)

	data_as_rst := make_header(.Data, 0, 1, 4)
	_, rst_ok := decode_rst_stream_payload(data_as_rst, payload[:])
	testing.expect_value(t, rst_ok, false)

	settings_as_ping := make_header(.Settings, 0, 0, 4)
	short := [?]u8{1, 2, 3, 4}
	_, ping_ok := decode_ping_payload(settings_as_ping, short[:])
	testing.expect_value(t, ping_ok, false)
}

@(test)
test_length_mismatch_rejected :: proc(t: ^testing.T) {
	payload := [?]u8{0, 0, 0, 8}

	short_header := make_header(.Rst_Stream, 0, 1, 3)
	_, short_ok := decode_rst_stream_payload(short_header, payload[:])
	testing.expect_value(t, short_ok, false)

	long_header := make_header(.Rst_Stream, 0, 1, 5)
	_, long_ok := decode_rst_stream_payload(long_header, payload[:])
	testing.expect_value(t, long_ok, false)

	window_header := make_header(.Window_Update, 0, 0, 5)
	_, window_ok := decode_window_update_payload(window_header, payload[:])
	testing.expect_value(t, window_ok, false)

	cont_header := make_header(.Continuation, FLAG_END_HEADERS, 1, 5)
	_, cont_ok := decode_continuation_payload(cont_header, payload[:3])
	testing.expect_value(t, cont_ok, false)
}

@(test)
test_encode_rejects_short_output :: proc(t: ^testing.T) {
	data := [?]u8{1, 2, 3}

	small: [2]u8
	_, ok := encode_data_payload(data[:], 0, small[:])
	testing.expect_value(t, ok, false)

	_, pad_ok := encode_data_payload(data[:], 4, small[:])
	testing.expect_value(t, pad_ok, false)

	fragment := [?]u8{0x88}
	_, headers_ok := encode_headers_payload(fragment[:], nil, 0, small[:0])
	testing.expect_value(t, headers_ok, false)

	priority := Headers_Priority {
		dependency = 1,
	}

	tiny: [5]u8
	_, priority_ok := encode_headers_payload(fragment[:], &priority, 0, tiny[:])
	testing.expect_value(t, priority_ok, false)

	rst: [3]u8
	testing.expect_value(t, encode_rst_stream_payload(.Cancel, rst[:]), false)

	debug := [?]u8{'x'}
	goaway: [8]u8
	_, goaway_ok := encode_goaway_payload(0, .No_Error, debug[:], goaway[:])
	testing.expect_value(t, goaway_ok, false)

	cont: [1]u8
	_, cont_ok := encode_continuation_payload(data[:], cont[:])
	testing.expect_value(t, cont_ok, false)
}
