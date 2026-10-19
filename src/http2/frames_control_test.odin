#+test
package http2

import "core:testing"

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
