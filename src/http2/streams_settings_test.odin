#+test
package http2

import "core:testing"

@(test)
test_settings_ack_pairs_with_sent :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	ack := make_header(.Settings, SETTINGS_ACK, 0, 0)

	testing.expect_value(
		t,
		streams_on_frame(&table, ack, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	testing.expect_value(t, settings_track_sent(&table), true)
	testing.expect_value(t, table.unacked_settings, u32(1))

	testing.expect_value(
		t,
		streams_on_frame(&table, ack, nil, .Received),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(t, table.unacked_settings, u32(0))

	testing.expect_value(
		t,
		streams_on_frame(&table, ack, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)
}

@(test)
test_settings_ack_payload_frame_size_error :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	testing.expect_value(t, settings_track_sent(&table), true)

	bad := make_header(.Settings, SETTINGS_ACK, 0, 6)
	testing.expect_value(
		t,
		streams_on_frame(&table, bad, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Frame_Size_Error},
	)

	testing.expect_value(t, table.unacked_settings, u32(1))
}

@(test)
test_settings_remote_bounds_rejected :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	push := [?]Settings_Parameter{{SETTINGS_ENABLE_PUSH, 2}}
	testing.expect_value(
		t,
		settings_remote_apply(&table, push[:]),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	window := [?]Settings_Parameter{{SETTINGS_INITIAL_WINDOW_SIZE, 0x80000000}}
	testing.expect_value(
		t,
		settings_remote_apply(&table, window[:]),
		Stream_Action{kind = .Connection_Error, code = .Flow_Control_Error},
	)

	frame_size := [?]Settings_Parameter{{SETTINGS_MAX_FRAME_SIZE, 16383}}
	testing.expect_value(
		t,
		settings_remote_apply(&table, frame_size[:]),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	unknown := [?]Settings_Parameter{{u16(0x9), 12345}}
	testing.expect_value(
		t,
		settings_remote_apply(&table, unknown[:]),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(t, table.remote.initial_window, u32(65535))

	bad_local := [?]Settings_Parameter{{SETTINGS_ENABLE_PUSH, 7}}
	testing.expect_value(t, settings_local_apply(&table, bad_local[:]), false)
}

@(test)
test_settings_window_change_propagates :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	first := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, first, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	second := make_header(.Headers, FLAG_END_HEADERS, 3, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, second, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	grow := [?]Settings_Parameter{{SETTINGS_INITIAL_WINDOW_SIZE, 100000}}
	testing.expect_value(t, settings_remote_apply(&table, grow[:]), Stream_Action{kind = .Accept})

	index1, _ := streams_find(&table, 1)
	index3, _ := streams_find(&table, 3)

	testing.expect_value(t, table.slots[index1].send_window.value, i64(100000))
	testing.expect_value(t, table.slots[index3].send_window.value, i64(100000))
	testing.expect_value(t, table.slots[index1].recv_window.value, i64(65535))

	shrink := [?]Settings_Parameter{{SETTINGS_INITIAL_WINDOW_SIZE, 100}}
	testing.expect_value(
		t,
		settings_remote_apply(&table, shrink[:]),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(t, table.slots[index1].send_window.value, i64(100))

	negative: Streams_Table
	neg_slots: [8]Stream
	neg_buf: [256]u8
	negative = make_stream_table(neg_slots[:], neg_buf[:], .Server)

	neg_open := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&negative, neg_open, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	neg_index, _ := streams_find(&negative, 1)
	negative.slots[neg_index].send_window.value = 10

	testing.expect_value(
		t,
		settings_remote_apply(&negative, shrink[:]),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(
		t,
		negative.slots[neg_index].send_window.value,
		i64(10) - i64(65535) + i64(100),
	)
}

@(test)
test_settings_window_overflow_connection_error :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	open := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	index, _ := streams_find(&table, 1)

	testing.expect_value(
		t,
		flow_window_add(&table.slots[index].send_window, u32(FLOW_MAX_WINDOW_SIZE) - 65535),
		true,
	)

	bump := [?]Settings_Parameter{{SETTINGS_INITIAL_WINDOW_SIZE, 65536}}
	testing.expect_value(
		t,
		settings_remote_apply(&table, bump[:]),
		Stream_Action{kind = .Connection_Error, code = .Flow_Control_Error},
	)

	testing.expect_value(t, table.remote.initial_window, u32(65535))
	testing.expect_value(t, table.slots[index].send_window.value, FLOW_MAX_WINDOW_SIZE)
}

@(test)
test_settings_new_stream_uses_current_windows :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	remote := [?]Settings_Parameter{{SETTINGS_INITIAL_WINDOW_SIZE, 100000}}
	testing.expect_value(
		t,
		settings_remote_apply(&table, remote[:]),
		Stream_Action{kind = .Accept},
	)

	local := [?]Settings_Parameter{{SETTINGS_INITIAL_WINDOW_SIZE, 100}}
	testing.expect_value(t, settings_local_apply(&table, local[:]), true)

	open := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	index, _ := streams_find(&table, 1)
	testing.expect_value(t, table.slots[index].send_window.value, i64(100000))
	testing.expect_value(t, table.slots[index].recv_window.value, i64(100))
}

@(test)
test_goaway_receive_drain_forbids_new_streams :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	open := make_header(.Headers, FLAG_END_HEADERS | FLAG_END_STREAM, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	payload := [?]u8{0, 0, 0, 1, 0, 0, 0, 0}
	header := make_header(.GoAway, 0, 0, u32(len(payload)))

	testing.expect_value(
		t,
		goaway_on_receive(&table, header, payload[:]),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(t, table.goaway_received, true)
	testing.expect_value(t, table.goaway_recv_last, u32(1))

	fresh := make_header(.Headers, FLAG_END_HEADERS, 5, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, fresh, nil, .Received),
		Stream_Action{kind = .Reject_Protocol_Error},
	)

	testing.expect_value(t, table.used, 1)

	data := make_header(.Data, 0, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, data, nil, .Sent),
		Stream_Action{kind = .Accept},
	)
}

@(test)
test_goaway_send_closes_remote_above_last :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	first := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, first, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	third := make_header(.Headers, FLAG_END_HEADERS, 3, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, third, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	testing.expect_value(t, goaway_on_send(&table, 1), true)

	index1, _ := streams_find(&table, 1)
	index3, _ := streams_find(&table, 3)

	testing.expect_value(t, table.slots[index1].state, Stream_State.Open)
	testing.expect_value(t, table.slots[index3].state, Stream_State.Closed)

	fresh := make_header(.Headers, FLAG_END_HEADERS, 5, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, fresh, nil, .Received),
		Stream_Action{kind = .Send_Rst, code = .Stream_Closed},
	)

	rst := make_header(.Rst_Stream, 0, 5, RST_STREAM_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, rst, nil, .Received),
		Stream_Action{kind = .Ignore},
	)

	testing.expect_value(t, goaway_on_send(&table, MAX_STREAM_ID + 1), false)
}

@(test)
test_goaway_receive_malformed_rejected :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	payload := [?]u8{0, 0, 0, 0, 0, 0, 0, 0}

	wrong_stream := make_header(.GoAway, 0, 1, u32(len(payload)))
	testing.expect_value(
		t,
		goaway_on_receive(&table, wrong_stream, payload[:]),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	short_payload := [?]u8{0, 0, 0, 0}
	short := make_header(.GoAway, 0, 0, u32(len(short_payload)))
	testing.expect_value(
		t,
		goaway_on_receive(&table, short, short_payload[:]),
		Stream_Action{kind = .Connection_Error, code = .Frame_Size_Error},
	)

	testing.expect_value(t, table.goaway_received, false)
}
