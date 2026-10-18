#+test
package http2

import "core:testing"

make_stream_table :: proc(slots: []Stream, buf: []u8, role: H2_Role) -> Streams_Table {
	table: Streams_Table

	streams_init(&table, slots, role, buf, 0)

	return table
}

@(test)
test_streams_lifecycle_remote_close :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	open := make_header(.Headers, FLAG_END_STREAM | FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	index, found := streams_find(&table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(t, table.slots[index].state, Stream_State.Half_Closed_Remote)

	data := make_header(.Data, 0, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, data, nil, .Received),
		Stream_Action{kind = .Send_Rst, code = .Stream_Closed},
	)

	out := make_header(.Data, 0, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, out, nil, .Sent),
		Stream_Action{kind = .Accept},
	)

	fin := make_header(.Data, FLAG_END_STREAM, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, fin, nil, .Sent),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(t, table.slots[index].state, Stream_State.Closed)

	testing.expect_value(
		t,
		streams_on_frame(&table, data, nil, .Received),
		Stream_Action{kind = .Send_Rst, code = .Stream_Closed},
	)

	rst := make_header(.Rst_Stream, 0, 1, RST_STREAM_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, rst, nil, .Received),
		Stream_Action{kind = .Ignore},
	)
}

@(test)
test_streams_lifecycle_local_close :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Client)

	open := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open, nil, .Sent),
		Stream_Action{kind = .Accept},
	)

	index, found := streams_find(&table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(t, table.slots[index].state, Stream_State.Open)

	fin := make_header(.Data, FLAG_END_STREAM, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, fin, nil, .Sent),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(t, table.slots[index].state, Stream_State.Half_Closed_Local)

	late := make_header(.Data, 0, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, late, nil, .Sent),
		Stream_Action{kind = .Send_Rst, code = .Stream_Closed},
	)

	peer_fin := make_header(.Data, FLAG_END_STREAM, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, peer_fin, nil, .Received),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(t, table.slots[index].state, Stream_State.Closed)
}

@(test)
test_streams_rst_closes_either_direction :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	open := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	rst := make_header(.Rst_Stream, 0, 1, RST_STREAM_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, rst, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	index, _ := streams_find(&table, 1)
	testing.expect_value(t, table.slots[index].state, Stream_State.Closed)

	open3 := make_header(.Headers, FLAG_END_HEADERS, 3, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open3, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	rst_out := make_header(.Rst_Stream, 0, 3, RST_STREAM_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, rst_out, nil, .Sent),
		Stream_Action{kind = .Accept},
	)

	index3, _ := streams_find(&table, 3)
	testing.expect_value(t, table.slots[index3].state, Stream_State.Closed)
}

@(test)
test_streams_reserved_push_transitions :: proc(t: ^testing.T) {
	action, next := stream_state_on_frame(.Reserved_Local, .Headers, 0, .Sent)
	testing.expect_value(t, action, Stream_Action{kind = .Accept})
	testing.expect_value(t, next, Stream_State.Half_Closed_Remote)

	fin_action, fin_next := stream_state_on_frame(
		.Reserved_Local,
		.Headers,
		FLAG_END_STREAM,
		.Sent,
	)
	testing.expect_value(t, fin_action, Stream_Action{kind = .Accept})
	testing.expect_value(t, fin_next, Stream_State.Closed)

	rst_action, rst_next := stream_state_on_frame(.Reserved_Local, .Rst_Stream, 0, .Received)
	testing.expect_value(t, rst_action, Stream_Action{kind = .Accept})
	testing.expect_value(t, rst_next, Stream_State.Closed)

	bad_action, _ := stream_state_on_frame(.Reserved_Local, .Data, 0, .Received)
	testing.expect_value(
		t,
		bad_action,
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	peer_action, peer_next := stream_state_on_frame(.Reserved_Remote, .Headers, 0, .Received)
	testing.expect_value(t, peer_action, Stream_Action{kind = .Accept})
	testing.expect_value(t, peer_next, Stream_State.Half_Closed_Local)

	peer_fin, peer_closed := stream_state_on_frame(
		.Reserved_Remote,
		.Headers,
		FLAG_END_STREAM,
		.Received,
	)
	testing.expect_value(t, peer_fin, Stream_Action{kind = .Accept})
	testing.expect_value(t, peer_closed, Stream_State.Closed)

	self_bad, _ := stream_state_on_frame(.Reserved_Remote, .Data, 0, .Sent)
	testing.expect_value(
		t,
		self_bad,
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)
}

@(test)
test_streams_push_promise_rejected_without_push :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	promise := make_header(.Push_Promise, FLAG_END_HEADERS, 2, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, promise, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	testing.expect_value(t, table.used, 0)
}

@(test)
test_streams_closed_rst_ignored_data_errors :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	open := make_header(.Headers, FLAG_END_STREAM | FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	fin := make_header(.Data, FLAG_END_STREAM, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, fin, nil, .Sent),
		Stream_Action{kind = .Accept},
	)

	index, _ := streams_find(&table, 1)
	testing.expect_value(t, table.slots[index].state, Stream_State.Closed)

	data := make_header(.Data, 0, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, data, nil, .Received),
		Stream_Action{kind = .Send_Rst, code = .Stream_Closed},
	)

	headers := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, headers, nil, .Received),
		Stream_Action{kind = .Send_Rst, code = .Stream_Closed},
	)

	rst := make_header(.Rst_Stream, 0, 1, RST_STREAM_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, rst, nil, .Received),
		Stream_Action{kind = .Ignore},
	)

	window := make_header(.Window_Update, 0, 1, WINDOW_UPDATE_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, window, nil, .Received),
		Stream_Action{kind = .Ignore},
	)

	priority := make_header(.Priority, 0, 1, PRIORITY_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, priority, nil, .Received),
		Stream_Action{kind = .Accept},
	)
}

@(test)
test_streams_unknown_frame_ignored :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	open := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	kind, raw := frame_type_from_byte(0x0b)
	testing.expect_value(t, kind, Frame_Type.Unknown)

	mystery := Frame_Header {
		length    = 0,
		kind      = kind,
		raw_type  = raw,
		flags     = 0,
		stream_id = 1,
	}

	testing.expect_value(
		t,
		streams_on_frame(&table, mystery, nil, .Received),
		Stream_Action{kind = .Ignore},
	)

	index, _ := streams_find(&table, 1)
	testing.expect_value(t, table.slots[index].state, Stream_State.Open)

	conn_mystery := Frame_Header {
		length    = 0,
		kind      = kind,
		raw_type  = raw,
		flags     = 0,
		stream_id = 0,
	}

	testing.expect_value(
		t,
		streams_on_frame(&table, conn_mystery, nil, .Received),
		Stream_Action{kind = .Ignore},
	)
}

@(test)
test_streams_zero_stream_id_rejected :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	data := make_header(.Data, 0, 0, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, data, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	headers := make_header(.Headers, FLAG_END_HEADERS, 0, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, headers, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	priority := make_header(.Priority, 0, 0, PRIORITY_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, priority, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	settings := make_header(.Settings, 0, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, settings, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)
}

@(test)
test_streams_connection_window_update_accepted :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	conn := make_header(.Window_Update, 0, 0, WINDOW_UPDATE_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, conn, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	short := make_header(.Window_Update, 0, 0, 3)
	testing.expect_value(
		t,
		streams_on_frame(&table, short, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Frame_Size_Error},
	)
}

@(test)
test_streams_idle_implies_closed :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	open5 := make_header(.Headers, FLAG_END_HEADERS, 5, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open5, nil, .Received),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(t, table.highest_remote, u32(5))

	retry := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, retry, nil, .Received),
		Stream_Action{kind = .Send_Rst, code = .Stream_Closed},
	)

	testing.expect_value(t, table.used, 1)

	rst := make_header(.Rst_Stream, 0, 1, RST_STREAM_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, rst, nil, .Received),
		Stream_Action{kind = .Ignore},
	)

	data := make_header(.Data, 0, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, data, nil, .Received),
		Stream_Action{kind = .Send_Rst, code = .Stream_Closed},
	)
}

@(test)
test_streams_max_concurrent_and_capacity :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	limit := [?]Settings_Parameter{{SETTINGS_MAX_CONCURRENT_STREAMS, 1}}
	testing.expect_value(t, settings_local_apply(&table, limit[:]), true)

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
		Stream_Action{kind = .Send_Rst, code = .Refused_Stream},
	)

	testing.expect_value(t, table.used, 1)

	rst := make_header(.Rst_Stream, 0, 1, RST_STREAM_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, rst, nil, .Received),
		Stream_Action{kind = .Accept},
	)

	third := make_header(.Headers, FLAG_END_HEADERS, 3, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, third, nil, .Received),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(t, table.used, 2)
}

@(test)
test_streams_capacity_bound_enforced :: proc(t: ^testing.T) {
	slots: [1]Stream
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
		Stream_Action{kind = .Send_Rst, code = .Refused_Stream},
	)
}

@(test)
test_streams_window_init_failure_is_flow_control_error :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	table.remote.initial_window = max(u32)

	open := make_header(.Headers, FLAG_END_HEADERS, 1, 0)
	testing.expect_value(
		t,
		streams_on_frame(&table, open, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Flow_Control_Error},
	)

	testing.expect_value(t, table.used, 0)
}

@(test)
test_continuation_golden_split_reassembly :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	full := [?]u8 {
		0x82,
		0x86,
		0x84,
		0x41,
		0x0f,
		0x77,
		0x77,
		0x77,
		0x2e,
		0x65,
		0x78,
		0x61,
		0x6d,
		0x70,
		0x6c,
		0x65,
		0x2e,
		0x63,
		0x6f,
		0x6d,
	}

	head := make_header(.Headers, FLAG_END_STREAM, 1, 8)
	testing.expect_value(
		t,
		streams_on_frame(&table, head, full[:8], .Received),
		Stream_Action{kind = .Accept},
	)

	index, _ := streams_find(&table, 1)
	testing.expect_value(t, table.slots[index].state, Stream_State.Half_Closed_Remote)
	testing.expect_value(t, table.assembler.active, true)

	middle := make_header(.Continuation, 0, 1, 8)
	testing.expect_value(
		t,
		streams_on_frame(&table, middle, full[8:16], .Received),
		Stream_Action{kind = .Accept},
	)

	testing.expect_value(t, table.assembler.active, true)

	tail := make_header(.Continuation, FLAG_END_HEADERS, 1, 4)
	testing.expect_value(
		t,
		streams_on_frame(&table, tail, full[16:], .Received),
		Stream_Action{kind = .Accept},
	)

	testing.expect_value(t, table.assembler.active, false)
	testing.expect_value(t, table.slots[index].state, Stream_State.Half_Closed_Remote)

	block := assembler_block(&table.assembler)
	testing.expect_value(t, len(block), 20)

	for i in 0 ..< len(block) {
		testing.expect_value(t, block[i], full[i])
	}
}

@(test)
test_continuation_interleaved_frame_rejected :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	fragment := [?]u8{0x82, 0x86}

	head := make_header(.Headers, 0, 1, 2)
	testing.expect_value(
		t,
		streams_on_frame(&table, head, fragment[:], .Received),
		Stream_Action{kind = .Accept},
	)

	other := make_header(.Headers, FLAG_END_HEADERS, 3, 1)
	other_fragment := [?]u8{0x88}
	testing.expect_value(
		t,
		streams_on_frame(&table, other, other_fragment[:], .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	ping := make_header(.Ping, 0, 0, PING_PAYLOAD_LEN)
	testing.expect_value(
		t,
		streams_on_frame(&table, ping, nil, .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	stray := make_header(.Continuation, FLAG_END_HEADERS, 3, 1)
	testing.expect_value(
		t,
		streams_on_frame(&table, stray, other_fragment[:], .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)

	tail := make_header(.Continuation, FLAG_END_HEADERS, 1, 1)
	testing.expect_value(
		t,
		streams_on_frame(&table, tail, other_fragment[:], .Received),
		Stream_Action{kind = .Accept},
	)
	testing.expect_value(t, table.assembler.active, false)
}

@(test)
test_continuation_without_assembly_rejected :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [256]u8
	table := make_stream_table(slots[:], buf[:], .Server)

	fragment := [?]u8{0x88}

	stray := make_header(.Continuation, FLAG_END_HEADERS, 1, 1)
	testing.expect_value(
		t,
		streams_on_frame(&table, stray, fragment[:], .Received),
		Stream_Action{kind = .Connection_Error, code = .Protocol_Error},
	)
}

@(test)
test_continuation_oversize_rejected :: proc(t: ^testing.T) {
	slots: [8]Stream
	buf: [64]u8
	small: Streams_Table
	streams_init(&small, slots[:], .Server, buf[:], 8)

	fragment := [?]u8{0x82, 0x86, 0x84, 0x41, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65}

	head := make_header(.Headers, 0, 1, 10)
	testing.expect_value(
		t,
		streams_on_frame(&small, head, fragment[:], .Received),
		Stream_Action{kind = .Connection_Error, code = .Compression_Error},
	)

	testing.expect_value(t, small.assembler.active, false)

	medium: Streams_Table
	streams_init(&medium, slots[:], .Server, buf[:], 16)

	part := [?]u8{0x82, 0x86, 0x84, 0x41, 0x0f, 0x77, 0x77, 0x77}

	head8 := make_header(.Headers, 0, 1, 8)
	testing.expect_value(
		t,
		streams_on_frame(&medium, head8, part[:], .Received),
		Stream_Action{kind = .Accept},
	)

	overflow := [?]u8{0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d}

	tail := make_header(.Continuation, FLAG_END_HEADERS, 1, 12)
	testing.expect_value(
		t,
		streams_on_frame(&medium, tail, overflow[:], .Received),
		Stream_Action{kind = .Connection_Error, code = .Compression_Error},
	)

	testing.expect_value(t, medium.assembler.active, false)
}

@(test)
test_preface_accepts_empty_and_sized_settings :: proc(t: ^testing.T) {
	magic := CLIENT_CONNECTION_PREFACE

	input: [33]u8

	for i in 0 ..< len(magic) {
		input[i] = magic[i]
	}

	input[24] = 0
	input[25] = 0
	input[26] = 0
	input[27] = 4
	input[28] = 0
	input[29] = 0
	input[30] = 0
	input[31] = 0
	input[32] = 0

	testing.expect_value(t, preface_server_check(input[:]), true)

	sized: [39]u8

	for i in 0 ..< len(magic) {
		sized[i] = magic[i]
	}

	sized[24] = 0
	sized[25] = 0
	sized[26] = 6
	sized[27] = 4
	sized[28] = 0
	sized[29] = 0
	sized[30] = 0
	sized[31] = 0
	sized[32] = 0
	sized[33] = 0
	sized[34] = 4
	sized[35] = 0
	sized[36] = 0
	sized[37] = 0
	sized[38] = 1

	testing.expect_value(t, preface_server_check(sized[:]), true)
}

@(test)
test_preface_rejects_bad_magic_and_first_frame :: proc(t: ^testing.T) {
	magic := CLIENT_CONNECTION_PREFACE

	input: [33]u8

	for i in 0 ..< len(magic) {
		input[i] = magic[i]
	}

	input[24] = 0
	input[25] = 0
	input[26] = 0
	input[27] = 4
	input[28] = 0
	input[29] = 0
	input[30] = 0
	input[31] = 0
	input[32] = 0

	input[0] = 'X'

	testing.expect_value(t, preface_server_check(input[:]), false)

	input[0] = magic[0]

	input[27] = 6

	testing.expect_value(t, preface_server_check(input[:]), false)

	testing.expect_value(t, preface_server_check(input[:10]), false)
	testing.expect_value(t, preface_server_check(nil), false)
}

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
