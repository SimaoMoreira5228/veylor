package http2

settings_list_apply :: proc(
	list: []Settings_Parameter,
	current: Stream_Endpoint_Settings,
) -> (
	next: Stream_Endpoint_Settings,
	ok: bool,
) {
	next = current

	for i in 0 ..< len(list) {
		id := list[i].id

		value := list[i].value

		if !settings_parameter_valid(id, value) {
			return current, false
		}

		switch id {
		case SETTINGS_HEADER_TABLE_SIZE:
			next.header_table_size = value
		case SETTINGS_ENABLE_PUSH:
			next.enable_push = value
		case SETTINGS_MAX_CONCURRENT_STREAMS:
			next.max_concurrent = value
		case SETTINGS_INITIAL_WINDOW_SIZE:
			next.initial_window = value
		case SETTINGS_MAX_FRAME_SIZE:
			next.max_frame_size = value
		case SETTINGS_MAX_HEADER_LIST_SIZE:
			next.max_header_list_size = value
		}
	}

	return next, true
}

settings_local_apply :: proc(table: ^Streams_Table, list: []Settings_Parameter) -> bool {
	next, ok := settings_list_apply(list, table.local)

	if !ok {
		return false
	}

	table.local = next
	table.assembler.limit = next.max_header_list_size

	return true
}

settings_remote_apply :: proc(table: ^Streams_Table, list: []Settings_Parameter) -> Stream_Action {
	for i in 0 ..< len(list) {
		if !settings_parameter_valid(list[i].id, list[i].value) {
			if list[i].id == SETTINGS_INITIAL_WINDOW_SIZE {
				return Stream_Action{kind = .Connection_Error, code = .Flow_Control_Error}
			}

			return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
		}
	}

	next, _ := settings_list_apply(list, table.remote)

	delta := i64(next.initial_window) - i64(table.remote.initial_window)

	if delta != 0 {
		for i in 0 ..< table.used {
			grown := table.slots[i].send_window.value + delta

			if grown > FLOW_MAX_WINDOW_SIZE {
				return Stream_Action{kind = .Connection_Error, code = .Flow_Control_Error}
			}
		}

		for i in 0 ..< table.used {
			table.slots[i].send_window.value += delta
		}
	}

	table.remote = next

	return Stream_Action{kind = .Accept}
}

settings_track_sent :: proc(table: ^Streams_Table) -> bool {
	if table.unacked_settings == max(u32) {
		return false
	}

	table.unacked_settings += 1

	return true
}

settings_on_ack :: proc(table: ^Streams_Table, header: Frame_Header) -> Stream_Action {
	if header.kind != .Settings {
		return Stream_Action{kind = .Reject_Protocol_Error}
	}

	if header.flags & SETTINGS_ACK == 0 {
		return Stream_Action{kind = .Reject_Protocol_Error}
	}

	if header.stream_id != 0 {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	if header.length != 0 {
		return Stream_Action{kind = .Connection_Error, code = .Frame_Size_Error}
	}

	if table.unacked_settings == 0 {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	table.unacked_settings -= 1

	return Stream_Action{kind = .Accept}
}

goaway_on_send :: proc(table: ^Streams_Table, last_stream_id: u32) -> bool {
	if last_stream_id > MAX_STREAM_ID {
		return false
	}

	table.goaway_sent = true
	table.goaway_send_last = last_stream_id

	for i in 0 ..< table.used {
		remote := !stream_is_local_id(table.role, table.slots[i].id)

		if remote && table.slots[i].id > last_stream_id {
			table.slots[i].state = .Closed
		}
	}

	return true
}

goaway_on_receive :: proc(
	table: ^Streams_Table,
	header: Frame_Header,
	payload: []u8,
) -> Stream_Action {
	if header.kind != .GoAway {
		return Stream_Action{kind = .Reject_Protocol_Error}
	}

	if header.stream_id != 0 {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	if !payload_length_matches(header, payload) {
		return Stream_Action{kind = .Connection_Error, code = .Frame_Size_Error}
	}

	if len(payload) < GOAWAY_FIXED_LEN {
		return Stream_Action{kind = .Connection_Error, code = .Frame_Size_Error}
	}

	last, code, _, payload_ok := decode_goaway_payload(header, payload)

	if !payload_ok {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	table.goaway_received = true
	table.goaway_recv_last = last
	table.goaway_recv_code = code

	return Stream_Action{kind = .Accept}
}
