package http2

half_close_target :: proc(dir: Frame_Direction) -> Stream_State {
	if dir == .Received {
		return .Half_Closed_Remote
	}

	return .Half_Closed_Local
}

rst_state_action :: proc(state: Stream_State) -> (Stream_Action, Stream_State) {
	if state == .Idle {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}, state
	}

	if state == .Closed {
		return Stream_Action{kind = .Ignore}, state
	}

	return Stream_Action{kind = .Accept}, .Closed
}

window_update_state_action :: proc(state: Stream_State) -> (Stream_Action, Stream_State) {
	if state == .Idle {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}, state
	}

	if state == .Closed {
		return Stream_Action{kind = .Ignore}, state
	}

	return Stream_Action{kind = .Accept}, state
}

continuation_state_action :: proc(state: Stream_State) -> (Stream_Action, Stream_State) {
	if state == .Idle {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}, state
	}

	if state == .Closed {
		return Stream_Action{kind = .Send_Rst, code = .Stream_Closed}, state
	}

	return Stream_Action{kind = .Accept}, state
}

data_headers_state_action :: proc(
	state: Stream_State,
	kind: Frame_Type,
	flags: u8,
	dir: Frame_Direction,
) -> (
	Stream_Action,
	Stream_State,
) {
	end := flags & FLAG_END_STREAM != 0

	if kind == .Continuation {
		return continuation_state_action(state)
	}

	switch state {
	case .Idle:
		if kind != .Headers {
			return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}, state
		}

		if end {
			return Stream_Action{kind = .Accept}, half_close_target(dir)
		}

		return Stream_Action{kind = .Accept}, .Open
	case .Reserved_Local:
		if dir == .Sent && kind == .Headers {
			if end {
				return Stream_Action{kind = .Accept}, .Closed
			}

			return Stream_Action{kind = .Accept}, .Half_Closed_Remote
		}

		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}, state
	case .Reserved_Remote:
		if dir == .Received && kind == .Headers {
			if end {
				return Stream_Action{kind = .Accept}, .Closed
			}

			return Stream_Action{kind = .Accept}, .Half_Closed_Local
		}

		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}, state
	case .Open:
		if end {
			return Stream_Action{kind = .Accept}, half_close_target(dir)
		}

		return Stream_Action{kind = .Accept}, state
	case .Half_Closed_Local:
		if dir == .Sent {
			return Stream_Action{kind = .Send_Rst, code = .Stream_Closed}, state
		}

		if end {
			return Stream_Action{kind = .Accept}, .Closed
		}

		return Stream_Action{kind = .Accept}, state
	case .Half_Closed_Remote:
		if dir == .Received {
			return Stream_Action{kind = .Send_Rst, code = .Stream_Closed}, state
		}

		if end {
			return Stream_Action{kind = .Accept}, .Closed
		}

		return Stream_Action{kind = .Accept}, state
	case .Closed:
		return Stream_Action{kind = .Send_Rst, code = .Stream_Closed}, state
	}

	return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}, state
}

stream_state_on_frame :: proc(
	state: Stream_State,
	kind: Frame_Type,
	flags: u8,
	dir: Frame_Direction,
) -> (
	Stream_Action,
	Stream_State,
) {
	if kind == .Unknown {
		return Stream_Action{kind = .Ignore}, state
	}

	if kind == .Priority {
		return Stream_Action{kind = .Accept}, state
	}

	if kind == .Settings || kind == .Ping || kind == .GoAway {
		return Stream_Action{kind = .Ignore}, state
	}

	if kind == .Push_Promise {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}, state
	}

	if kind == .Rst_Stream {
		return rst_state_action(state)
	}

	if kind == .Window_Update {
		return window_update_state_action(state)
	}

	return data_headers_state_action(state, kind, flags, dir)
}

stream_frame_length_ok :: proc(header: Frame_Header) -> bool {
	switch header.kind {
	case .Rst_Stream:
		return header.length == RST_STREAM_PAYLOAD_LEN
	case .Priority:
		return header.length == PRIORITY_PAYLOAD_LEN
	case .Window_Update:
		return header.length == WINDOW_UPDATE_PAYLOAD_LEN
	case .Data, .Headers, .Settings, .Push_Promise, .Ping, .GoAway, .Continuation, .Unknown:
		return true
	}

	return true
}

streams_on_connection_frame :: proc(table: ^Streams_Table, header: Frame_Header) -> Stream_Action {
	if header.stream_id != 0 {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	switch header.kind {
	case .Settings:
		if header.flags & SETTINGS_ACK == 0 {
			return Stream_Action{kind = .Accept}
		}

		return settings_on_ack(table, header)
	case .Ping:
		if header.length != PING_PAYLOAD_LEN {
			return Stream_Action{kind = .Connection_Error, code = .Frame_Size_Error}
		}

		return Stream_Action{kind = .Accept}
	case .GoAway:
		if header.length < GOAWAY_FIXED_LEN {
			return Stream_Action{kind = .Connection_Error, code = .Frame_Size_Error}
		}

		return Stream_Action{kind = .Accept}
	case .Data,
	     .Headers,
	     .Priority,
	     .Rst_Stream,
	     .Push_Promise,
	     .Window_Update,
	     .Continuation,
	     .Unknown:
		return Stream_Action{kind = .Ignore}
	}

	return Stream_Action{kind = .Ignore}
}

streams_gate_continuation :: proc(
	table: ^Streams_Table,
	header: Frame_Header,
	fragment: []u8,
) -> (
	blocked: bool,
	action: Stream_Action,
) {
	assembler := &table.assembler

	if !assembler.active {
		if header.kind == .Continuation {
			return true, Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
		}

		return false, Stream_Action{kind = .Accept}
	}

	if header.kind != .Continuation || header.stream_id != assembler.stream_id {
		return true, Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	end_headers := header.flags & FLAG_END_HEADERS != 0

	_, append_ok := assembler_append(assembler, header.stream_id, fragment, end_headers)

	if !append_ok {
		assembler_reset(assembler)

		return true, Stream_Action{kind = .Connection_Error, code = .Compression_Error}
	}

	return true, Stream_Action{kind = .Accept}
}

streams_on_known_stream :: proc(
	table: ^Streams_Table,
	index: int,
	header: Frame_Header,
	fragment: []u8,
	dir: Frame_Direction,
) -> Stream_Action {
	slot := &table.slots[index]

	action, next := stream_state_on_frame(slot.state, header.kind, header.flags, dir)

	if action.kind != .Accept {
		return action
	}

	slot.state = next

	if dir == .Received && header.kind == .Headers && header.flags & FLAG_END_HEADERS == 0 {
		if !assembler_begin(&table.assembler, slot.id, fragment) {
			return Stream_Action{kind = .Connection_Error, code = .Compression_Error}
		}
	}

	return action
}

streams_open_local :: proc(
	table: ^Streams_Table,
	header: Frame_Header,
	dir: Frame_Direction,
) -> Stream_Action {
	id := header.stream_id

	if id <= table.highest_local {
		action, _ := stream_state_on_frame(.Closed, header.kind, header.flags, dir)

		return action
	}

	if header.kind == .Priority {
		return Stream_Action{kind = .Accept}
	}

	if header.kind != .Headers {
		return Stream_Action{kind = .Reject_Protocol_Error}
	}

	if table.goaway_sent {
		return Stream_Action{kind = .Reject_Protocol_Error}
	}

	if !streams_window_init_ok(table) {
		return Stream_Action{kind = .Connection_Error, code = .Flow_Control_Error}
	}

	_, initial := stream_state_on_frame(.Idle, header.kind, header.flags, dir)

	_, created := streams_create(table, id, initial)

	if !created {
		return Stream_Action{kind = .Send_Rst, code = .Refused_Stream}
	}

	streams_note_id(table, id, true)

	return Stream_Action{kind = .Accept}
}

streams_open_remote :: proc(
	table: ^Streams_Table,
	header: Frame_Header,
	fragment: []u8,
	dir: Frame_Direction,
) -> Stream_Action {
	id := header.stream_id

	if id <= table.highest_remote {
		action, _ := stream_state_on_frame(.Closed, header.kind, header.flags, dir)

		return action
	}

	if header.kind == .Priority {
		return Stream_Action{kind = .Accept}
	}

	if header.kind != .Headers {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	if !streams_window_init_ok(table) {
		return Stream_Action{kind = .Connection_Error, code = .Flow_Control_Error}
	}

	_, initial := stream_state_on_frame(.Idle, header.kind, header.flags, dir)

	_, created := streams_create(table, id, initial)

	if !created {
		return Stream_Action{kind = .Send_Rst, code = .Refused_Stream}
	}

	streams_note_id(table, id, false)

	if header.flags & FLAG_END_HEADERS == 0 {
		if !assembler_begin(&table.assembler, id, fragment) {
			return Stream_Action{kind = .Connection_Error, code = .Compression_Error}
		}
	}

	return Stream_Action{kind = .Accept}
}

streams_on_new_stream :: proc(
	table: ^Streams_Table,
	header: Frame_Header,
	fragment: []u8,
	dir: Frame_Direction,
	local_initiated: bool,
) -> Stream_Action {
	if dir == .Received && local_initiated {
		if header.kind == .Priority {
			return Stream_Action{kind = .Accept}
		}

		if header.kind == .Rst_Stream {
			return Stream_Action{kind = .Ignore}
		}

		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	if dir == .Sent && !local_initiated {
		return Stream_Action{kind = .Reject_Protocol_Error}
	}

	if dir == .Received && table.role == .Client {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	if table.goaway_received {
		return Stream_Action{kind = .Reject_Protocol_Error}
	}

	if local_initiated {
		return streams_open_local(table, header, dir)
	}

	return streams_open_remote(table, header, fragment, dir)
}

streams_on_frame :: proc(
	table: ^Streams_Table,
	header: Frame_Header,
	fragment: []u8,
	dir: Frame_Direction,
) -> Stream_Action {
	if header.kind == .Unknown {
		return Stream_Action{kind = .Ignore}
	}

	if dir == .Received {
		blocked, gated := streams_gate_continuation(table, header, fragment)

		if blocked {
			return gated
		}
	}

	if header.kind == .Settings || header.kind == .Ping || header.kind == .GoAway {
		return streams_on_connection_frame(table, header)
	}

	if header.kind == .Window_Update && header.stream_id == 0 {
		if header.length != WINDOW_UPDATE_PAYLOAD_LEN {
			return Stream_Action{kind = .Connection_Error, code = .Frame_Size_Error}
		}

		return Stream_Action{kind = .Accept}
	}

	if header.stream_id == 0 {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	if header.kind == .Push_Promise {
		return Stream_Action{kind = .Connection_Error, code = .Protocol_Error}
	}

	if !stream_frame_length_ok(header) {
		return Stream_Action{kind = .Connection_Error, code = .Frame_Size_Error}
	}

	id := header.stream_id

	local_initiated := stream_is_local_id(table.role, id)

	if table.goaway_sent && dir == .Received && !local_initiated && id > table.goaway_send_last {
		action, _ := stream_state_on_frame(.Closed, header.kind, header.flags, dir)

		return action
	}

	index, found := streams_find(table, id)

	if found {
		return streams_on_known_stream(table, index, header, fragment, dir)
	}

	return streams_on_new_stream(table, header, fragment, dir, local_initiated)
}
