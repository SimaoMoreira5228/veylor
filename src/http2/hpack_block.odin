package http2

h2_scratch_alloc :: proc(conn: ^H2_Connection, n: int) -> ([]u8, bool) {
	if n < 0 {
		return nil, false
	}

	if n > len(conn.scratch) - conn.scratch_used {
		return nil, false
	}
	out := conn.scratch[conn.scratch_used:conn.scratch_used + n]
	conn.scratch_used += n

	return out, true
}

h2_push_event :: proc(out_events: []H2_Event, events: int, ev: H2_Event) -> (int, bool) {
	if events >= len(out_events) {
		return events, false
	}
	out_events[events] = ev

	return events + 1, true
}

h2_emit_maybe_close :: proc(
	conn: ^H2_Connection,
	out_events: []H2_Event,
	events: int,
	stream_id: u32,
	end_stream: bool,
) -> int {
	if !end_stream {
		return events
	}
	next, push_ok := h2_push_event(
		out_events,
		events,
		H2_Event{kind = .Stream_Closed, stream_id = stream_id},
	)

	if !push_ok {
		conn.close_pending = true
		conn.close_stream = stream_id

		return events
	}

	return next
}

h2_gate :: proc(
	conn: ^H2_Connection,
	header: Frame_Header,
	fragment: []u8,
	out_bytes: []u8,
	out_used: int,
) -> (
	action: Stream_Action,
	new_out: int,
	fatal: bool,
	stop: bool,
) {
	action = streams_on_frame(&conn.table, header, fragment, .Received)

	if action.kind == .Connection_Error {
		h2_fail(conn, action.code)

		return {}, out_used, true, true
	}

	if action.kind == .Send_Rst || action.kind == .Reject_Protocol_Error {
		code := action.code

		if action.kind == .Reject_Protocol_Error {
			code = .Refused_Stream
		}
		used, emit_fatal := h2_emit_out(
			conn,
			H2_Pending{kind = .Rst, stream_id = header.stream_id, code = code},
			out_bytes,
			out_used,
		)

		return {}, used, emit_fatal, true
	}

	if action.kind != .Accept {
		return action, out_used, false, true
	}

	return action, out_used, false, false
}

h2_decode_block :: proc(
	conn: ^H2_Connection,
	block: []u8,
	stream_id: u32,
	end_stream: bool,
	out_events: []H2_Event,
	events: int,
) -> (
	new_events: int,
	fatal: bool,
) {
	if len(conn.headers_store) - conn.headers_used <= 0 {
		h2_fail(conn, .Compression_Error)

		return events, true
	}
	dst_headers := conn.headers_store[conn.headers_used:]
	dst_scratch := conn.scratch[conn.scratch_used:]
	count, _, pinned, dec_ok := decode_header_block(block, &conn.hpack, dst_headers, dst_scratch)

	if !dec_ok {
		h2_fail(conn, .Compression_Error)

		return events, true
	}
	total := u64(0)

	for i in 0 ..< count {
		n := u64(len(dst_headers[i].name))
		v := u64(len(dst_headers[i].value))

		if n > max(u64) - v - u64(32) {
			h2_fail(conn, .Compression_Error)

			return events, true
		}
		total += n + v + u64(32)

		if total > u64(conn.table.local.max_header_list_size) {
			h2_fail(conn, .Compression_Error)

			return events, true
		}
	}
	conn.headers_used += count
	conn.scratch_used += pinned
	next, push_ok := h2_push_event(
		out_events,
		events,
		H2_Event {
			kind = .Headers_Ready,
			stream_id = stream_id,
			headers = dst_headers[:count],
			end_stream = end_stream,
		},
	)

	if !push_ok {
		h2_fail(conn, .Internal_Error)

		return events, true
	}

	return h2_emit_maybe_close(conn, out_events, next, stream_id, end_stream), false
}

h2_stage_data :: proc(conn: ^H2_Connection, data: []u8, stream_id: u32, end_stream: bool) -> bool {
	if conn.staged_active {
		return false
	}

	if conn.table.assembler.active {
		return false
	}

	if len(data) > len(conn.table.assembler.buf) {
		return false
	}

	for i in 0 ..< len(data) {
		conn.table.assembler.buf[i] = data[i]
	}
	conn.staged_active = true
	conn.staged_stream = stream_id
	conn.staged_end = end_stream
	conn.staged_used = len(data)

	return true
}

h2_staged_avail :: proc(conn: ^H2_Connection, index: int) -> i64 {
	avail := conn.conn_recv.value

	if conn.table.slots[index].recv_window.value < avail {
		avail = conn.table.slots[index].recv_window.value
	}

	return avail
}

h2_deliver_staged :: proc(
	conn: ^H2_Connection,
	out_events: []H2_Event,
	events: int,
) -> (
	new_events: int,
	fatal: bool,
) {
	if !conn.staged_active {
		return events, false
	}
	index, found := streams_find(&conn.table, conn.staged_stream)

	if !found {
		h2_fail(conn, .Internal_Error)

		return events, true
	}
	avail := h2_staged_avail(conn, index)

	if avail <= 0 {
		return events, false
	}
	n := conn.staged_used

	if i64(n) > avail {
		n = int(avail)
	}
	staged := conn.table.assembler.buf[:conn.staged_used]
	chunk, alloc_ok := h2_scratch_alloc(conn, n)

	if !alloc_ok {
		h2_fail(conn, .Compression_Error)

		return events, true
	}

	for i in 0 ..< n {
		chunk[i] = staged[i]
	}
	remain := conn.staged_used - n

	for i in 0 ..< remain {
		conn.table.assembler.buf[i] = staged[n + i]
	}
	conn.staged_used = remain
	conn.conn_recv.value -= i64(n)
	conn.table.slots[index].recv_window.value -= i64(n)
	done := remain == 0
	end := conn.staged_end && done
	stream := conn.staged_stream

	if done {
		conn.staged_active = false
		conn.staged_stream = 0
		conn.staged_end = false
	}
	next, push_ok := h2_push_event(
		out_events,
		events,
		H2_Event{kind = .Data_Ready, stream_id = stream, data = chunk, end_stream = end},
	)

	if !push_ok {
		h2_fail(conn, .Internal_Error)

		return events, true
	}

	return h2_emit_maybe_close(conn, out_events, next, stream, end), false
}

h2_handle_data :: proc(
	conn: ^H2_Connection,
	header: Frame_Header,
	payload: []u8,
	out_events: []H2_Event,
	events: int,
	out_bytes: []u8,
	out_used: int,
) -> (
	new_events: int,
	new_out: int,
	fatal: bool,
) {
	data, _, data_ok := decode_data_payload(header, payload)

	if !data_ok {
		h2_fail(conn, .Protocol_Error)

		return events, out_used, true
	}
	_, gated_out, gated_fatal, stop := h2_gate(conn, header, data, out_bytes, out_used)

	if stop {
		return events, gated_out, gated_fatal
	}

	if conn.staged_active {
		h2_fail(conn, .Flow_Control_Error)

		return events, out_used, true
	}
	index, found := streams_find(&conn.table, header.stream_id)

	if !found {
		h2_fail(conn, .Internal_Error)

		return events, out_used, true
	}
	slot := &conn.table.slots[index]
	avail := h2_staged_avail(conn, index)

	if avail <= 0 {
		end := header.flags & FLAG_END_STREAM != 0

		if !h2_stage_data(conn, data, header.stream_id, end) {
			h2_fail(conn, .Flow_Control_Error)

			return events, out_used, true
		}

		return events, out_used, false
	}
	deliver := len(data)

	if i64(deliver) > avail {
		deliver = int(avail)
	}
	conn.conn_recv.value -= i64(deliver)
	slot.recv_window.value -= i64(deliver)
	chunk, alloc_ok := h2_scratch_alloc(conn, deliver)

	if !alloc_ok {
		h2_fail(conn, .Compression_Error)

		return events, out_used, true
	}

	for i in 0 ..< deliver {
		chunk[i] = data[i]
	}
	full := deliver == len(data)
	end := (header.flags & FLAG_END_STREAM != 0) && full
	next, push_ok := h2_push_event(
		out_events,
		events,
		H2_Event{kind = .Data_Ready, stream_id = header.stream_id, data = chunk, end_stream = end},
	)

	if !push_ok {
		h2_fail(conn, .Internal_Error)

		return events, out_used, true
	}
	next = h2_emit_maybe_close(conn, out_events, next, header.stream_id, end)

	if !full {
		rest_end := header.flags & FLAG_END_STREAM != 0

		if !h2_stage_data(conn, data[deliver:], header.stream_id, rest_end) {
			h2_fail(conn, .Flow_Control_Error)

			return next, out_used, true
		}
	}

	return next, out_used, false
}

h2_handle_headers :: proc(
	conn: ^H2_Connection,
	header: Frame_Header,
	payload: []u8,
	out_events: []H2_Event,
	events: int,
	out_bytes: []u8,
	out_used: int,
) -> (
	new_events: int,
	new_out: int,
	fatal: bool,
) {
	if conn.staged_active {
		h2_fail(conn, .Compression_Error)

		return events, out_used, true
	}
	fragment, _, _, _, frag_ok := decode_headers_payload(header, payload)

	if !frag_ok {
		h2_fail(conn, .Protocol_Error)

		return events, out_used, true
	}
	_, gated_out, gated_fatal, stop := h2_gate(conn, header, fragment, out_bytes, out_used)

	if stop {
		return events, gated_out, gated_fatal
	}
	end := header.flags & FLAG_END_STREAM != 0

	if header.flags & FLAG_END_HEADERS != 0 {
		decoded, decode_fatal := h2_decode_block(
			conn,
			fragment,
			header.stream_id,
			end,
			out_events,
			events,
		)

		return decoded, out_used, decode_fatal
	}
	conn.hdr_end_stream = end

	return events, out_used, false
}

h2_handle_continuation :: proc(
	conn: ^H2_Connection,
	header: Frame_Header,
	payload: []u8,
	out_events: []H2_Event,
	events: int,
	out_bytes: []u8,
	out_used: int,
) -> (
	new_events: int,
	new_out: int,
	fatal: bool,
) {
	if conn.staged_active {
		h2_fail(conn, .Compression_Error)

		return events, out_used, true
	}
	fragment, frag_ok := decode_continuation_payload(header, payload)

	if !frag_ok {
		h2_fail(conn, .Protocol_Error)

		return events, out_used, true
	}
	_, gated_out, gated_fatal, stop := h2_gate(conn, header, fragment, out_bytes, out_used)

	if stop {
		return events, gated_out, gated_fatal
	}

	if conn.table.assembler.active {
		return events, out_used, false
	}
	block := assembler_block(&conn.table.assembler)
	decoded, decode_fatal := h2_decode_block(
		conn,
		block,
		header.stream_id,
		conn.hdr_end_stream,
		out_events,
		events,
	)

	return decoded, out_used, decode_fatal
}

h2_collapse_settings :: proc(
	payload: []u8,
	eff: []Settings_Parameter,
) -> (
	count: int,
	code: Error_Code,
	ok: bool,
) {
	seen := u8(0)
	values: [6]u32

	for off := 0; off < len(payload); off += SETTINGS_ENTRY_LEN {
		id := u16(payload[off]) << 8 | u16(payload[off + 1])
		value :=
			u32(payload[off + 2]) << 24 |
			u32(payload[off + 3]) << 16 |
			u32(payload[off + 4]) << 8 |
			u32(payload[off + 5])

		if !settings_parameter_valid(id, value) {
			if id == SETTINGS_INITIAL_WINDOW_SIZE {
				return 0, .Flow_Control_Error, false
			}

			return 0, .Protocol_Error, false
		}

		if id >= SETTINGS_HEADER_TABLE_SIZE && id <= SETTINGS_MAX_HEADER_LIST_SIZE {
			values[id - SETTINGS_HEADER_TABLE_SIZE] = value
			seen |= u8(1) << (id - SETTINGS_HEADER_TABLE_SIZE)
		}
	}
	n := 0

	for id := SETTINGS_HEADER_TABLE_SIZE; id <= SETTINGS_MAX_HEADER_LIST_SIZE; id += 1 {
		if seen & (u8(1) << (id - SETTINGS_HEADER_TABLE_SIZE)) == 0 {
			continue
		}

		if n >= len(eff) {
			return 0, .Protocol_Error, false
		}
		eff[n] = Settings_Parameter {
			id    = id,
			value = values[id - SETTINGS_HEADER_TABLE_SIZE],
		}
		n += 1
	}

	return n, .No_Error, true
}

h2_follow_remote_table_size :: proc(conn: ^H2_Connection) {
	limit := u64(conn.table.remote.header_table_size)

	if limit > conn.hpack_cap {
		limit = conn.hpack_cap
	}
	_ = hpack_table_set_max_size(&conn.hpack, limit)
}

h2_handle_settings :: proc(
	conn: ^H2_Connection,
	header: Frame_Header,
	payload: []u8,
	out_bytes: []u8,
	out_used: int,
) -> (
	new_out: int,
	fatal: bool,
) {
	action := streams_on_frame(&conn.table, header, nil, .Received)

	if action.kind == .Connection_Error {
		h2_fail(conn, action.code)

		return out_used, true
	}

	if header.flags & SETTINGS_ACK != 0 {
		return out_used, false
	}

	if len(payload) % SETTINGS_ENTRY_LEN != 0 {
		h2_fail(conn, .Frame_Size_Error)

		return out_used, true
	}
	eff: [6]Settings_Parameter
	n, code, collapse_ok := h2_collapse_settings(payload, eff[:])

	if !collapse_ok {
		h2_fail(conn, code)

		return out_used, true
	}
	apply_action := settings_remote_apply(&conn.table, eff[:n])

	if apply_action.kind == .Connection_Error {
		h2_fail(conn, apply_action.code)

		return out_used, true
	}
	h2_follow_remote_table_size(conn)
	used, emit_fatal := h2_emit_out(conn, H2_Pending{kind = .Settings_Ack}, out_bytes, out_used)

	return used, emit_fatal
}

h2_handle_window_update :: proc(
	conn: ^H2_Connection,
	header: Frame_Header,
	payload: []u8,
	out_events: []H2_Event,
	events: int,
	out_bytes: []u8,
	out_used: int,
) -> (
	new_events: int,
	new_out: int,
	fatal: bool,
) {
	_, gated_out, gated_fatal, stop := h2_gate(conn, header, nil, out_bytes, out_used)

	if stop {
		return events, gated_out, gated_fatal
	}
	increment, dec_ok := decode_window_update_payload(header, payload)

	if !dec_ok {
		h2_fail(conn, .Protocol_Error)

		return events, out_used, true
	}

	if header.stream_id == 0 {
		if !flow_window_add(&conn.conn_recv, increment) {
			h2_fail(conn, .Flow_Control_Error)

			return events, out_used, true
		}
	} else {
		index, found := streams_find(&conn.table, header.stream_id)

		if !found {
			h2_fail(conn, .Internal_Error)

			return events, out_used, true
		}

		if !flow_window_add(&conn.table.slots[index].recv_window, increment) {
			h2_fail(conn, .Flow_Control_Error)

			return events, out_used, true
		}
	}
	staged_events, staged_fatal := h2_deliver_staged(conn, out_events, events)

	return staged_events, out_used, staged_fatal
}
