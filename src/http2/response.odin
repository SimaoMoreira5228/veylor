package http2

h2_status_text :: proc(status: u16, out: []u8) -> (written: int, ok: bool) {
	if status < 100 || status > 999 {
		return 0, false
	}

	if len(out) < 3 {
		return 0, false
	}
	out[0] = u8(status / 100) + '0'
	out[1] = u8(status / 10 % 10) + '0'
	out[2] = u8(status % 10) + '0'

	return 3, true
}

h2conn_queue_response_headers :: proc(
	conn: ^H2_Connection,
	stream_id: u32,
	status: u16,
	end_stream: bool,
	out: []u8,
) -> (
	used: int,
	ok: bool,
) {
	if conn == nil {
		return 0, false
	}

	if stream_id == 0 || stream_id > MAX_STREAM_ID || conn.failed {
		return 0, false
	}
	index, found := streams_find(&conn.table, stream_id)

	if !found {
		return 0, false
	}
	slot := &conn.table.slots[index]

	if slot.state != .Open && slot.state != .Half_Closed_Remote {
		return 0, false
	}
	status_text: [3]u8
	_, status_ok := h2_status_text(status, status_text[:])

	if !status_ok {
		return 0, false
	}
	name := transmute([]u8)string(":status")
	value := status_text[:]
	fragment: [32]u8
	frag_used, frag_ok := encode_literal_header(
		.Without_Indexing,
		0,
		name,
		value,
		false,
		false,
		fragment[:],
	)

	if !frag_ok {
		return 0, false
	}
	flags := FLAG_END_HEADERS

	if end_stream {
		flags |= FLAG_END_STREAM
	}
	action, next := stream_state_on_frame(slot.state, .Headers, flags, .Sent)

	if action.kind == .Connection_Error {
		h2_fail(conn, action.code)

		return 0, false
	}

	if action.kind != .Accept {
		return 0, false
	}
	header := Frame_Header {
		length    = u32(frag_used),
		kind      = .Headers,
		stream_id = stream_id,
		flags     = flags,
	}

	if len(out) < FRAME_HEADER_LEN + frag_used {
		return 0, false
	}

	if !encode_frame_header(header, out[:FRAME_HEADER_LEN]) {
		return 0, false
	}
	payload_used, payload_ok := encode_headers_payload(
		fragment[:frag_used],
		nil,
		0,
		out[FRAME_HEADER_LEN:],
	)

	if !payload_ok {
		return 0, false
	}
	slot.state = next

	return FRAME_HEADER_LEN + payload_used, true
}

h2conn_send_credit :: proc(
	conn: ^H2_Connection,
	stream_id: u32,
) -> (
	conn_avail, stream_avail: i64,
	ok: bool,
) {
	if conn == nil || conn.failed {
		return 0, 0, false
	}

	if stream_id == 0 || stream_id > MAX_STREAM_ID {
		return 0, 0, false
	}
	index, found := streams_find(&conn.table, stream_id)

	if !found {
		return 0, 0, false
	}

	return conn.conn_send.value, conn.table.slots[index].send_window.value, true
}

h2conn_queue_response_data :: proc(
	conn: ^H2_Connection,
	stream_id: u32,
	data: []u8,
	end_stream: bool,
	out: []u8,
) -> (
	used: int,
	ok: bool,
) {
	if conn == nil {
		return 0, false
	}

	if stream_id == 0 || stream_id > MAX_STREAM_ID || conn.failed {
		return 0, false
	}
	index, found := streams_find(&conn.table, stream_id)

	if !found {
		return 0, false
	}
	slot := &conn.table.slots[index]

	if slot.state != .Open && slot.state != .Half_Closed_Remote {
		return 0, false
	}
	flags := u8(0)

	if end_stream {
		flags |= FLAG_END_STREAM
	}

	if len(data) == 0 {
		action, next := stream_state_on_frame(slot.state, .Data, flags, .Sent)

		if action.kind == .Connection_Error {
			h2_fail(conn, action.code)

			return 0, false
		}

		if action.kind != .Accept {
			return 0, false
		}
		header := Frame_Header {
			length    = 0,
			kind      = .Data,
			stream_id = stream_id,
			flags     = flags,
		}

		if len(out) < FRAME_HEADER_LEN {
			return 0, false
		}

		if !encode_frame_header(header, out[:FRAME_HEADER_LEN]) {
			return 0, false
		}
		slot.state = next

		return FRAME_HEADER_LEN, true
	}
	chunk := len(data)

	if i64(chunk) > conn.conn_send.value {
		chunk = int(conn.conn_send.value)
	}

	if i64(chunk) > slot.send_window.value {
		chunk = int(slot.send_window.value)
	}

	if u32(chunk) > conn.table.remote.max_frame_size {
		chunk = int(conn.table.remote.max_frame_size)
	}

	if chunk <= 0 {
		return 0, false
	}
	action, next := stream_state_on_frame(slot.state, .Data, flags, .Sent)

	if action.kind == .Connection_Error {
		h2_fail(conn, action.code)

		return 0, false
	}

	if action.kind != .Accept {
		return 0, false
	}
	header := Frame_Header {
		length    = u32(chunk),
		kind      = .Data,
		stream_id = stream_id,
		flags     = flags,
	}

	if len(out) < FRAME_HEADER_LEN + chunk {
		return 0, false
	}

	if !encode_frame_header(header, out[:FRAME_HEADER_LEN]) {
		return 0, false
	}
	payload_used, payload_ok := encode_data_payload(data[:chunk], 0, out[FRAME_HEADER_LEN:])

	if !payload_ok || payload_used != chunk {
		return 0, false
	}

	if !flow_window_take(&conn.conn_send, i64(chunk)) {
		return 0, false
	}

	if !flow_window_take(&slot.send_window, i64(chunk)) {
		return 0, false
	}
	slot.state = next

	return FRAME_HEADER_LEN + payload_used, true
}
