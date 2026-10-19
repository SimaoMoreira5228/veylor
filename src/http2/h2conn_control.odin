package http2

h2conn_queue_settings_ack :: proc(conn: ^H2_Connection, out: []u8) -> (used: int, ok: bool) {
	_ = conn

	return h2_encode_pending(H2_Pending{kind = .Settings_Ack}, out)
}

h2conn_queue_settings :: proc(
	conn: ^H2_Connection,
	params: []Settings_Parameter,
	out: []u8,
) -> (
	used: int,
	ok: bool,
) {
	if conn == nil {
		return 0, false
	}

	if len(params) > max(int) / SETTINGS_ENTRY_LEN {
		return 0, false
	}

	for i in 0 ..< len(params) {
		if !settings_parameter_valid(params[i].id, params[i].value) {
			return 0, false
		}
	}
	total := FRAME_HEADER_LEN + len(params) * SETTINGS_ENTRY_LEN

	if len(out) < total {
		return 0, false
	}

	if !settings_track_sent(&conn.table) {
		return 0, false
	}

	if !settings_local_apply(&conn.table, params) {
		conn.table.unacked_settings -= 1

		return 0, false
	}
	header := Frame_Header {
		length    = u32(len(params) * SETTINGS_ENTRY_LEN),
		kind      = .Settings,
		stream_id = 0,
	}

	if !encode_frame_header(header, out) {
		return 0, false
	}
	written, payload_ok := encode_settings_payload(params, out[FRAME_HEADER_LEN:])

	if !payload_ok {
		return 0, false
	}

	return FRAME_HEADER_LEN + written, true
}

h2conn_queue_window_update :: proc(
	conn: ^H2_Connection,
	stream_id: u32,
	increment: u32,
	out: []u8,
) -> (
	used: int,
	ok: bool,
) {
	if conn == nil {
		return 0, false
	}

	if !flow_increment_valid(increment) {
		return 0, false
	}

	if stream_id > MAX_STREAM_ID {
		return 0, false
	}

	if len(out) < FRAME_HEADER_LEN + WINDOW_UPDATE_PAYLOAD_LEN {
		return 0, false
	}

	if stream_id == 0 {
		if !flow_window_add(&conn.conn_recv, increment) {
			return 0, false
		}
	} else {
		index, found := streams_find(&conn.table, stream_id)

		if !found {
			return 0, false
		}

		if !flow_window_add(&conn.table.slots[index].recv_window, increment) {
			return 0, false
		}
	}
	n, enc_ok := h2_encode_pending(
		H2_Pending{kind = .Window_Update, stream_id = stream_id, increment = increment},
		out,
	)

	if !enc_ok {
		return 0, false
	}

	return n, true
}

h2conn_queue_rst_stream :: proc(
	conn: ^H2_Connection,
	stream_id: u32,
	code: Error_Code,
	out: []u8,
) -> (
	used: int,
	ok: bool,
) {
	if conn == nil {
		return 0, false
	}

	if stream_id == 0 || stream_id > MAX_STREAM_ID {
		return 0, false
	}

	if len(out) < FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN {
		return 0, false
	}
	index, found := streams_find(&conn.table, stream_id)

	if !found {
		return 0, false
	}
	slot := &conn.table.slots[index]

	if slot.state == .Idle {
		return 0, false
	}

	if slot.state != .Closed {
		slot.state = .Closed
	}

	return h2_encode_pending(H2_Pending{kind = .Rst, stream_id = stream_id, code = code}, out)
}

h2conn_queue_goaway :: proc(
	conn: ^H2_Connection,
	last_stream_id: u32,
	code: Error_Code,
	out: []u8,
) -> (
	used: int,
	ok: bool,
) {
	if conn == nil {
		return 0, false
	}

	if last_stream_id > MAX_STREAM_ID {
		return 0, false
	}

	if len(out) < FRAME_HEADER_LEN + GOAWAY_FIXED_LEN {
		return 0, false
	}

	if !goaway_on_send(&conn.table, last_stream_id) {
		return 0, false
	}

	return h2_encode_pending(
		H2_Pending{kind = .Goaway, last_id = last_stream_id, code = code},
		out,
	)
}
