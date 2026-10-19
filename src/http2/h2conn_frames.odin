package http2

h2_encode_pending :: proc(p: H2_Pending, out: []u8) -> (used: int, ok: bool) {
	switch p.kind {
	case .Settings_Ack:
		if len(out) < FRAME_HEADER_LEN {
			return 0, false
		}
		header := Frame_Header {
			length    = 0,
			kind      = .Settings,
			flags     = SETTINGS_ACK,
			stream_id = 0,
		}

		if !encode_frame_header(header, out) {
			return 0, false
		}

		return FRAME_HEADER_LEN, true
	case .Ping_Ack:
		if len(out) < FRAME_HEADER_LEN + PING_PAYLOAD_LEN {
			return 0, false
		}
		header := Frame_Header {
			length    = PING_PAYLOAD_LEN,
			kind      = .Ping,
			flags     = PING_ACK,
			stream_id = 0,
		}

		if !encode_frame_header(header, out) {
			return 0, false
		}

		if !encode_ping_payload(p.ping, out[FRAME_HEADER_LEN:]) {
			return 0, false
		}

		return FRAME_HEADER_LEN + PING_PAYLOAD_LEN, true
	case .Rst:
		if len(out) < FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN {
			return 0, false
		}
		header := Frame_Header {
			length    = RST_STREAM_PAYLOAD_LEN,
			kind      = .Rst_Stream,
			stream_id = p.stream_id,
		}

		if !encode_frame_header(header, out) {
			return 0, false
		}

		if !encode_rst_stream_payload(p.code, out[FRAME_HEADER_LEN:]) {
			return 0, false
		}

		return FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN, true
	case .Goaway:
		if len(out) < FRAME_HEADER_LEN + GOAWAY_FIXED_LEN {
			return 0, false
		}
		header := Frame_Header {
			length    = GOAWAY_FIXED_LEN,
			kind      = .GoAway,
			stream_id = 0,
		}

		if !encode_frame_header(header, out) {
			return 0, false
		}
		written, payload_ok := encode_goaway_payload(
			p.last_id,
			p.code,
			nil,
			out[FRAME_HEADER_LEN:],
		)

		if !payload_ok {
			return 0, false
		}

		return FRAME_HEADER_LEN + written, true
	case .Window_Update:
		if len(out) < FRAME_HEADER_LEN + WINDOW_UPDATE_PAYLOAD_LEN {
			return 0, false
		}
		header := Frame_Header {
			length    = WINDOW_UPDATE_PAYLOAD_LEN,
			kind      = .Window_Update,
			stream_id = p.stream_id,
		}

		if !encode_frame_header(header, out) {
			return 0, false
		}

		if !encode_window_update_payload(p.increment, out[FRAME_HEADER_LEN:]) {
			return 0, false
		}

		return FRAME_HEADER_LEN + WINDOW_UPDATE_PAYLOAD_LEN, true
	}

	return 0, false
}

h2_flush_pending :: proc(conn: ^H2_Connection, out_bytes: []u8, out_used: int) -> int {
	used := out_used

	for conn.pending_count > 0 {
		p := conn.pending[conn.pending_head]
		n, enc_ok := h2_encode_pending(p, out_bytes[used:])

		if !enc_ok {
			break
		}
		used += n
		conn.pending_head = (conn.pending_head + 1) % len(conn.pending)
		conn.pending_count -= 1
	}

	return used
}

h2_emit_out :: proc(
	conn: ^H2_Connection,
	p: H2_Pending,
	out_bytes: []u8,
	out_used: int,
) -> (
	new_used: int,
	fatal: bool,
) {
	n, enc_ok := h2_encode_pending(p, out_bytes[out_used:])

	if enc_ok {
		return out_used + n, false
	}

	if conn.pending_count >= len(conn.pending) {
		return out_used, true
	}
	slot := (conn.pending_head + conn.pending_count) % len(conn.pending)
	conn.pending[slot] = p
	conn.pending_count += 1

	return out_used, false
}

h2_handle_rst_stream :: proc(
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
	code, dec_ok := decode_rst_stream_payload(header, payload)

	if !dec_ok {
		h2_fail(conn, .Frame_Size_Error)

		return events, out_used, true
	}
	next, push_ok := h2_push_event(
		out_events,
		events,
		H2_Event{kind = .Stream_Reset, stream_id = header.stream_id, code = code},
	)

	if !push_ok {
		h2_fail(conn, .Internal_Error)

		return events, out_used, true
	}

	return next, out_used, false
}

h2_handle_goaway :: proc(
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
	last, code, _, dec_ok := decode_goaway_payload(header, payload)

	if !dec_ok {
		h2_fail(conn, .Frame_Size_Error)

		return events, out_used, true
	}
	recv_action := goaway_on_receive(&conn.table, header, payload)

	if recv_action.kind == .Connection_Error {
		h2_fail(conn, recv_action.code)

		return events, out_used, true
	}
	next, push_ok := h2_push_event(
		out_events,
		events,
		H2_Event{kind = .Goaway_Received, stream_id = 0, code = code, last_stream_id = last},
	)

	if !push_ok {
		h2_fail(conn, .Internal_Error)

		return events, out_used, true
	}

	return next, out_used, false
}

h2_handle_ping :: proc(
	conn: ^H2_Connection,
	header: Frame_Header,
	payload: []u8,
	out_bytes: []u8,
	out_used: int,
) -> (
	new_out: int,
	fatal: bool,
) {
	_, gated_out, gated_fatal, stop := h2_gate(conn, header, nil, out_bytes, out_used)

	if stop {
		return gated_out, gated_fatal
	}

	if header.flags & PING_ACK != 0 {
		return out_used, false
	}
	opaque, dec_ok := decode_ping_payload(header, payload)

	if !dec_ok {
		h2_fail(conn, .Frame_Size_Error)

		return out_used, true
	}

	return h2_emit_out(conn, H2_Pending{kind = .Ping_Ack, ping = opaque}, out_bytes, out_used)
}

h2_handle_frame :: proc(
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
	switch header.kind {
	case .Data:
		return h2_handle_data(conn, header, payload, out_events, events, out_bytes, out_used)
	case .Headers:
		return h2_handle_headers(conn, header, payload, out_events, events, out_bytes, out_used)
	case .Continuation:
		return h2_handle_continuation(
			conn,
			header,
			payload,
			out_events,
			events,
			out_bytes,
			out_used,
		)
	case .Settings:
		settings_out, settings_fatal := h2_handle_settings(
			conn,
			header,
			payload,
			out_bytes,
			out_used,
		)

		return events, settings_out, settings_fatal
	case .Window_Update:
		return h2_handle_window_update(
			conn,
			header,
			payload,
			out_events,
			events,
			out_bytes,
			out_used,
		)
	case .Rst_Stream:
		return h2_handle_rst_stream(conn, header, payload, out_events, events, out_bytes, out_used)
	case .GoAway:
		return h2_handle_goaway(conn, header, payload, out_events, events, out_bytes, out_used)
	case .Ping:
		ping_out, ping_fatal := h2_handle_ping(conn, header, payload, out_bytes, out_used)

		return events, ping_out, ping_fatal
	case .Priority:
		_, gated_out, gated_fatal, stop := h2_gate(conn, header, nil, out_bytes, out_used)

		if stop {
			return events, gated_out, gated_fatal
		}

		if !stream_frame_length_ok(header) {
			h2_fail(conn, .Frame_Size_Error)

			return events, out_used, true
		}

		return events, out_used, false
	case .Push_Promise, .Unknown:
		_, gated_out, gated_fatal, stop := h2_gate(conn, header, nil, out_bytes, out_used)

		if stop {
			return events, gated_out, gated_fatal
		}

		if header.kind == .Push_Promise {
			h2_fail(conn, .Protocol_Error)

			return events, out_used, true
		}

		return events, out_used, false
	}

	return events, out_used, false
}
