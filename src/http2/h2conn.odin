package http2

H2_Event_Kind :: enum u8 {
	Headers_Ready,
	Data_Ready,
	Stream_Reset,
	Stream_Closed,
	Goaway_Received,
}

H2_Event :: struct {
	kind:           H2_Event_Kind,
	stream_id:      u32,
	headers:        []Hpack_Header,
	data:           []u8,
	code:           Error_Code,
	end_stream:     bool,
	last_stream_id: u32,
}

H2_Pending_Kind :: enum u8 {
	Settings_Ack,
	Ping_Ack,
	Rst,
	Goaway,
	Window_Update,
}

H2_Pending :: struct {
	kind:      H2_Pending_Kind,
	stream_id: u32,
	code:      Error_Code,
	increment: u32,
	last_id:   u32,
	ping:      [8]u8,
}

H2_Connection :: struct {
	table:          Streams_Table,
	hpack:          Hpack_Table,
	scratch:        []u8,
	scratch_used:   int,
	headers_store:  []Hpack_Header,
	headers_used:   int,
	pending:        []H2_Pending,
	pending_head:   int,
	pending_count:  int,
	conn_recv:      Flow_Window,
	conn_send:      Flow_Window,
	max_frame_size: u32,
	hpack_cap:      u64,
	preface_done:   bool,
	failed:         bool,
	err:            Error_Code,
	staged_active:  bool,
	staged_stream:  u32,
	staged_end:     bool,
	staged_used:    int,
	hdr_end_stream: bool,
	close_pending:  bool,
	close_stream:   u32,
}

// NOTE: hpack_scratch and headers are per-connection-total budgets, not per-feed arenas: decoded HPACK table entries borrow scratch bytes for as long as the table retains them, and staged data plus emitted header and data spans borrow scratch until the host consumes the feed's events. A connection that exhausts either budget fails closed; size them for the connection's expected header and body volume and recycle with GOAWAY before they run out. See h2conn_budget.
h2conn_init :: proc(
	conn: ^H2_Connection,
	streams: []Stream,
	reasm: []u8,
	dyn_entries: []Hpack_Entry,
	hpack_scratch: []u8,
	headers: []Hpack_Header,
	pending: []H2_Pending,
	max_frame_size: u32 = DEFAULT_MAX_FRAME_SIZE,
	max_concurrent: u32 = max(u32),
	max_header_list_size: u32 = DEFAULT_MAX_HEADER_LIST_SIZE,
	header_table_cap: u64 = 4096,
) -> bool {
	if conn == nil {
		return false
	}

	if len(streams) == 0 || len(reasm) == 0 || len(hpack_scratch) == 0 {
		return false
	}

	if len(headers) == 0 || len(pending) == 0 {
		return false
	}

	if max_frame_size < SETTINGS_MAX_FRAME_SIZE_MIN ||
	   max_frame_size > SETTINGS_MAX_FRAME_SIZE_MAX {
		return false
	}
	streams_init(&conn.table, streams, .Server, reasm, max_header_list_size)

	if max_concurrent == 0 {
		conn.table.local.max_concurrent = max(u32)
	} else {
		conn.table.local.max_concurrent = max_concurrent
	}
	conn.table.local.max_frame_size = max_frame_size
	hpack_table_init(&conn.hpack, dyn_entries, header_table_cap)
	conn.scratch = hpack_scratch
	conn.scratch_used = 0
	conn.headers_store = headers
	conn.headers_used = 0
	conn.pending = pending
	conn.pending_head = 0
	conn.pending_count = 0
	conn.max_frame_size = max_frame_size
	conn.hpack_cap = header_table_cap
	conn.preface_done = false
	conn.failed = false
	conn.err = .No_Error
	conn.staged_active = false
	conn.staged_stream = 0
	conn.staged_end = false
	conn.staged_used = 0
	conn.hdr_end_stream = false
	conn.close_pending = false
	conn.close_stream = 0

	if !flow_window_init(&conn.conn_recv, FLOW_INITIAL_WINDOW_SIZE) {
		return false
	}

	if !flow_window_init(&conn.conn_send, FLOW_INITIAL_WINDOW_SIZE) {
		return false
	}

	return true
}

h2conn_error :: proc(conn: ^H2_Connection) -> (code: Error_Code, failed: bool) {
	if conn == nil {
		return .Internal_Error, true
	}

	return conn.err, conn.failed
}

h2conn_budget :: proc(
	conn: ^H2_Connection,
) -> (
	scratch_used: int,
	scratch_cap: int,
	headers_used: int,
	headers_cap: int,
) {
	if conn == nil {
		return 0, 0, 0, 0
	}

	return conn.scratch_used, len(conn.scratch), conn.headers_used, len(conn.headers_store)
}

h2_fail :: proc(conn: ^H2_Connection, code: Error_Code) {
	conn.failed = true
	conn.err = code
}

h2_parse_header :: proc(
	input: []u8,
	max_frame: u32,
) -> (
	header: Frame_Header,
	ok: bool,
	too_large: bool,
) {
	if len(input) < FRAME_HEADER_LEN {
		return {}, false, false
	}
	length := u32(input[0]) << 16 | u32(input[1]) << 8 | u32(input[2])

	if length > max_frame {
		return {}, false, true
	}

	if input[5] & 0x80 != 0 {
		return {}, false, false
	}
	kind, raw := frame_type_from_byte(input[3])
	sid := u32(input[5]) << 24 | u32(input[6]) << 16 | u32(input[7]) << 8 | u32(input[8])
	header = Frame_Header {
		length    = length,
		kind      = kind,
		raw_type  = raw,
		flags     = input[4],
		stream_id = sid,
	}

	return header, true, false
}

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

h2conn_feed :: proc(
	conn: ^H2_Connection,
	input: []u8,
	out_events: []H2_Event,
	out_bytes: []u8,
) -> (
	consumed: int,
	events: int,
	out_used: int,
	ok: bool,
) {
	consumed = 0
	events = 0
	out_used = 0
	ok = false

	if conn == nil {
		return consumed, events, out_used, false
	}

	if conn.failed {
		return consumed, events, out_used, false
	}
	out_used = h2_flush_pending(conn, out_bytes, 0)

	if conn.close_pending {
		if len(out_events) - events < 1 {
			return consumed, events, out_used, true
		}
		deferred, push_ok := h2_push_event(
			out_events,
			events,
			H2_Event{kind = .Stream_Closed, stream_id = conn.close_stream},
		)

		if !push_ok {
			h2_fail(conn, .Internal_Error)

			return consumed, events, out_used, false
		}
		events = deferred
		conn.close_pending = false
		conn.close_stream = 0
	}

	if conn.staged_active {
		if len(out_events) - events < 2 {
			return consumed, events, out_used, true
		}
		resumed, fatal := h2_deliver_staged(conn, out_events, events)
		events = resumed

		if fatal {
			return consumed, events, out_used, false
		}
	}
	offset := 0

	if !conn.preface_done {
		magic := CLIENT_CONNECTION_PREFACE

		if len(input) < len(magic) {
			return consumed, events, out_used, true
		}

		for i in 0 ..< len(magic) {
			if input[i] != magic[i] {
				h2_fail(conn, .Protocol_Error)

				return consumed, events, out_used, false
			}
		}

		if len(input) < len(magic) + FRAME_HEADER_LEN {
			return consumed, events, out_used, true
		}

		if !preface_server_check(input) {
			h2_fail(conn, .Protocol_Error)

			return consumed, events, out_used, false
		}
		conn.preface_done = true
		offset = len(magic)
	}

	for offset < len(input) {
		if len(out_events) - events < 2 {
			break
		}
		rest := input[offset:]

		if len(rest) < FRAME_HEADER_LEN {
			break
		}
		header, hdr_ok, too_large := h2_parse_header(rest, conn.max_frame_size)

		if too_large {
			h2_fail(conn, .Frame_Size_Error)

			return offset, events, out_used, false
		}

		if !hdr_ok {
			h2_fail(conn, .Protocol_Error)

			return offset, events, out_used, false
		}
		total := FRAME_HEADER_LEN + int(header.length)

		if len(rest) < total {
			break
		}
		payload := rest[FRAME_HEADER_LEN:total]
		next_events, next_out, fatal := h2_handle_frame(
			conn,
			header,
			payload,
			out_events,
			events,
			out_bytes,
			out_used,
		)
		events = next_events
		out_used = next_out

		if fatal {
			return offset, events, out_used, false
		}
		offset += total
	}

	return offset, events, out_used, true
}

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
