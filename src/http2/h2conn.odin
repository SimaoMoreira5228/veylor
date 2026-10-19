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
	delivery:       []u8,
	delivery_used:  int,
}

// NOTE: hpack_scratch is a per-connection-total budget, not a per-feed arena: decoded HPACK table entries borrow scratch bytes for as long as the table retains them. DATA spans do not borrow it: direct deliveries borrow the feed input, which stays stable until the turn ends, and assembler-held bytes are copied into the per-feed delivery ring below, which resets every feed once the host has consumed the prior events. A connection that exhausts the header budget fails closed; size scratch for the connection's expected header volume and recycle with GOAWAY before it runs out. See h2conn_budget.
h2conn_init :: proc(
	conn: ^H2_Connection,
	streams: []Stream,
	reasm: []u8,
	dyn_entries: []Hpack_Entry,
	hpack_scratch: []u8,
	headers: []Hpack_Header,
	pending: []H2_Pending,
	delivery: []u8,
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

	if len(headers) == 0 || len(pending) == 0 || len(delivery) == 0 {
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
	conn.delivery = delivery
	conn.delivery_used = 0
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
	conn.headers_used = 0
	conn.delivery_used = 0
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
