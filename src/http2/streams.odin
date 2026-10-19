package http2

DEFAULT_MAX_HEADER_LIST_SIZE :: u32(16384)

CLIENT_CONNECTION_PREFACE :: "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

H2_Role :: enum u8 {
	Client,
	Server,
}

Frame_Direction :: enum u8 {
	Received,
	Sent,
}

Stream_State :: enum u8 {
	Idle,
	Reserved_Local,
	Reserved_Remote,
	Open,
	Half_Closed_Local,
	Half_Closed_Remote,
	Closed,
}

Stream_Action_Kind :: enum u8 {
	Accept,
	Ignore,
	Reject_Protocol_Error,
	Send_Rst,
	Connection_Error,
}

Stream_Action :: struct {
	kind: Stream_Action_Kind,
	code: Error_Code,
}

Stream :: struct {
	id:          u32,
	state:       Stream_State,
	send_window: Flow_Window,
	recv_window: Flow_Window,
}

Stream_Endpoint_Settings :: struct {
	header_table_size:    u32,
	enable_push:          u32,
	max_concurrent:       u32,
	initial_window:       u32,
	max_frame_size:       u32,
	max_header_list_size: u32,
}

Headers_Assembler :: struct {
	buf:       []u8,
	used:      int,
	stream_id: u32,
	active:    bool,
	limit:     u32,
}

Streams_Table :: struct {
	slots:            []Stream,
	used:             int,
	role:             H2_Role,
	local:            Stream_Endpoint_Settings,
	remote:           Stream_Endpoint_Settings,
	highest_local:    u32,
	highest_remote:   u32,
	unacked_settings: u32,
	goaway_sent:      bool,
	goaway_send_last: u32,
	goaway_received:  bool,
	goaway_recv_last: u32,
	goaway_recv_code: Error_Code,
	assembler:        Headers_Assembler,
}

default_endpoint_settings :: proc() -> Stream_Endpoint_Settings {
	return Stream_Endpoint_Settings {
		header_table_size = 4096,
		enable_push = 1,
		max_concurrent = max(u32),
		initial_window = u32(FLOW_INITIAL_WINDOW_SIZE),
		max_frame_size = DEFAULT_MAX_FRAME_SIZE,
		max_header_list_size = max(u32),
	}
}

streams_init :: proc(
	table: ^Streams_Table,
	slots: []Stream,
	role: H2_Role,
	assembler_buf: []u8,
	max_header_list_size: u32,
) {
	table.slots = slots
	table.used = 0
	table.role = role
	table.local = default_endpoint_settings()
	table.remote = default_endpoint_settings()
	table.highest_local = 0
	table.highest_remote = 0
	table.unacked_settings = 0
	table.goaway_sent = false
	table.goaway_send_last = 0
	table.goaway_received = false
	table.goaway_recv_last = 0
	table.goaway_recv_code = .No_Error
	table.assembler.buf = assembler_buf
	table.assembler.used = 0
	table.assembler.stream_id = 0
	table.assembler.active = false

	effective := max_header_list_size

	if effective == 0 {
		effective = DEFAULT_MAX_HEADER_LIST_SIZE
	}

	table.local.max_header_list_size = effective
	table.assembler.limit = effective
}

stream_is_local_id :: proc(role: H2_Role, id: u32) -> bool {
	if role == .Client {
		return id & 1 == 1
	}

	return id & 1 == 0
}

streams_find :: proc(table: ^Streams_Table, id: u32) -> (index: int, found: bool) {
	for i in 0 ..< table.used {
		if table.slots[i].id == id {
			return i, true
		}
	}

	return 0, false
}

streams_active_count :: proc(table: ^Streams_Table) -> int {
	count := 0

	for i in 0 ..< table.used {
		switch table.slots[i].state {
		case .Open, .Half_Closed_Local, .Half_Closed_Remote, .Reserved_Local, .Reserved_Remote:
			count += 1
		case .Idle, .Closed:
			break
		}
	}

	return count
}

streams_create :: proc(
	table: ^Streams_Table,
	id: u32,
	state: Stream_State,
) -> (
	index: int,
	ok: bool,
) {
	if streams_active_count(table) >= int(table.local.max_concurrent) {
		return 0, false
	}

	if table.used < len(table.slots) {
		slot := &table.slots[table.used]

		slot.id = id
		slot.state = state

		if !flow_window_init(&slot.send_window, i64(table.remote.initial_window)) {
			return 0, false
		}

		if !flow_window_init(&slot.recv_window, i64(table.local.initial_window)) {
			return 0, false
		}
		index = table.used
		table.used += 1

		return index, true
	}

	// NOTE: closed streams carry no state worth keeping: late frames for old ids are still
	// guarded by highest_remote without the slot, so evict one instead of capping the
	// connection at a lifetime total.
	for i in 0 ..< table.used {
		if table.slots[i].state == .Closed {
			slot := &table.slots[i]

			slot.id = id
			slot.state = state

			if !flow_window_init(&slot.send_window, i64(table.remote.initial_window)) {
				return 0, false
			}

			if !flow_window_init(&slot.recv_window, i64(table.local.initial_window)) {
				return 0, false
			}

			return i, true
		}
	}

	return 0, false
}

streams_note_id :: proc(table: ^Streams_Table, id: u32, is_local: bool) {
	if is_local {
		if id > table.highest_local {
			table.highest_local = id
		}

		return
	}

	if id > table.highest_remote {
		table.highest_remote = id
	}
}

streams_window_init_ok :: proc(table: ^Streams_Table) -> bool {
	if i64(table.remote.initial_window) > FLOW_MAX_WINDOW_SIZE {
		return false
	}

	return i64(table.local.initial_window) <= FLOW_MAX_WINDOW_SIZE
}

assembler_begin :: proc(assembler: ^Headers_Assembler, stream_id: u32, fragment: []u8) -> bool {
	if assembler.active {
		return false
	}

	if u64(len(fragment)) > u64(assembler.limit) {
		return false
	}

	if len(fragment) > len(assembler.buf) {
		return false
	}

	for i in 0 ..< len(fragment) {
		assembler.buf[i] = fragment[i]
	}

	assembler.used = len(fragment)
	assembler.stream_id = stream_id
	assembler.active = true

	return true
}

assembler_append :: proc(
	assembler: ^Headers_Assembler,
	stream_id: u32,
	fragment: []u8,
	end_headers: bool,
) -> (
	done: bool,
	ok: bool,
) {
	if !assembler.active {
		return false, false
	}

	if assembler.stream_id != stream_id {
		return false, false
	}

	if u64(assembler.used) + u64(len(fragment)) > u64(assembler.limit) {
		return false, false
	}

	if assembler.used + len(fragment) > len(assembler.buf) {
		return false, false
	}

	for i in 0 ..< len(fragment) {
		assembler.buf[assembler.used + i] = fragment[i]
	}

	assembler.used += len(fragment)

	if end_headers {
		assembler.active = false

		return true, true
	}

	return false, true
}

assembler_block :: proc(assembler: ^Headers_Assembler) -> []u8 {
	return assembler.buf[:assembler.used]
}

assembler_reset :: proc(assembler: ^Headers_Assembler) {
	assembler.active = false
	assembler.used = 0
	assembler.stream_id = 0
}

preface_server_check :: proc(input: []u8) -> bool {
	magic := CLIENT_CONNECTION_PREFACE

	if len(input) < len(magic) + FRAME_HEADER_LEN {
		return false
	}

	for i in 0 ..< len(magic) {
		if input[i] != magic[i] {
			return false
		}
	}

	header, header_ok := parse_frame_header(input[len(magic):])

	if !header_ok {
		return false
	}

	if header.kind != .Settings {
		return false
	}

	return header.stream_id == 0
}
