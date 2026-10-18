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
	if table.used >= len(table.slots) {
		return 0, false
	}

	if streams_active_count(table) >= int(table.local.max_concurrent) {
		return 0, false
	}

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
