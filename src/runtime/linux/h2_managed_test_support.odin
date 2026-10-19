#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http2 "veylor:http2"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"

H2M_Fixture :: struct {
	table_slots:          [1]connections.Connection_Slot,
	table_free:           [1]u32,
	table:                connections.Connection_Table,
	timer_storage:        [8]transport.Timer_Expiration,
	timer_index:          [8 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32,
	timers:               transport.Timer_Queue,
	event_storage:        [8]host.Host_Event,
	host_queue:           host.Host_Queue,
	route_storage:        [8]router.Static_Route,
	route_index:          [16]router.Route_Index_Slot,
	routes:               router.Route_Table,
	route_method:         [3]u8,
	route_path:           [8]u8,
	upload_method:        [4]u8,
	upload_path:          [7]u8,
	upload_trailer_names: [2][16]u8,
	upload_trailers:      [2][]u8,
	static_path:          [7]u8,
	slot_store:           [1]H2_Worker_Connection,
	h2_free:              [1]u32,
	shutdown:             shutdown.State,
	worker:               Static_Worker,
	sockets:              [2]linux.Fd,
}

h2m_init :: proc(f: ^H2M_Fixture, t: ^testing.T) -> bool {
	table, table_result := connections.init_connection_table(
		7,
		5,
		f.table_slots[:],
		f.table_free[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)

	if table_result != .Ready {
		return false
	}
	f.table = table
	f.timers = transport.init_timer_queue(f.timer_storage[:], f.timer_index[:])
	f.host_queue = host.init_host_queue(f.event_storage[:])
	f.routes = router.init_route_table(f.route_storage[:], f.route_index[:])
	f.route_method = [3]u8{'G', 'E', 'T'}
	f.route_path = [8]u8{'/', 'm', 'a', 'n', 'a', 'g', 'e', 'd'}
	f.static_path = [7]u8{'/', 's', 't', 'a', 't', 'i', 'c'}
	testing.expect_value(
		t,
		router.register_managed_route(&f.routes, f.route_method[:], f.route_path[:]),
		router.Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		router.register_static_route(
			&f.routes,
			f.route_method[:],
			f.static_path[:],
			router.Static_Response{status = 200},
		),
		router.Route_Table_Result.Added,
	)
	f.upload_method = [4]u8{'P', 'O', 'S', 'T'}
	f.upload_path = [7]u8{'/', 'u', 'p', 'l', 'o', 'a', 'd'}
	copy(f.upload_trailer_names[0][:], transmute([]u8)string("x-checksum"))
	copy(f.upload_trailer_names[1][:], transmute([]u8)string("x-signature"))
	f.upload_trailers[0] = f.upload_trailer_names[0][:10]
	f.upload_trailers[1] = f.upload_trailer_names[1][:11]
	testing.expect_value(
		t,
		router.register_managed_route_with_trailers(
			&f.routes,
			f.upload_method[:],
			f.upload_path[:],
			f.upload_trailers[:],
		),
		router.Route_Table_Result.Added,
	)
	testing.expect_value(t, router.freeze_route_table(&f.routes), router.Route_Table_Result.Frozen)

	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &f.sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return false
	}
	token, allocation_result := connections.allocate_connection(&f.table, u64(f.sockets[0]))
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)

	if allocation_result != .Ready {
		linux.close(f.sockets[0])
		linux.close(f.sockets[1])

		return false
	}
	peer: transport.Peer_Address
	testing.expect(t, h2_worker_connection_init(&f.slot_store[0], f.sockets[0], token, peer))

	if !f.slot_store[0].active {
		linux.close(f.sockets[0])
		linux.close(f.sockets[1])

		return false
	}
	f.worker = Static_Worker {
		table = &f.table,
		host_queue = &f.host_queue,
		routes = &f.routes,
		timers = &f.timers,
		shutdown_state = &f.shutdown,
		timeouts = connections.Connection_Timeouts {
			request_head_ns = 10_000_000_000,
			body_progress_ns = 10_000_000_000,
			response_write_ns = 10_000_000_000,
			keep_alive_idle_ns = 5_000_000_000,
		},
		h2_states = f.slot_store[:],
		h2_free_stack = f.h2_free[:],
		h2_free_count = 0,
		byte_budget = 64 * 1024,
		event_budget = 128,
	}
	testing.expect_value(
		t,
		shutdown.register_connections(&f.shutdown, 1),
		shutdown.Result.Accepted,
	)
	schedule := connections.schedule_connection_phase_deadline(
		&f.table,
		&f.timers,
		token,
		1_000_000_000,
		f.worker.timeouts,
	)
	testing.expect_value(
		t,
		schedule.result,
		connections.Connection_Deadline_Schedule_Result.Scheduled,
	)

	return schedule.result == .Scheduled
}

h2m_close :: proc(f: ^H2M_Fixture) {
	if f.sockets[0] >= 0 {
		linux.close(f.sockets[0])
	}
	if f.sockets[1] >= 0 {
		linux.close(f.sockets[1])
	}
}

h2m_slot :: proc(f: ^H2M_Fixture) -> ^H2_Worker_Connection {
	return &f.slot_store[0]
}

h2m_write_all :: proc(t: ^testing.T, fd: linux.Fd, data: []u8) -> bool {
	written := 0

	for written < len(data) {
		n, write_error := linux.write(fd, data[written:])

		if write_error != .NONE {
			testing.expect_value(t, write_error, linux.Errno.NONE)

			return false
		}

		if n <= 0 {
			testing.expect(t, false)

			return false
		}
		written += n
	}

	return true
}

h2m_drain_socket :: proc(t: ^testing.T, fd: linux.Fd) -> int {
	drained := 0
	buf: [512]u8

	for {
		n, read_error := linux.read(fd, buf[:])

		if read_error != .NONE || n <= 0 {
			break
		}
		drained += n

		if n < len(buf) {
			break
		}
	}

	return drained
}

h2m_read_socket :: proc(t: ^testing.T, fd: linux.Fd, count: int) -> []u8 {
	out := make([]u8, count, context.temp_allocator)
	got := 0

	for got < count {
		n, read_error := linux.read(fd, out[got:])
		testing.expect_value(t, read_error, linux.Errno.NONE)

		if read_error != .NONE || n <= 0 {
			break
		}
		got += n
	}
	testing.expect_value(t, got, count)

	return out[:got]
}

h2m_put_frame_header :: proc(buf: []u8, type_byte, flags: u8, sid, length: u32) -> bool {
	kind := http2.Frame_Type.Unknown

	switch type_byte {
	case 0:
		kind = .Data
	case 1:
		kind = .Headers
	case 3:
		kind = .Rst_Stream
	case 4:
		kind = .Settings
	case 6:
		kind = .Ping
	case 7:
		kind = .GoAway
	}
	header := http2.Frame_Header {
		length    = length,
		kind      = kind,
		stream_id = sid,
		flags     = flags,
	}

	return http2.encode_frame_header(header, buf)
}

h2m_client_headers :: proc(
	buf: []u8,
	sid: u32,
	flags: u8,
	method_idx: u64,
	method_lit: []u8,
	path_idx: u64,
	path_lit: []u8,
	scheme_idx: u64,
	scheme_lit: []u8,
) -> (
	length: int,
	ok: bool,
) {
	block: [256]u8
	used := 0

	if method_idx > 0 {
		n, enc_ok := http2.encode_indexed_header(method_idx, block[used:])

		if !enc_ok {
			return 0, false
		}
		used += n
	} else {
		n, enc_ok := http2.encode_literal_header(
			.Without_Indexing,
			2,
			nil,
			method_lit,
			false,
			false,
			block[used:],
		)

		if !enc_ok {
			return 0, false
		}
		used += n
	}

	if scheme_idx > 0 {
		n, enc_ok := http2.encode_indexed_header(scheme_idx, block[used:])

		if !enc_ok {
			return 0, false
		}
		used += n
	} else {
		n, enc_ok := http2.encode_literal_header(
			.Without_Indexing,
			6,
			nil,
			scheme_lit,
			false,
			false,
			block[used:],
		)

		if !enc_ok {
			return 0, false
		}
		used += n
	}

	if path_idx > 0 {
		n, enc_ok := http2.encode_indexed_header(path_idx, block[used:])

		if !enc_ok {
			return 0, false
		}
		used += n
	} else {
		n, enc_ok := http2.encode_literal_header(
			.Without_Indexing,
			4,
			nil,
			path_lit,
			false,
			false,
			block[used:],
		)

		if !enc_ok {
			return 0, false
		}
		used += n
	}
	authority := transmute([]u8)string("example.test")
	n, auth_ok := http2.encode_literal_header(
		.Without_Indexing,
		1,
		nil,
		authority,
		false,
		false,
		block[used:],
	)

	if !auth_ok {
		return 0, false
	}
	used += n
	frag_used, frag_ok := http2.encode_headers_payload(block[:used], nil, 0, buf[9:])

	if !frag_ok {
		return 0, false
	}

	if !h2m_put_frame_header(buf[:9], 1, flags, sid, u32(frag_used)) {
		return 0, false
	}

	return 9 + frag_used, true
}

h2m_client_data :: proc(buf: []u8, sid: u32, flags: u8, payload: []u8) -> (length: int, ok: bool) {
	if !h2m_put_frame_header(buf[:9], 0, flags, sid, u32(len(payload))) {
		return 0, false
	}
	frag_used, frag_ok := http2.encode_data_payload(payload, 0, buf[9:])

	if !frag_ok {
		return 0, false
	}

	return 9 + frag_used, true
}

h2m_client_rst :: proc(buf: []u8, sid: u32, code: http2.Error_Code) -> (length: int, ok: bool) {
	if !h2m_put_frame_header(buf[:9], 3, 0, sid, http2.RST_STREAM_PAYLOAD_LEN) {
		return 0, false
	}

	if !http2.encode_rst_stream_payload(code, buf[9:]) {
		return 0, false
	}

	return 9 + http2.RST_STREAM_PAYLOAD_LEN, true
}

h2m_client_goaway :: proc(
	buf: []u8,
	last_id: u32,
	code: http2.Error_Code,
) -> (
	length: int,
	ok: bool,
) {
	if !h2m_put_frame_header(buf[:9], 7, 0, 0, http2.GOAWAY_FIXED_LEN) {
		return 0, false
	}
	_, payload_ok := http2.encode_goaway_payload(last_id, code, nil, buf[9:])

	if !payload_ok {
		return 0, false
	}

	return 9 + http2.GOAWAY_FIXED_LEN, true
}

h2m_feed_turn :: proc(
	t: ^testing.T,
	f: ^H2M_Fixture,
	wire: []u8,
) -> (
	turn: H2_Worker_Turn,
	ok: bool,
) {
	if !h2m_write_all(t, f.sockets[1], wire) {
		return {}, false
	}
	slot := h2m_slot(f)
	turn = h2_worker_connection_turn(slot, 64 * 1024)
	testing.expect_value(t, turn.result, H2_Worker_Turn_Result.Open)

	return turn, turn.result == .Open
}

h2m_handshake :: proc(t: ^testing.T, f: ^H2M_Fixture) -> bool {
	magic_string := http2.CLIENT_CONNECTION_PREFACE
	magic := transmute([]u8)magic_string

	if !h2m_write_all(t, f.sockets[1], magic) {
		return false
	}
	settings: [9]u8

	if !h2m_put_frame_header(settings[:], 4, 0, 0, 0) {
		testing.expect(t, false)

		return false
	}

	if !h2m_write_all(t, f.sockets[1], settings[:]) {
		return false
	}
	slot := h2m_slot(f)
	turn := h2_worker_connection_turn(slot, 64 * 1024)
	testing.expect_value(t, turn.result, H2_Worker_Turn_Result.Open)
	testing.expect_value(t, turn.events, 0)

	return turn.result == .Open
}

h2m_dispatch :: proc(t: ^testing.T, f: ^H2M_Fixture, events: int, now_ns: u64) -> bool {
	slot := h2m_slot(f)
	_, ok := h2_dispatch_stream_events(&f.worker, slot, events, now_ns)
	testing.expect_value(t, ok, true)

	return ok
}

h2m_feed_dispatch :: proc(t: ^testing.T, f: ^H2M_Fixture, wire: []u8, now_ns: u64) -> bool {
	turn, turn_ok := h2m_feed_turn(t, f, wire)

	if !turn_ok {
		return false
	}

	return h2m_dispatch(t, f, turn.events, now_ns)
}

h2m_get_managed :: proc(buf: []u8, sid: u32, flags: u8, path: []u8) -> (int, bool) {
	return h2m_client_headers(buf, sid, flags, 2, nil, 0, path, 6, nil)
}

h2m_poll_one :: proc(
	t: ^testing.T,
	queue: ^host.Host_Queue,
) -> (
	event: host.Host_Event,
	ok: bool,
) {
	batch: [4]host.Host_Event
	polled := host.poll_host_queue(queue, batch[:])
	testing.expect_value(t, polled.copied, 1)

	if polled.copied != 1 {
		return {}, false
	}

	return batch[0], true
}

h2m_parse_first_frame :: proc(
	t: ^testing.T,
	buf: []u8,
) -> (
	kind: http2.Frame_Type,
	stream: u32,
	flags: u8,
	length: u32,
	ok: bool,
) {
	header, hdr_ok, too_large := http2.h2_parse_header(buf, 1 << 24)
	testing.expect(t, hdr_ok && !too_large)

	if !hdr_ok || too_large {
		return http2.Frame_Type.Unknown, 0, 0, 0, false
	}

	return header.kind, header.stream_id, header.flags, header.length, true
}

h2m_collect_kinds :: proc(
	t: ^testing.T,
	buf: []u8,
	used: int,
) -> (
	kinds: [16]http2.Frame_Type,
	count: int,
) {
	rest := buf[:used]

	for len(rest) >= 9 && count < len(kinds) {
		header, hdr_ok, too_large := http2.h2_parse_header(rest, 1 << 24)
		testing.expect(t, hdr_ok && !too_large)

		if !hdr_ok || too_large {
			return kinds, count
		}
		kinds[count] = header.kind
		count += 1
		rest = rest[9 + int(header.length):]
	}
	testing.expect_value(t, len(rest), 0)

	return kinds, count
}

h2m_park :: proc(
	t: ^testing.T,
	f: ^H2M_Fixture,
	stream_id: u32,
	end_stream: bool,
	now_ns: u64,
) -> bool {
	wire: [512]u8
	flags := http2.FLAG_END_HEADERS

	if end_stream {
		flags |= http2.FLAG_END_STREAM
	}
	n, frame_ok := h2m_get_managed(wire[:], stream_id, flags, f.route_path[:])

	if !frame_ok {
		testing.expect(t, false)

		return false
	}

	if !h2m_feed_dispatch(t, f, wire[:n], now_ns) {
		return false
	}
	_, event_ok := h2m_poll_one(t, &f.host_queue)

	return event_ok
}

h2m_post_upload :: proc(buf: []u8, sid: u32, flags: u8, path: []u8) -> (int, bool) {
	return h2m_client_headers(buf, sid, flags, 3, nil, 0, path, 6, nil)
}

h2m_client_trailers :: proc(
	buf: []u8,
	sid: u32,
	flags: u8,
	names: []string,
	values: []string,
) -> (
	length: int,
	ok: bool,
) {
	block: [512]u8
	used := 0

	for i in 0 ..< len(names) {
		n, enc_ok := http2.encode_literal_header(
			.Without_Indexing,
			0,
			transmute([]u8)names[i],
			transmute([]u8)values[i],
			false,
			false,
			block[used:],
		)

		if !enc_ok {
			return 0, false
		}
		used += n
	}
	frag_used, frag_ok := http2.encode_headers_payload(block[:used], nil, 0, buf[9:])

	if !frag_ok {
		return 0, false
	}

	if !h2m_put_frame_header(buf[:9], 1, flags, sid, u32(frag_used)) {
		return 0, false
	}

	return 9 + frag_used, true
}

h2m_park_upload :: proc(
	t: ^testing.T,
	f: ^H2M_Fixture,
	stream_id: u32,
	end_stream: bool,
	now_ns: u64,
) -> bool {
	wire: [512]u8
	flags := http2.FLAG_END_HEADERS

	if end_stream {
		flags |= http2.FLAG_END_STREAM
	}
	n, frame_ok := h2m_post_upload(wire[:], stream_id, flags, f.upload_path[:])

	if !frame_ok {
		testing.expect(t, false)

		return false
	}

	if !h2m_feed_dispatch(t, f, wire[:n], now_ns) {
		return false
	}
	_, event_ok := h2m_poll_one(t, &f.host_queue)

	return event_ok
}

h2m_must_headers :: proc(
	t: ^testing.T,
	buf: []u8,
	sid: u32,
	method_idx: u64,
	method_lit: []u8,
	path_idx: u64,
	path_lit: []u8,
	scheme_idx: u64,
	scheme_lit: []u8,
) -> int {
	n, frame_ok := h2m_client_headers(
		buf,
		sid,
		http2.FLAG_END_HEADERS | http2.FLAG_END_STREAM,
		method_idx,
		method_lit,
		path_idx,
		path_lit,
		scheme_idx,
		scheme_lit,
	)
	testing.expect_value(t, frame_ok, true)

	return n
}
