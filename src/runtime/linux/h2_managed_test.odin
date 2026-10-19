#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import http2 "veylor:http2"
import "veylor:transport"

H2M_Fixture :: struct {
	table_slots:   [1]connections.Connection_Slot,
	table_free:    [1]u32,
	table:         connections.Connection_Table,
	timer_storage: [8]transport.Timer_Expiration,
	timer_index:   [8 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32,
	timers:        transport.Timer_Queue,
	event_storage: [8]host.Host_Event,
	host_queue:    host.Host_Queue,
	route_storage: [8]router.Static_Route,
	route_index:   [16]router.Route_Index_Slot,
	routes:        router.Route_Table,
	route_method:  [3]u8,
	route_path:    [8]u8,
	static_path:   [7]u8,
	slot_store:    [1]H2_Worker_Connection,
	worker:        Static_Worker,
	sockets:       [2]linux.Fd,
}

h2m_init :: proc(f: ^H2M_Fixture, t: ^testing.T) -> bool {
	table, table_result := connections.init_connection_table(7, 5, f.table_slots[:], f.table_free[:])
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
		table        = &f.table,
		host_queue   = &f.host_queue,
		routes       = &f.routes,
		timers       = &f.timers,
		timeouts     = connections.Connection_Timeouts {
			request_head_ns = 10_000_000_000,
			body_progress_ns = 10_000_000_000,
			response_write_ns = 10_000_000_000,
			keep_alive_idle_ns = 5_000_000_000,
		},
		h2_states    = f.slot_store[:],
		byte_budget  = 64 * 1024,
		event_budget = 128,
	}
	schedule := connections.schedule_connection_phase_deadline(
		&f.table,
		&f.timers,
		token,
		1_000_000_000,
		f.worker.timeouts,
	)
	testing.expect_value(t, schedule.result, connections.Connection_Deadline_Schedule_Result.Scheduled)

	return schedule.result == .Scheduled
}

h2m_close :: proc(f: ^H2M_Fixture) {
	linux.close(f.sockets[0])
	linux.close(f.sockets[1])
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

h2m_client_goaway :: proc(buf: []u8, last_id: u32, code: http2.Error_Code) -> (length: int, ok: bool) {
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

h2m_dispatch :: proc(
	t: ^testing.T,
	f: ^H2M_Fixture,
	events: int,
	now_ns: u64,
) -> bool {
	slot := h2m_slot(f)
	_, ok := h2_dispatch_stream_events(&f.worker, slot, events, now_ns)
	testing.expect_value(t, ok, true)

	return ok
}

h2m_feed_dispatch :: proc(
	t: ^testing.T,
	f: ^H2M_Fixture,
	wire: []u8,
	now_ns: u64,
) -> bool {
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

@(test)
test_h2_dispatch_parks_managed_stream :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}
	wire: [512]u8
	n, frame_ok := h2m_get_managed(
		wire[:],
		1,
		http2.FLAG_END_HEADERS | http2.FLAG_END_STREAM,
		f.route_path[:],
	)
	testing.expect_value(t, frame_ok, true)

	if !frame_ok {
		return
	}
	turn, turn_ok := h2m_feed_turn(t, &f, wire[:n])

	if !turn_ok {
		return
	}
	testing.expect_value(t, turn.events, 2)
	testing.expect_value(t, h2m_dispatch(t, &f, turn.events, 2_000_000_000), true)

	slot := h2m_slot(&f)
	record, found := h2_find_record(slot, 1)
	testing.expect_value(t, found, true)

	if !found {
		return
	}
	testing.expect_value(t, record.route_id, u64(1))
	testing.expect_value(t, record.end_received, true)
	testing.expect_value(t, string(record.method[:record.method_len]), "GET")

	kinds, kind_count := h2m_collect_kinds(t, slot.write_buf[:slot.write_used], slot.write_used)
	testing.expect_value(t, kind_count, 0)

	host_event, event_ok := h2m_poll_one(t, &f.host_queue)
	testing.expect_value(t, event_ok, true)
	testing.expect_value(t, host_event.kind, host.Host_Event_Kind.Request)
	testing.expect_value(t, host_event.token.stream_id, u32(1))
	testing.expect_value(t, host_event.token.slot, slot.token.slot)
	testing.expect_value(t, string(host_event.method[:host_event.method_length]), "GET")
	testing.expect_value(t, string(host_event.path[:host_event.path_length]), "/managed")
}

@(test)
test_h2_dispatch_refuses_static_route :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}
	wire: [512]u8
	n, frame_ok := h2m_client_headers(
		wire[:],
		1,
		http2.FLAG_END_HEADERS | http2.FLAG_END_STREAM,
		2,
		nil,
		0,
		f.static_path[:],
		6,
		nil,
	)
	testing.expect_value(t, frame_ok, true)

	if !frame_ok {
		return
	}
	turn, turn_ok := h2m_feed_turn(t, &f, wire[:n])

	if !turn_ok {
		return
	}
	testing.expect_value(t, turn.events, 2)
	testing.expect_value(t, h2m_dispatch(t, &f, turn.events, 2_000_000_000), true)
	_, found := h2_find_record(h2m_slot(&f), 1)
	testing.expect_value(t, found, false)
	testing.expect_value(t, host.pending_host_events(&f.host_queue), 0)

	slot := h2m_slot(&f)
	kinds, kind_count := h2m_collect_kinds(t, slot.write_buf[:slot.write_used], slot.write_used)
	testing.expect_value(t, kind_count, 1)
	testing.expect_value(t, kinds[0], http2.Frame_Type.Rst_Stream)
}

@(test)
test_h2_dispatch_refuses_bad_pseudo_headers :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}
	space_method := transmute([]u8)string("GE T")
	gopher := transmute([]u8)string("gopher")
	relative := transmute([]u8)string("managed")
	wire: [512]u8

	if !h2m_feed_dispatch(t, &f, wire[:h2m_must_headers(t, wire[:], 1, 0, space_method, 0, f.route_path[:], 6, nil)], 2_000_000_000) {
		return
	}
	_, found := h2_find_record(h2m_slot(&f), 1)
	testing.expect_value(t, found, false)

	if !h2m_feed_dispatch(t, &f, wire[:h2m_must_headers(t, wire[:], 3, 2, nil, 0, f.route_path[:], 0, gopher)], 2_000_000_000) {
		return
	}
	_, found = h2_find_record(h2m_slot(&f), 3)
	testing.expect_value(t, found, false)

	if !h2m_feed_dispatch(t, &f, wire[:h2m_must_headers(t, wire[:], 5, 2, nil, 0, relative, 6, nil)], 2_000_000_000) {
		return
	}
	_, found = h2_find_record(h2m_slot(&f), 5)
	testing.expect_value(t, found, false)
	testing.expect_value(t, host.pending_host_events(&f.host_queue), 0)
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

@(test)
test_h2_dispatch_stages_data_and_emits_body_available :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}
	wire: [4096]u8
	n, frame_ok := h2m_get_managed(wire[:], 1, http2.FLAG_END_HEADERS, f.route_path[:])
	testing.expect_value(t, frame_ok, true)

	if !frame_ok {
		return
	}

	if !h2m_feed_dispatch(t, &f, wire[:n], 2_000_000_000) {
		return
	}
	_, _ = h2m_poll_one(t, &f.host_queue)

	body := transmute([]u8)string("hello")
	m, data_ok := h2m_client_data(wire[:], 1, 0, body)
	testing.expect_value(t, data_ok, true)

	if !data_ok {
		return
	}

	if !h2m_feed_dispatch(t, &f, wire[:m], 2_000_000_000) {
		return
	}
	slot := h2m_slot(&f)
	record, found := h2_find_record(slot, 1)
	testing.expect_value(t, found, true)

	if !found {
		return
	}
	testing.expect_value(t, record.staged_len, 5)
	testing.expect_value(t, string(record.staged[:record.staged_len]), "hello")
	testing.expect_value(t, record.end_received, false)

	host_event, event_ok := h2m_poll_one(t, &f.host_queue)
	testing.expect_value(t, event_ok, true)
	testing.expect_value(t, host_event.kind, host.Host_Event_Kind.Body_Available)
	testing.expect_value(t, host_event.token.stream_id, u32(1))
	testing.expect_value(t, host_event.body_ready, u32(5))
	testing.expect_value(t, host_event.body_complete, u32(0))

	kinds, kind_count := h2m_collect_kinds(t, slot.write_buf[:slot.write_used], slot.write_used)
	grants := 0

	for i := 0; i < kind_count; i += 1 {
		if kinds[i] == .Window_Update {
			grants += 1
		}
	}
	testing.expect_value(t, grants, 2)
	testing.expect_value(t, record.ungranted, i64(0))
	testing.expect_value(t, slot.ungranted_conn, i64(0))
}

@(test)
test_h2_dispatch_marks_end_stream :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}

	if !h2m_park(t, &f, 1, false, 2_000_000_000) {
		return
	}
	wire: [512]u8
	body := transmute([]u8)string("bye")
	m, data_ok := h2m_client_data(wire[:], 1, http2.FLAG_END_STREAM, body)
	testing.expect_value(t, data_ok, true)

	if !data_ok {
		return
	}

	if !h2m_feed_dispatch(t, &f, wire[:m], 2_000_000_000) {
		return
	}
	slot := h2m_slot(&f)
	record, found := h2_find_record(slot, 1)
	testing.expect_value(t, found, true)

	if !found {
		return
	}
	testing.expect_value(t, record.end_received, true)

	host_event, event_ok := h2m_poll_one(t, &f.host_queue)
	testing.expect_value(t, event_ok, true)
	testing.expect_value(t, host_event.body_complete, u32(1))
}

@(test)
test_h2_dispatch_cancels_overflowing_stream :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}

	if !h2m_park(t, &f, 1, false, 2_000_000_000) {
		return
	}
	slot := h2m_slot(&f)
	record, found := h2_find_record(slot, 1)
	testing.expect_value(t, found, true)

	if !found {
		return
	}
	record.staged_len = len(record.staged)
	wire: [4096]u8
	big := transmute([]u8)string("overflow")
	m, data_ok := h2m_client_data(wire[:], 1, 0, big)
	testing.expect_value(t, data_ok, true)

	if !data_ok {
		return
	}
	base := slot.write_used

	if !h2m_feed_dispatch(t, &f, wire[:m], 2_000_000_000) {
		return
	}
	_, still_there := h2_find_record(slot, 1)
	testing.expect_value(t, still_there, false)

	kind, stream, _, length, frame_ok := h2m_parse_first_frame(t, slot.write_buf[base:])
	testing.expect_value(t, frame_ok, true)
	testing.expect_value(t, kind, http2.Frame_Type.Rst_Stream)
	testing.expect_value(t, stream, u32(1))
	testing.expect_value(t, length, u32(http2.RST_STREAM_PAYLOAD_LEN))
	header := http2.Frame_Header {
		length    = length,
		kind      = kind,
		stream_id = stream,
	}
	code, dec_ok := http2.decode_rst_stream_payload(header, slot.write_buf[base + 9:base + 9 + int(length)])
	testing.expect_value(t, dec_ok, true)
	testing.expect_value(t, code, http2.Error_Code.Cancel)
}

@(test)
test_h2_dispatch_trailing_headers_cancel_stream :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}

	if !h2m_park(t, &f, 1, false, 2_000_000_000) {
		return
	}
	wire: [512]u8
	n, frame_ok := h2m_get_managed(wire[:], 1, http2.FLAG_END_HEADERS | http2.FLAG_END_STREAM, f.route_path[:])
	testing.expect_value(t, frame_ok, true)

	if !frame_ok {
		return
	}

	if !h2m_feed_dispatch(t, &f, wire[:n], 2_000_000_000) {
		return
	}
	_, gone := h2_find_record(h2m_slot(&f), 1)
	testing.expect_value(t, gone, false)
}

@(test)
test_h2_dispatch_frees_reset_stream :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}

	if !h2m_park(t, &f, 1, false, 2_000_000_000) {
		return
	}
	_, found := h2_find_record(h2m_slot(&f), 1)
	testing.expect_value(t, found, true)

	wire: [64]u8
	m, rst_ok := h2m_client_rst(wire[:], 1, .Cancel)
	testing.expect_value(t, rst_ok, true)

	if !rst_ok {
		return
	}

	if !h2m_feed_dispatch(t, &f, wire[:m], 2_000_000_000) {
		return
	}
	_, gone := h2_find_record(h2m_slot(&f), 1)
	testing.expect_value(t, gone, false)
}

@(test)
test_h2_dispatch_goaway_marks_closing :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}
	wire: [64]u8
	m, goaway_ok := h2m_client_goaway(wire[:], 0, .No_Error)
	testing.expect_value(t, goaway_ok, true)

	if !goaway_ok {
		return
	}

	if !h2m_feed_dispatch(t, &f, wire[:m], 2_000_000_000) {
		return
	}
	slot := h2m_slot(&f)
	testing.expect_value(t, slot.closing, true)
	testing.expect_value(t, slot.close_reason, H2_Worker_Close_Reason.Peer_Closed)
}

@(test)
test_h2_command_target_validates_stream :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}

	if !h2m_park(t, &f, 1, true, 2_000_000_000) {
		return
	}
	token := h2_stream_token(h2m_slot(&f), 1)
	_, _, found := h2_command_target(&f.worker, token)
	testing.expect_value(t, found, true)

	bad_stream := token
	bad_stream.stream_id = 3
	_, _, missing := h2_command_target(&f.worker, bad_stream)
	testing.expect_value(t, missing, false)

	bad_seq := token
	bad_seq.request_seq += 1
	_, _, stale := h2_command_target(&f.worker, bad_seq)
	testing.expect_value(t, stale, false)

	bad_conn := token
	bad_conn.stream_id = 0
	_, _, conn_level := h2_command_target(&f.worker, bad_conn)
	testing.expect_value(t, conn_level, false)
}
