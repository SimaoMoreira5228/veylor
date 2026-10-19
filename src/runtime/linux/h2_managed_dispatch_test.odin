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

	if !h2m_feed_dispatch(
		t,
		&f,
		wire[:h2m_must_headers(t, wire[:], 1, 0, space_method, 0, f.route_path[:], 6, nil)],
		2_000_000_000,
	) {
		return
	}
	_, found := h2_find_record(h2m_slot(&f), 1)
	testing.expect_value(t, found, false)

	if !h2m_feed_dispatch(
		t,
		&f,
		wire[:h2m_must_headers(t, wire[:], 3, 2, nil, 0, f.route_path[:], 0, gopher)],
		2_000_000_000,
	) {
		return
	}
	_, found = h2_find_record(h2m_slot(&f), 3)
	testing.expect_value(t, found, false)

	if !h2m_feed_dispatch(
		t,
		&f,
		wire[:h2m_must_headers(t, wire[:], 5, 2, nil, 0, relative, 6, nil)],
		2_000_000_000,
	) {
		return
	}
	_, found = h2_find_record(h2m_slot(&f), 5)
	testing.expect_value(t, found, false)
	testing.expect_value(t, host.pending_host_events(&f.host_queue), 0)
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
	kinds, kind_count := h2m_collect_kinds(t, slot.write_buf[base:], slot.write_used - base)
	testing.expect_value(t, kind_count, 3)
	testing.expect_value(t, kinds[0], http2.Frame_Type.Window_Update)
	testing.expect_value(t, kinds[1], http2.Frame_Type.Window_Update)
	testing.expect_value(t, kinds[2], http2.Frame_Type.Rst_Stream)
	rest := slot.write_buf[base:]

	for i in 0 ..< 2 {
		header, hdr_ok, too_large := http2.h2_parse_header(rest, 1 << 24)
		testing.expect(t, hdr_ok && !too_large)

		if !hdr_ok || too_large {
			return
		}
		rest = rest[9 + int(header.length):]
	}
	kind, stream, _, length, frame_ok := h2m_parse_first_frame(t, rest)
	testing.expect_value(t, frame_ok, true)
	testing.expect_value(t, kind, http2.Frame_Type.Rst_Stream)
	testing.expect_value(t, stream, u32(1))
	testing.expect_value(t, length, u32(http2.RST_STREAM_PAYLOAD_LEN))
	header := http2.Frame_Header {
		length    = length,
		kind      = kind,
		stream_id = stream,
	}
	code, dec_ok := http2.decode_rst_stream_payload(
		header,
		slot.write_buf[base + 9:base + 9 + int(length)],
	)
	testing.expect_value(t, dec_ok, true)
	testing.expect_value(t, code, http2.Error_Code.Cancel)
}

@(test)
test_h2_dispatch_large_frame_splits_and_resumes :: proc(t: ^testing.T) {
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
	testing.expect_value(t, slot.conn.conn_recv.value, http2.FLOW_INITIAL_WINDOW_SIZE)
	stream_window_before := min(i64)

	for i in 0 ..< slot.conn.table.used {
		if slot.conn.table.slots[i].id == 1 {
			stream_window_before = slot.conn.table.slots[i].recv_window.value
		}
	}
	testing.expect_value(t, stream_window_before, i64(H2_WORKER_INITIAL_WINDOW_SIZE))
	payload := make([]u8, 3000, context.temp_allocator)
	for i in 0 ..< len(payload) {
		payload[i] = u8(i & 0xff)
	}
	wire: [4096]u8
	m, data_ok := h2m_client_data(wire[:], 1, 0, payload)
	testing.expect_value(t, data_ok, true)

	if !data_ok {
		return
	}

	if !h2m_feed_dispatch(t, &f, wire[:m], 2_000_000_000) {
		return
	}
	record, parked := h2_find_record(slot, 1)
	testing.expect_value(t, parked, true)

	if !parked {
		return
	}
	testing.expect_value(t, record.staged_len, 2048)
	testing.expect_value(t, h2m_drain_socket(t, f.sockets[1]), 54)

	host_event, event_ok := h2m_poll_one(t, &f.host_queue)
	testing.expect_value(t, event_ok, true)
	testing.expect_value(t, host_event.kind, host.Host_Event_Kind.Body_Available)
	testing.expect_value(t, host_event.body_ready, u32(2048))

	record.staged_len = 0
	sweep_turn := Static_Worker_Turn{}
	sweep_h2_receive_grants(&f.worker, 64 * 1024, 2_000_000_001, &sweep_turn)
	testing.expect_value(t, sweep_turn.result, Static_Worker_Result.Ready)
	record, parked = h2_find_record(slot, 1)
	testing.expect_value(t, parked, true)

	if !parked {
		return
	}
	testing.expect_value(t, record.staged_len, 952)
	testing.expect_value(t, slot.conn.conn_recv.value, http2.FLOW_INITIAL_WINDOW_SIZE)
	stream_window_after := min(i64)

	for i in 0 ..< slot.conn.table.used {
		if slot.conn.table.slots[i].id == 1 {
			stream_window_after = slot.conn.table.slots[i].recv_window.value
		}
	}
	testing.expect_value(t, stream_window_after, i64(H2_WORKER_INITIAL_WINDOW_SIZE))
	host_event, event_ok = h2m_poll_one(t, &f.host_queue)
	testing.expect_value(t, event_ok, true)
	testing.expect_value(t, host_event.kind, host.Host_Event_Kind.Body_Available)
	testing.expect_value(t, host_event.body_ready, u32(952))
	grants := h2m_read_socket(t, f.sockets[1], 52)
	testing.expect_value(t, len(grants), 52)
	expect_increments := [4]u32{2048, 2048, 952, 952}
	expect_streams := [4]u32{1, 0, 1, 0}
	rest := grants[:]

	for i in 0 ..< 4 {
		header, hdr_ok, too_large := http2.h2_parse_header(rest, 1 << 24)
		testing.expect(t, hdr_ok && !too_large)

		if !hdr_ok || too_large {
			return
		}
		testing.expect_value(t, header.kind, http2.Frame_Type.Window_Update)
		testing.expect_value(t, header.stream_id, expect_streams[i])
		increment, dec_ok := http2.decode_window_update_payload(header, rest[9:9 + 4])
		testing.expect_value(t, dec_ok, true)
		testing.expect_value(t, increment, expect_increments[i])
		rest = rest[13:]
	}
}
