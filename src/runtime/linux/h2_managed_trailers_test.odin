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
test_h2_response_write_waits_for_credit :: proc(t: ^testing.T) {
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
	command_storage: [host.HOST_COMMAND_QUEUE_DEPTH]host.Host_Command
	command_queue := host.init_host_command_queue(command_storage[:])
	f.worker.command_queue = &command_queue
	f.worker.event_budget = 64
	begin := h2m_response_cmd(.Response_Begin, 1)
	begin.status = 200
	begin.reason_length = 2
	begin.reason[0] = 'O'
	begin.reason[1] = 'K'
	execute_h2_host_command(&f.worker, begin, 2_000_000_000)
	slot := h2m_slot(&f)
	record, parked := h2_find_record(slot, 1)
	testing.expect_value(t, parked, true)

	if !parked {
		return
	}
	testing.expect_value(t, record.resp_started, true)
	slot.conn.conn_send.value = 3

	for i in 0 ..< slot.conn.table.used {
		if slot.conn.table.slots[i].id == 1 {
			slot.conn.table.slots[i].send_window.value = 3
		}
	}
	write := h2m_response_cmd(.Response_Write, 1)
	body_text := "hello"
	write.body_length = u32(len(body_text))
	copy(write.body[:len(body_text)], transmute([]u8)body_text)
	testing.expect_value(
		t,
		host.enqueue_host_command(&command_queue, write),
		host.Host_Command_Result.Enqueued,
	)
	write_base := slot.write_used
	drain_host_commands(&f.worker, 2_000_000_000)
	testing.expect_value(t, slot.write_used, write_base)
	testing.expect_value(t, slot.conn.conn_send.value, i64(3))
	testing.expect_value(t, host.pending_host_commands(&command_queue), 1)
	_, parked = h2_find_record(slot, 1)
	testing.expect_value(t, parked, true)
	slot.conn.conn_send.value = http2.FLOW_INITIAL_WINDOW_SIZE

	for i in 0 ..< slot.conn.table.used {
		if slot.conn.table.slots[i].id == 1 {
			slot.conn.table.slots[i].send_window.value = http2.FLOW_INITIAL_WINDOW_SIZE
		}
	}
	drain_host_commands(&f.worker, 2_000_000_001)
	testing.expect_value(t, host.pending_host_commands(&command_queue), 0)
	testing.expect_value(t, slot.write_used, write_base + 9 + len(body_text))
	_, parked = h2_find_record(slot, 1)
	testing.expect_value(t, parked, true)
	kinds, kind_count := h2m_collect_kinds(t, slot.write_buf[:slot.write_used], slot.write_used)

	for i in 0 ..< kind_count {
		testing.expect(t, kinds[i] != .Rst_Stream)
		testing.expect(t, kinds[i] != .GoAway)
	}
}

@(test)
test_h2_dispatch_trailing_pseudo_headers_cancel_stream :: proc(t: ^testing.T) {
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

	if !h2m_feed_dispatch(t, &f, wire[:n], 2_000_000_000) {
		return
	}
	_, gone := h2_find_record(h2m_slot(&f), 1)
	testing.expect_value(t, gone, false)
}

@(test)
test_h2_dispatch_unlisted_trailer_cancels_stream :: proc(t: ^testing.T) {
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
	names := [?]string{"x-checksum"}
	values := [?]string{"abc"}
	n, frame_ok := h2m_client_trailers(
		wire[:],
		1,
		http2.FLAG_END_HEADERS | http2.FLAG_END_STREAM,
		names[:],
		values[:],
	)
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
test_h2_dispatch_stages_listed_trailers :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}

	if !h2m_park_upload(t, &f, 1, false, 2_000_000_000) {
		return
	}
	wire: [512]u8
	body := transmute([]u8)string("hello")
	m, data_ok := h2m_client_data(wire[:], 1, 0, body)
	testing.expect_value(t, data_ok, true)

	if !data_ok {
		return
	}

	if !h2m_feed_dispatch(t, &f, wire[:m], 2_000_000_000) {
		return
	}
	_, event_ok := h2m_poll_one(t, &f.host_queue)
	testing.expect_value(t, event_ok, true)
	names := [?]string{"x-checksum"}
	values := [?]string{"abc"}
	n, frame_ok := h2m_client_trailers(
		wire[:],
		1,
		http2.FLAG_END_HEADERS | http2.FLAG_END_STREAM,
		names[:],
		values[:],
	)
	testing.expect_value(t, frame_ok, true)

	if !frame_ok {
		return
	}

	if !h2m_feed_dispatch(t, &f, wire[:n], 2_000_000_000) {
		return
	}
	slot := h2m_slot(&f)
	record, parked := h2_find_record(slot, 1)
	testing.expect_value(t, parked, true)

	if !parked {
		return
	}
	testing.expect_value(t, record.trailer_count, 1)
	testing.expect_value(t, record.end_received, true)
	name := string(
		record.trailer_data[record.trailers[0].name_offset:][:record.trailers[0].name_length],
	)
	testing.expect_value(t, name, "x-checksum")
	value := string(
		record.trailer_data[record.trailers[0].value_offset:][:record.trailers[0].value_length],
	)
	testing.expect_value(t, value, "abc")
	complete_event, complete_ok := h2m_poll_one(t, &f.host_queue)
	testing.expect_value(t, complete_ok, true)
	testing.expect_value(t, complete_event.kind, host.Host_Event_Kind.Body_Available)
	testing.expect_value(t, complete_event.body_complete, u32(1))
	testing.expect_value(t, complete_event.body_ready, u32(5))
}
