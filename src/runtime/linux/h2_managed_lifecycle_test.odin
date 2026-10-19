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

@(test)
test_h2_abandoned_stream_timeout_closes_connection :: proc(t: ^testing.T) {
	f: H2M_Fixture
	testing.expect(t, h2m_init(&f, t))
	defer h2m_close(&f)
	testing.expect(t, h2m_handshake(t, &f))

	slot := h2m_slot(&f)
	testing.expect(t, supervise_h2_worker_connection(&f.worker, slot, 1_000_000_000, false))
	phase, phase_result := connections.connection_phase(&f.table, slot.token)
	testing.expect_value(t, phase_result, connections.Connection_Table_Result.Found)
	testing.expect_value(t, phase, connections.Connection_Phase.Reading_Request_Body)

	testing.expect(t, h2m_park(t, &f, 1, false, 1_000_000_000))
	record, found := h2_find_record(slot, 1)
	testing.expect(t, found)
	testing.expect(t, !record.end_received)

	expired, pop_result := transport.pop_expired_timer(&f.timers, 12_000_000_000)
	testing.expect_value(t, pop_result, transport.Timer_Queue_Result.Expired)
	deadline, deadline_result := connections.expire_connection_deadline(&f.table, expired.timer_id)
	testing.expect_value(t, deadline_result, connections.Connection_Table_Result.Deadline_Expired)
	testing.expect_value(t, deadline.kind, connections.Connection_Deadline_Kind.Body_Progress)

	turn := Static_Worker_Turn {
		result = .Ready,
	}
	testing.expect(t, expire_h2_worker_deadline(&f.worker, int(slot.token.slot), deadline, &turn))
	testing.expect(t, !slot.active)
	testing.expect_value(t, turn.closed_count, 1)
	testing.expect_value(t, f.shutdown.active_connections, u64(0))
	testing.expect_value(
		t,
		f.worker.metrics.close_reasons[int(Static_Connection_Close_Reason.Timed_Out)],
		u64(1),
	)
	_, still_there := h2_find_record(slot, 1)
	testing.expect(t, !still_there)
	f.sockets[0] = -1
}

@(test)
test_h2_max_concurrent_streams_refused :: proc(t: ^testing.T) {
	f: H2M_Fixture
	testing.expect(t, h2m_init(&f, t))
	defer h2m_close(&f)
	testing.expect(t, h2m_handshake(t, &f))

	wire: [512]u8
	for i in 0 ..< 32 {
		sid := u32(1 + i * 2)

		if !h2m_park(t, &f, sid, false, 1_000_000_000) {
			return
		}
		_, found := h2_find_record(h2m_slot(&f), sid)
		testing.expect(t, found)

		if !found {
			return
		}
	}
	testing.expect_value(t, host.pending_host_events(&f.host_queue), 0)

	slot := h2m_slot(&f)
	base := slot.write_used
	n, frame_ok := h2m_client_headers(
		wire[:],
		65,
		http2.FLAG_END_HEADERS,
		2,
		nil,
		0,
		f.route_path[:],
		6,
		nil,
	)
	testing.expect_value(t, frame_ok, true)

	if !frame_ok {
		return
	}
	turn, turn_ok := h2m_feed_turn(t, &f, wire[:n])
	testing.expect_value(t, turn_ok, true)
	handled, dispatch_ok := h2_dispatch_stream_events(&f.worker, slot, turn.events, 1_000_000_000)
	testing.expect_value(t, dispatch_ok, true)
	testing.expect_value(t, handled, turn.events)
	_, refused := h2_find_record(slot, 65)
	testing.expect_value(t, refused, false)
	testing.expect_value(t, host.pending_host_events(&f.host_queue), 0)

	kind, stream, _, length, rst_ok := h2m_parse_first_frame(t, slot.write_buf[base:])
	testing.expect_value(t, rst_ok, true)
	testing.expect_value(t, kind, http2.Frame_Type.Rst_Stream)
	testing.expect_value(t, stream, u32(65))
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
	testing.expect_value(t, code, http2.Error_Code.Refused_Stream)

	code2, conn_failed := http2.h2conn_error(&slot.conn)
	testing.expect_value(t, conn_failed, false)
	testing.expect_value(t, code2, http2.Error_Code.No_Error)
	testing.expect(t, slot.active)
}
