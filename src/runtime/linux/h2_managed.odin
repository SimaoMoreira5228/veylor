#+build linux
package linux_runtime

import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import http2 "veylor:http2"

h2_command_target :: proc(
	worker: ^Static_Worker,
	token: connections.Connection_Token,
) -> (
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
	ok: bool,
) {
	if worker == nil || token.stream_id == 0 {
		return nil, nil, false
	}
	candidate, found := find_h2_worker_slot(worker, int(token.slot))

	if !found {
		return nil, nil, false
	}

	if candidate.token.request_seq != token.request_seq {
		return nil, nil, false
	}
	target, target_found := h2_find_record(candidate, token.stream_id)

	if !target_found {
		return nil, nil, false
	}

	return candidate, target, true
}

h2_dispatch_stream_events :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	event_count: int,
	now_ns: u64,
) -> (
	handled: int,
	ok: bool,
) {
	if worker == nil || slot == nil || !slot.active {
		return 0, false
	}

	if event_count < 0 || event_count > len(slot.events) {
		return 0, false
	}

	for i := 0; i < event_count; i += 1 {
		event := slot.events[i]
		handled += 1

		#partial switch event.kind {
		case .Headers_Ready:
			if !h2_dispatch_headers(worker, slot, event) {
				return handled, false
			}
		case .Data_Ready:
			if !h2_dispatch_data(worker, slot, event) {
				return handled, false
			}
		case .Stream_Reset:
			record, found := h2_find_record(slot, event.stream_id)

			if found {
				h2_release_record(slot, record)
			}
		case .Stream_Closed:
			record, found := h2_find_record(slot, event.stream_id)

			if found {
				record.end_received = true

				if record.resp_ended {
					h2_release_record(slot, record)
				}
			}
		case .Goaway_Received:
			slot.closing = true

			if slot.close_reason == .Unknown {
				slot.close_reason = .Peer_Closed
			}
		}
	}
	h2_grant_receive_credit(worker, slot)
	h2_emit_stalled_writables(worker, slot)

	return handled, true
}

h2_stream_token :: proc(
	slot: ^H2_Worker_Connection,
	stream_id: u32,
) -> connections.Connection_Token {
	token := slot.token
	token.stream_id = stream_id

	return token
}

h2_refuse_stream :: proc(slot: ^H2_Worker_Connection, stream_id: u32) -> bool {
	out := slot.write_buf[slot.write_used:]
	used, rst_ok := http2.h2conn_queue_rst_stream(&slot.conn, stream_id, .Refused_Stream, out)

	if !rst_ok {
		return false
	}
	slot.write_used += used

	return true
}

h2_cancel_stream_record :: proc(
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
) -> bool {
	out := slot.write_buf[slot.write_used:]
	used, rst_ok := http2.h2conn_queue_rst_stream(&slot.conn, record.stream_id, .Cancel, out)

	if !rst_ok {
		return false
	}
	slot.write_used += used
	h2_release_record(slot, record)

	return true
}

h2_pseudo_headers :: proc(headers: []http2.Hpack_Header) -> (method, path, scheme, authority: string) {
	for i := 0; i < len(headers); i += 1 {
		header := headers[i]

		if len(header.name) == 0 || header.name[0] != ':' {
			continue
		}

		if header.name == ":method" && method == "" {
			method = header.value
		}

		if header.name == ":path" && path == "" {
			path = header.value
		}

		if header.name == ":scheme" && scheme == "" {
			scheme = header.value
		}

		if header.name == ":authority" && authority == "" {
			authority = header.value
		}
	}

	return method, path, scheme, authority
}

h2_valid_dispatch_target :: proc(method, path, scheme, authority: string) -> bool {
	if len(method) == 0 || len(method) > H2_WORKER_STREAM_METHOD_CAPACITY {
		return false
	}

	for i := 0; i < len(method); i += 1 {
		if !http1.is_token(method[i]) {
			return false
		}
	}

	if len(path) == 0 || len(path) > host.HOST_MAX_PATH_BYTES {
		return false
	}

	if path[0] != '/' {
		return false
	}

	if scheme != "http" && scheme != "https" {
		return false
	}

	return len(authority) > 0
}

h2_dispatch_headers :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	event: http2.H2_Event,
) -> bool {
	record, parked := h2_find_record(slot, event.stream_id)

	if parked {
		return h2_cancel_stream_record(slot, record)
	}
	method, path, scheme, authority := h2_pseudo_headers(event.headers)

	if !h2_valid_dispatch_target(method, path, scheme, authority) {
		return h2_refuse_stream(slot, event.stream_id)
	}
	parked_record, acquired := h2_acquire_record(slot, event.stream_id)

	if !acquired {
		return h2_refuse_stream(slot, event.stream_id)
	}
	input: [H2_WORKER_STREAM_METHOD_CAPACITY + 1 + host.HOST_MAX_PATH_BYTES]u8

	for i := 0; i < len(method); i += 1 {
		input[i] = method[i]
	}
	target_offset := len(method) + 1

	for i := 0; i < len(path); i += 1 {
		input[target_offset + i] = path[i]
	}
	method_view := input[:len(method)]
	path_view := input[target_offset:target_offset + len(path)]
	head := http1.Request_Head {
		method      = http1.Span{offset = 0, length = len(method_view)},
		target      = http1.Span{offset = target_offset, length = len(path_view)},
		target_form = http1.Request_Target_Form.Origin_Form,
	}
	route, route_result := router.find_static_request_route(worker.routes, head, input[:])

	if route_result != .Found || route.kind != .Managed {
		h2_release_record(slot, parked_record)

		return h2_refuse_stream(slot, event.stream_id)
	}
	record = parked_record
	record.route_id = route.route_id
	record.method_len = len(method_view)
	copy(record.method[:len(method_view)], method_view)
	record.end_received = event.end_stream
	token := h2_stream_token(slot, event.stream_id)
	enqueue_result := host.enqueue_request(
		worker.host_queue,
		token,
		route.route_id,
		method_view,
		path_view,
	)

	if enqueue_result == .Full {
		h2_release_record(slot, record)

		return h2_refuse_stream(slot, event.stream_id)
	}

	if enqueue_result != .Enqueued {
		return false
	}

	return true
}

h2_dispatch_data :: proc(worker: ^Static_Worker, slot: ^H2_Worker_Connection, event: http2.H2_Event) -> bool {
	record, found := h2_find_record(slot, event.stream_id)

	if !found {
		h2_grant_dropped_data(slot, event)

		return true
	}

	if record.resp_ended {
		h2_grant_dropped_data(slot, event)

		return true
	}
	room := len(record.staged) - record.staged_len

	if room < 0 {
		room = 0
	}

	if len(event.data) > room {
		return h2_cancel_stream_record(slot, record)
	}
	copy(record.staged[record.staged_len:], event.data)
	record.staged_len += len(event.data)
	record.ungranted += i64(len(event.data))
	slot.ungranted_conn += i64(len(event.data))

	if event.end_stream {
		record.end_received = true
	}
	body_event := host.Host_Event {
		token         = h2_stream_token(slot, event.stream_id),
		route_id      = record.route_id,
		body_ready    = u32(record.staged_len),
		kind          = .Body_Available,
	}

	if record.end_received {
		body_event.body_complete = 1
	}

	if host.enqueue_body_available(worker.host_queue, body_event) != .Enqueued {
		return h2_cancel_stream_record(slot, record)
	}

	return true
}

h2_grant_dropped_data :: proc(slot: ^H2_Worker_Connection, event: http2.H2_Event) {
	if len(event.data) == 0 {
		return
	}

	if len(slot.write_buf) - slot.write_used < 2 * (http2.FRAME_HEADER_LEN + 4) {
		return
	}
	out := slot.write_buf[slot.write_used:]
	used, stream_ok := http2.h2conn_queue_window_update(
		&slot.conn,
		event.stream_id,
		u32(len(event.data)),
		out,
	)

	if !stream_ok {
		out = slot.write_buf[slot.write_used:]
		used, _ = http2.h2conn_queue_window_update(&slot.conn, 0, u32(len(event.data)), out)
	}

	if used > 0 {
		slot.write_used += used
	}
}

h2_grant_receive_credit :: proc(worker: ^Static_Worker, slot: ^H2_Worker_Connection) {
	if slot.ungranted_conn <= 0 {
		return
	}

	for i := 0; i < len(slot.records); i += 1 {
		record := &slot.records[i]

		if !record.occupied || record.ungranted <= 0 {
			continue
		}
		room := len(record.staged) - record.staged_len

		if room < 0 {
			room = 0
		}
		grant := record.ungranted

		if i64(room) < grant {
			grant = i64(room)
		}

		if grant <= 0 {
			continue
		}

		if len(slot.write_buf) - slot.write_used < 2 * (http2.FRAME_HEADER_LEN + 4) {
			break
		}
		out := slot.write_buf[slot.write_used:]
		stream_used, stream_ok := http2.h2conn_queue_window_update(
			&slot.conn,
			record.stream_id,
			u32(grant),
			out,
		)

		if !stream_ok {
			continue
		}
		slot.write_used += stream_used
		out = slot.write_buf[slot.write_used:]
		conn_used, conn_ok := http2.h2conn_queue_window_update(&slot.conn, 0, u32(grant), out)

		if !conn_ok {
			continue
		}
		slot.write_used += conn_used
		record.ungranted -= grant
		slot.ungranted_conn -= grant
	}
}

h2_emit_stream_writable :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
) -> bool {
	token := h2_stream_token(slot, record.stream_id)

	return host.enqueue_writable(worker.host_queue, token, record.route_id) == .Enqueued
}

h2_send_credit :: proc(
	slot: ^H2_Worker_Connection,
	stream_id: u32,
) -> (
	conn_avail, stream_avail: i64,
	ok: bool,
) {
	return http2.h2conn_send_credit(&slot.conn, stream_id)
}

h2_emit_stalled_writables :: proc(worker: ^Static_Worker, slot: ^H2_Worker_Connection) {
	if !slot.writable_check {
		return
	}
	slot.writable_check = false

	for i := 0; i < len(slot.records); i += 1 {
		record := &slot.records[i]

		if !record.occupied || !record.write_stalled {
			continue
		}

		if !record.resp_started || record.resp_ended {
			record.write_stalled = false

			continue
		}
		conn_avail, stream_avail, credit_ok := h2_send_credit(slot, record.stream_id)

		if !credit_ok || conn_avail <= 0 || stream_avail <= 0 {
			record.write_stalled = true
			slot.writable_check = true

			continue
		}

		if h2_emit_stream_writable(worker, slot, record) {
			record.write_stalled = false
		} else {
			record.write_stalled = true
			slot.writable_check = true
		}
	}
}

refresh_h2_connection_deadline :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	now_ns: u64,
) -> bool {
	if worker == nil || slot == nil || !slot.active {
		return false
	}
	cancel_result := connections.cancel_connection_phase_deadline(worker.table, worker.timers, slot.token)

	if cancel_result != .Cancelled && cancel_result != .Not_Found {
		return false
	}
	schedule := connections.schedule_connection_phase_deadline(
		worker.table,
		worker.timers,
		slot.token,
		now_ns,
		worker.timeouts,
	)

	return schedule.result == .Scheduled
}

h2_execute_read_body :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
	cmd: host.Host_Command,
	now_ns: u64,
) -> Managed_Read_Turn {
	if cmd.kind != .Read_Body {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	if cmd.max_bytes == 0 {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	if record.staged_len == 0 && !record.end_received {
		return Managed_Read_Turn{outcome = .Stalled}
	}
	body_event := host.Host_Event {
		token         = h2_stream_token(slot, record.stream_id),
		route_id      = record.route_id,
		body_ready    = u32(record.staged_len),
		kind          = .Body_Available,
	}

	if record.end_received {
		body_event.body_complete = 1
	}

	if host.enqueue_body_available(worker.host_queue, body_event) != .Enqueued {
		h2_cancel_stream_record(slot, record)

		return Managed_Read_Turn{outcome = .Dropped}
	}

	if !refresh_h2_connection_deadline(worker, slot, now_ns) {
		return Managed_Read_Turn{outcome = .Closed}
	}

	return Managed_Read_Turn{outcome = .Notified}
}

execute_h2_host_command :: proc(worker: ^Static_Worker, cmd: host.Host_Command, now_ns: u64) {
	if cmd.kind == .Cancel && cmd.token.stream_id == 0 {
		slot, found := find_h2_worker_slot(worker, int(cmd.token.slot))

		if !found || slot.token.request_seq != cmd.token.request_seq {
			return
		}
		close_result := close_h2_worker_connection(worker, slot)

		if close_result == .Error {
			worker.managed_result = .Error
		}

		return
	}
	slot, record, found := h2_command_target(worker, cmd.token)

	if !found {
		return
	}

	if cmd.kind == .Cancel {
		h2_cancel_stream_record(slot, record)

		return
	}

	if cmd.kind == .Read_Body {
		read_turn := h2_execute_read_body(worker, slot, record, cmd, now_ns)

		if read_turn.outcome == .Closed {
			close_result := close_h2_worker_connection(worker, slot)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}

	if cmd.kind == .Response_Start {
		response_outcome := h2_execute_response_start(worker, slot, record, cmd, now_ns)

		if response_outcome == .Closed {
			close_result := close_h2_worker_connection(worker, slot)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}

	if cmd.kind == .Response_Begin {
		response_outcome := h2_execute_response_begin(worker, slot, record, cmd, now_ns)

		if response_outcome == .Closed {
			close_result := close_h2_worker_connection(worker, slot)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}

	if cmd.kind == .Response_Write {
		response_outcome := h2_execute_response_write(worker, slot, record, cmd, now_ns)

		if response_outcome == .Closed {
			close_result := close_h2_worker_connection(worker, slot)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}

	if cmd.kind == .Response_End {
		response_outcome := h2_execute_response_end(worker, slot, record, cmd, now_ns)

		if response_outcome == .Closed {
			close_result := close_h2_worker_connection(worker, slot)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}
}
