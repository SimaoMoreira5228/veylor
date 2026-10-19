#+build linux
package linux_runtime

import http2 "veylor:http2"
import host "veylor:runtime/host"

h2_emit_stream_writable :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
) -> bool {
	token := h2_stream_token(slot, record.stream_id)

	return host.enqueue_writable(worker.host_queue, token, record.route_id) == .Enqueued
}

// NOTE: a queued response write is all-or-nothing: the command carries no remainder buffer, so
// partial send credit would silently truncate the tail. Gate execution on a full fit instead.
h2_write_ready_for :: proc(slot: ^H2_Worker_Connection, stream_id: u32, n: u32) -> bool {
	if slot == nil || stream_id == 0 || n == 0 {
		return false
	}

	if len(slot.write_buf) - slot.write_used < http2.FRAME_HEADER_LEN + int(n) {
		return false
	}
	conn_avail, stream_avail, credit_ok := h2_send_credit(slot, stream_id)

	if !credit_ok {
		return false
	}

	return conn_avail >= i64(n) && stream_avail >= i64(n)
}

// NOTE: a response command that is valid but transiently unfittable (send credit or write space
// short) goes back to the head so order is preserved and the next turn retries it. Phase or
// token problems flow through to execute, which drops them.
is_h2_response_command :: proc(cmd: host.Host_Command) -> bool {
	return(
		cmd.kind == .Response_Start ||
		cmd.kind == .Response_Begin ||
		cmd.kind == .Response_Write ||
		cmd.kind == .Response_End ||
		cmd.kind == .Response_Trailers \
	)
}

h2_hold_unready_response_command :: proc(worker: ^Static_Worker, cmd: host.Host_Command) -> bool {
	slot, record, found := h2_command_target(worker, cmd.token)

	if !found {
		return false
	}
	phase_open := false

	switch cmd.kind {
	case .Response_Start, .Response_Begin:
		phase_open = !record.resp_started && !record.resp_ended
	case .Response_Write, .Response_End, .Response_Trailers:
		phase_open = record.resp_started && !record.resp_ended
	case .Read_Body, .Cancel:
		phase_open = false
	}

	if !phase_open || !record.occupied {
		return false
	}

	if h2_response_command_ready(slot, record, cmd) {
		return false
	}
	record.write_stalled = true
	slot.writable_check = true

	if host.enqueue_host_command_front(worker.command_queue, cmd) == .Enqueued {
		return true
	}
	h2_cancel_stream_record(slot, record)

	return true
}

// NOTE: response commands carry the bytes; dequeuing one that cannot fit would truncate (no
// remainder buffer) or reorder (requeueing lets END overtake). Hold a stalled head until a full
// fit: heads and empty frames need a small reserve, bodies need their frame plus send credit.
h2_response_command_ready :: proc(
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
	cmd: host.Host_Command,
) -> bool {
	if slot == nil || record == nil || !record.occupied {
		return false
	}

	need := H2_RESPONSE_HEAD_RESERVE

	switch cmd.kind {
	case .Response_Start, .Response_Begin:
		if record.resp_started || record.resp_ended {
			return false
		}
	case .Response_Write, .Response_End:
		if !record.resp_started || record.resp_ended {
			return false
		}
	case .Response_Trailers:
		if !record.resp_started || record.resp_ended {
			return false
		}

		if cmd.body_length == 0 {
			return false
		}
		need += http2.FRAME_HEADER_LEN + int(cmd.body_length)
	case .Read_Body, .Cancel:
		return true
	}

	if cmd.kind == .Response_Write || cmd.kind == .Response_Start {
		if cmd.body_length > 0 {
			if !h2_write_ready_for(slot, record.stream_id, cmd.body_length) {
				return false
			}
			need += http2.FRAME_HEADER_LEN + int(cmd.body_length)
		}
	}

	if len(slot.write_buf) - slot.write_used < need {
		return false
	}

	return true
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
