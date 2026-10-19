#+build linux
package linux_runtime

import http1 "veylor:http1"
import response "veylor:http1/response"
import host "veylor:runtime/host"
import http2 "veylor:http2"

h2_execute_response_start :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
	cmd: host.Host_Command,
	now_ns: u64,
) -> Managed_Response_Outcome {
	if cmd.kind != .Response_Start {
		return .Dropped
	}

	if record.resp_started || record.resp_ended {
		return .Dropped
	}
	if cmd.status < 200 || cmd.status > 599 {
		h2_cancel_stream_record(slot, record)

		return .Dropped
	}

	if cmd.reason_length == 0 ||
	   cmd.reason_length > host.HOST_MAX_RESPONSE_REASON_BYTES ||
	   cmd.body_length > host.HOST_MAX_RESPONSE_BODY_BYTES {
		h2_cancel_stream_record(slot, record)

		return .Dropped
	}

	reason_storage := cmd.reason
	reason := reason_storage[:cmd.reason_length]

	if !response.valid_response_value(reason) {
		h2_cancel_stream_record(slot, record)

		return .Dropped
	}
	head_only := http1.bytes_equal(record.method[:record.method_len], "HEAD")
	empty := head_only || cmd.body_length == 0
	snapshot := slot.write_used
	head_used, head_ok := http2.h2conn_queue_response_headers(
		&slot.conn,
		record.stream_id,
		u16(cmd.status),
		empty,
		slot.write_buf[slot.write_used:],
	)

	if !head_ok {
		slot.write_used = snapshot
		h2_cancel_stream_record(slot, record)

		return .Dropped
	}
	slot.write_used += head_used
	record.resp_started = true

	if !empty {
		body_storage := cmd.body
		body := body_storage[:cmd.body_length]
		data_used, data_ok := http2.h2conn_queue_response_data(
			&slot.conn,
			record.stream_id,
			body,
			true,
			slot.write_buf[slot.write_used:],
		)

		if !data_ok || data_used != 9 + len(body) {
			slot.write_used = snapshot
			h2_cancel_stream_record(slot, record)

			return .Dropped
		}
		slot.write_used += data_used
	}
	record.resp_ended = true

	if record.end_received {
		h2_release_record(slot, record)
	}

	if !refresh_h2_connection_deadline(worker, slot, now_ns) {
		return .Closed
	}

	return .Responded
}

h2_execute_response_begin :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
	cmd: host.Host_Command,
	now_ns: u64,
) -> Managed_Response_Outcome {
	if cmd.kind != .Response_Begin {
		return .Dropped
	}

	if record.resp_started || record.resp_ended {
		return .Dropped
	}

	if http1.bytes_equal(record.method[:record.method_len], "HEAD") {
		h2_cancel_stream_record(slot, record)

		return .Dropped
	}
	if cmd.status < 200 || cmd.status > 599 {
		h2_cancel_stream_record(slot, record)

		return .Dropped
	}

	if cmd.reason_length == 0 ||
	   cmd.reason_length > host.HOST_MAX_RESPONSE_REASON_BYTES ||
	   cmd.body_length != 0 {
		h2_cancel_stream_record(slot, record)

		return .Dropped
	}

	reason_storage := cmd.reason
	reason := reason_storage[:cmd.reason_length]

	if !response.valid_response_value(reason) {
		h2_cancel_stream_record(slot, record)

		return .Dropped
	}
	head_used, head_ok := http2.h2conn_queue_response_headers(
		&slot.conn,
		record.stream_id,
		u16(cmd.status),
		false,
		slot.write_buf[slot.write_used:],
	)

	if !head_ok {
		h2_cancel_stream_record(slot, record)

		return .Dropped
	}
	slot.write_used += head_used
	record.resp_started = true

	if !h2_emit_stream_writable(worker, slot, record) {
		record.write_stalled = true
		slot.writable_check = true
	}

	if !refresh_h2_connection_deadline(worker, slot, now_ns) {
		return .Closed
	}

	return .Responded
}

h2_execute_response_write :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
	cmd: host.Host_Command,
	now_ns: u64,
) -> Managed_Response_Outcome {
	if cmd.kind != .Response_Write {
		return .Dropped
	}

	if !record.resp_started || record.resp_ended {
		return .Dropped
	}

	if cmd.body_length == 0 || cmd.body_length > host.HOST_MAX_RESPONSE_BODY_BYTES {
		h2_cancel_stream_record(slot, record)

		return .Dropped
	}
	body_storage := cmd.body
	body := body_storage[:cmd.body_length]
	data_used, data_ok := http2.h2conn_queue_response_data(
		&slot.conn,
		record.stream_id,
		body,
		false,
		slot.write_buf[slot.write_used:],
	)

	if !data_ok || data_used != 9 + len(body) {
		record.write_stalled = true
		slot.writable_check = true

		return .Dropped
	}
	slot.write_used += data_used
	conn_avail, stream_avail, credit_ok := h2_send_credit(slot, record.stream_id)

	if !credit_ok || conn_avail <= 0 || stream_avail <= 0 {
		record.write_stalled = true
		slot.writable_check = true
	} else if !h2_emit_stream_writable(worker, slot, record) {
		record.write_stalled = true
		slot.writable_check = true
	}

	if !refresh_h2_connection_deadline(worker, slot, now_ns) {
		return .Closed
	}

	return .Responded
}

h2_execute_response_end :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
	cmd: host.Host_Command,
	now_ns: u64,
) -> Managed_Response_Outcome {
	if cmd.kind != .Response_End {
		return .Dropped
	}

	if !record.resp_started || record.resp_ended {
		return .Dropped
	}
	empty: [0]u8
	data_used, data_ok := http2.h2conn_queue_response_data(
		&slot.conn,
		record.stream_id,
		empty[:],
		true,
		slot.write_buf[slot.write_used:],
	)

	if !data_ok {
		return .Dropped
	}
	slot.write_used += data_used
	record.resp_ended = true

	if record.end_received {
		h2_release_record(slot, record)
	}

	if !refresh_h2_connection_deadline(worker, slot, now_ns) {
		return .Closed
	}

	return .Responded
}
