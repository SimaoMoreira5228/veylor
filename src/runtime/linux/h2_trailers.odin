#+build linux
package linux_runtime

import http1 "veylor:http1"
import response "veylor:http1/response"
import http2 "veylor:http2"
import router "veylor:router"
import host "veylor:runtime/host"

h2_trailer_allowed :: proc(
	worker: ^Static_Worker,
	record: ^H2_Stream_Record,
	name: string,
) -> bool {
	if worker == nil || record == nil || len(name) == 0 {
		return false
	}

	if worker.routes == nil {
		return false
	}

	if record.route_id == 0 || int(record.route_id) > len(worker.routes.routes) {
		return false
	}
	route := &worker.routes.routes[record.route_id - 1]

	if route.kind != .Managed {
		return false
	}

	for i := 0; i < route.managed_trailer_count; i += 1 {
		if http1.ascii_case_insensitive_bytes_equal(
			transmute([]u8)name,
			route.managed_trailers[i],
		) {
			return true
		}
	}

	return false
}

// NOTE: a second HEADERS block on a parked stream is its trailers, not a new request. Pseudo
// headers, off-allowlist names, and over-cap storage cancel the stream; the connection survives
// and the host's token goes stale like any other worker-side teardown.
h2_dispatch_trailers :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	record: ^H2_Stream_Record,
	event: http2.H2_Event,
) -> bool {
	if worker == nil || slot == nil || record == nil {
		return false
	}

	if record.resp_ended {
		return true
	}
	if !event.end_stream {
		return h2_cancel_stream_record(slot, record)
	}

	for i := 0; i < len(event.headers); i += 1 {
		header := event.headers[i]

		if len(header.name) == 0 || header.name[0] == ':' {
			return h2_cancel_stream_record(slot, record)
		}

		if !router.valid_trailer_name(transmute([]u8)header.name) {
			return h2_cancel_stream_record(slot, record)
		}

		if !response.valid_response_value(transmute([]u8)header.value) {
			return h2_cancel_stream_record(slot, record)
		}

		if !h2_trailer_allowed(worker, record, header.name) {
			return h2_cancel_stream_record(slot, record)
		}

		if record.trailer_count >= len(record.trailers) {
			return h2_cancel_stream_record(slot, record)
		}
		need := len(header.name) + len(header.value)

		if need > len(record.trailer_data) - record.trailer_used {
			return h2_cancel_stream_record(slot, record)
		}
		field := &record.trailers[record.trailer_count]
		field.name_offset = record.trailer_used
		field.name_length = len(header.name)
		copy(record.trailer_data[record.trailer_used:], header.name)
		record.trailer_used += len(header.name)
		field.value_offset = record.trailer_used
		field.value_length = len(header.value)
		copy(record.trailer_data[record.trailer_used:], header.value)
		record.trailer_used += len(header.value)
		record.trailer_count += 1
	}
	record.end_received = event.end_stream

	if !record.end_received {
		return true
	}
	body_event := host.Host_Event {
		token         = h2_stream_token(slot, event.stream_id),
		route_id      = record.route_id,
		body_ready    = u32(record.staged_len),
		body_complete = 1,
		kind          = .Body_Available,
	}

	if host.enqueue_body_available(worker.host_queue, body_event) != .Enqueued {
		return h2_cancel_stream_record(slot, record)
	}

	return true
}
