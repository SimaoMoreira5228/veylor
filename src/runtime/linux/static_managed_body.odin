#+build linux
package linux_runtime

import http1 "veylor:http1"
import body "veylor:http1/body"
import connection "veylor:http1/connection"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

park_static_connection_for_host :: proc(
	poller: epoll.Epoll,
	state: ^Static_Connection,
	head: http1.Request_Head,
	routes: ^router.Route_Table,
	workspace: ^Static_Connection_Workspace,
	input_length: int,
	bytes_read: int,
	body_limit: u64,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
	timeouts: connections.Connection_Timeouts,
	now_ns: u64,
) -> Static_Connection_Turn {
	if state.host_queue == nil {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	input := workspace.request_input[:input_length]
	method := input[head.method.offset:head.method.offset + head.method.length]
	target := input[head.target.offset:head.target.offset + head.target.length]
	path, _, path_result := router.request_path(head.target_form, method, target)

	if path_result != .Found {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}

	if head.has_content_length && head.content_length > body_limit {
		oversize := encode_static_error_response(workspace.response, 413, "Content Too Large")

		if oversize.result != .Written {
			state.close_reason = .Response_Buffer_Limit
			state.phase = .Failed

			return Static_Connection_Turn{bytes_read = bytes_read, result = .Response_Buffer_Small}
		}
		state.response_length = oversize.written
		state.keep_alive = false
		state.close_reason = .Request_Rejected
		state.phase = .Writing_Response
		write_turn := register_static_response_write(poller, state, table)
		write_turn.bytes_read += bytes_read

		return write_turn
	}
	managed_route, managed_result := router.find_static_request_route(routes, head, input)

	if managed_result != .Found || managed_route.kind != .Managed {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	workspace.managed_route_id = managed_route.route_id

	if state.token.request_seq == max(u32) {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	state.token.request_seq += 1
	stash_managed_request_context(state, head, input, method)
	bind_managed_trailer_context(workspace, managed_route, head)
	enqueue_result := host.enqueue_request(
		state.host_queue,
		state.token,
		managed_route.route_id,
		method,
		path,
	)

	if enqueue_result == .Full {
		state.close_reason = .Host_Queue_Full
		state.phase = .Failed

		return Static_Connection_Turn{bytes_read = bytes_read, result = .Capacity_Limit}
	}

	if enqueue_result != .Enqueued {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	body_expected := (head.has_content_length && head.content_length > 0) || head.is_chunked

	if body_expected {
		if connections.transition_connection(table, state.token, .Reading_Request_Body) !=
		   .Transitioned {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
		}

		if !arm_parked_request_deadline(state, table, timers, timeouts, now_ns) {
			return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
		}

		workspace.body_driver = connection.begin_connection_body(
			static_connection_event_id(state.token),
			head,
		)
		workspace.body_start = head.length
		workspace.body_valid = input_length
		state.phase = .Awaiting_Host

		return Static_Connection_Turn{bytes_read = bytes_read, result = .Need_More}
	}

	if connections.transition_connection(table, state.token, .Awaiting_Handler) != .Transitioned {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}

	if !arm_parked_request_deadline(state, table, timers, timeouts, now_ns) {
		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}

	workspace.body_driver = {}
	workspace.body_start = head.length
	workspace.body_valid = input_length
	workspace.staged_length = 0
	workspace.body_complete = true
	workspace.streaming = false
	workspace.stream_ended = false
	workspace.stream_terminal = false
	workspace.stream_writable = false

	if input_length > head.length {
		state.phase = .Awaiting_Host

		return Static_Connection_Turn{bytes_read = bytes_read, result = .Need_More}
	}

	if !release_static_workspace(state) {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	state.phase = .Awaiting_Host

	return Static_Connection_Turn{bytes_read = bytes_read, result = .Need_More}
}

arm_parked_request_deadline :: proc(
	state: ^Static_Connection,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
	timeouts: connections.Connection_Timeouts,
	now_ns: u64,
) -> bool {
	schedule := connections.schedule_connection_phase_deadline(
		table,
		timers,
		state.token,
		now_ns,
		timeouts,
	)

	if schedule.result != .Scheduled &&
	   schedule.result != .Duplicate_Timer &&
	   schedule.result != .Connection_Already_Timed {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		if schedule.result == .Full {
			state.close_reason = .Deadline_Capacity
		}

		return false
	}

	return true
}

stash_managed_request_context :: proc(
	state: ^Static_Connection,
	head: http1.Request_Head,
	input: []u8,
	method: []u8,
) {
	state.managed_method_length = 0
	state.managed_http11 = false
	state.managed_req_close = false
	state.managed_req_keep_alive = false

	if len(method) == 0 || len(method) > len(state.managed_method) {
		return
	}
	copy(state.managed_method[:len(method)], method)
	state.managed_method_length = len(method)

	if head.version.offset < 0 ||
	   head.version.length < 0 ||
	   head.version.offset > len(input) ||
	   head.version.length > len(input) - head.version.offset {
		return
	}
	version := input[head.version.offset:head.version.offset + head.version.length]
	state.managed_http11 = http1.bytes_equal(version, "HTTP/1.1")
	state.managed_req_close = head.connection.close
	state.managed_req_keep_alive = head.connection.keep_alive
}

bind_managed_trailer_context :: proc(
	workspace: ^Static_Connection_Workspace,
	route: router.Static_Route,
	head: http1.Request_Head,
) {
	workspace.trailer_head = head.headers
	workspace.trailer_allow_count = 0
	workspace.trailer_count = 0
	workspace.trailer_data_used = 0
	count := route.managed_trailer_count

	if count > len(workspace.trailer_allow) {
		count = len(workspace.trailer_allow)
	}

	for i := 0; i < count; i += 1 {
		workspace.trailer_allow[i] = string(route.managed_trailers[i])
		workspace.trailer_allow_count += 1
	}
}

Managed_Read_Outcome :: enum {
	Notified,
	Stalled,
	Closed,
	Dropped,
}

Managed_Read_Turn :: struct {
	outcome:    Managed_Read_Outcome,
	bytes_read: int,
}

execute_managed_read_body :: proc(
	state: ^Static_Connection,
	cmd: host.Host_Command,
	byte_budget: int,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
	timeouts: connections.Connection_Timeouts,
	now_ns: u64,
) -> Managed_Read_Turn {
	if cmd.kind != .Read_Body {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	if cmd.max_bytes == 0 {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	resource_id, lookup_result := connections.lookup_connection(table, cmd.token)

	if lookup_result != .Found || resource_id != u64(state.fd) || cmd.token != state.token {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	phase, phase_result := connections.connection_phase(table, cmd.token)

	if phase_result != .Found || phase != .Reading_Request_Body {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	if state.workspace_slot < 0 {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Managed_Read_Turn{outcome = .Closed}
	}

	workspace := &state.workspace_pool.storage[state.workspace_slot]

	if workspace.body_driver.state != .Reading {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	schedule := connections.schedule_connection_phase_deadline(
		table,
		timers,
		state.token,
		now_ns,
		timeouts,
	)

	if schedule.result != .Scheduled &&
	   schedule.result != .Duplicate_Timer &&
	   schedule.result != .Connection_Already_Timed {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		if schedule.result == .Full {
			state.close_reason = .Deadline_Capacity
		}

		return Managed_Read_Turn{outcome = .Closed}
	}

	staged_before := workspace.staged_length
	trailer_before := workspace.trailer_count
	staged_free := len(workspace.staged_body) - staged_before

	if staged_free < 0 {
		staged_free = 0
	}
	max_command_bytes := int(cmd.max_bytes)
	body_event: connection.Connection_Body_Event
	bytes_read := 0
	trailer_ctx := body.Chunked_Trailer_Context {
		headers                 = workspace.trailer_head,
		head_input              = workspace.request_input,
		permitted_trailer_names = workspace.trailer_allow[:workspace.trailer_allow_count],
	}

	if workspace.body_valid > workspace.body_start {
		feed_length := workspace.body_valid - workspace.body_start

		if feed_length > staged_free {
			feed_length = staged_free
		}

		if feed_length > max_command_bytes {
			feed_length = max_command_bytes
		}

		if feed_length <= 0 {
			return Managed_Read_Turn{outcome = .Stalled}
		}
		spans: [64]http1.Span
		body_event = connection.consume_buffered_body(
			&workspace.body_driver,
			workspace.request_input[workspace.body_start:workspace.body_start + feed_length],
			spans[:],
			trailer_ctx,
		)
		workspace.body_start += body_event.consumed
		stage_managed_body_spans(workspace, body_event, workspace.body_start - body_event.consumed)

		if body_event.result == .Too_Large {
			state.close_reason = .Request_Rejected
			state.phase = .Failed

			return Managed_Read_Turn{outcome = .Closed}
		}

		if body_event.result == .Invalid {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return Managed_Read_Turn{outcome = .Closed}
		}

		if body_event.result == .Trailer {
			if !stage_managed_trailer(
				workspace,
				body_event.trailer_name,
				body_event.trailer_value,
			) {
				state.close_reason = .Internal_Error
				state.phase = .Failed

				return Managed_Read_Turn{outcome = .Closed}
			}
		}

		if workspace.body_driver.state == .Failed || workspace.body_driver.state == .Cancelled {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return Managed_Read_Turn{outcome = .Closed}
		}
	} else {
		input_free := len(workspace.request_input) - workspace.body_valid

		if input_free < 0 {
			input_free = 0
		}
		read_cap := byte_budget

		if staged_free < read_cap {
			read_cap = staged_free
		}

		if input_free < read_cap {
			read_cap = input_free
		}

		if max_command_bytes < read_cap {
			read_cap = max_command_bytes
		}

		if read_cap <= 0 {
			return Managed_Read_Turn{outcome = .Stalled}
		}
		budget, budget_valid := socket.begin_stream_byte_budget(read_cap)

		if !budget_valid {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return Managed_Read_Turn{outcome = .Closed}
		}
		read := socket.read_stream_budgeted(
			state.fd,
			workspace.request_input[workspace.body_valid:],
			&budget,
		)

		if read.io.kind != .Progress {
			if read.io.kind == .Would_Block || read.io.kind == .Interrupted {
				return Managed_Read_Turn{outcome = .Stalled}
			}

			if read.io.kind == .End_Of_Stream || read.io.kind == .Closed {
				state.close_reason = .Peer_Closed
			} else {
				state.close_reason = .Transport_Error
			}
			state.phase = .Failed

			return Managed_Read_Turn{outcome = .Closed}
		}
		workspace.body_valid += read.io.transferred_bytes
		bytes_read = read.io.transferred_bytes
		feed := workspace.request_input[workspace.body_valid - bytes_read:workspace.body_valid]
		spans: [64]http1.Span
		completion := transport.IO_Completion {
			connection_id = static_connection_event_id(state.token),
			operation_id  = workspace.body_driver.operation_id + 1,
			operation     = .Read,
			result        = read.io,
		}
		body_event = connection.consume_body_completion(
			&workspace.body_driver,
			completion,
			feed,
			spans[:],
			trailer_ctx,
		)

		if body_event.result == .Invalid || body_event.result == .Span_Capacity {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
		}

		if body_event.result == .Too_Large {
			state.close_reason = .Request_Rejected
			state.phase = .Failed

			return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
		}

		if body_event.result == .Trailer {
			if !stage_managed_trailer(
				workspace,
				body_event.trailer_name,
				body_event.trailer_value,
			) {
				state.close_reason = .Internal_Error
				state.phase = .Failed

				return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
			}
		}
		workspace.body_start += body_event.consumed
		stage_managed_body_spans(workspace, body_event, workspace.body_valid - bytes_read)

		if workspace.body_driver.state == .Failed || workspace.body_driver.state == .Cancelled {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
		}
	}

	terminal := body_event.body_complete || workspace.body_driver.state != .Reading

	if terminal {
		workspace.body_complete = true
		cancel_result := connections.cancel_connection_phase_deadline(table, timers, state.token)

		if cancel_result != .Cancelled && cancel_result != .Not_Found {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
		}

		if connections.transition_connection(table, state.token, .Awaiting_Handler) !=
		   .Transitioned {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
		}

		return enqueue_managed_body_event(state)
	}

	if workspace.staged_length == staged_before && workspace.trailer_count == trailer_before {
		return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Stalled}
	}
	refresh := connections.cancel_connection_phase_deadline(table, timers, state.token)

	if refresh != .Cancelled {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
	}
	reschedule := connections.schedule_connection_phase_deadline(
		table,
		timers,
		state.token,
		now_ns,
		timeouts,
	)

	if reschedule.result != .Scheduled {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		if reschedule.result == .Full {
			state.close_reason = .Deadline_Capacity
		}

		return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
	}

	return enqueue_managed_body_event(state)
}

stage_managed_body_spans :: proc(
	workspace: ^Static_Connection_Workspace,
	event: connection.Connection_Body_Event,
	feed_base: int,
) {
	for span in event.spans {
		if span.length <= 0 {
			continue
		}

		if workspace.staged_length + span.length > len(workspace.staged_body) {
			continue
		}
		copy(
			workspace.staged_body[workspace.staged_length:],
			workspace.request_input[feed_base + span.offset:feed_base + span.offset + span.length],
		)
		workspace.staged_length += span.length
	}
}

stage_managed_trailer :: proc(
	workspace: ^Static_Connection_Workspace,
	name: []u8,
	value: []u8,
) -> bool {
	if workspace.trailer_count >= len(workspace.trailer_fields) {
		return false
	}

	if len(name) + len(value) > len(workspace.trailer_data) - workspace.trailer_data_used {
		return false
	}
	field := &workspace.trailer_fields[workspace.trailer_count]
	field.name_offset = workspace.trailer_data_used
	field.name_length = len(name)
	copy(workspace.trailer_data[field.name_offset:], name)
	workspace.trailer_data_used += len(name)
	field.value_offset = workspace.trailer_data_used
	field.value_length = len(value)
	copy(workspace.trailer_data[field.value_offset:], value)
	workspace.trailer_data_used += len(value)
	workspace.trailer_count += 1

	return true
}

enqueue_managed_body_event :: proc(state: ^Static_Connection) -> Managed_Read_Turn {
	if state.host_queue == nil || state.workspace_pool == nil {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Managed_Read_Turn{outcome = .Closed}
	}

	if state.workspace_slot < 0 || state.workspace_slot >= len(state.workspace_pool.storage) {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return Managed_Read_Turn{outcome = .Closed}
	}
	workspace := &state.workspace_pool.storage[state.workspace_slot]
	complete: u32

	if workspace.body_complete {
		complete = 1
	}
	event := host.Host_Event {
		token         = state.token,
		route_id      = workspace.managed_route_id,
		body_ready    = u32(workspace.staged_length),
		body_complete = complete,
		kind          = .Body_Available,
	}

	if host.enqueue_body_available(state.host_queue, event) != .Enqueued {
		state.close_reason = .Host_Queue_Full
		state.phase = .Failed

		return Managed_Read_Turn{outcome = .Closed}
	}

	return Managed_Read_Turn{outcome = .Notified}
}
