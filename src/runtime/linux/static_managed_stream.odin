#+build linux
package linux_runtime

import http1 "veylor:http1"
import response "veylor:http1/response"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import "veylor:transport"
import epoll "veylor:transport/linux"

STREAM_TERMINAL_CHUNK :: "0\r\n\r\n"
STREAM_TERMINAL_HEAD :: "0\r\n"
STREAM_TERMINAL_TAIL :: "\r\n"

encode_stream_chunk :: proc(data: []u8, out: []u8) -> (written: int, ok: bool) {
	if len(data) == 0 || len(data) > host.HOST_MAX_RESPONSE_BODY_BYTES {
		return 0, false
	}

	digits := 0
	shift := 60

	for shift >= 0 {
		nibble := (u64(len(data)) >> u64(shift)) & 0xf

		if digits > 0 || nibble != 0 || shift == 0 {
			if digits >= 16 {
				return 0, false
			}

			digits += 1
		}

		shift -= 4
	}

	frame_length := digits + 2 + len(data) + 2

	if frame_length > len(out) {
		return 0, false
	}

	for i := digits - 1; i >= 0; i -= 1 {
		nibble := u8((u64(len(data)) >> u64(i * 4)) & 0xf)

		if nibble < 10 {
			out[digits - 1 - i] = '0' + nibble
		} else {
			out[digits - 1 - i] = 'a' + nibble - 10
		}
	}
	out[digits] = '\r'
	out[digits + 1] = '\n'
	copy(out[digits + 2:], data)
	out[digits + 2 + len(data)] = '\r'
	out[digits + 2 + len(data) + 1] = '\n'

	return frame_length, true
}

execute_managed_response_begin :: proc(
	state: ^Static_Connection,
	cmd: host.Host_Command,
	poller: epoll.Epoll,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
) -> Managed_Response_Outcome {
	if cmd.kind != .Response_Begin {
		return .Dropped
	}

	if cmd.status < 200 || cmd.status > 599 {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	if cmd.reason_length == 0 ||
	   cmd.reason_length > host.HOST_MAX_RESPONSE_REASON_BYTES ||
	   cmd.body_length != 0 {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	resource_id, lookup_result := connections.lookup_connection(table, cmd.token)

	if lookup_result != .Found || resource_id != u64(state.fd) || cmd.token != state.token {
		return .Dropped
	}

	phase, phase_result := connections.connection_phase(table, cmd.token)

	if phase_result != .Found {
		return .Dropped
	}

	if phase != .Awaiting_Handler && phase != .Reading_Request_Body {
		return .Dropped
	}

	retained := state.workspace_slot >= 0
	workspace, acquired := acquire_static_workspace(state)

	if !acquired {
		state.close_reason = .Workspace_Exhausted
		state.phase = .Failed

		return .Closed
	}

	if state.managed_method_length <= 0 {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	method := state.managed_method[:state.managed_method_length]

	if http1.bytes_equal(method, "HEAD") {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	reason_storage := cmd.reason
	reason := reason_storage[:cmd.reason_length]

	if !response.valid_response_value(reason) {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	body_complete := phase == .Awaiting_Handler && (!retained || workspace.body_complete)
	keep_alive :=
		body_complete &&
		!state.managed_req_close &&
		(state.managed_http11 || state.managed_req_keep_alive)
	policy := response.Connection_Policy.Close

	if keep_alive {
		policy = .Keep_Alive
	}

	connection_header := response.response_connection_header(
		method,
		u16(cmd.status),
		policy,
		state.managed_http11,
	)
	head_event := response.encode_response_head(
		method,
		u16(cmd.status),
		reason,
		[]response.Response_Header{},
		.Chunked,
		0,
		workspace.response,
		connection_header,
	)

	if head_event.result != .Written || !head_event.body_allowed {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	state.response_length = head_event.written
	state.keep_alive = keep_alive

	if !keep_alive {
		state.close_reason = .Normal
	}

	workspace.streaming = true
	workspace.stream_ended = false
	workspace.stream_terminal = false
	workspace.stream_writable = false

	cancel_result := connections.cancel_connection_phase_deadline(table, timers, state.token)

	if cancel_result != .Cancelled && cancel_result != .Not_Found {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	if connections.transition_connection(table, state.token, .Writing_Response) != .Transitioned {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	state.phase = .Writing_Response
	write_turn := register_static_response_write(poller, state, table)

	if write_turn.result != .Response_Progress {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	return .Responded
}

execute_managed_response_write :: proc(
	state: ^Static_Connection,
	cmd: host.Host_Command,
	poller: epoll.Epoll,
	table: ^connections.Connection_Table,
) -> Managed_Response_Outcome {
	if cmd.kind != .Response_Write {
		return .Dropped
	}

	if cmd.body_length == 0 || cmd.body_length > host.HOST_MAX_RESPONSE_BODY_BYTES {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	resource_id, lookup_result := connections.lookup_connection(table, cmd.token)

	if lookup_result != .Found || resource_id != u64(state.fd) || cmd.token != state.token {
		return .Dropped
	}

	phase, phase_result := connections.connection_phase(table, cmd.token)

	if phase_result != .Found || phase != .Writing_Response {
		return .Dropped
	}

	if state.phase != .Writing_Response {
		return .Dropped
	}

	if state.workspace_slot < 0 || state.workspace_pool == nil {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	if state.workspace_slot >= len(state.workspace_pool.storage) {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	workspace := &state.workspace_pool.storage[state.workspace_slot]

	if !workspace.streaming || workspace.stream_ended {
		return .Dropped
	}

	body_storage := cmd.body
	chunk, chunk_ok := encode_stream_chunk(
		body_storage[:cmd.body_length],
		workspace.response[state.response_length:],
	)

	if !chunk_ok {
		return .Dropped
	}

	state.response_length += chunk
	workspace.stream_writable = false

	if state.write_driver.state != .Pending {
		write_turn := register_static_response_write(poller, state, table)

		if write_turn.result != .Response_Progress {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return .Closed
		}
	}

	return .Responded
}

execute_managed_response_end :: proc(
	state: ^Static_Connection,
	cmd: host.Host_Command,
	poller: epoll.Epoll,
	table: ^connections.Connection_Table,
) -> Managed_Response_Outcome {
	if cmd.kind != .Response_End {
		return .Dropped
	}

	resource_id, lookup_result := connections.lookup_connection(table, cmd.token)

	if lookup_result != .Found || resource_id != u64(state.fd) || cmd.token != state.token {
		return .Dropped
	}

	phase, phase_result := connections.connection_phase(table, cmd.token)

	if phase_result != .Found || phase != .Writing_Response {
		return .Dropped
	}

	if state.phase != .Writing_Response {
		return .Dropped
	}

	if state.workspace_slot < 0 || state.workspace_pool == nil {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	if state.workspace_slot >= len(state.workspace_pool.storage) {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	workspace := &state.workspace_pool.storage[state.workspace_slot]

	if !workspace.streaming || workspace.stream_ended {
		return .Dropped
	}

	workspace.stream_ended = true
	workspace.stream_writable = false

	if state.write_driver.state != .Pending {
		if state.response_length == 0 {
			if len(STREAM_TERMINAL_CHUNK) > len(workspace.response) {
				state.close_reason = .Internal_Error
				state.phase = .Failed

				return .Closed
			}

			copy(workspace.response, STREAM_TERMINAL_CHUNK)
			state.response_length = len(STREAM_TERMINAL_CHUNK)
			workspace.stream_terminal = true
		}

		write_turn := register_static_response_write(poller, state, table)

		if write_turn.result != .Response_Progress {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return .Closed
		}
	}

	return .Responded
}

execute_managed_response_trailers :: proc(
	state: ^Static_Connection,
	cmd: host.Host_Command,
	poller: epoll.Epoll,
	table: ^connections.Connection_Table,
) -> Managed_Response_Outcome {
	if cmd.kind != .Response_Trailers {
		return .Dropped
	}

	if cmd.body_length == 0 || cmd.body_length > host.HOST_MAX_RESPONSE_BODY_BYTES {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	resource_id, lookup_result := connections.lookup_connection(table, cmd.token)

	if lookup_result != .Found || resource_id != u64(state.fd) || cmd.token != state.token {
		return .Dropped
	}

	phase, phase_result := connections.connection_phase(table, cmd.token)

	if phase_result != .Found || phase != .Writing_Response {
		return .Dropped
	}

	if state.phase != .Writing_Response {
		return .Dropped
	}

	if state.workspace_slot < 0 || state.workspace_pool == nil {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	if state.workspace_slot >= len(state.workspace_pool.storage) {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	workspace := &state.workspace_pool.storage[state.workspace_slot]

	if !workspace.streaming || workspace.stream_ended {
		return .Dropped
	}

	body_storage := cmd.body
	frame := body_storage[:cmd.body_length]
	framed := len(STREAM_TERMINAL_HEAD) + len(frame) + len(STREAM_TERMINAL_TAIL)

	if framed > len(workspace.response) - state.response_length {
		return .Dropped
	}

	head_room := len(STREAM_TERMINAL_HEAD)
	copy(workspace.response[state.response_length:], STREAM_TERMINAL_HEAD)
	copy(workspace.response[state.response_length + head_room:], frame)
	copy(
		workspace.response[state.response_length + head_room + len(frame):],
		STREAM_TERMINAL_TAIL,
	)
	state.response_length += framed
	workspace.stream_ended = true
	workspace.stream_terminal = true
	workspace.stream_writable = false

	if state.write_driver.state != .Pending {
		write_turn := register_static_response_write(poller, state, table)

		if write_turn.result != .Response_Progress {
			state.close_reason = .Internal_Error
			state.phase = .Failed

			return .Closed
		}
	}

	return .Responded
}

Stream_Continue :: enum {
	Normal,
	Progress,
	Error,
}

continue_managed_stream :: proc(
	state: ^Static_Connection,
	poller: epoll.Epoll,
	table: ^connections.Connection_Table,
) -> Stream_Continue {
	if state.workspace_slot < 0 || state.workspace_pool == nil {
		return .Normal
	}

	if state.workspace_slot >= len(state.workspace_pool.storage) {
		return .Normal
	}

	workspace := &state.workspace_pool.storage[state.workspace_slot]

	if !workspace.streaming {
		return .Normal
	}

	flushed := len(state.write_driver.cursor.bytes)

	if flushed < 0 || flushed > state.response_length {
		return .Error
	}

	remaining := state.response_length - flushed

	if remaining > 0 {
		copy(workspace.response, workspace.response[flushed:state.response_length])
	}

	state.response_length = remaining

	if workspace.stream_ended && remaining == 0 && !workspace.stream_terminal {
		if len(STREAM_TERMINAL_CHUNK) > len(workspace.response) {
			return .Error
		}

		copy(workspace.response, STREAM_TERMINAL_CHUNK)
		state.response_length = len(STREAM_TERMINAL_CHUNK)
		workspace.stream_terminal = true
	}

	if state.response_length > 0 {
		write_turn := register_static_response_write(poller, state, table)

		if write_turn.result != .Response_Progress {
			return .Error
		}

		return .Progress
	}

	if workspace.stream_ended && workspace.stream_terminal {
		return .Normal
	}

	// NOTE: best-effort; a full host queue already stalls the peer elsewhere.
	if !workspace.stream_writable {
		workspace.stream_writable = true
		host.enqueue_writable(state.host_queue, state.token, workspace.managed_route_id)
	}

	modify_result, _ := epoll.modify(
		poller,
		state.fd,
		static_connection_event_id(state.token),
		epoll.Interest{readable = true},
	)

	if modify_result != .Ready {
		return .Error
	}

	return .Progress
}
