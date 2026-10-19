#+build linux
package linux_runtime

import response "veylor:http1/response"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import "veylor:transport"
import epoll "veylor:transport/linux"

Managed_Response_Outcome :: enum {
	Responded,
	Closed,
	Dropped,
}

execute_managed_response_start :: proc(
	state: ^Static_Connection,
	cmd: host.Host_Command,
	poller: epoll.Epoll,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
) -> Managed_Response_Outcome {
	if cmd.kind != .Response_Start {
		return .Dropped
	}

	if cmd.status < 200 || cmd.status > 599 {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}

	if cmd.reason_length == 0 ||
	   cmd.reason_length > host.HOST_MAX_RESPONSE_REASON_BYTES ||
	   cmd.body_length > host.HOST_MAX_RESPONSE_BODY_BYTES {
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
	reason_storage := cmd.reason
	body_storage := cmd.body
	reason := reason_storage[:cmd.reason_length]
	body := body_storage[:cmd.body_length]

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
	content_length := u64(len(body))

	if cmd.status == 204 || cmd.status == 205 {
		content_length = 0
	}
	head_event := response.encode_response_head(
		method,
		u16(cmd.status),
		reason,
		[]response.Response_Header{},
		.Fixed_Length,
		content_length,
		workspace.response,
		connection_header,
	)

	if head_event.result == .Buffer_Small {
		state.close_reason = .Response_Buffer_Limit
		state.phase = .Failed

		return .Closed
	}

	if head_event.result != .Written {
		state.close_reason = .Internal_Error
		state.phase = .Failed

		return .Closed
	}
	written := head_event.written

	if head_event.body_allowed && len(body) > 0 {
		if written > len(workspace.response) - len(body) {
			state.close_reason = .Response_Buffer_Limit
			state.phase = .Failed

			return .Closed
		}
		copy(workspace.response[written:], body)
		written += len(body)
	}
	state.response_length = written
	state.keep_alive = keep_alive

	if !keep_alive {
		state.close_reason = .Normal
	}

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

Managed_Cancel_Outcome :: enum {
	Cancelled,
	Dropped,
}

execute_managed_cancel :: proc(
	state: ^Static_Connection,
	cmd: host.Host_Command,
	table: ^connections.Connection_Table,
) -> Managed_Cancel_Outcome {
	if cmd.kind != .Cancel {
		return .Dropped
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
		if phase != .Writing_Response || state.phase != .Writing_Response {
			return .Dropped
		}

		if state.workspace_slot < 0 || state.workspace_pool == nil {
			return .Dropped
		}

		if state.workspace_slot >= len(state.workspace_pool.storage) {
			return .Dropped
		}
		workspace := &state.workspace_pool.storage[state.workspace_slot]

		if !workspace.streaming {
			return .Dropped
		}
	} else if state.phase != .Awaiting_Host {
		return .Dropped
	}

	state.close_reason = .Cancelled
	state.phase = .Failed

	return .Cancelled
}
