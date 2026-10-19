#+build linux
package linux_runtime

import connection "veylor:http1/connection"
import connections "veylor:runtime/connections"
import epoll "veylor:transport/linux"

register_static_response_write :: proc(
	poller: epoll.Epoll,
	state: ^Static_Connection,
	table: ^connections.Connection_Table,
) -> Static_Connection_Turn {
	if state == nil ||
	   !valid_static_workspace_pool(state.workspace_pool) ||
	   state.workspace_slot < 0 ||
	   state.workspace_slot >= len(state.workspace_pool.storage) {
		return Static_Connection_Turn{result = .Invalid}
	}
	workspace := &state.workspace_pool.storage[state.workspace_slot]
	state.write_driver = connection.begin_connection_write(
		static_connection_event_id(state.token),
		workspace.response[:state.response_length],
	)

	if state.write_driver.state != .Pending {
		state.phase = .Failed

		return Static_Connection_Turn{result = .Invalid}
	}

	if state.phase != .Writing_Response {
		if connections.transition_connection(table, state.token, .Writing_Response) !=
		   .Transitioned {
			state.phase = .Failed

			return Static_Connection_Turn{result = .Invalid}
		}
	}
	result, error_code := epoll.modify(
		poller,
		state.fd,
		static_connection_event_id(state.token),
		epoll.Interest{writable = true},
	)

	if result != .Ready {
		state.phase = .Failed

		return Static_Connection_Turn{result = .Transport_Error, error_code = error_code}
	}
	state.phase = .Writing_Response

	return Static_Connection_Turn{result = .Response_Progress}
}

static_connection_event_id :: proc(token: connections.Connection_Token) -> u64 {
	return u64(token.slot) + 1
}
