#+build linux
package linux_runtime

import connections "veylor:runtime/connections"
import "veylor:transport"
import epoll "veylor:transport/linux"

begin_static_file_body :: proc(
	state: ^Static_Connection,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
	timeouts: connections.Connection_Timeouts,
	now_ns: u64,
) -> Static_Connection_Turn {
	if state == nil || !state.has_file || state.file_remaining == 0 {
		return Static_Connection_Turn{result = .Invalid}
	}

	if !release_static_workspace(state) {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Static_Connection_Turn{result = .Invalid}
	}

	schedule := connections.schedule_connection_phase_deadline(
		table,
		timers,
		state.token,
		static_connection_event_id(state.token),
		now_ns,
		timeouts,
	)
	if schedule.result != .Scheduled {
		state.close_reason = .Deadline_Capacity
		state.phase = .Failed
		if schedule.result == .Full {
			return Static_Connection_Turn{result = .Capacity_Limit}
		}
		return Static_Connection_Turn{result = .Invalid}
	}
	state.phase = .Writing_File_Body
	return Static_Connection_Turn{result = .Response_Progress}
}

static_file_body_turn :: proc(
	poller: epoll.Epoll,
	state: ^Static_Connection,
	readiness: transport.Readiness_Event,
	byte_budget: int,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
	timeouts: connections.Connection_Timeouts,
	now_ns: u64,
) -> Static_Connection_Turn {
	if !readiness.writable {
		return Static_Connection_Turn{result = .Response_Progress}
	}

	file_turn := write_static_file_turn(
		&state.file,
		state.fd,
		&state.file_offset,
		&state.file_remaining,
		byte_budget,
	)
	turn := Static_Connection_Turn {
		bytes_written = file_turn.bytes_written,
		result        = .Response_Progress,
		error_code    = file_turn.error_code,
	}
	switch file_turn.result {
	case .Would_Block, .Interrupted:
		return turn
	case .Progress:
		if !refresh_static_file_write_deadline(state, table, timers, timeouts, now_ns) {
			state.close_reason = .Deadline_Capacity
			state.phase = .Failed
			turn.result = .Capacity_Limit
		}
		return turn
	case .Complete:
		if !close_static_file(&state.file) {
			state.close_reason = .Transport_Error
			state.phase = .Failed
			turn.result = .Transport_Error
			return turn
		}

		state.has_file = false
		if !cancel_static_file_write_deadline(state, table, timers) {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			turn.result = .Invalid
			return turn
		}

		if state.keep_alive {
			modify_result, error_code := epoll.modify(
				poller,
				state.fd,
				static_connection_event_id(state.token),
				epoll.Interest{readable = true},
			)
			if modify_result != .Ready {
				state.close_reason = .Transport_Error
				state.phase = .Failed
				return Static_Connection_Turn {
					bytes_written = file_turn.bytes_written,
					result = .Transport_Error,
					error_code = error_code,
				}
			}
			idle := connections.start_keep_alive_idle_deadline(
				table,
				timers,
				state.token,
				static_connection_event_id(state.token),
				now_ns,
				timeouts,
			)
			if idle.result != .Scheduled {
				state.close_reason = .Deadline_Capacity
				state.phase = .Failed
				return Static_Connection_Turn{result = .Capacity_Limit}
			}
			state.phase = .Keep_Alive_Idle
			turn.result = .Keep_Alive
			return turn
		}
		state.phase = .Closed
		if state.close_reason == .Unknown {
			state.close_reason = .Normal
		}
		turn.result = .Closed
		return turn
	case .Truncated:
		state.close_reason = .Transport_Error
		state.phase = .Failed
		turn.result = .Transport_Error
		return turn
	case .Error:
		state.close_reason = .Transport_Error
		state.phase = .Failed
		turn.result = .Transport_Error
		return turn
	case .Invalid:
		state.close_reason = .Internal_Error
		state.phase = .Failed
		turn.result = .Invalid
		return turn
	}
	state.close_reason = .Internal_Error
	state.phase = .Failed
	return Static_Connection_Turn{result = .Invalid}
}

refresh_static_file_write_deadline :: proc(
	state: ^Static_Connection,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
	timeouts: connections.Connection_Timeouts,
	now_ns: u64,
) -> bool {
	if !cancel_static_file_write_deadline(state, table, timers) {
		return false
	}
	schedule := connections.schedule_connection_phase_deadline(
		table,
		timers,
		state.token,
		static_connection_event_id(state.token),
		now_ns,
		timeouts,
	)
	return schedule.result == .Scheduled
}

cancel_static_file_write_deadline :: proc(
	state: ^Static_Connection,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
) -> bool {
	result := connections.cancel_connection_phase_deadline(table, timers, state.token)
	return result == .Cancelled
}
