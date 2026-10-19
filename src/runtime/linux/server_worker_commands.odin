#+build linux
package linux_runtime

import connections "veylor:runtime/connections"
import host "veylor:runtime/host"

drain_host_commands :: proc(worker: ^Static_Worker, now_ns: u64) {
	if worker.command_queue == nil {
		return
	}
	budget := worker.event_budget

	for budget > 0 {
		cmd, ok := host.dequeue_host_command(worker.command_queue)

		if !ok {
			return
		}
		budget -= 1

		if is_h2_response_command(cmd) && h2_hold_unready_response_command(worker, cmd) {
			return
		}
		execute_host_command(worker, cmd, now_ns)
	}
}

execute_host_command :: proc(worker: ^Static_Worker, cmd: host.Host_Command, now_ns: u64) {
	if cmd.kind != .Read_Body &&
	   cmd.kind != .Response_Start &&
	   cmd.kind != .Response_Begin &&
	   cmd.kind != .Response_Write &&
	   cmd.kind != .Response_End &&
	   cmd.kind != .Response_Trailers &&
	   cmd.kind != .Cancel {
		return
	}

	if cmd.token.stream_id != 0 {
		execute_h2_host_command(worker, cmd, now_ns)

		return
	}
	resource_id, lookup_result := connections.lookup_connection(worker.table, cmd.token)

	if lookup_result != .Found {
		return
	}
	slot := int(cmd.token.slot)

	if slot < 0 || slot >= len(worker.connection_states) {
		return
	}
	state := &worker.connection_states[slot]

	if !state.active || state.token != cmd.token || resource_id != u64(state.fd) {
		return
	}

	if cmd.kind == .Cancel {
		if execute_managed_cancel(state, cmd, worker.table) == .Cancelled {
			close_result := close_static_worker_connection(worker, state)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}

	if cmd.kind == .Response_Start {
		response_outcome := execute_managed_response_start(
			state,
			cmd,
			worker.poller,
			worker.table,
			worker.timers,
		)

		if response_outcome == .Closed {
			close_result := close_static_worker_connection(worker, state)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}

	if cmd.kind == .Response_Begin {
		response_outcome := execute_managed_response_begin(
			state,
			cmd,
			worker.poller,
			worker.table,
			worker.timers,
		)

		if response_outcome == .Closed {
			close_result := close_static_worker_connection(worker, state)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}

	if cmd.kind == .Response_Write {
		response_outcome := execute_managed_response_write(state, cmd, worker.poller, worker.table)

		if response_outcome == .Closed {
			close_result := close_static_worker_connection(worker, state)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}

	if cmd.kind == .Response_End {
		response_outcome := execute_managed_response_end(state, cmd, worker.poller, worker.table)

		if response_outcome == .Closed {
			close_result := close_static_worker_connection(worker, state)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}

	if cmd.kind == .Response_Trailers {
		response_outcome := execute_managed_response_trailers(
			state,
			cmd,
			worker.poller,
			worker.table,
		)

		if response_outcome == .Closed {
			close_result := close_static_worker_connection(worker, state)

			if close_result == .Error {
				worker.managed_result = .Error
			}
		}

		return
	}
	turn := execute_managed_read_body(
		state,
		cmd,
		worker.byte_budget,
		worker.table,
		worker.timers,
		worker.timeouts,
		now_ns,
	)
	worker.metrics.bytes_read += u64(turn.bytes_read)

	if turn.outcome != .Closed {
		return
	}
	close_result := close_static_worker_connection(worker, state)

	if close_result == .Error {
		worker.managed_result = .Error
	}
}
