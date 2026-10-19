#+build linux
package linux_runtime

import "core:sys/linux"
import connections "veylor:runtime/connections"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import epoll "veylor:transport/linux"

force_static_worker_connections :: proc(worker: ^Static_Worker, turn: ^Static_Worker_Turn) {
	shutdown_result := shutdown.force_shutdown(worker.shutdown_state, worker.timers)

	if shutdown_result == .Completed {
		return
	}

	if shutdown_result != .Force_Connections {
		turn.result = .Error

		return
	}

	for i := 0; i < len(worker.connection_states); i += 1 {
		state := &worker.connection_states[i]

		if !state.active {
			continue
		}
		state.close_reason = .Shutdown_Forced

		if close_static_worker_connection(worker, state) == .Error {
			turn.result = .Error
		}
		turn.closed_count += 1
	}
}

valid_static_worker :: proc(worker: ^Static_Worker) -> bool {
	if worker == nil ||
	   worker.shutdown_state == nil ||
	   worker.table == nil ||
	   worker.routes == nil ||
	   worker.timers == nil {
		return false
	}

	if worker.routes.state != .Frozen {
		return false
	}

	if worker.file_root != nil && !valid_static_file_root(worker.file_root) {
		return false
	}

	if worker.poller.fd < 0 || worker.listener.fd < 0 || worker.table.free_count < 0 {
		return false
	}

	if worker.table.free_count > len(worker.table.slots) ||
	   len(worker.connection_states) < len(worker.table.slots) {
		return false
	}

	if len(worker.h2_states) != len(worker.h2_free_stack) {
		return false
	}

	if worker.h2_free_count < 0 || worker.h2_free_count > len(worker.h2_states) {
		return false
	}

	if !valid_static_workspace_pool(worker.workspace_pool) {
		return false
	}

	if worker.event_budget <= 0 || worker.byte_budget <= 0 || worker.accept_attempt_budget <= 0 {
		return false
	}

	if worker.timeout_ms < -1 || len(worker.expired_timer_storage) == 0 {
		return false
	}

	if len(worker.native_events) == 0 ||
	   len(worker.readiness_events) == 0 ||
	   len(worker.accepted_storage) == 0 ||
	   len(worker.accepted_connections) == 0 {
		return false
	}

	return true
}

drain_static_worker_control :: proc(fd: linux.Fd) -> bool {
	buffer: [8]u8

	for {
		read_length, read_error := linux.read(fd, buffer[:])

		if read_error == .EINTR {
			continue
		}

		if read_error == .EAGAIN {
			return true
		}

		return read_error == .NONE && read_length == len(buffer)
	}
}

process_static_worker_expirations :: proc(
	worker: ^Static_Worker,
	expired: []transport.Timer_Expiration,
	turn: ^Static_Worker_Turn,
) {
	for timer in expired {
		if timer.timer_id == STATIC_LISTENER_RETRY_TIMER_ID {
			worker.descriptor_retry_scheduled = false
			worker.descriptor_limit_paused = false
			continue
		}

		if worker.shutdown_state.has_deadline && timer.timer_id == worker.shutdown_state.timer_id {
			if shutdown.process_deadline(worker.shutdown_state, timer) == .Force_Connections {
				worker.shutdown_state.phase = .Forcing
			}
			continue
		}

		deadline, result := connections.expire_connection_deadline(worker.table, timer.timer_id)

		if result != .Deadline_Expired {
			continue
		}

		slot_index := int(deadline.token.slot)

		if slot_index >= len(worker.connection_states) {
			turn.result = .Invalid
			continue
		}
		state := &worker.connection_states[slot_index]

		if !state.active {
			if !expire_h2_worker_deadline(worker, slot_index, deadline, turn) {
				turn.result = .Invalid
			}
			continue
		}

		if state.token.generation != deadline.token.generation {
			turn.result = .Invalid
			continue
		}

		state.close_reason = .Timed_Out

		if close_static_worker_connection(worker, state) == .Error {
			turn.result = .Error
		}
		turn.closed_count += 1
	}
}

schedule_static_worker_listener_retry :: proc(worker: ^Static_Worker, now_ns: u64) -> bool {
	if worker.descriptor_retry_scheduled {
		return true
	}

	if STATIC_LISTENER_RETRY_DELAY_NS > 0xffff_ffff_ffff_ffff - now_ns {
		return false
	}
	timer := transport.Timer_Expiration {
		timer_id    = STATIC_LISTENER_RETRY_TIMER_ID,
		deadline_ns = now_ns + STATIC_LISTENER_RETRY_DELAY_NS,
	}

	if transport.schedule_timer(worker.timers, timer) != .Scheduled {
		return false
	}
	worker.descriptor_retry_scheduled = true

	return true
}

cancel_static_worker_listener_retry :: proc(worker: ^Static_Worker) -> bool {
	if !worker.descriptor_retry_scheduled {
		return true
	}
	timer_result := transport.cancel_timer(worker.timers, STATIC_LISTENER_RETRY_TIMER_ID)
	worker.descriptor_retry_scheduled = false

	return timer_result == .Cancelled || timer_result == .Not_Found
}

close_static_worker_connection :: proc(
	worker: ^Static_Worker,
	state: ^Static_Connection,
) -> Static_Worker_Result {
	if worker == nil || state == nil || !state.active {
		return .Invalid
	}

	result := Static_Worker_Result.Ready

	if state.idle_counted {
		if worker.metrics.idle_connections == 0 {
			result = .Error
		} else {
			worker.metrics.idle_connections -= 1
		}
		state.idle_counted = false
	}

	if state.registered {
		unregister_result, _ := epoll.unregister(worker.poller, state.fd)

		if unregister_result != .Ready {
			result = .Error
		}
		state.registered = false
	}

	if !release_static_workspace(state) {
		result = .Error
	}

	if state.has_file {
		if !close_static_file(&state.file) {
			result = .Error
		}
		state.has_file = false
	}

	release := release_worker_connection(
		&Worker_Connection{token = state.token, fd = state.fd},
		worker.table,
		worker.timers,
		worker.shutdown_state,
	)

	if release.table_result != .Released && release.table_result != .Retired {
		return .Error
	}

	record_static_worker_close(worker, state.close_reason)

	if release.socket_result != .Ready {
		result = .Error
	}

	if release.shutdown_result != .Connection_Closed && release.shutdown_result != .Completed {
		result = .Error
	}
	state^ = Static_Connection{}

	return result
}

record_static_worker_close :: proc(
	worker: ^Static_Worker,
	reason: Static_Connection_Close_Reason,
) {
	if worker == nil {
		return
	}
	index := int(reason)

	if index < 0 || index >= len(worker.metrics.close_reasons) {
		index = int(Static_Connection_Close_Reason.Unknown)
	}
	worker.metrics.closed_connections += 1
	worker.metrics.close_reasons[index] += 1
}

static_connection_close_reason :: proc(
	result: Static_Connection_Result,
) -> Static_Connection_Close_Reason {
	switch result {
	case .Closed:
		return .Normal
	case .Capacity_Limit:
		return .Deadline_Capacity
	case .Response_Buffer_Small:
		return .Response_Buffer_Limit
	case .Transport_Error:
		return .Transport_Error
	case .Invalid:
		return .Internal_Error
	case .Need_More, .Response_Progress, .Keep_Alive:
		return .Unknown
	}

	return .Unknown
}

sync_static_worker_idle_count :: proc(worker: ^Static_Worker, state: ^Static_Connection) -> bool {
	is_idle := state.phase == .Keep_Alive_Idle

	if is_idle == state.idle_counted {
		return true
	}

	if is_idle {
		worker.metrics.idle_connections += 1
		state.idle_counted = true

		return true
	}

	if worker.metrics.idle_connections == 0 {
		return false
	}
	worker.metrics.idle_connections -= 1
	state.idle_counted = false

	return true
}

refresh_static_worker_gauges :: proc(worker: ^Static_Worker) {
	if worker == nil {
		return
	}
	worker.metrics.active_connections = worker.shutdown_state.active_connections
	worker.metrics.timer_count = worker.timers.count
	worker.metrics.workspace_active = worker.workspace_pool.active_count
	worker.metrics.workspace_capacity = len(worker.workspace_pool.storage)
}

static_worker_poll_result :: proc(result: epoll.Result) -> Static_Worker_Result {
	switch result {
	case .Ready:
		return .Ready
	case .Timeout, .Interrupted:
		return .Not_Ready
	case .Invalid:
		return .Invalid
	case .System_Error:
		return .Error
	}

	return .Invalid
}
