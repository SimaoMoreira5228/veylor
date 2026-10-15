#+build linux
package linux_runtime

import "core:sys/linux"
import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

STATIC_WORKER_CONTROL_EVENT_ID :: u64(0xffff_ffff_ffff_ffff)

Static_Connection_Buffers :: struct {
	request_input: []u8,
	headers:       []http1.Header,
	response:      []u8,
}

Static_Worker :: struct {
	poller:                  epoll.Epoll,
	listener:                socket.Listener,
	listener_registered:     bool,
	descriptor_limit_paused: bool,
	control_fd:              linux.Fd,
	control_registered:      bool,
	shutdown_state:          ^shutdown.State,
	table:                   ^connections.Connection_Table,
	routes:                  ^router.Route_Table,
	connection_states:       []Static_Connection,
	connection_buffers:      []Static_Connection_Buffers,
	native_events:           []linux.EPoll_Event,
	readiness_events:        []transport.Readiness_Event,
	accepted_storage:        []socket.Accepted_Connection,
	accepted_connections:    []Worker_Connection,
	timers:                  ^transport.Timer_Queue,
	expired_timer_storage:   []transport.Timer_Expiration,
	event_budget:            int,
	byte_budget:             int,
	accept_attempt_budget:   int,
	timeout_ms:              i32,
	body_limit:              u64,
	timeouts:                connections.Connection_Timeouts,
}

Static_Worker_Result :: enum {
	Ready,
	Not_Ready,
	Error,
	Invalid,
}

Static_Worker_Turn :: struct {
	worker:          Worker_Turn,
	accepted_count:  int,
	processed_count: int,
	closed_count:    int,
	result:          Static_Worker_Result,
}

run_static_worker_turn :: proc(worker: ^Static_Worker) -> Static_Worker_Turn {
	if !valid_static_worker(worker) {
		return Static_Worker_Turn{result = .Invalid}
	}
	if worker.shutdown_state.phase == .Forcing {
		turn := Static_Worker_Turn {
			result = .Ready,
		}

		if worker.listener_registered {
			unregister_result, _ := epoll.unregister(worker.poller, worker.listener.fd)

			if unregister_result != .Ready {
				turn.result = .Error
				return turn
			}
			worker.listener_registered = false
		}
		force_static_worker_connections(worker, &turn)
		return turn
	}
	turn := run_worker_turn(
		worker.poller,
		worker.listener,
		0,
		&worker.listener_registered,
		&worker.descriptor_limit_paused,
		worker.shutdown_state,
		worker.table,
		worker.native_events,
		worker.readiness_events,
		worker.accepted_storage,
		worker.accepted_connections,
		worker.timers,
		worker.expired_timer_storage,
		worker.event_budget,
		worker.byte_budget,
		worker.accept_attempt_budget,
		worker.timeout_ms,
	)
	result := Static_Worker_Turn {
		worker = turn,
		result = static_worker_poll_result(turn.poll_result),
	}

	if turn.accept_result == .Listener_Failed ||
	   turn.accept_result == .Error ||
	   turn.accept_result == .Invalid {
		result.result = .Error
	}
	process_static_worker_expirations(worker, turn.expired_timers, &result)

	for &accepted in turn.accepted {
		result.accepted_count += 1
		slot_index := int(accepted.token.slot)
		buffers := worker.connection_buffers[slot_index]
		state, init_result := init_static_connection(
			accepted,
			buffers.request_input,
			buffers.headers,
			buffers.response,
		)

		if init_result != .Need_More {
			release_worker_connection(
				&accepted,
				worker.table,
				worker.timers,
				worker.shutdown_state,
			)
			result.result = .Error
			continue
		}
		worker.connection_states[slot_index] = state
		register_result, _ := register_static_connection(
			worker.poller,
			&worker.connection_states[slot_index],
		)

		if register_result != .Ready {
			result.result = .Error
			close_static_worker_connection(worker, &worker.connection_states[slot_index])
		}
	}

	for readiness in turn.stream_events {
		if readiness.connection_id == STATIC_WORKER_CONTROL_EVENT_ID {
			if !worker.control_registered ||
			   worker.control_fd < 0 ||
			   !drain_static_worker_control(worker.control_fd) {
				result.result = .Error
			}
			continue
		}
		if readiness.connection_id == 0 {
			result.result = .Invalid
			continue
		}
		slot_index := int(readiness.connection_id - 1)

		if slot_index < 0 || slot_index >= len(worker.connection_states) {
			result.result = .Invalid
			continue
		}
		state := &worker.connection_states[slot_index]

		if !state.active {
			continue
		}
		if state.event_id != readiness.connection_id {
			result.result = .Invalid
			continue
		}
		result.processed_count += 1
		connection_turn := static_connection_turn(
			worker.poller,
			state,
			readiness,
			worker.routes,
			worker.body_limit,
			turn.byte_budget_per_connection,
			worker.table,
			worker.timers,
			worker.timeouts,
			turn.now_ns,
		)

		if connection_turn.result == .Closed ||
		   connection_turn.result == .Transport_Error ||
		   connection_turn.result == .Response_Buffer_Small ||
		   connection_turn.result == .Invalid {
			close_result := close_static_worker_connection(worker, state)

			if close_result == .Error {
				result.result = .Error
			}
			result.closed_count += 1
		}
	}

	if worker.shutdown_state.phase == .Forcing {
		force_static_worker_connections(worker, &result)
	}
	return result
}

force_static_worker_connections :: proc(worker: ^Static_Worker, turn: ^Static_Worker_Turn) {
	for i := 0; i < len(worker.connection_states); i += 1 {
		state := &worker.connection_states[i]

		if !state.active {
			continue
		}
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
	if worker.poller.fd < 0 || worker.listener.fd < 0 || worker.table.free_count < 0 {
		return false
	}
	if worker.table.free_count > len(worker.table.slots) ||
	   len(worker.connection_states) < len(worker.table.slots) ||
	   len(worker.connection_buffers) < len(worker.table.slots) {
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

		if !state.active || state.worker.token.generation != deadline.token.generation {
			turn.result = .Invalid
			continue
		}
		if close_static_worker_connection(worker, state) == .Error {
			turn.result = .Error
		}
		turn.closed_count += 1
	}
}

close_static_worker_connection :: proc(
	worker: ^Static_Worker,
	state: ^Static_Connection,
) -> Static_Worker_Result {
	if worker == nil || state == nil || !state.active {
		return .Invalid
	}
	result := Static_Worker_Result.Ready

	if state.registered {
		unregister_result, _ := epoll.unregister(worker.poller, state.worker.fd)

		if unregister_result != .Ready {
			result = .Error
		}
		state.registered = false
	}
	release := release_worker_connection(
		&state.worker,
		worker.table,
		worker.timers,
		worker.shutdown_state,
	)

	if release.table_result != .Released && release.table_result != .Retired {
		return .Error
	}
	if release.socket_result != .Ready {
		result = .Error
	}
	if release.shutdown_result != .Connection_Closed && release.shutdown_result != .Completed {
		result = .Error
	}
	state^ = Static_Connection{}
	return result
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
