#+build linux
package linux_runtime

import "core:encoding/endian"
import "core:sys/linux"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

STATIC_WORKER_CONTROL_EVENT_ID :: u64(0xffff_ffff_ffff_ffff)
STATIC_LISTENER_RETRY_TIMER_ID :: u64(0xffff_ffff_ffff_fffd)
STATIC_LISTENER_RETRY_DELAY_NS :: u64(100_000_000)

Host_Wakeup :: struct {
	fd: linux.Fd,
}

Static_Worker :: struct {
	poller:                     epoll.Epoll,
	listener:                   socket.Listener,
	listener_registered:        bool,
	descriptor_limit_paused:    bool,
	descriptor_retry_scheduled: bool,
	control_fd:                 linux.Fd,
	control_registered:         bool,
	managed_result:             Static_Worker_Result,
	shutdown_state:             ^shutdown.State,
	table:                      ^connections.Connection_Table,
	routes:                     ^router.Route_Table,
	file_root:                  ^Static_File_Root,
	native_routes:              ^router.Native_Table,
	host_queue:                 ^host.Host_Queue,
	command_queue:              ^host.Host_Command_Queue,
	host_wakeup:                ^Host_Wakeup,
	connection_states:          []Static_Connection,
	workspace_pool:             ^Static_Workspace_Pool,
	native_events:              []linux.EPoll_Event,
	readiness_events:           []transport.Readiness_Event,
	accepted_storage:           []socket.Accepted_Connection,
	accepted_connections:       []Worker_Connection,
	timers:                     ^transport.Timer_Queue,
	expired_timer_storage:      []transport.Timer_Expiration,
	metrics:                    Static_Worker_Metrics,
	event_budget:               int,
	byte_budget:                int,
	accept_attempt_budget:      int,
	timeout_ms:                 i32,
	body_limit:                 u64,
	timeouts:                   connections.Connection_Timeouts,
}

Static_Worker_Result :: enum {
	Ready,
	Not_Ready,
	Error,
	Invalid,
}

Static_Worker_Metrics :: struct {
	accepted_connections: u64,
	active_connections:   u64,
	idle_connections:     u64,
	closed_connections:   u64,
	close_reasons:        [int(Static_Connection_Close_Reason.Count)]u64,
	bytes_read:           u64,
	bytes_written:        u64,
	timer_count:          int,
	workspace_active:     int,
	workspace_capacity:   int,
}

Static_Worker_Turn :: struct {
	worker:          Worker_Turn,
	accepted_count:  int,
	processed_count: int,
	closed_count:    int,
	bytes_read:      u64,
	bytes_written:   u64,
	result:          Static_Worker_Result,
}

run_static_worker_turn :: proc(worker: ^Static_Worker) -> Static_Worker_Turn {
	if !valid_static_worker(worker) {
		return Static_Worker_Turn{result = .Invalid}
	}
	refresh_static_worker_gauges(worker)

	defer signal_host_wakeup_if_queued(worker)

	if worker.shutdown_state.phase == .Forcing {
		turn := Static_Worker_Turn {
			result = .Ready,
		}

		if !cancel_static_worker_listener_retry(worker) {
			turn.result = .Error
		}

		if worker.listener_registered {
			unregister_result, _ := epoll.unregister(worker.poller, worker.listener.fd)

			if unregister_result != .Ready {
				turn.result = .Error
			}
			worker.listener_registered = false
		}
		force_static_worker_connections(worker, &turn)
		refresh_static_worker_gauges(worker)

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

	if turn.accept_result == .Descriptor_Limit {
		if !schedule_static_worker_listener_retry(worker, turn.now_ns) {
			result.result = .Error
		}
	}
	process_static_worker_expirations(worker, turn.expired_timers, &result)
	drain_host_commands(worker, turn.now_ns)

	for &accepted in turn.accepted {
		result.accepted_count += 1
		worker.metrics.accepted_connections += 1
		slot_index := int(accepted.token.slot)
		state, init_result := init_static_connection(
			accepted,
			worker.workspace_pool,
			worker.file_root,
			worker.native_routes,
			worker.host_queue,
		)

		if init_result != .Need_More {
			release := release_worker_connection(
				&accepted,
				worker.table,
				worker.timers,
				worker.shutdown_state,
			)

			if release.table_result == .Released || release.table_result == .Retired {
				record_static_worker_close(worker, .Initialization_Failed)
			}
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
			worker.connection_states[slot_index].close_reason = .Transport_Error
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

		if static_connection_event_id(state.token) != readiness.connection_id {
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

		if !sync_static_worker_idle_count(worker, state) {
			state.close_reason = .Internal_Error
			connection_turn.result = .Invalid
		}

		if connection_turn.bytes_read > 0 {
			result.bytes_read += u64(connection_turn.bytes_read)
			worker.metrics.bytes_read += u64(connection_turn.bytes_read)
		}

		if connection_turn.bytes_written > 0 {
			result.bytes_written += u64(connection_turn.bytes_written)
			worker.metrics.bytes_written += u64(connection_turn.bytes_written)
		}

		if connection_turn.result == .Closed ||
		   connection_turn.result == .Capacity_Limit ||
		   connection_turn.result == .Transport_Error ||
		   connection_turn.result == .Response_Buffer_Small ||
		   connection_turn.result == .Invalid {
			if state.close_reason == .Unknown {
				state.close_reason = static_connection_close_reason(connection_turn.result)
			}
			close_result := close_static_worker_connection(worker, state)

			if close_result == .Error {
				result.result = .Error
			}
			result.closed_count += 1
		}
	}

	if worker.shutdown_state.phase == .Forcing {
		if !cancel_static_worker_listener_retry(worker) {
			result.result = .Error
		}
		force_static_worker_connections(worker, &result)
	}
	refresh_static_worker_gauges(worker)

	return result
}

signal_host_wakeup_if_queued :: proc(worker: ^Static_Worker) {
	if worker.host_wakeup == nil || worker.host_wakeup.fd < 0 {
		return
	}

	if host.pending_host_events(worker.host_queue) == 0 {
		return
	}

	signal: [8]u8

	if !endian.put_u64(signal[:], endian.PLATFORM_BYTE_ORDER, 1) {
		return
	}

	for {
		_, write_error := linux.write(worker.host_wakeup.fd, signal[:])

		if write_error == .EINTR {
			continue
		}

		return
	}
}

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
		execute_host_command(worker, cmd, now_ns)
	}
}

execute_host_command :: proc(worker: ^Static_Worker, cmd: host.Host_Command, now_ns: u64) {
	if cmd.kind != .Read_Body &&
	   cmd.kind != .Response_Start &&
	   cmd.kind != .Response_Begin &&
	   cmd.kind != .Response_Write &&
	   cmd.kind != .Response_End &&
	   cmd.kind != .Cancel {
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

		if !state.active || state.token.generation != deadline.token.generation {
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
