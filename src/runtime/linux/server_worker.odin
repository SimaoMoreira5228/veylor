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
	h2_states:                  []H2_Worker_Connection,
	h2_free_stack:              []u32,
	h2_free_count:              int,
	h2_listener:                socket.Listener,
	h2_listener_registered:     bool,
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

		if worker.h2_listener_registered {
			h2_unregister_result, _ := epoll.unregister(worker.poller, worker.h2_listener.fd)

			if h2_unregister_result != .Ready {
				turn.result = .Error
			}
			worker.h2_listener_registered = false
		}
		force_static_worker_connections(worker, &turn)
		force_h2_worker_connections(worker, &turn)
		refresh_static_worker_gauges(worker)

		return turn
	}

	if !sync_h2_listener_registration(worker) {
		return Static_Worker_Turn{result = .Error}
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

		if readiness.connection_id == STATIC_WORKER_H2_LISTENER_ID {
			accept_h2_worker_connections(worker, readiness, turn.now_ns, &result)
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
			dispatch_h2_worker_readiness(
				worker,
				readiness,
				turn.byte_budget_per_connection,
				turn.now_ns,
				&result,
			)
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

	if worker.shutdown_state.phase == .Draining {
		if !drain_h2_worker_connections(worker) {
			result.result = .Error
		}
	}
	sweep_h2_receive_grants(worker, turn.byte_budget_per_connection, turn.now_ns, &result)
	drive_h2_worker_connections(worker, turn.byte_budget_per_connection, turn.now_ns, &result)

	if worker.shutdown_state.phase == .Forcing {
		if !cancel_static_worker_listener_retry(worker) {
			result.result = .Error
		}
		force_static_worker_connections(worker, &result)
		force_h2_worker_connections(worker, &result)
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
