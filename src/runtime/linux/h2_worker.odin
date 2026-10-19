#+build linux
package linux_runtime

import connections "veylor:runtime/connections"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import epoll "veylor:transport/linux"

STATIC_WORKER_H2_LISTENER_ID :: u64(0xffff_ffff_ffff_fffe)

init_h2_worker_pool :: proc(
	states: []H2_Worker_Connection,
	free_stack: []u32,
) -> bool {
	if len(states) != H2_WORKER_CONNECTION_CAPACITY {
		return false
	}

	if len(free_stack) != H2_WORKER_CONNECTION_CAPACITY {
		return false
	}

	for i := 0; i < H2_WORKER_CONNECTION_CAPACITY; i += 1 {
		states[i] = H2_Worker_Connection{fd = -1}
		free_stack[i] = u32(H2_WORKER_CONNECTION_CAPACITY - i - 1)
	}

	return true
}

acquire_h2_worker_slot :: proc(
	states: []H2_Worker_Connection,
	free_stack: []u32,
	free_count: ^int,
) -> (
	slot: ^H2_Worker_Connection,
	ok: bool,
) {
	if free_count == nil || free_count^ <= 0 || free_count^ > len(states) {
		return nil, false
	}

	if len(free_stack) < len(states) {
		return nil, false
	}
	index := free_stack[free_count^ - 1]

	if int(index) >= len(states) {
		return nil, false
	}

	if states[index].active {
		return nil, false
	}
	free_count^ -= 1

	return &states[index], true
}

release_h2_worker_slot :: proc(
	states: []H2_Worker_Connection,
	free_stack: []u32,
	free_count: ^int,
	slot: ^H2_Worker_Connection,
) -> bool {
	if slot == nil || free_count == nil {
		return false
	}

	if free_count^ < 0 || free_count^ >= len(states) {
		return false
	}
	index := -1

	for i := 0; i < len(states); i += 1 {
		if &states[i] == slot {
			index = i
			break
		}
	}

	if index < 0 {
		return false
	}
	slot^ = H2_Worker_Connection{fd = -1}
	free_stack[free_count^] = u32(index)
	free_count^ += 1

	return true
}

find_h2_worker_slot :: proc(
	worker: ^Static_Worker,
	slot_index: int,
) -> (
	slot: ^H2_Worker_Connection,
	found: bool,
) {
	if worker == nil || worker.table == nil {
		return nil, false
	}

	if slot_index < 0 || slot_index >= len(worker.table.slots) {
		return nil, false
	}

	for i := 0; i < len(worker.h2_states); i += 1 {
		candidate := &worker.h2_states[i]

		if !candidate.active {
			continue
		}

		if int(candidate.token.slot) != slot_index {
			continue
		}
		resource_id, lookup_result := connections.lookup_connection(
			worker.table,
			candidate.token,
		)

		if lookup_result != .Found || resource_id != u64(candidate.fd) {
			continue
		}

		return candidate, true
	}

	return nil, false
}

sync_h2_listener_registration :: proc(worker: ^Static_Worker) -> bool {
	if worker == nil {
		return false
	}

	if len(worker.h2_states) == 0 {
		return true
	}

	if worker.h2_listener.fd < 0 {
		return true
	}
	accepting := worker.shutdown_state.phase == .Running
	should_register :=
		accepting &&
		worker.table.free_count > 0 &&
		!worker.descriptor_limit_paused &&
		worker.h2_free_count > 0

	if !should_register && worker.h2_listener_registered {
		result, _ := epoll.unregister(worker.poller, worker.h2_listener.fd)

		if result != .Ready {
			return false
		}
		worker.h2_listener_registered = false
	}

	if should_register && !worker.h2_listener_registered {
		result, _ := epoll.register(
			worker.poller,
			worker.h2_listener.fd,
			STATIC_WORKER_H2_LISTENER_ID,
			epoll.Interest{readable = true},
		)

		if result != .Ready {
			return false
		}
		worker.h2_listener_registered = true
	}

	return true
}

accept_h2_worker_connections :: proc(
	worker: ^Static_Worker,
	readiness: transport.Readiness_Event,
	now_ns: u64,
	turn: ^Static_Worker_Turn,
) {
	accept_turn := accept_listener_readiness(
		worker.h2_listener,
		STATIC_WORKER_H2_LISTENER_ID,
		readiness,
		worker.table,
		worker.accepted_storage,
		worker.accepted_connections,
		worker.accept_attempt_budget,
	)

	if accept_turn.result == .Listener_Failed ||
	   accept_turn.result == .Error ||
	   accept_turn.result == .Invalid {
		turn.result = .Error

		return
	}

	if accept_turn.result == .Descriptor_Limit {
		pause_result, _ := pause_listener_for_descriptor_limit(
			worker.poller,
			worker.h2_listener,
			&worker.h2_listener_registered,
			&worker.descriptor_limit_paused,
		)

		if pause_result != .Ready {
			turn.result = .Error

			return
		}

		if !schedule_static_worker_listener_retry(worker, now_ns) {
			turn.result = .Error
		}

		return
	}

	if len(accept_turn.connections) > 0 {
		shutdown_result := shutdown.register_connections(
			worker.shutdown_state,
			u64(len(accept_turn.connections)),
		)

		if shutdown_result != .Accepted {
			discard_untracked_worker_connections(accept_turn.connections, worker.table)
			turn.result = .Error

			return
		}
	}

	for &accepted in accept_turn.connections {
		turn.accepted_count += 1
		worker.metrics.accepted_connections += 1
		slot, slot_ok := acquire_h2_worker_slot(
			worker.h2_states,
			worker.h2_free_stack,
			&worker.h2_free_count,
		)

		if !slot_ok {
			release := release_worker_connection(
				&accepted,
				worker.table,
				worker.timers,
				worker.shutdown_state,
			)

			if release.table_result == .Released || release.table_result == .Retired {
				record_static_worker_close(worker, .Initialization_Failed)
			}
			turn.result = .Error

			continue
		}

		if !h2_worker_connection_init(slot, accepted.fd, accepted.token, accepted.peer) {
			release_h2_worker_slot(worker.h2_states, worker.h2_free_stack, &worker.h2_free_count, slot)
			release := release_worker_connection(
				&accepted,
				worker.table,
				worker.timers,
				worker.shutdown_state,
			)

			if release.table_result == .Released || release.table_result == .Retired {
				record_static_worker_close(worker, .Initialization_Failed)
			}
			turn.result = .Error

			continue
		}
		schedule := connections.schedule_connection_phase_deadline(
			worker.table,
			worker.timers,
			accepted.token,
			now_ns,
			worker.timeouts,
		)

		if schedule.result != .Scheduled {
			slot.close_reason = .Internal_Error
			close_result := close_h2_worker_connection(worker, slot)

			if close_result == .Error {
				turn.result = .Error
			}

			continue
		}
		register_result, _ := epoll.register(
			worker.poller,
			slot.fd,
			u64(accepted.token.slot) + 1,
			epoll.Interest{readable = true},
		)

		if register_result != .Ready {
			slot.close_reason = .Internal_Error
			close_result := close_h2_worker_connection(worker, slot)

			if close_result == .Error {
				turn.result = .Error
			}

			continue
		}
		slot.registered = true
	}
}

dispatch_h2_worker_readiness :: proc(
	worker: ^Static_Worker,
	readiness: transport.Readiness_Event,
	byte_budget: int,
	now_ns: u64,
	turn: ^Static_Worker_Turn,
) {
	slot_index := int(readiness.connection_id - 1)
	slot, found := find_h2_worker_slot(worker, slot_index)

	if !found {
		return
	}
	turn.processed_count += 1
	connection_turn := h2_worker_connection_turn(slot, byte_budget)

	if connection_turn.bytes_read > 0 {
		turn.bytes_read += u64(connection_turn.bytes_read)
		worker.metrics.bytes_read += u64(connection_turn.bytes_read)
	}

	if connection_turn.bytes_written > 0 {
		turn.bytes_written += u64(connection_turn.bytes_written)
		worker.metrics.bytes_written += u64(connection_turn.bytes_written)
	}

	if connection_turn.result == .Open {
		if h2_after_open_turn(worker, slot, connection_turn, byte_budget, now_ns, turn) {
			return
		}

		return
	}
	close_result := close_h2_worker_connection(worker, slot)

	if close_result == .Error ||
	   connection_turn.result == .Error ||
	   connection_turn.result == .Invalid {
		turn.result = .Error
	}
	turn.closed_count += 1
}

h2_after_open_turn :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	connection_turn: H2_Worker_Turn,
	byte_budget: int,
	now_ns: u64,
	turn: ^Static_Worker_Turn,
) -> (
	closed: bool,
) {
	handled, dispatch_ok := h2_dispatch_stream_events(worker, slot, connection_turn.events, now_ns)

	if !dispatch_ok {
		slot.close_reason = .Internal_Error
		close_h2_worker_connection(worker, slot)
		turn.result = .Error
		turn.closed_count += 1

		return true
	}

	if slot.write_used > slot.write_sent {
		flush_budget := byte_budget
		flush := h2_worker_flush_writes(slot, &flush_budget)

		if flush.transferred > 0 {
			turn.bytes_written += u64(flush.transferred)
			worker.metrics.bytes_written += u64(flush.transferred)
		}

		if flush.fatal {
			slot.close_reason = .Internal_Error
			close_h2_worker_connection(worker, slot)
			turn.result = .Error
			turn.closed_count += 1

			return true
		}

		if flush.peer_closed {
			slot.close_reason = .Peer_Closed
			close_h2_worker_connection(worker, slot)
			turn.closed_count += 1

			return true
		}
	}

	if !supervise_h2_worker_connection(
		worker,
		slot,
		now_ns,
		connection_turn.bytes_read > 0 || handled > 0,
	) {
		slot.close_reason = .Internal_Error
		close_h2_worker_connection(worker, slot)
		turn.result = .Error
		turn.closed_count += 1

		return true
	}

	if slot.closing && slot.write_sent >= slot.write_used && !h2_has_live_records(slot) {
		close_h2_worker_connection(worker, slot)
		turn.closed_count += 1

		return true
	}

	return false
}

supervise_h2_worker_connection :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
	now_ns: u64,
	active: bool,
) -> bool {
	if worker == nil || slot == nil || !slot.active {
		return false
	}
	phase, phase_result := connections.connection_phase(worker.table, slot.token)

	if phase_result != .Found {
		return false
	}

	if phase == .Reading_Request_Head {
		if !slot.conn.preface_done {
			return true
		}

		if connections.cancel_connection_phase_deadline(worker.table, worker.timers, slot.token) !=
		   .Cancelled {
			return false
		}

		if connections.transition_connection(worker.table, slot.token, .Reading_Request_Body) !=
		   .Transitioned {
			return false
		}

		return connections.schedule_connection_phase_deadline(
			worker.table,
			worker.timers,
			slot.token,
			now_ns,
			worker.timeouts,
		).result == .Scheduled
	}

	if phase != .Reading_Request_Body {
		return false
	}

	if !active {
		return true
	}

	if connections.cancel_connection_phase_deadline(worker.table, worker.timers, slot.token) !=
	   .Cancelled {
		return false
	}

	return connections.schedule_connection_phase_deadline(
		worker.table,
		worker.timers,
		slot.token,
		now_ns,
		worker.timeouts,
	).result == .Scheduled
}

drive_h2_worker_connections :: proc(
	worker: ^Static_Worker,
	byte_budget: int,
	now_ns: u64,
	turn: ^Static_Worker_Turn,
) {
	for i := 0; i < len(worker.h2_states); i += 1 {
		slot := &worker.h2_states[i]

		if !slot.active {
			continue
		}

		if !slot.closing && slot.write_used <= slot.write_sent {
			continue
		}
		connection_turn := h2_worker_connection_turn(slot, byte_budget)

		if connection_turn.bytes_read > 0 {
			turn.bytes_read += u64(connection_turn.bytes_read)
			worker.metrics.bytes_read += u64(connection_turn.bytes_read)
		}

		if connection_turn.bytes_written > 0 {
			turn.bytes_written += u64(connection_turn.bytes_written)
			worker.metrics.bytes_written += u64(connection_turn.bytes_written)
		}

		if connection_turn.result != .Open {
			close_result := close_h2_worker_connection(worker, slot)

			if close_result == .Error {
				turn.result = .Error
			}
			turn.closed_count += 1

			continue
		}

		if h2_after_open_turn(worker, slot, connection_turn, byte_budget, now_ns, turn) {
			continue
		}
	}
}

drain_h2_worker_connections :: proc(worker: ^Static_Worker) -> bool {
	for i := 0; i < len(worker.h2_states); i += 1 {
		slot := &worker.h2_states[i]

		if !slot.active {
			continue
		}

		if !h2_worker_connection_begin_shutdown(slot) {
			slot.close_reason = .Internal_Error

			return false
		}
	}

	return true
}

force_h2_worker_connections :: proc(worker: ^Static_Worker, turn: ^Static_Worker_Turn) {
	for i := 0; i < len(worker.h2_states); i += 1 {
		slot := &worker.h2_states[i]

		if !slot.active {
			continue
		}
		slot.close_reason = .Shutdown_Forced

		if close_h2_worker_connection(worker, slot) == .Error {
			turn.result = .Error
		}
		turn.closed_count += 1
	}
}

expire_h2_worker_deadline :: proc(
	worker: ^Static_Worker,
	slot_index: int,
	deadline: connections.Connection_Deadline,
	turn: ^Static_Worker_Turn,
) -> bool {
	slot, found := find_h2_worker_slot(worker, slot_index)

	if !found || slot.token.generation != deadline.token.generation {
		return false
	}
	slot.close_reason = .Timed_Out

	if close_h2_worker_connection(worker, slot) == .Error {
		turn.result = .Error
	}
	turn.closed_count += 1

	return true
}

close_h2_worker_connection :: proc(
	worker: ^Static_Worker,
	slot: ^H2_Worker_Connection,
) -> Static_Worker_Result {
	if worker == nil || slot == nil || !slot.active {
		return .Invalid
	}
	result := Static_Worker_Result.Ready

	if slot.registered {
		unregister_result, _ := epoll.unregister(worker.poller, slot.fd)

		if unregister_result != .Ready {
			result = .Error
		}
		slot.registered = false
	}
	accepted := Worker_Connection {
		token = slot.token,
		fd    = slot.fd,
		peer  = slot.peer,
	}
	release := release_worker_connection(
		&accepted,
		worker.table,
		worker.timers,
		worker.shutdown_state,
	)

	if release.table_result != .Released && release.table_result != .Retired {
		return .Error
	}
	record_static_worker_close(worker, h2_worker_close_reason(slot.close_reason))

	if release.socket_result != .Ready {
		result = .Error
	}

	if release.shutdown_result != .Connection_Closed && release.shutdown_result != .Completed {
		result = .Error
	}

	if !release_h2_worker_slot(worker.h2_states, worker.h2_free_stack, &worker.h2_free_count, slot) {
		return .Error
	}

	return result
}

h2_worker_close_reason :: proc(
	reason: H2_Worker_Close_Reason,
) -> Static_Connection_Close_Reason {
	switch reason {
	case .Peer_Closed:
		return .Peer_Closed
	case .Connection_Error:
		return .Transport_Error
	case .Shutdown_Forced:
		return .Shutdown_Forced
	case .Timed_Out:
		return .Timed_Out
	case .Shutdown:
		return .Normal
	case .Internal_Error:
		return .Internal_Error
	case .Unknown:
		return .Unknown
	}

	return .Unknown
}
