#+build linux
package abi

import odin_runtime "base:runtime"
import "core:sys/linux"
import host "veylor:runtime/host"

@(export)
vyl_server_poll_events :: proc "c" (
	server: ^Vyl_Server,
	events: [^]Vyl_Host_Event,
	event_capacity: u32,
	data: [^]u8,
	data_capacity: u32,
	timeout_ns: u64,
	out_info: ^Vyl_Batch_Info,
) -> i32 {
	context = odin_runtime.default_context()

	if server == nil || out_info == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}

	if event_capacity > 0 && events == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}

	if data_capacity > 0 && data == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}

	if server.state != .Running && server.state != .Stop_Requested {
		return i32(Vyl_Result.Bad_State)
	}

	out_info^ = Vyl_Batch_Info{}
	worker_count := int(server.config.worker_count)

	if worker_count <= 0 || worker_count > len(server.server_workers) {
		return i32(Vyl_Result.Invalid_Argument)
	}

	event_slice := events[:int(event_capacity)]
	data_slice := data[:int(data_capacity)]
	copied, used, data_need := drain_host_event_queues(
		server,
		worker_count,
		event_slice,
		data_slice,
		data_capacity,
	)

	if copied == 0 && timeout_ns != 0 && remaining_host_events(server, worker_count) == 0 {
		wait_host_wakeup(server, timeout_ns)
		copied, used, data_need = drain_host_event_queues(
			server,
			worker_count,
			event_slice,
			data_slice,
			data_capacity,
		)
	}
	server.poll_cursor = (server.poll_cursor + 1) % worker_count
	remaining := remaining_host_events(server, worker_count)
	out_info^ = Vyl_Batch_Info {
		event_count     = u32(copied),
		data_used       = used,
		events_required = u32(remaining),
		data_required   = data_need,
	}

	if copied == 0 && remaining > 0 {
		return i32(Vyl_Result.Buffer_Too_Small)
	}

	return i32(Vyl_Result.Ok)
}

drain_host_event_queues :: proc(
	server: ^Vyl_Server,
	worker_count: int,
	event_slice: []Vyl_Host_Event,
	data_slice: []u8,
	data_capacity: u32,
) -> (
	copied: int,
	used: u32,
	data_need: u32,
) {
	start := server.poll_cursor % worker_count

	for w := 0; w < worker_count; w += 1 {
		worker := server.server_workers[(start + w) % worker_count]

		if worker == nil || worker.host_queue == nil {
			continue
		}

		for host.pending_host_events(worker.host_queue) > 0 {
			if copied >= len(event_slice) {
				break
			}
			front, peek_ok := host.peek_host_queue(worker.host_queue)

			if !peek_ok {
				break
			}
			need := u32(front.method_length + front.path_length)

			if used + need > data_capacity {
				if data_need == 0 {
					data_need = need
				}
				break
			}
			single: [1]host.Host_Event
			dequeued := host.poll_host_queue(worker.host_queue, single[:])

			if dequeued.copied != 1 {
				break
			}
			taken := single[0]
			event_kind := Vyl_Host_Event_Kind.Request

			if taken.kind == .Shutdown {
				event_kind = .Shutdown
			}

			if taken.kind == .Body_Available {
				event_kind = .Body_Available
			}

			if taken.kind == .Writable {
				event_kind = .Writable
			}
			out_event := &event_slice[copied]
			out_event^ = Vyl_Host_Event {
				kind = event_kind,
				token = Vyl_Request_Token {
					worker_id = taken.token.worker_id,
					slot = taken.token.slot,
					request_seq = taken.token.request_seq,
					worker_generation = taken.token.worker_generation,
					connection_generation = taken.token.generation,
				},
				route_id = taken.route_id,
				method_offset = used,
				method_length = u32(taken.method_length),
				path_offset = used + u32(taken.method_length),
				path_length = u32(taken.path_length),
				body_ready = taken.body_ready,
				body_complete = taken.body_complete,
			}
			copy(data_slice[used:], taken.method[:taken.method_length])
			copy(data_slice[used + u32(taken.method_length):], taken.path[:taken.path_length])
			used += need
			copied += 1
		}

		if copied >= len(event_slice) {
			break
		}
	}

	return copied, used, data_need
}

HOST_WAKEUP_POLL_MS_MAX :: i32(0x7fffffff)

wait_host_wakeup :: proc(server: ^Vyl_Server, timeout_ns: u64) {
	if !server.host_wakeup_ready || server.host_wakeup.fd < 0 {
		return
	}
	now, clock_ok := host_wakeup_now_ns()

	if !clock_ok {
		return
	}
	deadline := now + timeout_ns

	if deadline < now {
		deadline = max(u64)
	}

	for {
		tick, tick_ok := host_wakeup_now_ns()

		if !tick_ok || tick >= deadline {
			return
		}
		wait_ms := (deadline - tick + 999_999) / 1_000_000
		clamped_ms := HOST_WAKEUP_POLL_MS_MAX

		if wait_ms < u64(clamped_ms) {
			clamped_ms = i32(wait_ms)
		}
		poll_fds := [1]linux.Poll_Fd{{fd = server.host_wakeup.fd, events = {.IN}}}
		ready, poll_error := linux.poll(poll_fds[:], clamped_ms)

		if poll_error == .EINTR {
			continue
		}

		if poll_error != .NONE {
			return
		}

		if ready == 0 {
			continue
		}
		drain_host_wakeup_fd(server)

		if remaining_host_events(server, int(server.config.worker_count)) > 0 {
			return
		}
	}
}

drain_host_wakeup_fd :: proc(server: ^Vyl_Server) {
	buffer: [8]u8

	for {
		_, read_error := linux.read(server.host_wakeup.fd, buffer[:])

		if read_error == .EINTR {
			continue
		}

		return
	}
}

host_wakeup_now_ns :: proc() -> (u64, bool) {
	now, clock_error := linux.clock_gettime(.MONOTONIC)

	if clock_error != .NONE {
		return 0, false
	}

	return u64(now.time_sec) * 1_000_000_000 + u64(now.time_nsec), true
}

remaining_host_events :: proc(server: ^Vyl_Server, worker_count: int) -> int {
	remaining := 0

	for i := 0; i < worker_count; i += 1 {
		worker := server.server_workers[i]

		if worker == nil || worker.host_queue == nil {
			continue
		}
		remaining += host.pending_host_events(worker.host_queue)
	}

	return remaining
}
