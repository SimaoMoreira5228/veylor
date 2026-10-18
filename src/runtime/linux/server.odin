#+build linux
package linux_runtime

import "core:encoding/endian"
import "core:sync"
import "core:sys/linux"
import host "veylor:runtime/host"
import shutdown "veylor:runtime/shutdown"
import workers "veylor:runtime/workers"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

Managed_Server_Phase :: enum {
	Unstarted,
	Running,
	Stop_Requested,
	Joined,
	Destroyed,
}

MANAGED_SERVER_SHUTDOWN_TIMER_ID :: u64(0xffff_ffff_ffff_fffe)

Managed_Server_Result :: enum {
	Initialized,
	Started,
	Stop_Requested,
	Already_Stopping,
	Joined,
	Destroyed,
	Busy,
	Capacity,
	Unsupported,
	Thread_Create_Failed,
	Worker_Error,
	System_Error,
	Invalid,
}

Managed_Server :: struct {
	server_workers:       []^Static_Worker,
	worker_pool:          workers.Worker_Pool,
	phase:                Managed_Server_Phase,
	stop_requested:       bool,
	force_stop_requested: bool,
	shutdown_grace_ns:    u64,
}

init_managed_server :: proc(
	server: ^Managed_Server,
	server_workers: []^Static_Worker,
	worker_storage: []workers.Worker_Thread,
	shutdown_grace_ns: u64,
) -> Managed_Server_Result {
	if server == nil || len(server_workers) == 0 {
		return .Invalid
	}
	if len(worker_storage) < len(server_workers) {
		return .Capacity
	}
	if server.phase != .Unstarted {
		return .Invalid
	}

	listener_fd := linux.Fd(-1)
	for i := 0; i < len(server_workers); i += 1 {
		worker := server_workers[i]
		if !valid_managed_server_worker(worker) {
			return .Invalid
		}
		if worker.shutdown_state.phase != .Running ||
		   worker.shutdown_state.active_connections != 0 ||
		   worker.shutdown_state.has_deadline ||
		   worker.table.free_count != len(worker.table.slots) ||
		   len(worker.table.slots) == 0 ||
		   len(worker.table.free_stack) < len(worker.table.slots) ||
		   worker.timers.count != 0 ||
		   worker.listener_registered ||
		   worker.control_registered {
			return .Invalid
		}
		if worker.timeouts.request_head_ns == 0 ||
		   worker.timeouts.response_write_ns == 0 ||
		   worker.timeouts.keep_alive_idle_ns == 0 {
			return .Invalid
		}
		if worker.timeout_ms == 0 {
			return .Invalid
		}
		required_timers := len(worker.table.slots) + 2
		if len(worker.timers.storage) < required_timers {
			return .Capacity
		}
		if i == 0 {
			listener_fd = worker.listener.fd
		} else {
			first_listener := server_workers[0].listener.local_address
			address := worker.listener.local_address
			if worker.listener.fd == listener_fd ||
			   address.family != first_listener.family ||
			   address.address != first_listener.address ||
			   address.port != first_listener.port {
				return .Invalid
			}
		}
		for j := 0; j < i; j += 1 {
			previous := server_workers[j]
			if worker == previous ||
			   worker.table == previous.table ||
			   &worker.table.slots[0] == &previous.table.slots[0] ||
			   &worker.table.free_stack[0] == &previous.table.free_stack[0] ||
			   worker.shutdown_state == previous.shutdown_state ||
			   worker.workspace_pool == previous.workspace_pool ||
			   &worker.workspace_pool.storage[0] == &previous.workspace_pool.storage[0] ||
			   worker.timers == previous.timers ||
			   &worker.timers.storage[0] == &previous.timers.storage[0] ||
			   worker.poller.fd == previous.poller.fd ||
			   worker.listener.fd == previous.listener.fd ||
			   &worker.connection_states[0] == &previous.connection_states[0] ||
			   worker.table.worker_id == previous.table.worker_id {
				return .Invalid
			}
		}
	}

	for worker in server_workers {
		worker.control_fd = -1
		worker.managed_result = .Ready
	}

	server^ = Managed_Server {
		server_workers    = server_workers,
		worker_pool       = workers.init_worker_pool(worker_storage),
		phase             = .Unstarted,
		shutdown_grace_ns = shutdown_grace_ns,
	}
	return .Initialized
}

start_managed_server :: proc(server: ^Managed_Server) -> Managed_Server_Result {
	if server == nil || server.phase != .Unstarted || len(server.server_workers) == 0 {
		return .Invalid
	}
	for i := 0; i < len(server.server_workers); i += 1 {
		worker := server.server_workers[i]
		control_fd, eventfd_error := linux.eventfd(0, {.CLOEXEC, .NONBLOCK})
		if eventfd_error != .NONE {
			rollback_managed_server_controls(server, i)
			return .System_Error
		}
		result, _ := epoll.register(
			worker.poller,
			control_fd,
			STATIC_WORKER_CONTROL_EVENT_ID,
			epoll.Interest{readable = true},
		)
		if result != .Ready {
			linux.close(control_fd)
			rollback_managed_server_controls(server, i)
			return .System_Error
		}
		worker.control_fd = control_fd
		worker.control_registered = true
		worker.managed_result = .Ready
	}

	sync.atomic_store_explicit(&server.stop_requested, false, .Relaxed)
	sync.atomic_store_explicit(&server.force_stop_requested, false, .Relaxed)
	pool_result := workers.start_worker_pool(
		&server.worker_pool,
		len(server.server_workers),
		managed_server_worker,
		rawptr(server),
	)

	if pool_result != .Started {
		rollback_managed_server_controls(server, len(server.server_workers))
		return managed_server_start_result(pool_result)
	}
	server.phase = .Running
	return .Started
}

request_managed_server_stop :: proc(server: ^Managed_Server) -> Managed_Server_Result {
	if server == nil || len(server.server_workers) == 0 {
		return .Invalid
	}
	if server.phase == .Stop_Requested {
		return .Already_Stopping
	}
	if server.phase != .Running {
		return .Invalid
	}
	sync.atomic_store_explicit(&server.stop_requested, true, .Release)
	write_result := signal_managed_server_workers(server)
	server.phase = .Stop_Requested

	if !write_result {
		return .System_Error
	}
	return .Stop_Requested
}

request_managed_server_force_stop :: proc(server: ^Managed_Server) -> Managed_Server_Result {
	if server == nil {
		return .Invalid
	}
	if server.phase == .Running {
		sync.atomic_store_explicit(&server.force_stop_requested, true, .Release)
		return request_managed_server_stop(server)
	}
	if server.phase != .Stop_Requested {
		return .Invalid
	}
	if sync.atomic_load_explicit(&server.force_stop_requested, .Acquire) {
		return .Already_Stopping
	}
	sync.atomic_store_explicit(&server.force_stop_requested, true, .Release)
	if !signal_managed_server_workers(server) {
		return .System_Error
	}
	return .Stop_Requested
}

join_managed_server :: proc(server: ^Managed_Server) -> Managed_Server_Result {
	if server == nil || server.phase != .Stop_Requested {
		return .Invalid
	}
	if workers.join_worker_pool(&server.worker_pool) != .Joined {
		return .Worker_Error
	}
	server.phase = .Joined

	for worker in server.server_workers {
		if worker.managed_result != .Ready {
			return .Worker_Error
		}
	}
	return .Joined
}

stop_managed_server :: proc(server: ^Managed_Server) -> Managed_Server_Result {
	if server == nil {
		return .Invalid
	}
	if server.phase == .Running {
		stop_result := request_managed_server_stop(server)

		if stop_result != .Stop_Requested {
			return stop_result
		}
	}
	if server.phase != .Stop_Requested {
		return .Invalid
	}
	return join_managed_server(server)
}

destroy_managed_server :: proc(server: ^Managed_Server) -> Managed_Server_Result {
	if server == nil || len(server.server_workers) == 0 {
		return .Invalid
	}
	if server.phase == .Destroyed {
		return .Destroyed
	}
	if server.phase == .Running || server.phase == .Stop_Requested {
		return .Busy
	}
	for worker in server.server_workers {
		if worker == nil || worker.shutdown_state == nil {
			return .Invalid
		}
		if server.phase == .Joined &&
		   (worker.shutdown_state.phase != .Complete ||
				   worker.shutdown_state.active_connections != 0 ||
				   worker.timers.count != 0) {
			return .Busy
		}
	}
	if server.phase == .Unstarted {
		for worker in server.server_workers {
			worker.shutdown_state.phase = .Complete
		}
	}
	for worker in server.server_workers {
		if worker.shutdown_state.phase == .Complete &&
		   shutdown.destroy(worker.shutdown_state) != .Destroyed {
			return .Worker_Error
		}
	}

	pool_result := workers.destroy_worker_pool(&server.worker_pool)

	if pool_result != .Destroyed {
		return .Worker_Error
	}

	result := Managed_Server_Result.Destroyed
	for worker in server.server_workers {
		if worker.control_registered {
			unregister_result, _ := epoll.unregister(worker.poller, worker.control_fd)
			if unregister_result != .Ready {
				result = .System_Error
			}
			worker.control_registered = false
		}
		if worker.control_fd >= 0 {
			if linux.close(worker.control_fd) != .NONE {
				result = .System_Error
			}
			worker.control_fd = -1
		}
		if worker.poller.fd >= 0 {
			poller_result, _ := epoll.close(&worker.poller)
			if poller_result != .Ready {
				result = .System_Error
			}
		}
		if worker.listener.fd >= 0 {
			listener_result, _ := socket.close_listener(&worker.listener)
			if listener_result != .Ready {
				result = .System_Error
			}
		}
	}
	server.phase = .Destroyed
	return result
}

managed_server_worker :: proc(worker_id: u32, user_data: rawptr) {
	server := cast(^Managed_Server)user_data
	if server == nil || int(worker_id) >= len(server.server_workers) {
		return
	}
	worker := server.server_workers[int(worker_id)]
	if worker == nil {
		return
	}
	for {
		if sync.atomic_load_explicit(&server.stop_requested, .Acquire) {
			force_requested := sync.atomic_load_explicit(&server.force_stop_requested, .Acquire)
			if force_requested && worker.shutdown_state.phase != .Complete {
				if !cancel_static_worker_listener_retry(worker) {
					worker.managed_result = .Error
				}
				force_result := shutdown.force_shutdown(worker.shutdown_state, worker.timers)
				if force_result != .Force_Connections && force_result != .Completed {
					worker.managed_result = .Error
				}
			}
			if !force_requested && worker.shutdown_state.phase == .Running {
				if !cancel_static_worker_listener_retry(worker) {
					fail_managed_server_worker(server, worker)
					continue
				}
				now, clock_error := linux.clock_gettime(.MONOTONIC)

				if clock_error != .NONE {
					fail_managed_server_worker(server, worker)
					continue
				}
				now_ns := u64(now.time_sec) * 1_000_000_000 + u64(now.time_nsec)
				shutdown_result := shutdown.request_shutdown(
					worker.shutdown_state,
					worker.timers,
					MANAGED_SERVER_SHUTDOWN_TIMER_ID,
					now_ns,
					server.shutdown_grace_ns,
				)

				if shutdown_result != .Draining &&
				   shutdown_result != .Force_Connections &&
				   shutdown_result != .Completed {
					fail_managed_server_worker(server, worker)
				}
			}
		}

		if worker.shutdown_state.phase == .Complete {
			// NOTE: best-effort; a full host queue already fails the connection elsewhere.
			host.enqueue_shutdown(worker.host_queue)

			signal_host_wakeup_if_queued(worker)

			return
		}

		turn := run_static_worker_turn(worker)

		if turn.result == .Error || turn.result == .Invalid {
			fail_managed_server_worker(server, worker)
		}
	}
}

fail_managed_server_worker :: proc(server: ^Managed_Server, worker: ^Static_Worker) {
	worker.managed_result = .Error
	sync.atomic_store_explicit(&server.force_stop_requested, true, .Release)
	sync.atomic_store_explicit(&server.stop_requested, true, .Release)
	signal_managed_server_workers(server)
	shutdown.force_shutdown(worker.shutdown_state, worker.timers)
}

signal_managed_server_workers :: proc(server: ^Managed_Server) -> bool {
	signaled := true
	for worker in server.server_workers {
		if worker.control_fd < 0 || !signal_managed_server_worker(worker.control_fd) {
			signaled = false
		}
	}
	return signaled
}

rollback_managed_server_controls :: proc(server: ^Managed_Server, count: int) {
	for i := 0; i < count; i += 1 {
		worker := server.server_workers[i]
		if worker.control_registered {
			epoll.unregister(worker.poller, worker.control_fd)
			worker.control_registered = false
		}
		if worker.control_fd >= 0 {
			linux.close(worker.control_fd)
			worker.control_fd = -1
		}
	}
}

valid_managed_server_worker :: proc(worker: ^Static_Worker) -> bool {
	return worker != nil && valid_static_worker(worker)
}

signal_managed_server_worker :: proc(control_fd: linux.Fd) -> bool {
	buffer: [8]u8
	if !endian.put_u64(buffer[:], endian.PLATFORM_BYTE_ORDER, 1) {
		return false
	}
	for {
		written, write_error := linux.write(control_fd, buffer[:])

		if write_error == .EINTR {
			continue
		}
		if write_error == .EAGAIN {
			return true
		}
		return write_error == .NONE && written == len(buffer)
	}
}

signal_static_worker :: proc(worker: ^Static_Worker) -> bool {
	if worker == nil || worker.control_fd < 0 {
		return false
	}
	return signal_managed_server_worker(worker.control_fd)
}

managed_server_start_result :: proc(result: workers.Worker_Pool_Result) -> Managed_Server_Result {
	switch result {
	case .Started:
		return .Started
	case .Capacity:
		return .Capacity
	case .Unsupported:
		return .Unsupported
	case .Thread_Create_Failed:
		return .Thread_Create_Failed
	case .Joined, .Destroyed, .Busy, .Invalid:
		return .Invalid
	}
	return .Invalid
}
