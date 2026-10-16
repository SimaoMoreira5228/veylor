#+build linux
package linux_runtime

import "core:encoding/endian"
import "core:sync"
import "core:sys/linux"
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
	worker:               ^Static_Worker,
	worker_pool:          workers.Worker_Pool,
	phase:                Managed_Server_Phase,
	control_fd:           linux.Fd,
	stop_requested:       bool,
	force_stop_requested: bool,
	shutdown_grace_ns:    u64,
	worker_result:        Static_Worker_Result,
}

init_managed_server :: proc(
	server: ^Managed_Server,
	worker: ^Static_Worker,
	worker_storage: []workers.Worker_Thread,
	shutdown_grace_ns: u64,
) -> Managed_Server_Result {
	if server == nil || worker == nil || !valid_static_worker(worker) {
		return .Invalid
	}
	if server.phase != .Unstarted {
		return .Invalid
	}
	if worker.shutdown_state.phase != .Running ||
	   worker.shutdown_state.active_connections != 0 ||
	   worker.shutdown_state.has_deadline ||
	   worker.table.free_count != len(worker.table.slots) ||
	   len(worker.table.slots) == 0 ||
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
	required_timers := len(worker.table.slots) + 2
	if len(worker.timers.storage) < required_timers {
		return .Capacity
	}
	for i := 0; i < len(worker.table.slots); i += 1 {
		buffers := worker.connection_buffers[i]

		if len(buffers.request_input) == 0 ||
		   len(buffers.headers) == 0 ||
		   len(buffers.response) == 0 {
			return .Invalid
		}
	}
	if len(worker_storage) == 0 {
		return .Capacity
	}
	server^ = Managed_Server {
		worker            = worker,
		worker_pool       = workers.init_worker_pool(worker_storage),
		phase             = .Unstarted,
		control_fd        = -1,
		shutdown_grace_ns = shutdown_grace_ns,
		worker_result     = .Ready,
	}
	return .Initialized
}

start_managed_server :: proc(server: ^Managed_Server) -> Managed_Server_Result {
	if server == nil || server.phase != .Unstarted || server.worker == nil {
		return .Invalid
	}
	control_fd, eventfd_error := linux.eventfd(0, {.CLOEXEC, .NONBLOCK})

	if eventfd_error != .NONE {
		return .System_Error
	}
	result, _ := epoll.register(
		server.worker.poller,
		control_fd,
		STATIC_WORKER_CONTROL_EVENT_ID,
		epoll.Interest{readable = true},
	)

	if result != .Ready {
		linux.close(control_fd)
		return .System_Error
	}
	server.control_fd = control_fd
	server.worker.control_fd = control_fd
	server.worker.control_registered = true
	sync.atomic_store_explicit(&server.stop_requested, false, .Relaxed)
	sync.atomic_store_explicit(&server.force_stop_requested, false, .Relaxed)
	server.worker_result = .Ready
	pool_result := workers.start_worker_pool(
		&server.worker_pool,
		1,
		managed_server_worker,
		rawptr(server),
	)

	if pool_result != .Started {
		epoll.unregister(server.worker.poller, control_fd)
		server.worker.control_registered = false
		server.worker.control_fd = -1
		server.control_fd = -1
		linux.close(control_fd)
		return managed_server_start_result(pool_result)
	}
	server.phase = .Running
	return .Started
}

request_managed_server_stop :: proc(server: ^Managed_Server) -> Managed_Server_Result {
	if server == nil || server.control_fd < 0 {
		return .Invalid
	}
	if server.phase == .Stop_Requested {
		return .Already_Stopping
	}
	if server.phase != .Running {
		return .Invalid
	}
	sync.atomic_store_explicit(&server.stop_requested, true, .Release)
	write_result := signal_managed_server_worker(server.control_fd)
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
	if !signal_managed_server_worker(server.control_fd) {
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

	if server.worker_result != .Ready {
		return .Worker_Error
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
	if server == nil || server.worker == nil || server.worker.shutdown_state == nil {
		return .Invalid
	}
	if server.phase == .Destroyed {
		return .Destroyed
	}
	if server.phase == .Running || server.phase == .Stop_Requested {
		return .Busy
	}
	if server.phase == .Joined {
		if server.worker.shutdown_state.phase != .Complete ||
		   server.worker.shutdown_state.active_connections != 0 ||
		   server.worker.timers.count != 0 {
			return .Busy
		}
	}
	if server.phase == .Unstarted {
		server.worker.shutdown_state.phase = .Complete
	}
	if server.worker.shutdown_state.phase == .Complete {
		if shutdown.destroy(server.worker.shutdown_state) != .Destroyed {
			return .Worker_Error
		}
	}
	pool_result := workers.destroy_worker_pool(&server.worker_pool)

	if pool_result != .Destroyed {
		return .Worker_Error
	}
	result := Managed_Server_Result.Destroyed

	if server.worker.control_registered {
		unregister_result, _ := epoll.unregister(server.worker.poller, server.control_fd)

		if unregister_result != .Ready {
			result = .System_Error
		}
		server.worker.control_registered = false
	}
	if server.control_fd >= 0 {
		if linux.close(server.control_fd) != .NONE {
			result = .System_Error
		}
		server.control_fd = -1
		server.worker.control_fd = -1
	}
	if server.worker.poller.fd >= 0 {
		poller_result, _ := epoll.close(&server.worker.poller)

		if poller_result != .Ready {
			result = .System_Error
		}
	}
	if server.worker.listener.fd >= 0 {
		listener_result, _ := socket.close_listener(&server.worker.listener)

		if listener_result != .Ready {
			result = .System_Error
		}
	}
	server.phase = .Destroyed
	return result
}

managed_server_worker :: proc(worker_id: u32, user_data: rawptr) {
	server := cast(^Managed_Server)user_data

	if server == nil || worker_id != 0 {
		return
	}
	for {
		if sync.atomic_load_explicit(&server.stop_requested, .Acquire) {
			force_requested := sync.atomic_load_explicit(&server.force_stop_requested, .Acquire)
			if force_requested && server.worker.shutdown_state.phase != .Complete {
				if !cancel_static_worker_listener_retry(server.worker) {
					server.worker_result = .Error
				}
				force_result := shutdown.force_shutdown(
					server.worker.shutdown_state,
					server.worker.timers,
				)
				if force_result != .Force_Connections && force_result != .Completed {
					server.worker_result = .Error
				}
			}
			if !force_requested && server.worker.shutdown_state.phase == .Running {
				if !cancel_static_worker_listener_retry(server.worker) {
					server.worker_result = .Error
					shutdown.force_shutdown(
						server.worker.shutdown_state,
						server.worker.timers,
					)
					continue
				}
				now, clock_error := linux.clock_gettime(.MONOTONIC)

				if clock_error != .NONE {
					server.worker_result = .Error
					shutdown.force_shutdown(
						server.worker.shutdown_state,
						server.worker.timers,
					)
					continue
				}
				now_ns := u64(now.time_sec) * 1_000_000_000 + u64(now.time_nsec)
				shutdown_result := shutdown.request_shutdown(
					server.worker.shutdown_state,
					server.worker.timers,
					MANAGED_SERVER_SHUTDOWN_TIMER_ID,
					now_ns,
					server.shutdown_grace_ns,
				)

				if shutdown_result != .Draining &&
				   shutdown_result != .Force_Connections &&
				   shutdown_result != .Completed {
					server.worker_result = .Error
					shutdown.force_shutdown(
						server.worker.shutdown_state,
						server.worker.timers,
					)
				}
			}
		}
		if server.worker.shutdown_state.phase == .Complete {
			return
		}
		turn := run_static_worker_turn(server.worker)

		if turn.result == .Error || turn.result == .Invalid {
			server.worker_result = .Error
			shutdown.force_shutdown(
				server.worker.shutdown_state,
				server.worker.timers,
			)
		}
	}
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
