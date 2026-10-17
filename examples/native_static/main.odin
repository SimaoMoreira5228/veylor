package main

import "core:fmt"
import "core:os"
import "core:strconv"
import "core:sys/linux"
import runtime "veylor:runtime/linux"

static_server_storage: Static_Server_Storage

main :: proc() {
	duration_seconds, port, worker_count, valid := server_options()
	if !valid {
		fmt.eprintln(
			"usage: veylor-static [duration-seconds:1..3600] [port:1..65535] [workers:1..4]",
		)
		os.exit(2)
	}

	if !run_static_server(duration_seconds, port, worker_count) {
		os.exit(1)
	}
}

server_options :: proc() -> (duration_seconds: int, port: u16, worker_count: int, valid: bool) {
	duration_seconds = 60
	port = 8080
	worker_count = 1
	args := os.args

	if len(args) > 1 {
		value, parsed := strconv.parse_int(args[1])
		if !parsed || value < 1 || value > 3600 {
			return 0, 0, 0, false
		}
		duration_seconds = value
	}

	if len(args) > 2 {
		value, parsed := strconv.parse_int(args[2])
		if !parsed || value < 1 || value > 65_535 {
			return 0, 0, 0, false
		}
		port = u16(value)
	}

	if len(args) > 3 {
		value, parsed := strconv.parse_int(args[3])
		if !parsed || value < 1 || value > MAX_STATIC_WORKERS {
			return 0, 0, 0, false
		}
		worker_count = value
	}

	if len(args) > 4 {
		return 0, 0, 0, false
	}

	return duration_seconds, port, worker_count, true
}

run_static_server :: proc(duration_seconds: int, port: u16, worker_count: int) -> bool {
	loopback := [4]u8{127, 0, 0, 1}
	storage := &static_server_storage

	if !initialize_static_server_storage(storage, loopback, port, worker_count) {
		return false
	}

	server: runtime.Managed_Server
	server_result := runtime.init_managed_server(
		&server,
		storage.server_workers[:worker_count],
		storage.worker_threads[:worker_count],
		5_000_000_000,
	)
	if server_result != .Initialized {
		fmt.eprintln("server initialization failed:", server_result)
		close_unmanaged_static_server_storage(storage)
		return false
	}

	start_result := runtime.start_managed_server(&server)
	if start_result != .Started {
		fmt.eprintln("server start failed:", start_result)
		return finish_server(&server, storage, true) == .Destroyed
	}

	fmt.println(
		"Listening on http://127.0.0.1:",
		storage.worker_storage[0].worker.listener.local_address.port,
		"/health /assets/<path> workers:",
		worker_count,
		sep = "",
	)

	started, clock_error := linux.clock_gettime(.MONOTONIC)
	if clock_error != .NONE {
		fmt.eprintln("clock read failed:", clock_error)
		return finish_server(&server, storage, true) == .Destroyed
	}

	started_ns := u64(started.time_sec) * 1_000_000_000 + u64(started.time_nsec)
	deadline_ns := started_ns + u64(duration_seconds) * 1_000_000_000

	for {
		now, now_error := linux.clock_gettime(.MONOTONIC)
		if now_error != .NONE {
			fmt.eprintln("clock read failed:", now_error)
			return finish_server(&server, storage, true) == .Destroyed
		}

		now_ns := u64(now.time_sec) * 1_000_000_000 + u64(now.time_nsec)
		if now_ns >= deadline_ns {
			break
		}

		sleep := linux.Time_Spec {
			time_sec  = 0,
			time_nsec = 100_000_000,
		}
		remaining: linux.Time_Spec
		sleep_error := linux.nanosleep(&sleep, &remaining)
		if sleep_error != .NONE && sleep_error != .EINTR {
			fmt.eprintln("sleep failed:", sleep_error)
			return finish_server(&server, storage, true) == .Destroyed
		}
	}

	finish_result := finish_server(&server, storage, false)
	if finish_result != .Destroyed {
		fmt.eprintln("server shutdown failed:", finish_result)
		return false
	}

	accepted_connections: u64
	closed_connections: u64
	bytes_read: u64
	bytes_written: u64
	workspace_high_water: int
	for worker_index in 0 ..< worker_count {
		worker := storage.server_workers[worker_index]
		accepted_connections += worker.metrics.accepted_connections
		closed_connections += worker.metrics.closed_connections
		bytes_read += worker.metrics.bytes_read
		bytes_written += worker.metrics.bytes_written
		workspace_high_water += worker.workspace_pool.high_water_mark
	}

	fmt.println(
		"accepted connections:",
		accepted_connections,
		"closed connections:",
		closed_connections,
		"bytes read:",
		bytes_read,
		"bytes written:",
		bytes_written,
		"workspace high-water total:",
		workspace_high_water,
	)

	for worker_index in 0 ..< worker_count {
		worker := storage.server_workers[worker_index]
		fmt.println(
			"worker ",
			worker_index,
			" accepted:",
			worker.metrics.accepted_connections,
			" closed:",
			worker.metrics.closed_connections,
			" workspace peak:",
			worker.workspace_pool.high_water_mark,
		)
		print_worker_close_reasons(worker_index, worker)
	}

	return true
}

print_worker_close_reasons :: proc(worker_index: int, worker: ^runtime.Static_Worker) {
	for reason_index in 0 ..< int(runtime.Static_Connection_Close_Reason.Count) {
		count := worker.metrics.close_reasons[reason_index]
		if count == 0 {
			continue
		}

		reason := runtime.Static_Connection_Close_Reason(reason_index)
		fmt.println("worker ", worker_index, " close reason ", reason, ": ", count)
	}
}

finish_server :: proc(
	server: ^runtime.Managed_Server,
	storage: ^Static_Server_Storage,
	force: bool,
) -> runtime.Managed_Server_Result {
	if server.phase == .Running {
		if force {
			runtime.request_managed_server_force_stop(server)
		} else {
			runtime.request_managed_server_stop(server)
		}
	}

	if server.phase == .Stop_Requested {
		runtime.join_managed_server(server)
	}

	if server.phase == .Joined || server.phase == .Unstarted {
		destroy_result := runtime.destroy_managed_server(server)
		if destroy_result != .Destroyed {
			return destroy_result
		}

		if storage.static_file_root_fd >= 0 {
			close_error := linux.close(storage.static_file_root_fd)
			storage.static_file_root_fd = -1
			if close_error != .NONE {
				return .System_Error
			}
		}

		return destroy_result
	}

	return .Worker_Error
}
