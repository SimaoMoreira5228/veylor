package main

import "core:fmt"
import "core:sys/linux"
import router "veylor:router"
import runtime "veylor:runtime/linux"
import workers "veylor:runtime/workers"
import socket "veylor:transport/linux/socket"

MAX_STATIC_WORKERS :: 4

Static_Server_Storage :: struct {
	route_storage:       [1]router.Static_Route,
	route_index_storage: [2]router.Route_Index_Slot,
	routes:              router.Route_Table,
	worker_storage:      [MAX_STATIC_WORKERS]runtime.Static_Worker_Storage,
	static_file_root_fd: linux.Fd,
	static_file_root:    runtime.Static_File_Root,
	server_workers:      [MAX_STATIC_WORKERS]^runtime.Static_Worker,
	worker_threads:      [MAX_STATIC_WORKERS]workers.Worker_Thread,
}

initialize_static_server_storage :: proc(
	storage: ^Static_Server_Storage,
	address: [4]u8,
	port: u16,
	worker_count: int,
) -> bool {
	if storage == nil || worker_count <= 0 || worker_count > MAX_STATIC_WORKERS {
		return false
	}

	storage.static_file_root_fd = -1

	file_root_fd, file_root_error := linux.open(
		"examples/native_static/public",
		{.CLOEXEC, .DIRECTORY},
	)
	if file_root_error != .NONE {
		fmt.eprintln("static file root open failed:", file_root_error)
		return false
	}

	file_mount_prefix := "/assets/"
	if !runtime.init_static_file_root(
		&storage.static_file_root,
		file_root_fd,
		transmute([]u8)file_mount_prefix,
	) {
		fmt.eprintln("static file root setup failed")
		linux.close(file_root_fd)
		return false
	}
	storage.static_file_root_fd = file_root_fd

	storage.routes = router.init_route_table(
		storage.route_storage[:],
		storage.route_index_storage[:],
	)
	method := "GET"
	path := "/health"
	reason := "OK"
	response_body := "ok\n"
	static_response := router.Static_Response {
		status = 200,
		reason = transmute([]u8)reason,
		body   = transmute([]u8)response_body,
	}

	register_result := router.register_static_route(
		&storage.routes,
		transmute([]u8)method,
		transmute([]u8)path,
		static_response,
	)
	if register_result != .Added || router.freeze_route_table(&storage.routes) != .Frozen {
		fmt.eprintln("route setup failed:", register_result)
		close_unmanaged_static_server_storage(storage)
		return false
	}

	for i := 0; i < worker_count; i += 1 {
		listen_port := port
		if i > 0 {
			listen_port = storage.worker_storage[0].worker.listener.local_address.port
		}

		listener, listen_result, listen_error := socket.listen_ipv4(
			address,
			listen_port,
			128,
			worker_count > 1,
		)
		if listen_result != .Ready {
			fmt.eprintln("listen failed:", listen_result, listen_error)
			close_unmanaged_static_server_storage(storage)
			return false
		}

		worker_storage := &storage.worker_storage[i]
		worker_result := runtime.init_static_worker_storage(
			worker_storage,
			listener,
			u32(i + 1),
			1,
			&storage.routes,
			&storage.static_file_root,
		)
		if worker_result.error != .None {
			fmt.eprintln("worker setup failed:", worker_result.error)
			if worker_result.system_error != 0 {
				fmt.eprintln("worker setup system error:", worker_result.system_error)
			}

			close_unmanaged_static_server_storage(storage)
			return false
		}
		storage.server_workers[i] = &worker_storage.worker
	}
	return true
}

close_unmanaged_static_server_storage :: proc(storage: ^Static_Server_Storage) {
	for i := 0; i < MAX_STATIC_WORKERS; i += 1 {
		runtime.close_unmanaged_static_worker_storage(&storage.worker_storage[i])
	}

	if storage.static_file_root_fd >= 0 {
		linux.close(storage.static_file_root_fd)
		storage.static_file_root_fd = -1
	}
}
