package main

import "core:fmt"
import "core:sys/linux"
import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
import runtime "veylor:runtime/linux"
import shutdown "veylor:runtime/shutdown"
import workers "veylor:runtime/workers"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

MAX_STATIC_WORKERS :: 4
CONNECTIONS_PER_WORKER :: 128
WORKSPACES_PER_WORKER :: 8
REQUEST_INPUT_CAPACITY :: 16 * 1024
HEADERS_PER_WORKSPACE :: 100
RESPONSE_CAPACITY :: 1024
WORKER_EVENT_CAPACITY :: 128
ACCEPT_BATCH_CAPACITY :: 16

Static_Server_Storage :: struct {
	route_storage:         [1]router.Static_Route,
	route_index_storage:   [2]router.Route_Index_Slot,
	routes:                router.Route_Table,
	connection_slots:      [MAX_STATIC_WORKERS][CONNECTIONS_PER_WORKER]connections.Connection_Slot,
	connection_free_stack: [MAX_STATIC_WORKERS][CONNECTIONS_PER_WORKER]u32,
	connection_tables:     [MAX_STATIC_WORKERS]connections.Connection_Table,
	shutdown_states:       [MAX_STATIC_WORKERS]shutdown.State,
	timer_storage:         [MAX_STATIC_WORKERS][CONNECTIONS_PER_WORKER + 2]transport.Timer_Expiration,
	timers:                [MAX_STATIC_WORKERS]transport.Timer_Queue,
	connection_states:     [MAX_STATIC_WORKERS][CONNECTIONS_PER_WORKER]runtime.Static_Connection,
	workspace_storage:     [MAX_STATIC_WORKERS][WORKSPACES_PER_WORKER]runtime.Static_Connection_Workspace,
	workspace_free_stacks: [MAX_STATIC_WORKERS][WORKSPACES_PER_WORKER]u32,
	workspace_pools:       [MAX_STATIC_WORKERS]runtime.Static_Workspace_Pool,
	request_input_storage: [MAX_STATIC_WORKERS][WORKSPACES_PER_WORKER][REQUEST_INPUT_CAPACITY]u8,
	header_storage:        [MAX_STATIC_WORKERS][WORKSPACES_PER_WORKER][HEADERS_PER_WORKSPACE]http1.Header,
	response_storage:      [MAX_STATIC_WORKERS][WORKSPACES_PER_WORKER][RESPONSE_CAPACITY]u8,
	native_events:         [MAX_STATIC_WORKERS][WORKER_EVENT_CAPACITY]linux.EPoll_Event,
	readiness_events:      [MAX_STATIC_WORKERS][WORKER_EVENT_CAPACITY]transport.Readiness_Event,
	accepted_storage:      [MAX_STATIC_WORKERS][ACCEPT_BATCH_CAPACITY]socket.Accepted_Connection,
	accepted_connections:  [MAX_STATIC_WORKERS][ACCEPT_BATCH_CAPACITY]runtime.Worker_Connection,
	expired_timer_storage: [MAX_STATIC_WORKERS][CONNECTIONS_PER_WORKER + 2]transport.Timer_Expiration,
	static_file_root_fd:   linux.Fd,
	static_file_root:      runtime.Static_File_Root,
	server_worker_storage: [MAX_STATIC_WORKERS]runtime.Static_Worker,
	server_workers:        [MAX_STATIC_WORKERS]^runtime.Static_Worker,
	worker_threads:        [MAX_STATIC_WORKERS]workers.Worker_Thread,
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

	for i := 0; i < MAX_STATIC_WORKERS; i += 1 {
		storage.server_worker_storage[i].poller.fd = -1
		storage.server_worker_storage[i].listener.fd = -1
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
			listen_port = storage.server_worker_storage[0].listener.local_address.port
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

		if !initialize_static_worker_storage(storage, listener, i) {
			close_unmanaged_static_server_storage(storage)
			return false
		}
	}
	return true
}

initialize_static_worker_storage :: proc(
	storage: ^Static_Server_Storage,
	listener: socket.Listener,
	worker_index: int,
) -> bool {
	storage.server_worker_storage[worker_index].poller.fd = -1
	storage.server_worker_storage[worker_index].listener = listener

	poller, poll_result, poll_error := epoll.create()
	if poll_result != .Ready {
		fmt.eprintln("epoll create failed:", poll_result, poll_error)
		return false
	}

	table, table_result := connections.init_connection_table(
		u32(worker_index + 1),
		1,
		storage.connection_slots[worker_index][:],
		storage.connection_free_stack[worker_index][:],
	)
	if table_result != .Ready {
		fmt.eprintln("connection table setup failed:", table_result)
		epoll.close(&poller)
		return false
	}
	storage.connection_tables[worker_index] = table

	storage.timers[worker_index] = transport.init_timer_queue(
		storage.timer_storage[worker_index][:],
	)

	for workspace_index := 0; workspace_index < WORKSPACES_PER_WORKER; workspace_index += 1 {
		storage.workspace_storage[worker_index][workspace_index] =
			runtime.Static_Connection_Workspace {
				request_input = storage.request_input_storage[worker_index][workspace_index][:],
				headers       = storage.header_storage[worker_index][workspace_index][:],
				response      = storage.response_storage[worker_index][workspace_index][:],
			}
	}

	workspace_pool, pool_ready := runtime.init_static_workspace_pool(
		storage.workspace_storage[worker_index][:],
		storage.workspace_free_stacks[worker_index][:],
	)
	if !pool_ready {
		fmt.eprintln("workspace pool setup failed")
		epoll.close(&poller)
		return false
	}
	storage.workspace_pools[worker_index] = workspace_pool

	worker := runtime.Static_Worker {
		poller = poller,
		listener = listener,
		shutdown_state = &storage.shutdown_states[worker_index],
		table = &storage.connection_tables[worker_index],
		routes = &storage.routes,
		file_root = &storage.static_file_root,
		connection_states = storage.connection_states[worker_index][:],
		workspace_pool = &storage.workspace_pools[worker_index],
		native_events = storage.native_events[worker_index][:],
		readiness_events = storage.readiness_events[worker_index][:],
		accepted_storage = storage.accepted_storage[worker_index][:],
		accepted_connections = storage.accepted_connections[worker_index][:],
		timers = &storage.timers[worker_index],
		expired_timer_storage = storage.expired_timer_storage[worker_index][:],
		event_budget = WORKER_EVENT_CAPACITY,
		byte_budget = 64 * 1024,
		accept_attempt_budget = ACCEPT_BATCH_CAPACITY,
		timeout_ms = 100,
		body_limit = 16 * 1024 * 1024,
		timeouts = connections.Connection_Timeouts {
			request_head_ns = 10_000_000_000,
			response_write_ns = 10_000_000_000,
			keep_alive_idle_ns = 5_000_000_000,
		},
	}
	storage.server_worker_storage[worker_index] = worker
	storage.server_workers[worker_index] = &storage.server_worker_storage[worker_index]
	return true
}

close_unmanaged_static_server_storage :: proc(storage: ^Static_Server_Storage) {
	for i := 0; i < MAX_STATIC_WORKERS; i += 1 {
		worker := &storage.server_worker_storage[i]
		if worker.poller.fd >= 0 {
			epoll.close(&worker.poller)
		}
		if worker.listener.fd >= 0 {
			socket.close_listener(&worker.listener)
		}
	}

	if storage.static_file_root_fd >= 0 {
		linux.close(storage.static_file_root_fd)
		storage.static_file_root_fd = -1
	}
}
