#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import shutdown "veylor:runtime/shutdown"
import workers "veylor:runtime/workers"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

Managed_Server_Test_Instance :: struct {
	routes:               router.Route_Table,
	route_storage:        [1]router.Static_Route,
	route_index_storage:  [2]router.Route_Index_Slot,
	table:                connections.Connection_Table,
	connection_slots:     [32]connections.Connection_Slot,
	free_stack:           [32]u32,
	shutdown_state:       shutdown.State,
	timers:               transport.Timer_Queue,
	timer_storage:        [34]transport.Timer_Expiration,
	timer_index_storage:  [34 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32,
	connection_states:    [32]Static_Connection,
	host_event_storage:   [host.HOST_QUEUE_DEPTH]host.Host_Event,
	host_queue:           host.Host_Queue,
	workspace_storage:    [32]Static_Connection_Workspace,
	workspace_free_stack: [32]u32,
	workspace_pool:       Static_Workspace_Pool,
	request_input:        [32][128]u8,
	request_headers:      [32][4]http1.Header,
	response_buffer:      [32][256]u8,
	native_events:        [2]linux.EPoll_Event,
	readiness_events:     [2]transport.Readiness_Event,
	accepted_storage:     [1]socket.Accepted_Connection,
	accepted_connections: [1]Worker_Connection,
	expired_timers:       [34]transport.Timer_Expiration,
	worker:               Static_Worker,
	server_workers:       [1]^Static_Worker,
	worker_storage:       [1]workers.Worker_Thread,
	server:               Managed_Server,
	initialized:          bool,
}

init_managed_server_test_instance :: proc(
	instance: ^Managed_Server_Test_Instance,
	worker_id: u32,
	path, body: string,
	shutdown_grace_ns: u64,
) -> bool {
	loopback := [4]u8{127, 0, 0, 1}
	listener, listener_result, _ := socket.listen_ipv4(loopback, 0, 4)

	if listener_result != .Ready {
		return false
	}

	if !init_managed_server_test_worker(instance, worker_id, path, body, listener) {
		socket.close_listener(&listener)
		instance.worker.listener.fd = -1
		return false
	}
	instance.server_workers[0] = &instance.worker

	server_result := init_managed_server(
		&instance.server,
		instance.server_workers[:],
		instance.worker_storage[:],
		shutdown_grace_ns,
	)
	if server_result != .Initialized {
		epoll.close(&instance.worker.poller)
		socket.close_listener(&instance.worker.listener)
		instance.worker.listener.fd = -1
		return false
	}
	instance.initialized = true
	if start_managed_server(&instance.server) != .Started {
		return false
	}
	return true
}

init_managed_server_test_worker :: proc(
	instance: ^Managed_Server_Test_Instance,
	worker_id: u32,
	path, body: string,
	listener: socket.Listener,
	timeout_ms: i32 = -1,
	connection_capacity: int = 32,
	workspace_capacity: int = 32,
) -> bool {
	if connection_capacity <= 0 ||
	   connection_capacity > len(instance.connection_slots) ||
	   workspace_capacity <= 0 ||
	   workspace_capacity > len(instance.workspace_storage) {
		return false
	}

	instance.worker.poller.fd = -1
	instance.worker.listener.fd = listener.fd
	instance.worker.listener.local_address = listener.local_address
	instance.worker.control_fd = -1
	poller, poller_result, _ := epoll.create()

	if poller_result != .Ready {
		return false
	}

	instance.routes = router.init_route_table(
		instance.route_storage[:],
		instance.route_index_storage[:],
	)
	method := "GET"
	reason := "OK"
	static_response := router.Static_Response {
		status = 200,
		reason = transmute([]u8)reason,
		body   = transmute([]u8)body,
	}
	route_result := router.register_static_route(
		&instance.routes,
		transmute([]u8)method,
		transmute([]u8)path,
		static_response,
	)

	if route_result != .Added || router.freeze_route_table(&instance.routes) != .Frozen {
		epoll.close(&poller)
		return false
	}

	table_result: connections.Connection_Table_Result
	instance.table, table_result = connections.init_connection_table(
		worker_id,
		1,
		instance.connection_slots[:connection_capacity],
		instance.free_stack[:connection_capacity],
	)

	if table_result != .Ready {
		epoll.close(&poller)
		return false
	}
	instance.timers = transport.init_timer_queue(
		instance.timer_storage[:],
		instance.timer_index_storage[:],
	)
	for i := 0; i < len(instance.workspace_storage); i += 1 {
		instance.workspace_storage[i] = Static_Connection_Workspace {
			request_input = instance.request_input[i][:],
			headers       = instance.request_headers[i][:],
			response      = instance.response_buffer[i][:],
		}
	}
	workspace_pool, pool_ready := init_static_workspace_pool(
		instance.workspace_storage[:workspace_capacity],
		instance.workspace_free_stack[:workspace_capacity],
	)
	if !pool_ready {
		epoll.close(&poller)
		return false
	}
	instance.workspace_pool = workspace_pool

	instance.host_queue = host.init_host_queue(instance.host_event_storage[:])

	instance.worker = Static_Worker {
		poller = poller,
		listener = socket.Listener{fd = listener.fd, local_address = listener.local_address},
		shutdown_state = &instance.shutdown_state,
		table = &instance.table,
		routes = &instance.routes,
		host_queue = &instance.host_queue,
		connection_states = instance.connection_states[:],
		workspace_pool = &instance.workspace_pool,
		native_events = instance.native_events[:],
		readiness_events = instance.readiness_events[:],
		accepted_storage = instance.accepted_storage[:],
		accepted_connections = instance.accepted_connections[:],
		timers = &instance.timers,
		expired_timer_storage = instance.expired_timers[:],
		event_budget = 2,
		byte_budget = 128,
		accept_attempt_budget = 1,
		timeout_ms = timeout_ms,
		body_limit = 1024,
		timeouts = connections.Connection_Timeouts {
			request_head_ns = 1_000_000_000,
			response_write_ns = 1_000_000_000,
			keep_alive_idle_ns = 1_000_000_000,
		},
	}
	return true
}

cleanup_managed_server_test_instance :: proc(instance: ^Managed_Server_Test_Instance) {
	if instance.initialized {
		if instance.server.phase == .Running {
			request_managed_server_stop(&instance.server)
		}
		if instance.server.phase == .Stop_Requested {
			join_managed_server(&instance.server)
		}
		if instance.server.phase == .Joined || instance.server.phase == .Unstarted {
			destroy_managed_server(&instance.server)
		}
		return
	}
	if instance.worker.poller.fd >= 0 {
		epoll.close(&instance.worker.poller)
	}
	if instance.worker.listener.fd >= 0 {
		socket.close_listener(&instance.worker.listener)
	}
}

@(test)
test_managed_server_rejects_zero_poll_timeout :: proc(t: ^testing.T) {
	instance: Managed_Server_Test_Instance
	loopback := [4]u8{127, 0, 0, 1}
	listener, listen_result, _ := socket.listen_ipv4(loopback, 0, 4)
	testing.expect_value(t, listen_result, socket.Result.Ready)
	if listen_result != .Ready {
		return
	}

	worker_ready := init_managed_server_test_worker(&instance, 41, "/ready", "ready", listener, 0)
	testing.expect_value(t, worker_ready, true)
	if !worker_ready {
		cleanup_managed_server_test_instance(&instance)
		return
	}

	server_workers := [?]^Static_Worker{&instance.worker}
	worker_storage: [1]workers.Worker_Thread
	server: Managed_Server
	init_result := init_managed_server(&server, server_workers[:], worker_storage[:], 0)
	testing.expect_value(t, init_result, Managed_Server_Result.Invalid)
	cleanup_managed_server_test_instance(&instance)
}

@(test)
test_managed_server_starts_serves_stops_joins_and_destroys :: proc(t: ^testing.T) {
	loopback := [4]u8{127, 0, 0, 1}
	listener, listen_result, listen_error := socket.listen_ipv4(loopback, 0, 4)
	testing.expect_value(t, listen_result, socket.Result.Ready)
	testing.expect_value(t, listen_error, i32(0))

	if listen_result != .Ready {
		return
	}

	poller, poll_result, poll_error := epoll.create()
	testing.expect_value(t, poll_result, epoll.Result.Ready)
	testing.expect_value(t, poll_error, i32(0))

	if poll_result != .Ready {
		socket.close_listener(&listener)
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	method := "GET"
	path := "/ready"
	reason := "OK"
	body := "ready"
	static_response := router.Static_Response {
		status = 200,
		reason = transmute([]u8)reason,
		body   = transmute([]u8)body,
	}
	register_result := router.register_static_route(
		&routes,
		transmute([]u8)method,
		transmute([]u8)path,
		static_response,
	)
	testing.expect_value(t, register_result, router.Route_Table_Result.Added)
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	connection_slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(
		4,
		1,
		connection_slots[:],
		free_stack[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	shutdown_state: shutdown.State
	timer_storage: [3]transport.Timer_Expiration
	timer_index_storage: [3 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	connection_states: [1]Static_Connection
	request_input: [128]u8
	request_headers: [4]http1.Header
	response_buffer: [256]u8
	workspace_storage: [1]Static_Connection_Workspace
	workspace_storage[0] = Static_Connection_Workspace {
		request_input = request_input[:],
		headers       = request_headers[:],
		response      = response_buffer[:],
	}
	workspace_free_stack: [1]u32
	workspace_pool, pool_ready := init_static_workspace_pool(
		workspace_storage[:],
		workspace_free_stack[:],
	)
	testing.expect_value(t, pool_ready, true)
	native_events: [2]linux.EPoll_Event
	readiness_events: [2]transport.Readiness_Event
	accepted_storage: [1]socket.Accepted_Connection
	accepted_connections: [1]Worker_Connection
	expired_timers: [2]transport.Timer_Expiration
	worker := Static_Worker {
		poller = poller,
		listener = listener,
		shutdown_state = &shutdown_state,
		table = &table,
		routes = &routes,
		connection_states = connection_states[:],
		workspace_pool = &workspace_pool,
		native_events = native_events[:],
		readiness_events = readiness_events[:],
		accepted_storage = accepted_storage[:],
		accepted_connections = accepted_connections[:],
		timers = &timers,
		expired_timer_storage = expired_timers[:],
		event_budget = 2,
		byte_budget = 128,
		accept_attempt_budget = 1,
		timeout_ms = -1,
		body_limit = 1024,
		timeouts = connections.Connection_Timeouts {
			request_head_ns = 1_000_000_000,
			response_write_ns = 1_000_000_000,
			keep_alive_idle_ns = 1_000_000_000,
		},
	}
	worker_storage: [1]workers.Worker_Thread
	server_workers: [1]^Static_Worker
	server_workers[0] = &worker
	server: Managed_Server
	testing.expect_value(
		t,
		init_managed_server(&server, server_workers[:], worker_storage[:], 0),
		Managed_Server_Result.Initialized,
	)
	testing.expect_value(t, start_managed_server(&server), Managed_Server_Result.Started)
	testing.expect_value(
		t,
		init_managed_server(&server, server_workers[:], worker_storage[:], 0),
		Managed_Server_Result.Invalid,
	)
	testing.expect_value(t, server.phase, Managed_Server_Phase.Running)
	testing.expect_value(t, destroy_managed_server(&server), Managed_Server_Result.Busy)

	client_flags: linux.Socket_FD_Flags = {.CLOEXEC}
	client, client_error := linux.socket(.INET, .STREAM, client_flags, .TCP)
	testing.expect_value(t, client_error, linux.Errno.NONE)

	if client_error != .NONE {
		request_managed_server_stop(&server)
		join_managed_server(&server)
		destroy_managed_server(&server)
		return
	}
	destination := linux.Sock_Addr_In {
		sin_family = .INET,
		sin_port   = u16be(listener.local_address.port),
		sin_addr   = loopback,
	}
	connect_error := linux.connect(client, &destination)
	testing.expect_value(t, connect_error, linux.Errno.NONE)

	if connect_error != .NONE {
		linux.close(client)
		request_managed_server_stop(&server)
		join_managed_server(&server)
		destroy_managed_server(&server)
		return
	}
	request := "GET /ready HTTP/1.1\r\nHost: example.test\r\n\r\n"
	written, write_error := linux.write(client, transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	received: [256]u8
	response_length, read_error := linux.read(client, received[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(received[:response_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nready",
	)
	testing.expect_value(
		t,
		request_managed_server_stop(&server),
		Managed_Server_Result.Stop_Requested,
	)
	testing.expect_value(
		t,
		request_managed_server_stop(&server),
		Managed_Server_Result.Already_Stopping,
	)
	testing.expect_value(t, join_managed_server(&server), Managed_Server_Result.Joined)
	testing.expect_value(t, worker.managed_result, Static_Worker_Result.Ready)
	testing.expect_value(t, shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, shutdown_state.active_connections, u64(0))

	close_length, close_error := linux.read(client, received[:])
	testing.expect_value(t, close_error, linux.Errno.NONE)
	testing.expect_value(t, close_length, 0)
	linux.close(client)
	testing.expect_value(t, destroy_managed_server(&server), Managed_Server_Result.Destroyed)
	testing.expect_value(t, server.phase, Managed_Server_Phase.Destroyed)
}

@(test)
test_managed_server_repeats_start_stop_join_and_destroy :: proc(t: ^testing.T) {
	for cycle in 0 ..< 32 {
		instance: Managed_Server_Test_Instance
		initialized := init_managed_server_test_instance(
			&instance,
			u32(cycle + 20),
			"/cycle",
			"cycle",
			0,
		)
		testing.expect_value(t, initialized, true)
		if !initialized {
			cleanup_managed_server_test_instance(&instance)
			return
		}

		stop_result := request_managed_server_stop(&instance.server)
		testing.expect_value(t, stop_result, Managed_Server_Result.Stop_Requested)
		join_result := join_managed_server(&instance.server)
		testing.expect_value(t, join_result, Managed_Server_Result.Joined)
		testing.expect_value(t, instance.worker.managed_result, Static_Worker_Result.Ready)
		destroy_result := destroy_managed_server(&instance.server)
		testing.expect_value(t, destroy_result, Managed_Server_Result.Destroyed)
		testing.expect_value(t, instance.server.phase, Managed_Server_Phase.Destroyed)
		testing.expect_value(
			t,
			instance.server.worker_pool.phase,
			workers.Worker_Pool_Phase.Destroyed,
		)
		testing.expect_value(t, instance.worker.shutdown_state.phase, shutdown.Phase.Destroyed)
		testing.expect_value(t, instance.worker.control_fd, linux.Fd(-1))
		testing.expect_value(t, instance.worker.control_fd, linux.Fd(-1))
		testing.expect_value(t, instance.worker.poller.fd, linux.Fd(-1))
		testing.expect_value(t, instance.worker.listener.fd, linux.Fd(-1))
	}
}

@(test)
test_managed_server_instances_keep_state_and_shutdown_independent :: proc(t: ^testing.T) {
	first: Managed_Server_Test_Instance
	second: Managed_Server_Test_Instance
	first_ready := init_managed_server_test_instance(&first, 5, "/first", "first", 0)
	second_ready := init_managed_server_test_instance(
		&second,
		6,
		"/second",
		"second",
		1_000_000_000,
	)
	testing.expect_value(t, first_ready, true)
	testing.expect_value(t, second_ready, true)

	if !first_ready || !second_ready {
		cleanup_managed_server_test_instance(&first)
		cleanup_managed_server_test_instance(&second)
		return
	}
	first_client := connect_managed_server_test_client(first.worker.listener.local_address.port)
	second_client := connect_managed_server_test_client(second.worker.listener.local_address.port)

	if first_client < 0 || second_client < 0 {
		if first_client >= 0 {
			linux.close(first_client)
		}
		if second_client >= 0 {
			linux.close(second_client)
		}
		cleanup_managed_server_test_instance(&first)
		cleanup_managed_server_test_instance(&second)
		return
	}
	first_request := "GET /first HTTP/1.1\r\nHost: first.example\r\n\r\n"
	second_request := "GET /second HTTP/1.1\r\nHost: second.example\r\n\r\n"
	first_written, first_write_error := linux.write(first_client, transmute([]u8)first_request)
	second_written, second_write_error := linux.write(second_client, transmute([]u8)second_request)
	testing.expect_value(t, first_write_error, linux.Errno.NONE)
	testing.expect_value(t, first_written, len(first_request))
	testing.expect_value(t, second_write_error, linux.Errno.NONE)
	testing.expect_value(t, second_written, len(second_request))

	first_response: [256]u8
	first_response_length, first_read_error := linux.read(first_client, first_response[:])
	testing.expect_value(t, first_read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(first_response[:first_response_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst",
	)
	second_response: [256]u8
	second_response_length, second_read_error := linux.read(second_client, second_response[:])
	testing.expect_value(t, second_read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(second_response[:second_response_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond",
	)
	partial_request := "GET /second HTTP/1.1\r\nHost: second.example\r\n"
	partial_written, partial_write_error := linux.write(
		second_client,
		transmute([]u8)partial_request,
	)
	testing.expect_value(t, partial_write_error, linux.Errno.NONE)
	testing.expect_value(t, partial_written, len(partial_request))
	testing.expect_value(
		t,
		request_managed_server_stop(&first.server),
		Managed_Server_Result.Stop_Requested,
	)
	testing.expect_value(
		t,
		request_managed_server_stop(&second.server),
		Managed_Server_Result.Stop_Requested,
	)
	linux.close(second_client)
	testing.expect_value(t, join_managed_server(&first.server), Managed_Server_Result.Joined)
	testing.expect_value(t, join_managed_server(&second.server), Managed_Server_Result.Joined)
	testing.expect_value(t, first.shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, second.shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, first.shutdown_state.active_connections, u64(0))
	testing.expect_value(t, second.shutdown_state.active_connections, u64(0))
	testing.expect_value(t, first.timers.count, 0)
	testing.expect_value(t, second.timers.count, 0)
	linux.close(first_client)
	testing.expect_value(t, destroy_managed_server(&first.server), Managed_Server_Result.Destroyed)
	testing.expect_value(
		t,
		destroy_managed_server(&second.server),
		Managed_Server_Result.Destroyed,
	)
}

connect_managed_server_test_client :: proc(port: u16) -> linux.Fd {
	flags: linux.Socket_FD_Flags = {.CLOEXEC}
	client, socket_error := linux.socket(.INET, .STREAM, flags, .TCP)

	if socket_error != .NONE {
		return -1
	}
	destination := linux.Sock_Addr_In {
		sin_family = .INET,
		sin_port   = u16be(port),
		sin_addr   = [4]u8{127, 0, 0, 1},
	}
	connect_error := linux.connect(client, &destination)

	if connect_error != .NONE {
		linux.close(client)
		return -1
	}
	return client
}
