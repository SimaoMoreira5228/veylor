#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
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
	connection_slots:     [1]connections.Connection_Slot,
	free_stack:           [1]u32,
	shutdown_state:       shutdown.State,
	timers:               transport.Timer_Queue,
	timer_storage:        [3]transport.Timer_Expiration,
	connection_states:    [1]Static_Connection,
	request_input:        [128]u8,
	request_headers:      [4]http1.Header,
	response_buffer:      [256]u8,
	connection_buffers:   [1]Static_Connection_Buffers,
	native_events:        [2]linux.EPoll_Event,
	readiness_events:     [2]transport.Readiness_Event,
	accepted_storage:     [1]socket.Accepted_Connection,
	accepted_connections: [1]Worker_Connection,
	expired_timers:       [2]transport.Timer_Expiration,
	worker:               Static_Worker,
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
	instance.worker.poller.fd = -1
	instance.worker.listener.fd = -1
	instance.worker.control_fd = -1
	loopback := [4]u8{127, 0, 0, 1}
	listener, listener_result, _ := socket.listen_ipv4(loopback, 0, 4)

	if listener_result != .Ready {
		return false
	}
	poller, poller_result, _ := epoll.create()

	if poller_result != .Ready {
		socket.close_listener(&listener)
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
		socket.close_listener(&listener)
		return false
	}
	table_result: connections.Connection_Table_Result
	instance.table, table_result = connections.init_connection_table(
		worker_id,
		1,
		instance.connection_slots[:],
		instance.free_stack[:],
	)

	if table_result != .Ready {
		epoll.close(&poller)
		socket.close_listener(&listener)
		return false
	}
	instance.timers = transport.init_timer_queue(instance.timer_storage[:])
	instance.connection_buffers[0] = Static_Connection_Buffers {
		request_input = instance.request_input[:],
		headers       = instance.request_headers[:],
		response      = instance.response_buffer[:],
	}
	instance.worker = Static_Worker {
		poller = poller,
		listener = listener,
		shutdown_state = &instance.shutdown_state,
		table = &instance.table,
		routes = &instance.routes,
		connection_states = instance.connection_states[:],
		connection_buffers = instance.connection_buffers[:],
		native_events = instance.native_events[:],
		readiness_events = instance.readiness_events[:],
		accepted_storage = instance.accepted_storage[:],
		accepted_connections = instance.accepted_connections[:],
		timers = &instance.timers,
		expired_timer_storage = instance.expired_timers[:],
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
	server_result := init_managed_server(
		&instance.server,
		&instance.worker,
		instance.worker_storage[:],
		shutdown_grace_ns,
	)

	if server_result != .Initialized {
		return false
	}
	instance.initialized = true
	if start_managed_server(&instance.server) != .Started {
		return false
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
	timers := transport.init_timer_queue(timer_storage[:])
	connection_states: [1]Static_Connection
	request_input: [128]u8
	request_headers: [4]http1.Header
	response_buffer: [256]u8
	connection_buffers: [1]Static_Connection_Buffers
	connection_buffers[0] = Static_Connection_Buffers {
		request_input = request_input[:],
		headers       = request_headers[:],
		response      = response_buffer[:],
	}
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
		connection_buffers = connection_buffers[:],
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
	server: Managed_Server
	testing.expect_value(
		t,
		init_managed_server(&server, &worker, worker_storage[:], 0),
		Managed_Server_Result.Initialized,
	)
	testing.expect_value(t, start_managed_server(&server), Managed_Server_Result.Started)
	testing.expect_value(
		t,
		init_managed_server(&server, &worker, worker_storage[:], 0),
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
	testing.expect_value(t, server.worker_result, Static_Worker_Result.Ready)
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
		testing.expect_value(t, instance.server.worker_result, Static_Worker_Result.Ready)
		destroy_result := destroy_managed_server(&instance.server)
		testing.expect_value(t, destroy_result, Managed_Server_Result.Destroyed)
		testing.expect_value(t, instance.server.phase, Managed_Server_Phase.Destroyed)
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

@(test)
test_static_worker_serves_cold_request_under_sustained_hot_socket_load :: proc(t: ^testing.T) {
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
	hot_clients: [3]linux.Fd
	for &client in hot_clients {
		client = -1
	}
	hot_clients_ready := true
	for &client in hot_clients {
		client = connect_managed_server_test_client(listener.local_address.port)
		if client < 0 {
			hot_clients_ready = false
			break
		}
	}
	cold_client := connect_managed_server_test_client(listener.local_address.port)

	if !hot_clients_ready || cold_client < 0 {
		for client in hot_clients {
			if client >= 0 {
				linux.close(client)
			}
		}
		if cold_client >= 0 {
			linux.close(cold_client)
		}
		epoll.close(&poller)
		socket.close_listener(&listener)
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	method := "GET"
	path := "/ready"
	reason := "OK"
	body := "ok"
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

	connection_slots: [4]connections.Connection_Slot
	free_stack: [4]u32
	table, table_result := connections.init_connection_table(
		8,
		1,
		connection_slots[:],
		free_stack[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	shutdown_state: shutdown.State
	timer_storage: [4]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	connection_states: [4]Static_Connection
	request_inputs: [4][8192]u8
	request_headers: [4][4]http1.Header
	response_buffers: [4][256]u8
	connection_buffers: [4]Static_Connection_Buffers
	for i := 0; i < len(connection_buffers); i += 1 {
		connection_buffers[i] = Static_Connection_Buffers {
			request_input = request_inputs[i][:],
			headers       = request_headers[i][:],
			response      = response_buffers[i][:],
		}
	}
	native_events: [1]linux.EPoll_Event
	readiness_events: [1]transport.Readiness_Event
	accepted_storage: [4]socket.Accepted_Connection
	accepted_connections: [4]Worker_Connection
	expired_timers: [5]transport.Timer_Expiration
	worker := Static_Worker {
		poller = poller,
		listener = listener,
		shutdown_state = &shutdown_state,
		table = &table,
		routes = &routes,
		connection_states = connection_states[:],
		connection_buffers = connection_buffers[:],
		native_events = native_events[:],
		readiness_events = readiness_events[:],
		accepted_storage = accepted_storage[:],
		accepted_connections = accepted_connections[:],
		timers = &timers,
		expired_timer_storage = expired_timers[:],
		event_budget = 1,
		byte_budget = 64,
		accept_attempt_budget = 4,
		timeout_ms = 0,
		body_limit = 1024,
		timeouts = connections.Connection_Timeouts {
			request_head_ns = 1_000_000_000,
			response_write_ns = 1_000_000_000,
			keep_alive_idle_ns = 1_000_000_000,
		},
	}
	initial_turn := run_static_worker_turn(&worker)
	testing.expect_value(t, initial_turn.result, Static_Worker_Result.Ready)
	testing.expect_value(t, initial_turn.accepted_count, 4)

	hot_prefix := "GET /hot HTTP/1.1\r\nHost: example.test\r\nX-Hot: "
	hot_request: [8192]u8
	copy(hot_request[:len(hot_prefix)], transmute([]u8)hot_prefix)
	for i := len(hot_prefix); i < len(hot_prefix) + 8000; i += 1 {
		hot_request[i] = 'x'
	}
	hot_request_length := len(hot_prefix) + 8000
	cold_request := "GET /ready HTTP/1.1\r\nHost: cold.example\r\n\r\n"
	for client in hot_clients {
		hot_written, hot_write_error := linux.write(client, hot_request[:hot_request_length])
		testing.expect_value(t, hot_write_error, linux.Errno.NONE)
		testing.expect_value(t, hot_written, hot_request_length)
	}
	cold_written, cold_write_error := linux.write(cold_client, transmute([]u8)cold_request)
	testing.expect_value(t, cold_write_error, linux.Errno.NONE)
	testing.expect_value(t, cold_written, len(cold_request))

	cold_served := false
	for turn_index := 0; turn_index < 16; turn_index += 1 {
		turn := run_static_worker_turn(&worker)

		if turn.result == .Error || turn.result == .Invalid {
			testing.expect_value(t, turn.result, Static_Worker_Result.Ready)
			break
		}
		for state in connection_states {
			if state.phase == .Keep_Alive_Idle {
				cold_served = true
			}
		}
		if cold_served {
			break
		}
	}
	testing.expect_value(t, cold_served, true)

	hot_prefix_count := 0
	for state in connection_states {
		if !state.active ||
		   state.phase != .Reading_Request_Head ||
		   state.head_driver.input_length <= 0 {
			continue
		}
		if http1.bytes_equal(state.request_input[:len(hot_prefix)], hot_prefix) {
			hot_prefix_count += 1
			testing.expect_value(t, state.head_driver.input_length < hot_request_length, true)
		}
	}
	testing.expect_value(t, hot_prefix_count, len(hot_clients))

	served_again := run_static_worker_turn(&worker)
	testing.expect_value(t, served_again.result, Static_Worker_Result.Ready)
	testing.expect_value(t, served_again.processed_count, 1)
	response: [256]u8
	response_length, read_error := linux.read(cold_client, response[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(response[:response_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
	)

	for client in hot_clients {
		linux.close(client)
	}
	linux.close(cold_client)
	testing.expect_value(
		t,
		shutdown.request_shutdown(&shutdown_state, &timers, 99, 100, 0),
		shutdown.Result.Force_Connections,
	)
	cleanup_turn := run_static_worker_turn(&worker)
	testing.expect_value(t, cleanup_turn.closed_count, 4)
	testing.expect_value(t, shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, shutdown_state.active_connections, u64(0))
	epoll.close(&poller)
	socket.close_listener(&listener)
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
