#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

Static_Connection_Test_Context :: struct {
	poller:   epoll.Epoll,
	state:    ^Static_Connection,
	routes:   ^router.Route_Table,
	table:    ^connections.Connection_Table,
	timers:   ^transport.Timer_Queue,
	timeouts: connections.Connection_Timeouts,
}

drive_static_connection_to_keep_alive :: proc(
	ctx: ^Static_Connection_Test_Context,
	native_events: []linux.EPoll_Event,
	readiness_events: []transport.Readiness_Event,
) -> Static_Connection_Turn {
	for attempt in 0 ..< 8 {
		batch := epoll.wait(ctx.poller, native_events, readiness_events, 100)

		if batch.result != .Ready {
			continue
		}
		for readiness in batch.events {
			turn := static_connection_turn(
				ctx.poller,
				ctx.state,
				readiness,
				ctx.routes,
				1024,
				128,
				ctx.table,
				ctx.timers,
				ctx.timeouts,
				1000 + u64(attempt),
			)

			if turn.result == .Keep_Alive || turn.result == .Closed {
				return turn
			}
			if turn.result != .Need_More && turn.result != .Response_Progress {
				return turn
			}
		}
	}
	return Static_Connection_Turn{result = .Invalid}
}

drive_static_worker_to_keep_alive :: proc(worker: ^Static_Worker) -> Static_Worker_Turn {
	accumulated := Static_Worker_Turn{}
	for _ in 0 ..< 8 {
		turn := run_static_worker_turn(worker)
		accumulated.accepted_count += turn.accepted_count
		accumulated.processed_count += turn.processed_count
		accumulated.closed_count += turn.closed_count
		accumulated.worker = turn.worker

		if turn.result == .Error || turn.result == .Invalid {
			accumulated.result = turn.result
			return accumulated
		}
		if worker.connection_states[0].phase == .Keep_Alive_Idle {
			accumulated.result = turn.result
			return accumulated
		}
	}
	accumulated.result = .Invalid
	return accumulated
}

@(test)
test_static_connection_routes_two_sequential_requests_on_one_socket :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_result := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_result, linux.Errno.NONE)

	if socket_result != .NONE {
		return
	}
	poller, poll_result, poll_error := epoll.create()
	testing.expect_value(t, poll_result, epoll.Result.Ready)
	testing.expect_value(t, poll_error, i32(0))

	if poll_result != .Ready {
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	method := "GET"
	path := "/health"
	reason := "OK"
	body := "ok"
	route_response := router.Static_Response {
		status = 200,
		reason = transmute([]u8)reason,
		body   = transmute([]u8)body,
	}
	register_result := router.register_static_route(
		&routes,
		transmute([]u8)method,
		transmute([]u8)path,
		route_response,
	)
	testing.expect_value(t, register_result, router.Route_Table_Result.Added)
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	connection_slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(
		9,
		1,
		connection_slots[:],
		free_stack[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token, allocation_result := connections.allocate_connection(&table, u64(sockets[0]))
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)
	timer_storage: [2]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	shutdown_state: shutdown.State
	testing.expect_value(
		t,
		shutdown.register_connection(&shutdown_state),
		shutdown.Result.Accepted,
	)
	worker := Worker_Connection {
		token = token,
		fd    = sockets[0],
	}
	request_input: [128]u8
	request_headers: [4]http1.Header
	response_output: [256]u8
	state, state_result := init_static_connection(
		worker,
		request_input[:],
		request_headers[:],
		response_output[:],
	)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	static_register_result, register_error := register_static_connection(poller, &state)
	testing.expect_value(t, static_register_result, epoll.Result.Ready)
	testing.expect_value(t, register_error, i32(0))
	timeouts := connections.Connection_Timeouts {
		request_head_ns    = 1_000_000_000,
		response_write_ns  = 1_000_000_000,
		keep_alive_idle_ns = 1_000_000_000,
	}
	ctx := Static_Connection_Test_Context {
		poller   = poller,
		state    = &state,
		routes   = &routes,
		table    = &table,
		timers   = &timers,
		timeouts = timeouts,
	}
	native_events: [2]linux.EPoll_Event
	readiness_events: [2]transport.Readiness_Event
	request := "GET /health HTTP/1.1\r\nHost: example.test\r\n\r\n"
	written, write_error := linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	turn := drive_static_connection_to_keep_alive(&ctx, native_events[:], readiness_events[:])
	testing.expect_value(t, turn.result, Static_Connection_Result.Keep_Alive)
	testing.expect_value(t, state.phase, Static_Connection_Phase.Keep_Alive_Idle)

	first_response: [256]u8
	first_length, read_error := linux.read(sockets[1], first_response[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(first_response[:first_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
	)

	written, write_error = linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	turn = drive_static_connection_to_keep_alive(&ctx, native_events[:], readiness_events[:])
	testing.expect_value(t, turn.result, Static_Connection_Result.Keep_Alive)

	second_response: [256]u8
	second_length, second_read_error := linux.read(sockets[1], second_response[:])
	testing.expect_value(t, second_read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(second_response[:second_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
	)

	epoll.unregister(poller, sockets[0])
	epoll.close(&poller)
	linux.close(sockets[1])
	release := release_worker_connection(&state.worker, &table, &timers, &shutdown_state)
	testing.expect_value(t, release.table_result, connections.Connection_Table_Result.Released)
	testing.expect_value(t, release.socket_result, socket.Result.Ready)
	testing.expect_value(t, release.shutdown_result, shutdown.Result.Connection_Closed)
}

@(test)
test_static_worker_accepts_and_routes_loopback_keep_alive_requests :: proc(t: ^testing.T) {
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

	client_flags: linux.Socket_FD_Flags = {.CLOEXEC}
	client, client_error := linux.socket(.INET, .STREAM, client_flags, .TCP)
	testing.expect_value(t, client_error, linux.Errno.NONE)

	if client_error != .NONE {
		epoll.close(&poller)
		socket.close_listener(&listener)
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
		epoll.close(&poller)
		socket.close_listener(&listener)
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	method := "GET"
	path := "/health"
	reason := "OK"
	body := "ok"
	static_response := router.Static_Response {
		status = 200,
		reason = transmute([]u8)reason,
		body   = transmute([]u8)body,
	}
	testing.expect_value(
		t,
		router.register_static_route(
			&routes,
			transmute([]u8)method,
			transmute([]u8)path,
			static_response,
		),
		router.Route_Table_Result.Added,
	)
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	connection_slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(
		2,
		1,
		connection_slots[:],
		free_stack[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	shutdown_state: shutdown.State
	timer_storage: [2]transport.Timer_Expiration
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
		timeout_ms = 100,
		body_limit = 1024,
		timeouts = connections.Connection_Timeouts {
			request_head_ns = 1_000_000_000,
			response_write_ns = 1_000_000_000,
			keep_alive_idle_ns = 1_000_000_000,
		},
	}
	request := "GET /health HTTP/1.1\r\nHost: example.test\r\n\r\n"
	written, write_error := linux.write(client, transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	turn := drive_static_worker_to_keep_alive(&worker)
	testing.expect_value(t, turn.result, Static_Worker_Result.Ready)
	testing.expect_value(t, turn.accepted_count, 1)
	testing.expect_value(t, turn.closed_count, 0)

	first_response: [256]u8
	first_length, read_error := linux.read(client, first_response[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(first_response[:first_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
	)
	written, write_error = linux.write(client, transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	turn = drive_static_worker_to_keep_alive(&worker)
	testing.expect_value(t, turn.result, Static_Worker_Result.Ready)
	testing.expect_value(t, turn.accepted_count, 0)

	second_response: [256]u8
	second_length, second_read_error := linux.read(client, second_response[:])
	testing.expect_value(t, second_read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(second_response[:second_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
	)

	malformed_request := "GET /health HTTP/1.1\n\n"
	written, write_error = linux.write(client, transmute([]u8)malformed_request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(malformed_request))

	for _ in 0 ..< 8 {
		turn = run_static_worker_turn(&worker)

		if !connection_states[0].active {
			break
		}
	}
	testing.expect_value(t, connection_states[0].active, false)
	bad_response: [256]u8
	bad_response_length, bad_read_error := linux.read(client, bad_response[:])
	testing.expect_value(t, bad_read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(bad_response[:bad_response_length]),
		"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
	)
	linux.close(client)
	if worker.listener_registered {
		epoll.unregister(poller, listener.fd)
	}
	if connection_states[0].active {
		close_static_worker_connection(&worker, &connection_states[0])
	}
	epoll.close(&poller)
	socket.close_listener(&listener)
}
