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

@(test)
test_request_head_timeout_records_a_close_reason :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_result := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_result, linux.Errno.NONE)
	if socket_result != .NONE {
		return
	}

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(21, 1, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token, allocation_result := connections.allocate_connection(&table, u64(sockets[0]))
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)

	shutdown_state: shutdown.State
	testing.expect_value(
		t,
		shutdown.register_connection(&shutdown_state),
		shutdown.Result.Accepted,
	)
	timer_storage: [1]transport.Timer_Expiration
	timer_index_storage: [transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])

	request_input: [128]u8
	headers: [4]http1.Header
	response: [128]u8
	workspace_storage: [1]Static_Connection_Workspace
	workspace_storage[0] = Static_Connection_Workspace {
		request_input = request_input[:],
		headers       = headers[:],
		response      = response[:],
	}
	workspace_free_stack: [1]u32
	workspace_pool, pool_ready := init_static_workspace_pool(
		workspace_storage[:],
		workspace_free_stack[:],
	)
	testing.expect_value(t, pool_ready, true)
	state, state_result := init_static_connection(
		Worker_Connection{token = token, fd = sockets[0]},
		&workspace_pool,
	)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	states := [?]Static_Connection{state}

	timeouts := connections.Connection_Timeouts {
		request_head_ns = 100,
	}
	schedule := connections.schedule_connection_phase_deadline(
		&table,
		&timers,
		token,
		1_000,
		timeouts,
	)
	testing.expect_value(
		t,
		schedule.result,
		connections.Connection_Deadline_Schedule_Result.Scheduled,
	)
	timer, timer_result := transport.pop_expired_timer(&timers, schedule.timer.deadline_ns)
	testing.expect_value(t, timer_result, transport.Timer_Queue_Result.Expired)
	expired := [?]transport.Timer_Expiration{timer}
	worker := Static_Worker {
		shutdown_state    = &shutdown_state,
		table             = &table,
		workspace_pool    = &workspace_pool,
		timers            = &timers,
		connection_states = states[:],
	}
	turn := Static_Worker_Turn{}
	process_static_worker_expirations(&worker, expired[:], &turn)

	testing.expect_value(t, turn.closed_count, 1)
	testing.expect_value(t, worker.metrics.closed_connections, u64(1))
	testing.expect_value(
		t,
		worker.metrics.close_reasons[int(Static_Connection_Close_Reason.Timed_Out)],
		u64(1),
	)
	testing.expect_value(t, states[0].active, false)
	testing.expect_value(t, table.free_count, 1)
	testing.expect_value(t, shutdown_state.active_connections, u64(0))
	linux.close(sockets[1])
}

@(test)
test_static_worker_gracefully_drains_a_backpressured_response :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}
	send_buffer_size := i32(4096)
	setsockopt_error := linux.setsockopt(sockets[0], .SOCKET, .SNDBUF, &send_buffer_size)
	testing.expect_value(t, setsockopt_error, linux.Errno.NONE)

	loopback := [4]u8{127, 0, 0, 1}
	listener, listen_result, listen_error := socket.listen_ipv4(loopback, 0, 4)
	testing.expect_value(t, listen_result, socket.Result.Ready)
	testing.expect_value(t, listen_error, i32(0))

	if listen_result != .Ready {
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}
	poller, poll_result, poll_error := epoll.create()
	testing.expect_value(t, poll_result, epoll.Result.Ready)
	testing.expect_value(t, poll_error, i32(0))

	if poll_result != .Ready {
		socket.close_listener(&listener)
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	method := "GET"
	path := "/large"
	reason := "OK"
	body: [131_072]u8
	for i := 0; i < len(body); i += 1 {
		body[i] = 'x'
	}
	static_response := router.Static_Response {
		status = 200,
		reason = transmute([]u8)reason,
		body   = body[:],
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
		12,
		1,
		connection_slots[:],
		free_stack[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token, allocation_result := connections.allocate_connection(&table, u64(sockets[0]))
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)
	shutdown_state: shutdown.State
	testing.expect_value(
		t,
		shutdown.register_connection(&shutdown_state),
		shutdown.Result.Accepted,
	)
	timers_storage: [2]transport.Timer_Expiration
	timers_index_storage: [2 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timers_storage[:], timers_index_storage[:])
	worker_connection := Worker_Connection {
		token = token,
		fd    = sockets[0],
	}
	request_input: [128]u8
	request_headers: [4]http1.Header
	response_storage: [131_328]u8
	workspace_storage: [1]Static_Connection_Workspace
	workspace_storage[0] = Static_Connection_Workspace {
		request_input = request_input[:],
		headers       = request_headers[:],
		response      = response_storage[:],
	}
	workspace_free_stack: [1]u32
	workspace_pool, pool_ready := init_static_workspace_pool(
		workspace_storage[:],
		workspace_free_stack[:],
	)
	testing.expect_value(t, pool_ready, true)
	state, state_result := init_static_connection(worker_connection, &workspace_pool)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	connection_register_result, register_error := register_static_connection(poller, &state)
	testing.expect_value(t, connection_register_result, epoll.Result.Ready)
	testing.expect_value(t, register_error, i32(0))

	timeouts := connections.Connection_Timeouts {
		request_head_ns    = 10_000_000_000,
		response_write_ns  = 10_000_000_000,
		keep_alive_idle_ns = 10_000_000_000,
	}
	native_events: [1]linux.EPoll_Event
	readiness_events: [1]transport.Readiness_Event
	request := "GET /large HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n"
	written, write_error := linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	batch := epoll.wait(poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)

	if batch.result != .Ready {
		epoll.close(&poller)
		socket.close_listener(&listener)
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}
	clock, clock_error := linux.clock_gettime(.MONOTONIC)
	testing.expect_value(t, clock_error, linux.Errno.NONE)
	now_ns := u64(clock.time_sec) * 1_000_000_000 + u64(clock.time_nsec)
	turn := static_connection_turn(
		poller,
		&state,
		batch.events[0],
		&routes,
		u64(len(body)),
		16_384,
		&table,
		&timers,
		timeouts,
		now_ns,
	)
	testing.expect_value(t, turn.result, Static_Connection_Result.Response_Progress)

	blocked := false
	for attempt := 0; attempt < 32; attempt += 1 {
		batch = epoll.wait(poller, native_events[:], readiness_events[:], 0)

		if batch.result == .Timeout {
			blocked = true
			break
		}
		if batch.result != .Ready {
			continue
		}
		clock, clock_error = linux.clock_gettime(.MONOTONIC)
		if clock_error != .NONE {
			break
		}
		now_ns = u64(clock.time_sec) * 1_000_000_000 + u64(clock.time_nsec)
		for readiness in batch.events {
			turn = static_connection_turn(
				poller,
				&state,
				readiness,
				&routes,
				u64(len(body)),
				16_384,
				&table,
				&timers,
				timeouts,
				now_ns,
			)
		}
	}
	testing.expect_value(t, blocked, true)
	testing.expect_value(t, state.write_driver.cursor.offset > 0, true)
	testing.expect_value(t, state.write_driver.cursor.offset < state.response_length, true)
	full_response_length := state.response_length
	clock, clock_error = linux.clock_gettime(.MONOTONIC)
	testing.expect_value(t, clock_error, linux.Errno.NONE)
	now_ns = u64(clock.time_sec) * 1_000_000_000 + u64(clock.time_nsec)
	testing.expect_value(
		t,
		shutdown.request_shutdown(&shutdown_state, &timers, 99, now_ns, 10_000_000_000),
		shutdown.Result.Draining,
	)

	connection_states: [1]Static_Connection
	connection_states[0] = state
	worker_native_events: [1]linux.EPoll_Event
	worker_readiness_events: [1]transport.Readiness_Event
	accepted_storage: [1]socket.Accepted_Connection
	accepted_connections: [1]Worker_Connection
	expired_timer_storage: [1]transport.Timer_Expiration
	worker := Static_Worker {
		poller                = poller,
		listener              = listener,
		shutdown_state        = &shutdown_state,
		table                 = &table,
		routes                = &routes,
		connection_states     = connection_states[:],
		workspace_pool        = &workspace_pool,
		native_events         = worker_native_events[:],
		readiness_events      = worker_readiness_events[:],
		accepted_storage      = accepted_storage[:],
		accepted_connections  = accepted_connections[:],
		timers                = &timers,
		expired_timer_storage = expired_timer_storage[:],
		event_budget          = 1,
		byte_budget           = 16_384,
		accept_attempt_budget = 1,
		timeout_ms            = 0,
		body_limit            = u64(len(body)),
		timeouts              = timeouts,
	}
	total_received := 0
	peer_closed := false
	for attempt := 0; attempt < 128; attempt += 1 {
		worker_turn := run_static_worker_turn(&worker)
		testing.expect_value(
			t,
			worker_turn.result != .Error && worker_turn.result != .Invalid,
			true,
		)

		received: [16_384]u8
		read_length, read_error := linux.read(sockets[1], received[:])

		if read_error != .NONE {
			break
		}
		if read_length == 0 {
			peer_closed = true
			break
		}
		total_received += read_length
	}
	testing.expect_value(t, peer_closed, true)
	testing.expect_value(t, total_received, full_response_length)
	testing.expect_value(t, connection_states[0].active, false)
	testing.expect_value(t, timers.count, 0)
	testing.expect_value(t, shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, shutdown_state.active_connections, u64(0))
	linux.close(sockets[1])
	epoll.close(&poller)
	socket.close_listener(&listener)
}

@(test)
test_static_worker_forces_shutdown_at_deadline_during_partial_request_head :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		return
	}
	loopback := [4]u8{127, 0, 0, 1}
	listener, listen_result, listen_error := socket.listen_ipv4(loopback, 0, 4)
	testing.expect_value(t, listen_result, socket.Result.Ready)
	testing.expect_value(t, listen_error, i32(0))
	if listen_result != .Ready {
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}
	poller, poll_result, poll_error := epoll.create()
	testing.expect_value(t, poll_result, epoll.Result.Ready)
	testing.expect_value(t, poll_error, i32(0))
	if poll_result != .Ready {
		socket.close_listener(&listener)
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	method := "GET"
	path := "/never"
	reason := "OK"
	body := "unused"
	route_result := router.register_static_route(
		&routes,
		transmute([]u8)method,
		transmute([]u8)path,
		router.Static_Response {
			status = 200,
			reason = transmute([]u8)reason,
			body = transmute([]u8)body,
		},
	)
	testing.expect_value(t, route_result, router.Route_Table_Result.Added)
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(16, 2, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token, allocation_result := connections.allocate_connection(&table, u64(sockets[0]))
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)
	shutdown_state: shutdown.State
	testing.expect_value(
		t,
		shutdown.register_connection(&shutdown_state),
		shutdown.Result.Accepted,
	)
	timer_storage: [2]transport.Timer_Expiration
	timer_index_storage: [2 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	worker_connection := Worker_Connection {
		token = token,
		fd    = sockets[0],
	}
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
	state, state_result := init_static_connection(worker_connection, &workspace_pool)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	_, register_error := register_static_connection(poller, &state)
	testing.expect_value(t, register_error, i32(0))

	timeouts := connections.Connection_Timeouts {
		request_head_ns = 1_000_000_000,
	}
	partial_request := "GET /never HTTP/1.1\r\nHost: example.test\r\n"
	written, write_error := linux.write(sockets[1], transmute([]u8)partial_request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(partial_request))
	native_events: [1]linux.EPoll_Event
	readiness_events: [1]transport.Readiness_Event
	batch := epoll.wait(poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	testing.expect_value(t, len(batch.events), 1)

	clock, clock_error := linux.clock_gettime(.MONOTONIC)
	testing.expect_value(t, clock_error, linux.Errno.NONE)
	now_ns := u64(clock.time_sec) * 1_000_000_000 + u64(clock.time_nsec)
	if batch.result == .Ready && len(batch.events) == 1 {
		read_turn := static_connection_turn(
			poller,
			&state,
			batch.events[0],
			&routes,
			1024,
			64,
			&table,
			&timers,
			timeouts,
			now_ns,
		)
		testing.expect_value(t, read_turn.result, Static_Connection_Result.Need_More)
		testing.expect_value(t, read_turn.bytes_read, len(partial_request))
		testing.expect_value(
			t,
			state.workspace_pool.storage[state.workspace_slot].head_driver.input_length,
			len(partial_request),
		)
	}
	testing.expect_value(t, timers.count, 1)
	testing.expect_value(
		t,
		shutdown.request_shutdown(&shutdown_state, &timers, 77, 0, 1),
		shutdown.Result.Draining,
	)

	connection_states := [?]Static_Connection{state}
	worker_native_events: [1]linux.EPoll_Event
	worker_readiness_events: [1]transport.Readiness_Event
	accepted_storage: [1]socket.Accepted_Connection
	accepted_connections: [1]Worker_Connection
	expired_timer_storage: [2]transport.Timer_Expiration
	worker := Static_Worker {
		poller                = poller,
		listener              = listener,
		shutdown_state        = &shutdown_state,
		table                 = &table,
		routes                = &routes,
		connection_states     = connection_states[:],
		workspace_pool        = &workspace_pool,
		native_events         = worker_native_events[:],
		readiness_events      = worker_readiness_events[:],
		accepted_storage      = accepted_storage[:],
		accepted_connections  = accepted_connections[:],
		timers                = &timers,
		expired_timer_storage = expired_timer_storage[:],
		event_budget          = 1,
		byte_budget           = 64,
		accept_attempt_budget = 1,
		timeout_ms            = -1,
		body_limit            = 1024,
		timeouts              = timeouts,
	}
	worker_turn := run_static_worker_turn(&worker)
	testing.expect_value(t, worker_turn.result, Static_Worker_Result.Not_Ready)
	testing.expect_value(t, worker_turn.closed_count, 1)
	testing.expect_value(
		t,
		worker.metrics.close_reasons[int(Static_Connection_Close_Reason.Shutdown_Forced)],
		u64(1),
	)
	testing.expect_value(t, connection_states[0].active, false)
	testing.expect_value(t, table.free_count, 1)
	testing.expect_value(t, timers.count, 0)
	testing.expect_value(t, shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, shutdown_state.active_connections, u64(0))
	response: [1]u8
	read_length, read_error := linux.read(sockets[1], response[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(t, read_length, 0)
	epoll.close(&poller)
	socket.close_listener(&listener)
	linux.close(sockets[1])
}
