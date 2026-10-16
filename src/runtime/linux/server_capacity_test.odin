#+build linux
#+test
package linux_runtime

import "core:mem"
import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import connection "veylor:http1/connection"
import router "veylor:router"
import connections "veylor:runtime/connections"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import epoll "veylor:transport/linux"

static_workspace_allocation_client :: proc(t: ^testing.T) {
	request_input: [128]u8
	headers: [4]http1.Header
	response: [128]u8
	storage: [1]Static_Connection_Workspace
	storage[0] = Static_Connection_Workspace {
		request_input = request_input[:],
		headers       = headers[:],
		response      = response[:],
	}
	free_stack: [1]u32
	pool, initialized := init_static_workspace_pool(storage[:], free_stack[:])
	testing.expect_value(t, initialized, true)

	for _ in 0 ..< 64 {
		state := Static_Connection {
			token = connections.Connection_Token {
				slot = 0,
			},
			workspace_pool = &pool,
			workspace_slot = -1,
		}
		workspace, acquired := acquire_static_workspace(&state)
		testing.expect_value(t, acquired, true)
		if acquired {
			workspace.head_driver.input_length = 17
		}
		testing.expect_value(t, release_static_workspace(&state), true)
	}
}

static_workspace_allocation_verifier :: proc(t: ^testing.T, tracker: ^mem.Tracking_Allocator) {
	testing.expect_value(t, tracker.total_allocation_count, i64(0))
	testing.expect_value(t, len(tracker.allocation_map), 0)
}

@(test)
test_warm_workspace_acquire_and_release_allocates_no_heap_memory :: proc(t: ^testing.T) {
	testing.expect_leaks(t, static_workspace_allocation_client, static_workspace_allocation_verifier)
}

@(test)
test_idle_connection_control_metadata_stays_within_256_bytes :: proc(t: ^testing.T) {
	idle_metadata_bytes := size_of(Static_Connection) +
		size_of(connections.Connection_Slot) +
		size_of(u32)
	testing.expect_value(t, idle_metadata_bytes <= 256, true)
}

@(test)
test_static_workspace_pool_reuses_bounded_caller_storage :: proc(t: ^testing.T) {
	request_input: [1]u8
	headers: [1]http1.Header
	response: [1]u8
	storage: [1]Static_Connection_Workspace
	storage[0] = Static_Connection_Workspace {
		request_input = request_input[:],
		headers       = headers[:],
		response      = response[:],
	}
	free_stack: [1]u32
	pool, initialized := init_static_workspace_pool(storage[:], free_stack[:])
	testing.expect_value(t, initialized, true)

	first := Static_Connection {
		token = connections.Connection_Token {
			slot = 0,
		},
		workspace_pool = &pool,
		workspace_slot = -1,
	}
	second := Static_Connection {
		token = connections.Connection_Token {
			slot = 1,
		},
		workspace_pool = &pool,
		workspace_slot = -1,
	}
	workspace, acquired := acquire_static_workspace(&first)
	testing.expect_value(t, acquired, true)
	testing.expect_value(t, first.workspace_slot, 0)
	testing.expect_value(t, pool.free_count, 0)
	testing.expect_value(t, pool.active_count, 1)
	testing.expect_value(t, pool.high_water_mark, 1)
	testing.expect_value(t, pool.acquisition_count, u64(1))
	if acquired {
		workspace.head_driver.input_length = 17
	}

	_, acquired = acquire_static_workspace(&second)
	testing.expect_value(t, acquired, false)
	testing.expect_value(t, second.workspace_slot, -1)
	testing.expect_value(t, pool.free_count, 0)
	testing.expect_value(t, pool.exhaustion_count, u64(1))
	testing.expect_value(t, release_static_workspace(&first), true)
	testing.expect_value(t, pool.free_count, 1)
	testing.expect_value(t, pool.active_count, 0)
	testing.expect_value(t, pool.release_count, u64(1))

	workspace, acquired = acquire_static_workspace(&second)
	testing.expect_value(t, acquired, true)
	testing.expect_value(t, second.workspace_slot, 0)
	testing.expect_value(t, pool.acquisition_count, u64(2))
	if acquired {
		testing.expect_value(t, workspace.head_driver.input_length, 0)
	}
	testing.expect_value(t, release_static_workspace(&second), true)
	testing.expect_value(t, pool.free_count, 1)
	testing.expect_value(t, pool.active_count, 0)
	testing.expect_value(t, pool.release_count, u64(2))
}

@(test)
test_full_deadline_queue_closes_request_without_blame_response :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		return
	}
	poller, poller_result, poller_error := epoll.create()
	testing.expect_value(t, poller_result, epoll.Result.Ready)
	testing.expect_value(t, poller_error, i32(0))
	if poller_result != .Ready {
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(2, 1, slots[:], free_stack[:])
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
	timers := transport.init_timer_queue(timer_storage[:])
	testing.expect_value(
		t,
		transport.schedule_timer(
			&timers,
			transport.Timer_Expiration {
				timer_id    = 99,
				deadline_ns = 1_000,
			},
		),
		transport.Timer_Queue_Result.Scheduled,
	)

	request_input: [128]u8
	request_headers: [4]http1.Header
	response_buffer: [128]u8
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
	timeouts := connections.Connection_Timeouts {
		request_head_ns = 100,
	}
	state, state_result := init_static_connection(
		Worker_Connection {
			token = token,
			fd    = sockets[0],
		},
		&workspace_pool,
	)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	_, register_error := register_static_connection(poller, &state)
	testing.expect_value(t, register_error, i32(0))

	request := "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n"
	written, write_error := linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	native_events: [1]linux.EPoll_Event
	readiness_events: [1]transport.Readiness_Event
	batch := epoll.wait(poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	testing.expect_value(t, len(batch.events), 1)

	if batch.result == .Ready && len(batch.events) == 1 {
		turn := static_connection_turn(
			poller,
			&state,
			batch.events[0],
			&routes,
			1024,
			64,
			&table,
			&timers,
			timeouts,
			1,
		)
		testing.expect_value(t, turn.result, Static_Connection_Result.Capacity_Limit)
		testing.expect_value(t, turn.bytes_read, 0)
		testing.expect_value(t, state.phase, Static_Connection_Phase.Failed)
	}

	_, unregister_error := epoll.unregister(poller, sockets[0])
	testing.expect_value(t, unregister_error, i32(0))
	state.registered = false
	_, register_error = epoll.register(poller, sockets[1], 2, epoll.Interest{readable = true})
	testing.expect_value(t, register_error, i32(0))
	response_batch := epoll.wait(poller, native_events[:], readiness_events[:], 0)
	testing.expect_value(t, response_batch.result, epoll.Result.Timeout)

	worker := Static_Worker {
		poller         = poller,
		shutdown_state = &shutdown_state,
		table          = &table,
		timers         = &timers,
	}
	close_result := close_static_worker_connection(&worker, &state)
	testing.expect_value(t, close_result, Static_Worker_Result.Ready)
	testing.expect_value(t, state.active, false)
	testing.expect_value(t, shutdown_state.active_connections, u64(0))
	testing.expect_value(t, shutdown_state.phase, shutdown.Phase.Running)
	epoll.close(&poller)
	transport.cancel_timer(&timers, 99)
	linux.close(sockets[1])
}

@(test)
test_full_workspace_pool_closes_request_without_response :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_result := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_result, linux.Errno.NONE)
	if socket_result != .NONE {
		return
	}
	poller, poller_result, _ := epoll.create()
	testing.expect_value(t, poller_result, epoll.Result.Ready)
	if poller_result != .Ready {
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	connection_slots: [1]connections.Connection_Slot
	connection_free_stack: [1]u32
	table, table_result := connections.init_connection_table(
		17,
		1,
		connection_slots[:],
		connection_free_stack[:],
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
	timer_storage: [2]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])

	request_input: [128]u8
	request_headers: [4]http1.Header
	response_buffer: [128]u8
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
	held_request := Static_Connection {
		token = connections.Connection_Token {
			slot = 1,
		},
		workspace_pool = &workspace_pool,
		workspace_slot = -1,
	}
	_, acquired := acquire_static_workspace(&held_request)
	testing.expect_value(t, acquired, true)

	state, state_result := init_static_connection(
		Worker_Connection {
			token = token,
			fd    = sockets[0],
		},
		&workspace_pool,
	)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	_, register_error := register_static_connection(poller, &state)
	testing.expect_value(t, register_error, i32(0))

	request := "GET /health HTTP/1.1\r\nHost: example.test\r\n\r\n"
	written, write_error := linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))

	native_events: [1]linux.EPoll_Event
	readiness_events: [1]transport.Readiness_Event
	batch := epoll.wait(poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	if batch.result == .Ready && len(batch.events) == 1 {
		turn := static_connection_turn(
			poller,
			&state,
			batch.events[0],
			&routes,
			1024,
			128,
			&table,
			&timers,
			connections.Connection_Timeouts {},
			100,
		)
		testing.expect_value(t, turn.result, Static_Connection_Result.Capacity_Limit)
	}
	testing.expect_value(t, state.workspace_slot, -1)
	testing.expect_value(t, workspace_pool.free_count, 0)
	testing.expect_value(t, workspace_pool.active_count, 1)
	testing.expect_value(t, workspace_pool.exhaustion_count, u64(1))

	worker := Static_Worker {
		poller = poller,
		shutdown_state = &shutdown_state,
		table = &table,
		timers = &timers,
	}
	close_result := close_static_worker_connection(&worker, &state)
	testing.expect_value(t, close_result, Static_Worker_Result.Ready)
	testing.expect_value(t, release_static_workspace(&held_request), true)
	testing.expect_value(t, workspace_pool.free_count, 1)
	testing.expect_value(t, workspace_pool.active_count, 0)
	epoll.close(&poller)
	linux.close(sockets[1])
}

@(test)
test_full_deadline_queue_closes_response_without_writing :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		return
	}
	poller, poller_result, poller_error := epoll.create()
	testing.expect_value(t, poller_result, epoll.Result.Ready)
	testing.expect_value(t, poller_error, i32(0))
	if poller_result != .Ready {
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(3, 1, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token, allocation_result := connections.allocate_connection(&table, u64(sockets[0]))
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)
	testing.expect_value(
		t,
		connections.transition_connection(&table, token, .Writing_Response),
		connections.Connection_Table_Result.Transitioned,
	)
	shutdown_state: shutdown.State
	testing.expect_value(
		t,
		shutdown.register_connection(&shutdown_state),
		shutdown.Result.Accepted,
	)

	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	testing.expect_value(
		t,
		transport.schedule_timer(
			&timers,
			transport.Timer_Expiration {
				timer_id    = 99,
				deadline_ns = 1_000,
			},
		),
		transport.Timer_Queue_Result.Scheduled,
	)

	message := "response body"
	request_input: [1]u8
	request_headers: [1]http1.Header
	response_buffer: [128]u8
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
	state := Static_Connection {
		token          = token,
		fd             = sockets[0],
		workspace_pool = &workspace_pool,
		workspace_slot = -1,
	}
	workspace, acquired := acquire_static_workspace(&state)
	testing.expect_value(t, acquired, true)
	copy(response_buffer[:len(message)], transmute([]u8)message)
	state.write_driver = connection.begin_connection_write(
		1,
		workspace.response[:len(message)],
	)
	state.phase = .Writing_Response
	state.response_length = len(message)
	state.active = true
	state.registered = true
	_, register_error := epoll.register(
		poller,
		sockets[0],
		static_connection_event_id(state.token),
		epoll.Interest{writable = true},
	)
	testing.expect_value(t, register_error, i32(0))

	native_events: [1]linux.EPoll_Event
	readiness_events: [1]transport.Readiness_Event
	batch := epoll.wait(poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	testing.expect_value(t, len(batch.events), 1)

	if batch.result == .Ready && len(batch.events) == 1 {
		turn := static_connection_turn(
			poller,
			&state,
			batch.events[0],
			&routes,
			1024,
			64,
			&table,
			&timers,
			connections.Connection_Timeouts {
				response_write_ns = 100,
			},
			1,
		)
		testing.expect_value(t, turn.result, Static_Connection_Result.Capacity_Limit)
		testing.expect_value(t, turn.bytes_written, 0)
		testing.expect_value(t, state.phase, Static_Connection_Phase.Failed)
	}

	_, unregister_error := epoll.unregister(poller, sockets[0])
	testing.expect_value(t, unregister_error, i32(0))
	state.registered = false
	_, register_error = epoll.register(poller, sockets[1], 2, epoll.Interest{readable = true})
	testing.expect_value(t, register_error, i32(0))
	response_batch := epoll.wait(poller, native_events[:], readiness_events[:], 0)
	testing.expect_value(t, response_batch.result, epoll.Result.Timeout)

	worker := Static_Worker {
		poller         = poller,
		shutdown_state = &shutdown_state,
		table          = &table,
		timers         = &timers,
	}
	close_result := close_static_worker_connection(&worker, &state)
	testing.expect_value(t, close_result, Static_Worker_Result.Ready)
	testing.expect_value(t, state.active, false)
	testing.expect_value(t, shutdown_state.active_connections, u64(0))
	epoll.close(&poller)
	transport.cancel_timer(&timers, 99)
	linux.close(sockets[1])
}

@(test)
test_response_larger_than_caller_buffer_closes_without_partial_output :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		return
	}
	poller, poller_result, poller_error := epoll.create()
	testing.expect_value(t, poller_result, epoll.Result.Ready)
	testing.expect_value(t, poller_error, i32(0))
	if poller_result != .Ready {
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	body: [96]u8
	for &byte in body {
		byte = 'x'
	}
	method := "GET"
	path := "/large"
	reason := "OK"
	route_result := router.register_static_route(
		&routes,
		transmute([]u8)method,
		transmute([]u8)path,
		router.Static_Response {
			status = 200,
			reason = transmute([]u8)reason,
			body   = body[:],
		},
	)
	testing.expect_value(t, route_result, router.Route_Table_Result.Added)
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(4, 1, slots[:], free_stack[:])
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
	timers := transport.init_timer_queue(timer_storage[:])
	request_input: [128]u8
	request_headers: [4]http1.Header
	response_buffer: [96]u8
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
	state, state_result := init_static_connection(
		Worker_Connection {
			token = token,
			fd    = sockets[0],
		},
		&workspace_pool,
	)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	_, register_error := register_static_connection(poller, &state)
	testing.expect_value(t, register_error, i32(0))

	request := "GET /large HTTP/1.1\r\nHost: example.test\r\n\r\n"
	written, write_error := linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	native_events: [1]linux.EPoll_Event
	readiness_events: [1]transport.Readiness_Event
	batch := epoll.wait(poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	testing.expect_value(t, len(batch.events), 1)

	timeouts := connections.Connection_Timeouts {
		request_head_ns = 100,
	}
	if batch.result == .Ready && len(batch.events) == 1 {
		turn := static_connection_turn(
			poller,
			&state,
			batch.events[0],
			&routes,
			1024,
			64,
			&table,
			&timers,
			timeouts,
			1,
		)
		testing.expect_value(t, turn.result, Static_Connection_Result.Response_Buffer_Small)
		testing.expect_value(t, state.phase, Static_Connection_Phase.Failed)
	}

	worker := Static_Worker {
		poller         = poller,
		shutdown_state = &shutdown_state,
		table          = &table,
		timers         = &timers,
	}
	close_result := close_static_worker_connection(&worker, &state)
	testing.expect_value(t, close_result, Static_Worker_Result.Ready)
	testing.expect_value(t, state.active, false)
	testing.expect_value(t, shutdown_state.active_connections, u64(0))
	response: [128]u8
	response_length, read_error := linux.read(sockets[1], response[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(t, response_length, 0)
	epoll.close(&poller)
	linux.close(sockets[1])
}
