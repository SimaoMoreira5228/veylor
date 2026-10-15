#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import connection "veylor:http1/connection"
import router "veylor:router"
import connections "veylor:runtime/connections"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import epoll "veylor:transport/linux"

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
	timeouts := connections.Connection_Timeouts {
		request_head_ns = 100,
	}
	state, state_result := init_static_connection(
		Worker_Connection {
			token = token,
			fd    = sockets[0],
		},
		request_input[:],
		request_headers[:],
		response_buffer[:],
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
	response_buffer: [128]u8
	copy(response_buffer[:len(message)], transmute([]u8)message)
	state := Static_Connection {
		worker = Worker_Connection {
			token = token,
			fd    = sockets[0],
		},
		event_id        = 1,
		write_driver    = connection.begin_connection_write(1, response_buffer[:len(message)]),
		response        = response_buffer[:],
		phase           = .Writing_Response,
		response_length = len(message),
		active          = true,
		registered      = true,
	}
	_, register_error := epoll.register(
		poller,
		sockets[0],
		state.event_id,
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
