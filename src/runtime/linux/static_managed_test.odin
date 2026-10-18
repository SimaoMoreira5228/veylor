#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import connection "veylor:http1/connection"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

MANAGED_TEST_TIMEOUTS :: connections.Connection_Timeouts {
	request_head_ns    = 10_000_000_000,
	body_progress_ns   = 10_000_000_000,
	response_write_ns  = 10_000_000_000,
	keep_alive_idle_ns = 5_000_000_000,
}

@(test)
test_managed_route_parks_connection_and_queues_request_handle :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_result := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_result, linux.Errno.NONE)
	if socket_result != .NONE {
		return
	}
	defer linux.close(sockets[0])
	defer linux.close(sockets[1])

	poller, poll_result, _ := epoll.create()
	testing.expect_value(t, poll_result, epoll.Result.Ready)
	if poll_result != .Ready {
		return
	}
	defer epoll.close(&poller)

	route_storage: [2]router.Static_Route
	route_index_storage: [4]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	testing.expect_value(
		t,
		router.register_managed_route(
			&routes,
			transmute([]u8)string("GET"),
			transmute([]u8)string("/managed"),
		),
		router.Route_Table_Result.Added,
	)
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	connection_slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(
		21,
		1,
		connection_slots[:],
		free_stack[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token, allocation_result := connections.allocate_connection(&table, u64(sockets[0]))
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)

	timer_storage: [2]transport.Timer_Expiration
	timer_index_storage: [2 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	shutdown_state: shutdown.State
	testing.expect_value(
		t,
		shutdown.register_connection(&shutdown_state),
		shutdown.Result.Accepted,
	)

	request_input: [128]u8
	request_headers: [4]http1.Header
	response_output: [256]u8
	workspace_storage: [1]Static_Connection_Workspace
	workspace_storage[0] = Static_Connection_Workspace {
		request_input = request_input[:],
		headers       = request_headers[:],
		response      = response_output[:],
	}
	workspace_free_stack: [1]u32
	workspace_pool, pool_ready := init_static_workspace_pool(
		workspace_storage[:],
		workspace_free_stack[:],
	)
	testing.expect_value(t, pool_ready, true)

	host_storage: [4]host.Host_Event
	queue := host.init_host_queue(host_storage[:])
	worker := Worker_Connection {
		token = token,
		fd    = sockets[0],
	}
	state, state_result := init_static_connection(worker, &workspace_pool, nil, nil, &queue)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	register_result, _ := register_static_connection(poller, &state)
	testing.expect_value(t, register_result, epoll.Result.Ready)

	request := "GET /managed HTTP/1.1\r\nHost: example.test\r\n\r\n"
	write_length, write_error := linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(request))

	native_events: [4]linux.EPoll_Event
	readiness_events: [4]transport.Readiness_Event
	batch := epoll.wait(poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	if batch.result != .Ready || len(batch.events) == 0 {
		return
	}
	turn := static_connection_turn(
		poller,
		&state,
		batch.events[0],
		&routes,
		16 * 1024 * 1024,
		128,
		&table,
		&timers,
		MANAGED_TEST_TIMEOUTS,
		1000,
	)
	testing.expect_value(t, turn.result, Static_Connection_Result.Need_More)
	testing.expect_value(t, state.phase, Static_Connection_Phase.Awaiting_Host)
	testing.expect_value(t, host.pending_host_events(&queue), 1)

	phase, phase_result := connections.connection_phase(&table, token)
	testing.expect_value(t, phase_result, connections.Connection_Table_Result.Found)
	testing.expect_value(t, phase, connections.Connection_Phase.Awaiting_Handler)

	polled: [4]host.Host_Event
	poll := host.poll_host_queue(&queue, polled[:])
	testing.expect_value(t, poll.copied, 1)
	testing.expect_value(t, poll.required, 1)
	testing.expect_value(t, polled[0].token, token)
	testing.expect_value(t, polled[0].route_id, u64(1))
	testing.expect_value(t, polled[0].kind, host.Host_Event_Kind.Request)
	testing.expect_value(t, string(polled[0].method[:polled[0].method_length]), "GET")
	testing.expect_value(t, string(polled[0].path[:polled[0].path_length]), "/managed")

	parked_readiness := transport.Readiness_Event {
		connection_id = static_connection_event_id(token),
	}
	parked_turn := static_connection_turn(
		poller,
		&state,
		parked_readiness,
		&routes,
		16 * 1024 * 1024,
		128,
		&table,
		&timers,
		MANAGED_TEST_TIMEOUTS,
		2000,
	)
	testing.expect_value(t, parked_turn.result, Static_Connection_Result.Need_More)
	testing.expect_value(t, state.phase, Static_Connection_Phase.Awaiting_Host)
	testing.expect_value(t, host.pending_host_events(&queue), 0)
}

@(test)
test_managed_route_rejects_oversized_body_without_queuing :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_result := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_result, linux.Errno.NONE)
	if socket_result != .NONE {
		return
	}
	defer linux.close(sockets[0])
	defer linux.close(sockets[1])

	poller, poll_result, _ := epoll.create()
	testing.expect_value(t, poll_result, epoll.Result.Ready)
	if poll_result != .Ready {
		return
	}
	defer epoll.close(&poller)

	route_storage: [2]router.Static_Route
	route_index_storage: [4]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	testing.expect_value(
		t,
		router.register_managed_route(
			&routes,
			transmute([]u8)string("POST"),
			transmute([]u8)string("/upload"),
		),
		router.Route_Table_Result.Added,
	)
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	connection_slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(
		22,
		1,
		connection_slots[:],
		free_stack[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token, allocation_result := connections.allocate_connection(&table, u64(sockets[0]))
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)

	timer_storage: [2]transport.Timer_Expiration
	timer_index_storage: [2 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	shutdown_state: shutdown.State
	testing.expect_value(
		t,
		shutdown.register_connection(&shutdown_state),
		shutdown.Result.Accepted,
	)

	request_input: [128]u8
	request_headers: [4]http1.Header
	response_output: [256]u8
	workspace_storage: [1]Static_Connection_Workspace
	workspace_storage[0] = Static_Connection_Workspace {
		request_input = request_input[:],
		headers       = request_headers[:],
		response      = response_output[:],
	}
	workspace_free_stack: [1]u32
	workspace_pool, pool_ready := init_static_workspace_pool(
		workspace_storage[:],
		workspace_free_stack[:],
	)
	testing.expect_value(t, pool_ready, true)

	host_storage: [4]host.Host_Event
	queue := host.init_host_queue(host_storage[:])
	worker := Worker_Connection {
		token = token,
		fd    = sockets[0],
	}
	state, state_result := init_static_connection(worker, &workspace_pool, nil, nil, &queue)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	register_result, _ := register_static_connection(poller, &state)
	testing.expect_value(t, register_result, epoll.Result.Ready)

	request := "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 999999\r\n\r\n"
	write_length, write_error := linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(request))

	native_events: [4]linux.EPoll_Event
	readiness_events: [4]transport.Readiness_Event
	batch := epoll.wait(poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	if batch.result != .Ready || len(batch.events) == 0 {
		return
	}
	turn := static_connection_turn(
		poller,
		&state,
		batch.events[0],
		&routes,
		16,
		128,
		&table,
		&timers,
		MANAGED_TEST_TIMEOUTS,
		1000,
	)
	testing.expect_value(t, turn.result, Static_Connection_Result.Response_Progress)
	testing.expect_value(t, state.close_reason, Static_Connection_Close_Reason.Request_Rejected)
	testing.expect_value(t, host.pending_host_events(&queue), 0)
	testing.expect_value(
		t,
		string(response_output[:state.response_length]),
		"HTTP/1.1 413 Content Too Large\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
	)
}

@(test)
test_managed_route_with_body_retains_workspace_without_armed_deadline :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_result := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_result, linux.Errno.NONE)
	if socket_result != .NONE {
		return
	}
	defer linux.close(sockets[0])
	defer linux.close(sockets[1])

	poller, poll_result, _ := epoll.create()
	testing.expect_value(t, poll_result, epoll.Result.Ready)
	if poll_result != .Ready {
		return
	}
	defer epoll.close(&poller)

	route_storage: [2]router.Static_Route
	route_index_storage: [4]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	testing.expect_value(
		t,
		router.register_managed_route(
			&routes,
			transmute([]u8)string("POST"),
			transmute([]u8)string("/upload"),
		),
		router.Route_Table_Result.Added,
	)
	testing.expect_value(t, router.freeze_route_table(&routes), router.Route_Table_Result.Frozen)

	connection_slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(
		22,
		1,
		connection_slots[:],
		free_stack[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token, allocation_result := connections.allocate_connection(&table, u64(sockets[0]))
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)

	timer_storage: [2]transport.Timer_Expiration
	timer_index_storage: [2 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	shutdown_state: shutdown.State
	testing.expect_value(
		t,
		shutdown.register_connection(&shutdown_state),
		shutdown.Result.Accepted,
	)

	request_input: [256]u8
	request_headers: [4]http1.Header
	response_output: [256]u8
	workspace_storage: [1]Static_Connection_Workspace
	workspace_storage[0] = Static_Connection_Workspace {
		request_input = request_input[:],
		headers       = request_headers[:],
		response      = response_output[:],
	}
	workspace_free_stack: [1]u32
	workspace_pool, pool_ready := init_static_workspace_pool(
		workspace_storage[:],
		workspace_free_stack[:],
	)
	testing.expect_value(t, pool_ready, true)

	host_storage: [4]host.Host_Event
	queue := host.init_host_queue(host_storage[:])
	worker := Worker_Connection {
		token = token,
		fd    = sockets[0],
	}
	state, state_result := init_static_connection(worker, &workspace_pool, nil, nil, &queue)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	register_result, _ := register_static_connection(poller, &state)
	testing.expect_value(t, register_result, epoll.Result.Ready)

	request := "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 10\r\n\r\nhelloworld"
	write_length, write_error := linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(request))

	native_events: [4]linux.EPoll_Event
	readiness_events: [4]transport.Readiness_Event
	batch := epoll.wait(poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	if batch.result != .Ready || len(batch.events) == 0 {
		return
	}
	turn := static_connection_turn(
		poller,
		&state,
		batch.events[0],
		&routes,
		16 * 1024 * 1024,
		128,
		&table,
		&timers,
		MANAGED_TEST_TIMEOUTS,
		1000,
	)
	testing.expect_value(t, turn.result, Static_Connection_Result.Need_More)
	testing.expect_value(t, state.phase, Static_Connection_Phase.Awaiting_Host)
	testing.expect_value(t, state.workspace_slot != -1, true)
	testing.expect_value(
		t,
		workspace_storage[0].body_driver.state,
		connection.Connection_Body_State.Reading,
	)
	testing.expect_value(
		t,
		workspace_storage[0].body_valid >= workspace_storage[0].body_start,
		true,
	)
	testing.expect_value(t, workspace_storage[0].body_start > 0, true)
	phase, phase_result := connections.connection_phase(&table, token)
	testing.expect_value(t, phase_result, connections.Connection_Table_Result.Found)
	testing.expect_value(t, phase, connections.Connection_Phase.Reading_Request_Body)
	testing.expect_value(t, table.slots[int(token.slot)].deadline_timer_id, u64(0))
	testing.expect_value(t, host.pending_host_events(&queue), 1)
}
