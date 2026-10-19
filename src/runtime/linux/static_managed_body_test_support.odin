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
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

Managed_Body_Fixture :: struct {
	sockets:              [2]linux.Fd,
	poller:               epoll.Epoll,
	routes:               router.Route_Table,
	route_storage:        [2]router.Static_Route,
	route_index_storage:  [4]router.Route_Index_Slot,
	table:                connections.Connection_Table,
	connection_slots:     [1]connections.Connection_Slot,
	free_stack:           [1]u32,
	timers:               transport.Timer_Queue,
	timer_storage:        [2]transport.Timer_Expiration,
	timer_index_storage:  [2 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32,
	shutdown_state:       shutdown.State,
	workspace_storage:    [1]Static_Connection_Workspace,
	workspace_free_stack: [1]u32,
	request_input:        [256]u8,
	headers:              [4]http1.Header,
	response_output:      [256]u8,
	workspace_pool:       Static_Workspace_Pool,
	host_storage:         [4]host.Host_Event,
	host_queue:           host.Host_Queue,
	command_storage:      [4]host.Host_Command,
	command_queue:        host.Host_Command_Queue,
	worker_states:        [1]Static_Connection,
	worker:               Static_Worker,
	token:                connections.Connection_Token,
}

setup_managed_body_fixture :: proc(
	t: ^testing.T,
	fixture: ^Managed_Body_Fixture,
	method: string,
	path: string,
	request: string,
	body_limit: u64,
	trailers: []string = nil,
) -> bool {
	socket_result := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &fixture.sockets)
	testing.expect_value(t, socket_result, linux.Errno.NONE)
	if socket_result != .NONE {
		return false
	}

	poller, poll_result, _ := epoll.create()
	testing.expect_value(t, poll_result, epoll.Result.Ready)
	if poll_result != .Ready {
		return false
	}
	fixture.poller = poller

	fixture.routes = router.init_route_table(
		fixture.route_storage[:],
		fixture.route_index_storage[:],
	)
	if len(trailers) == 0 {
		testing.expect_value(
			t,
			router.register_managed_route(
				&fixture.routes,
				transmute([]u8)method,
				transmute([]u8)path,
			),
			router.Route_Table_Result.Added,
		)
	} else {
		names := make([][]u8, len(trailers), context.temp_allocator)
		for allowed, i in trailers {
			names[i] = transmute([]u8)allowed
		}
		testing.expect_value(
			t,
			router.register_managed_route_with_trailers(
				&fixture.routes,
				transmute([]u8)method,
				transmute([]u8)path,
				names,
			),
			router.Route_Table_Result.Added,
		)
	}
	testing.expect_value(
		t,
		router.freeze_route_table(&fixture.routes),
		router.Route_Table_Result.Frozen,
	)

	table, table_result := connections.init_connection_table(
		23,
		1,
		fixture.connection_slots[:],
		fixture.free_stack[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	fixture.table = table
	token, allocation_result := connections.allocate_connection(
		&fixture.table,
		u64(fixture.sockets[0]),
	)
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)
	fixture.token = token

	fixture.timers = transport.init_timer_queue(
		fixture.timer_storage[:],
		fixture.timer_index_storage[:],
	)
	testing.expect_value(
		t,
		shutdown.register_connection(&fixture.shutdown_state),
		shutdown.Result.Accepted,
	)

	fixture.workspace_storage[0] = Static_Connection_Workspace {
		request_input = fixture.request_input[:],
		headers       = fixture.headers[:],
		response      = fixture.response_output[:],
	}
	workspace_pool, pool_ready := init_static_workspace_pool(
		fixture.workspace_storage[:],
		fixture.workspace_free_stack[:],
	)
	testing.expect_value(t, pool_ready, true)
	fixture.workspace_pool = workspace_pool

	fixture.host_queue = host.init_host_queue(fixture.host_storage[:])
	fixture.command_queue = host.init_host_command_queue(fixture.command_storage[:])

	worker_connection := Worker_Connection {
		token = token,
		fd    = fixture.sockets[0],
	}
	state, state_result := init_static_connection(
		worker_connection,
		&fixture.workspace_pool,
		nil,
		nil,
		&fixture.host_queue,
	)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	register_result, _ := register_static_connection(fixture.poller, &state)
	testing.expect_value(t, register_result, epoll.Result.Ready)
	fixture.worker_states[0] = state

	write_length, write_error := linux.write(fixture.sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(request))

	native_events: [4]linux.EPoll_Event
	readiness_events: [4]transport.Readiness_Event
	batch := epoll.wait(fixture.poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	if batch.result != .Ready || len(batch.events) == 0 {
		return false
	}
	turn := static_connection_turn(
		fixture.poller,
		&fixture.worker_states[0],
		batch.events[0],
		&fixture.routes,
		body_limit,
		128,
		&fixture.table,
		&fixture.timers,
		MANAGED_TEST_TIMEOUTS,
		1000,
	)
	testing.expect_value(t, turn.result, Static_Connection_Result.Need_More)
	testing.expect_value(t, fixture.worker_states[0].phase, Static_Connection_Phase.Awaiting_Host)
	if turn.result != .Need_More {
		return false
	}

	fixture.worker = Static_Worker {
		table             = &fixture.table,
		timers            = &fixture.timers,
		timeouts          = MANAGED_TEST_TIMEOUTS,
		host_queue        = &fixture.host_queue,
		command_queue     = &fixture.command_queue,
		connection_states = fixture.worker_states[:],
		event_budget      = 8,
		byte_budget       = 4096,
	}
	return true
}

close_managed_body_fixture :: proc(fixture: ^Managed_Body_Fixture) {
	epoll.close(&fixture.poller)
	linux.close(fixture.sockets[0])
	linux.close(fixture.sockets[1])
}
