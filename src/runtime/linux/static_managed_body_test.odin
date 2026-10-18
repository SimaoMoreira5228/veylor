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
	testing.expect_value(
		t,
		router.register_managed_route(&fixture.routes, transmute([]u8)method, transmute([]u8)path),
		router.Route_Table_Result.Added,
	)
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

@(test)
test_managed_read_command_stages_partial_body_and_notifies :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 10\r\n\r\nhello"
	if !setup_managed_body_fixture(t, &fixture, "POST", "/upload", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)
		return
	}
	testing.expect_value(t, host.pending_host_events(&fixture.host_queue), 1)
	polled: [4]host.Host_Event
	poll := host.poll_host_queue(&fixture.host_queue, polled[:])
	testing.expect_value(t, poll.copied, 1)

	cmd := host.Host_Command {
		kind      = .Read_Body,
		token     = fixture.token,
		max_bytes = 1024,
	}
	testing.expect_value(
		t,
		host.enqueue_host_command(&fixture.command_queue, cmd),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 2000)
	testing.expect_value(t, host.pending_host_events(&fixture.host_queue), 1)

	notified := host.poll_host_queue(&fixture.host_queue, polled[:])
	testing.expect_value(t, notified.copied, 1)
	testing.expect_value(t, polled[0].kind, host.Host_Event_Kind.Body_Available)
	testing.expect_value(t, polled[0].token, fixture.token)
	testing.expect_value(t, polled[0].body_ready, u32(5))
	testing.expect_value(t, fixture.workspace_storage[0].staged_length, 5)
	testing.expect_value(
		t,
		string(
			fixture.workspace_storage[0].staged_body[:fixture.workspace_storage[0].staged_length],
		),
		"hello",
	)
	phase, phase_result := connections.connection_phase(&fixture.table, fixture.token)
	testing.expect_value(t, phase_result, connections.Connection_Table_Result.Found)
	testing.expect_value(t, phase, connections.Connection_Phase.Reading_Request_Body)
	testing.expect_value(
		t,
		fixture.table.slots[int(fixture.token.slot)].deadline_timer_id != 0,
		true,
	)
	close_managed_body_fixture(&fixture)
}

@(test)
test_managed_read_completes_body_and_returns_to_awaiting_handler :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 10\r\n\r\nhelloworld"
	if !setup_managed_body_fixture(t, &fixture, "POST", "/upload", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)
		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)

	cmd := host.Host_Command {
		kind      = .Read_Body,
		token     = fixture.token,
		max_bytes = 1024,
	}
	testing.expect_value(
		t,
		host.enqueue_host_command(&fixture.command_queue, cmd),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 2000)

	notified := host.poll_host_queue(&fixture.host_queue, polled[:])
	testing.expect_value(t, notified.copied, 1)
	testing.expect_value(t, polled[0].kind, host.Host_Event_Kind.Body_Available)
	testing.expect_value(t, polled[0].body_ready, u32(10))
	testing.expect_value(t, fixture.workspace_storage[0].staged_length, 10)
	testing.expect_value(
		t,
		string(
			fixture.workspace_storage[0].staged_body[:fixture.workspace_storage[0].staged_length],
		),
		"helloworld",
	)
	phase, phase_result := connections.connection_phase(&fixture.table, fixture.token)
	testing.expect_value(t, phase_result, connections.Connection_Table_Result.Found)
	testing.expect_value(t, phase, connections.Connection_Phase.Awaiting_Handler)
	testing.expect_value(t, host.pending_host_events(&fixture.host_queue), 0)

	late := host.Host_Command {
		kind      = .Read_Body,
		token     = fixture.token,
		max_bytes = 1024,
	}
	testing.expect_value(
		t,
		host.enqueue_host_command(&fixture.command_queue, late),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 3000)
	testing.expect_value(t, host.pending_host_events(&fixture.host_queue), 0)
	close_managed_body_fixture(&fixture)
}

@(test)
test_managed_read_command_with_stale_token_is_dropped :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 10\r\n\r\nhello"
	if !setup_managed_body_fixture(t, &fixture, "POST", "/upload", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)
		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)

	stale_token := fixture.token
	stale_token.generation += 1
	cmd := host.Host_Command {
		kind      = .Read_Body,
		token     = stale_token,
		max_bytes = 1024,
	}
	testing.expect_value(
		t,
		host.enqueue_host_command(&fixture.command_queue, cmd),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 2000)
	testing.expect_value(t, host.pending_host_events(&fixture.host_queue), 0)
	testing.expect_value(t, fixture.workspace_storage[0].staged_length, 0)
	phase, phase_result := connections.connection_phase(&fixture.table, fixture.token)
	testing.expect_value(t, phase_result, connections.Connection_Table_Result.Found)
	testing.expect_value(t, phase, connections.Connection_Phase.Reading_Request_Body)
	close_managed_body_fixture(&fixture)
}

@(test)
test_managed_read_command_closes_connection_when_event_queue_is_full :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 10\r\n\r\nhello"
	if !setup_managed_body_fixture(t, &fixture, "POST", "/upload", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)
		return
	}
	filler := host.Host_Event {
		token    = fixture.token,
		route_id = 1,
		kind     = .Body_Available,
	}
	filler.method[0] = 'G'
	filler.method_length = 1
	filler.path[0] = '/'
	filler.path_length = 1
	for host.pending_host_events(&fixture.host_queue) < 4 {
		testing.expect_value(
			t,
			host.enqueue_body_available(&fixture.host_queue, filler),
			host.Host_Queue_Result.Enqueued,
		)
	}
	testing.expect_value(t, host.pending_host_events(&fixture.host_queue), 4)

	cmd := host.Host_Command {
		kind      = .Read_Body,
		token     = fixture.token,
		max_bytes = 1024,
	}
	testing.expect_value(
		t,
		host.enqueue_host_command(&fixture.command_queue, cmd),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 2000)
	testing.expect_value(t, fixture.worker_states[0].phase, Static_Connection_Phase.Failed)
	testing.expect_value(
		t,
		fixture.worker_states[0].close_reason,
		Static_Connection_Close_Reason.Host_Queue_Full,
	)
	close_managed_body_fixture(&fixture)
}
