#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import "veylor:transport"
import epoll "veylor:transport/linux"

@(test)
test_park_fails_closed_when_host_queue_full :: proc(t: ^testing.T) {
	route_storage: [2]router.Static_Route
	index_storage: [4]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], index_storage[:])
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

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(1, 1, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token, allocation_result := connections.allocate_connection(&table, 7)
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)

	head_storage: [4]http1.Header
	response_storage: [1024]u8
	request_text := "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5\r\n\r\nhello"
	workspace := Static_Connection_Workspace {
		request_input = transmute([]u8)request_text,
		headers       = head_storage[:],
		response      = response_storage[:],
	}
	parser: http1.Parser
	head, parse_result := http1.parse_request_head(
		&parser,
		workspace.request_input,
		workspace.headers,
	)
	testing.expect_value(t, parse_result, http1.Parse_Result.Ready)

	event_storage: [2]host.Host_Event
	queue := host.init_host_queue(event_storage[:])
	timer_storage: [2]transport.Timer_Expiration
	timer_index_storage: [2 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timer_queue := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	dummy := connections.Connection_Token{}
	testing.expect_value(
		t,
		host.enqueue_request(
			&queue,
			dummy,
			9,
			transmute([]u8)string("GET"),
			transmute([]u8)string("/x"),
		),
		host.Host_Queue_Result.Enqueued,
	)
	testing.expect_value(
		t,
		host.enqueue_request(
			&queue,
			dummy,
			9,
			transmute([]u8)string("GET"),
			transmute([]u8)string("/x"),
		),
		host.Host_Queue_Result.Enqueued,
	)
	testing.expect_value(t, host.pending_host_events(&queue), 2)

	poller, poller_result, _ := epoll.create()
	testing.expect_value(t, poller_result, epoll.Result.Ready)

	if poller_result != .Ready {
		return
	}

	defer epoll.close(&poller)

	state := Static_Connection {
		token      = token,
		fd         = 7,
		host_queue = &queue,
	}
	turn := park_static_connection_for_host(
		poller,
		&state,
		head,
		&routes,
		&workspace,
		len(request_text),
		len(request_text),
		16 * 1024 * 1024,
		&table,
		&timer_queue,
		MANAGED_TEST_TIMEOUTS,
		1000,
	)
	testing.expect_value(t, turn.result, Static_Connection_Result.Capacity_Limit)
	testing.expect_value(t, state.phase, Static_Connection_Phase.Failed)
	testing.expect_value(t, state.close_reason, Static_Connection_Close_Reason.Host_Queue_Full)

	drained: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&queue, drained[:]).copied, 2)
	testing.expect_value(t, host.pending_host_events(&queue), 0)

	state.phase = Static_Connection_Phase.Reading_Request_Head
	state.close_reason = .Unknown

	turn = park_static_connection_for_host(
		poller,
		&state,
		head,
		&routes,
		&workspace,
		len(request_text),
		len(request_text),
		16 * 1024 * 1024,
		&table,
		&timer_queue,
		MANAGED_TEST_TIMEOUTS,
		1000,
	)
	testing.expect_value(t, turn.result, Static_Connection_Result.Need_More)
	testing.expect_value(t, state.phase, Static_Connection_Phase.Awaiting_Host)
	testing.expect_value(t, host.pending_host_events(&queue), 1)
}

@(test)
test_parked_request_expires_on_body_deadline :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 10\r\n\r\nhello"

	if !setup_managed_body_fixture(t, &fixture, "POST", "/upload", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)

		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	expired_storage: [4]transport.Timer_Expiration
	expired := expire_worker_timers_at(&fixture.timers, expired_storage[:], 1000 + 11_000_000_000)
	testing.expect_value(t, len(expired), 1)

	turn := Static_Worker_Turn{}
	process_static_worker_expirations(&fixture.worker, expired, &turn)
	testing.expect_value(t, fixture.worker_states[0].active, false)
	testing.expect_value(t, fixture.worker.metrics.closed_connections, u64(1))
	close_managed_body_fixture(&fixture)
}

@(test)
test_pipelined_second_request_served_after_keep_alive :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "GET /managed HTTP/1.1\r\nHost: example.test\r\n\r\nGET /managed HTTP/1.1\r\nHost: example.test\r\n\r\n"
	if !setup_managed_body_fixture(t, &fixture, "GET", "/managed", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)
		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	first_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	drain_managed_stream_command(
		t,
		&fixture,
		make_managed_response_command(first_token, 200, "OK", "first"),
	)
	turn := drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Need_More)
	testing.expect_value(t, fixture.worker_states[0].phase, Static_Connection_Phase.Awaiting_Host)
	notified := host.poll_host_queue(&fixture.host_queue, polled[:])
	testing.expect_value(t, notified.copied, 1)
	testing.expect_value(t, polled[0].kind, host.Host_Event_Kind.Request)
	second_token := polled[0].token

	response_buffer: [256]u8
	testing.expect_value(
		t,
		read_managed_socket_text(t, &fixture, response_buffer[:]),
		"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst",
	)

	drain_managed_stream_command(
		t,
		&fixture,
		make_managed_response_command(second_token, 200, "OK", "second"),
	)
	turn = drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Keep_Alive)
	testing.expect_value(
		t,
		fixture.worker_states[0].phase,
		Static_Connection_Phase.Keep_Alive_Idle,
	)

	second_buffer: [256]u8
	testing.expect_value(
		t,
		read_managed_socket_text(t, &fixture, second_buffer[:]),
		"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecond",
	)
	close_managed_body_fixture(&fixture)
}
