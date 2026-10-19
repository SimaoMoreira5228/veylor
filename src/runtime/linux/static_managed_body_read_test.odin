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
	request_token := polled[0].token

	cmd := host.Host_Command {
		kind      = .Read_Body,
		token     = request_token,
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
	testing.expect_value(t, polled[0].token, request_token)
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
	request_token := polled[0].token

	cmd := host.Host_Command {
		kind      = .Read_Body,
		token     = request_token,
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
		token     = request_token,
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
	request_token := polled[0].token

	stale_token := request_token
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
	front, front_ok := host.peek_host_queue(&fixture.host_queue)
	testing.expect(t, front_ok)
	request_token := front.token

	cmd := host.Host_Command {
		kind      = .Read_Body,
		token     = request_token,
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

make_managed_response_command :: proc(
	token: connections.Connection_Token,
	status: i32,
	reason: string,
	body: string,
) -> host.Host_Command {
	cmd := host.Host_Command {
		kind   = .Response_Start,
		token  = token,
		status = status,
	}
	copy(cmd.reason[:len(reason)], transmute([]u8)reason)
	cmd.reason_length = u32(len(reason))
	copy(cmd.body[:len(body)], transmute([]u8)body)
	cmd.body_length = u32(len(body))
	return cmd
}

drive_managed_write_turn :: proc(
	t: ^testing.T,
	fixture: ^Managed_Body_Fixture,
) -> Static_Connection_Turn {
	readiness := transport.Readiness_Event {
		connection_id = static_connection_event_id(fixture.token),
		writable      = true,
	}

	return static_connection_turn(
		fixture.poller,
		&fixture.worker_states[0],
		readiness,
		&fixture.routes,
		16 * 1024 * 1024,
		128,
		&fixture.table,
		&fixture.timers,
		MANAGED_TEST_TIMEOUTS,
		2000,
	)
}

read_managed_socket_text :: proc(
	t: ^testing.T,
	fixture: ^Managed_Body_Fixture,
	buffer: []u8,
) -> string {
	read_length, read_error := linux.read(fixture.sockets[1], buffer)
	testing.expect_value(t, read_error, linux.Errno.NONE)
	if read_error != .NONE {
		return ""
	}
	return string(buffer[:read_length])
}
