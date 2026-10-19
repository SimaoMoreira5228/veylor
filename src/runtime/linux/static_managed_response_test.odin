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
test_managed_response_start_serves_keep_alive_get :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "GET /managed HTTP/1.1\r\nHost: example.test\r\n\r\n"
	if !setup_managed_body_fixture(t, &fixture, "GET", "/managed", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)
		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	request_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	cmd := make_managed_response_command(request_token, 200, "OK", "hello")
	testing.expect_value(
		t,
		host.enqueue_host_command(&fixture.command_queue, cmd),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 2000)
	testing.expect_value(
		t,
		fixture.worker_states[0].phase,
		Static_Connection_Phase.Writing_Response,
	)

	turn := drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Keep_Alive)

	response_buffer: [256]u8
	testing.expect_value(
		t,
		read_managed_socket_text(t, &fixture, response_buffer[:]),
		"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
	)

	second_request := "GET /managed HTTP/1.1\r\nHost: example.test\r\n\r\n"
	write_length, write_error := linux.write(fixture.sockets[1], transmute([]u8)second_request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(second_request))

	native_events: [4]linux.EPoll_Event
	readiness_events: [4]transport.Readiness_Event
	batch := epoll.wait(fixture.poller, native_events[:], readiness_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	if batch.result != .Ready || len(batch.events) == 0 {
		close_managed_body_fixture(&fixture)
		return
	}
	second_turn := static_connection_turn(
		fixture.poller,
		&fixture.worker_states[0],
		batch.events[0],
		&fixture.routes,
		16 * 1024 * 1024,
		128,
		&fixture.table,
		&fixture.timers,
		MANAGED_TEST_TIMEOUTS,
		3000,
	)
	testing.expect_value(t, second_turn.result, Static_Connection_Result.Need_More)
	testing.expect_value(t, fixture.worker_states[0].phase, Static_Connection_Phase.Awaiting_Host)
	close_managed_body_fixture(&fixture)
}

@(test)
test_managed_response_start_closes_with_unread_body :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 10\r\n\r\nhello"
	if !setup_managed_body_fixture(t, &fixture, "POST", "/upload", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)
		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	request_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	cmd := make_managed_response_command(request_token, 200, "OK", "done")
	testing.expect_value(
		t,
		host.enqueue_host_command(&fixture.command_queue, cmd),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 2000)
	testing.expect_value(
		t,
		fixture.worker_states[0].phase,
		Static_Connection_Phase.Writing_Response,
	)

	turn := drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Closed)

	response_buffer: [256]u8
	testing.expect_value(
		t,
		read_managed_socket_text(t, &fixture, response_buffer[:]),
		"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\ndone",
	)
	close_managed_body_fixture(&fixture)
}

make_managed_cancel_command :: proc(token: connections.Connection_Token) -> host.Host_Command {
	return host.Host_Command{kind = .Cancel, token = token}
}

@(test)
test_managed_cancel_closes_parked_connection :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "GET /managed HTTP/1.1\r\nHost: example.test\r\n\r\n"
	if !setup_managed_body_fixture(t, &fixture, "GET", "/managed", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)
		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	request_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	testing.expect_value(
		t,
		host.enqueue_host_command(
			&fixture.command_queue,
			make_managed_cancel_command(request_token),
		),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 2000)
	testing.expect_value(t, fixture.worker_states[0].active, false)
	testing.expect_value(t, fixture.worker.metrics.closed_connections, u64(1))
	testing.expect_value(
		t,
		fixture.worker.metrics.close_reasons[int(Static_Connection_Close_Reason.Cancelled)],
		u64(1),
	)
	testing.expect_value(t, host.pending_host_events(&fixture.host_queue), 0)
	// NOTE: the worker already closed sockets[0]; closing it again could
	// release an fd number reused by a parallel test.
	epoll.close(&fixture.poller)
	linux.close(fixture.sockets[1])
}

@(test)
test_managed_cancel_with_stale_token_is_dropped :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "GET /managed HTTP/1.1\r\nHost: example.test\r\n\r\n"
	if !setup_managed_body_fixture(t, &fixture, "GET", "/managed", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)
		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	request_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	stale_token := request_token
	stale_token.generation += 1
	testing.expect_value(
		t,
		host.enqueue_host_command(
			&fixture.command_queue,
			make_managed_cancel_command(stale_token),
		),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 2000)
	testing.expect_value(t, fixture.worker_states[0].active, true)
	testing.expect_value(t, fixture.worker_states[0].phase, Static_Connection_Phase.Awaiting_Host)
	testing.expect_value(t, fixture.worker.metrics.closed_connections, u64(0))
	close_managed_body_fixture(&fixture)
}

@(test)
test_managed_cancel_during_response_write_is_dropped :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "GET /managed HTTP/1.1\r\nHost: example.test\r\n\r\n"
	if !setup_managed_body_fixture(t, &fixture, "GET", "/managed", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)
		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	request_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	respond := make_managed_response_command(request_token, 200, "OK", "hello")
	testing.expect_value(
		t,
		host.enqueue_host_command(&fixture.command_queue, respond),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 2000)
	testing.expect_value(
		t,
		fixture.worker_states[0].phase,
		Static_Connection_Phase.Writing_Response,
	)

	testing.expect_value(
		t,
		host.enqueue_host_command(
			&fixture.command_queue,
			make_managed_cancel_command(request_token),
		),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 3000)
	testing.expect_value(
		t,
		fixture.worker_states[0].phase,
		Static_Connection_Phase.Writing_Response,
	)
	testing.expect_value(t, fixture.worker.metrics.closed_connections, u64(0))

	turn := drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Keep_Alive)
	close_managed_body_fixture(&fixture)
}
