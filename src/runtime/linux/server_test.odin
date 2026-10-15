#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
import shutdown "veylor:runtime/shutdown"
import workers "veylor:runtime/workers"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

@(test)
test_managed_server_starts_serves_stops_joins_and_destroys :: proc(t: ^testing.T) {
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

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	method := "GET"
	path := "/ready"
	reason := "OK"
	body := "ready"
	static_response := router.Static_Response {
		status = 200,
		reason = transmute([]u8)reason,
		body   = transmute([]u8)body,
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
		4,
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
		timeout_ms = -1,
		body_limit = 1024,
		timeouts = connections.Connection_Timeouts {
			request_head_ns = 1_000_000_000,
			response_write_ns = 1_000_000_000,
			keep_alive_idle_ns = 1_000_000_000,
		},
	}
	worker_storage: [1]workers.Worker_Thread
	server: Managed_Server
	testing.expect_value(
		t,
		init_managed_server(&server, &worker, worker_storage[:], 0),
		Managed_Server_Result.Initialized,
	)
	testing.expect_value(t, start_managed_server(&server), Managed_Server_Result.Started)
	testing.expect_value(
		t,
		init_managed_server(&server, &worker, worker_storage[:], 0),
		Managed_Server_Result.Invalid,
	)
	testing.expect_value(t, server.phase, Managed_Server_Phase.Running)
	testing.expect_value(t, destroy_managed_server(&server), Managed_Server_Result.Busy)

	client_flags: linux.Socket_FD_Flags = {.CLOEXEC}
	client, client_error := linux.socket(.INET, .STREAM, client_flags, .TCP)
	testing.expect_value(t, client_error, linux.Errno.NONE)

	if client_error != .NONE {
		request_managed_server_stop(&server)
		join_managed_server(&server)
		destroy_managed_server(&server)
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
		request_managed_server_stop(&server)
		join_managed_server(&server)
		destroy_managed_server(&server)
		return
	}
	request := "GET /ready HTTP/1.1\r\nHost: example.test\r\n\r\n"
	written, write_error := linux.write(client, transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	received: [256]u8
	response_length, read_error := linux.read(client, received[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(received[:response_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nready",
	)
	testing.expect_value(
		t,
		request_managed_server_stop(&server),
		Managed_Server_Result.Stop_Requested,
	)
	testing.expect_value(
		t,
		request_managed_server_stop(&server),
		Managed_Server_Result.Already_Stopping,
	)
	testing.expect_value(t, join_managed_server(&server), Managed_Server_Result.Joined)
	testing.expect_value(t, server.worker_result, Static_Worker_Result.Ready)
	testing.expect_value(t, shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, shutdown_state.active_connections, u64(0))

	close_length, close_error := linux.read(client, received[:])
	testing.expect_value(t, close_error, linux.Errno.NONE)
	testing.expect_value(t, close_length, 0)
	linux.close(client)
	testing.expect_value(t, destroy_managed_server(&server), Managed_Server_Result.Destroyed)
	testing.expect_value(t, server.phase, Managed_Server_Phase.Destroyed)
}
