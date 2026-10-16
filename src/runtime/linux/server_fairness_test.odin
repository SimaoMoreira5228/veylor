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
test_static_worker_serves_cold_request_under_sustained_hot_socket_load :: proc(t: ^testing.T) {
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
	hot_clients: [3]linux.Fd
	for &client in hot_clients {
		client = -1
	}
	hot_clients_ready := true
	for &client in hot_clients {
		client = connect_managed_server_test_client(listener.local_address.port)
		if client < 0 {
			hot_clients_ready = false
			break
		}
	}
	cold_client := connect_managed_server_test_client(listener.local_address.port)

	if !hot_clients_ready || cold_client < 0 {
		for client in hot_clients {
			if client >= 0 {
				linux.close(client)
			}
		}
		if cold_client >= 0 {
			linux.close(cold_client)
		}
		epoll.close(&poller)
		socket.close_listener(&listener)
		return
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	method := "GET"
	path := "/ready"
	reason := "OK"
	body := "ok"
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

	connection_slots: [4]connections.Connection_Slot
	free_stack: [4]u32
	table, table_result := connections.init_connection_table(
		8,
		1,
		connection_slots[:],
		free_stack[:],
	)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	shutdown_state: shutdown.State
	timer_storage: [4]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	connection_states: [4]Static_Connection
	request_inputs: [4][32 * 1024]u8
	request_headers: [4][4]http1.Header
	response_buffers: [4][256]u8
	connection_buffers: [4]Static_Connection_Buffers
	for i := 0; i < len(connection_buffers); i += 1 {
		connection_buffers[i] = Static_Connection_Buffers {
			request_input = request_inputs[i][:],
			headers       = request_headers[i][:],
			response      = response_buffers[i][:],
		}
	}
	native_events: [1]linux.EPoll_Event
	readiness_events: [1]transport.Readiness_Event
	accepted_storage: [4]socket.Accepted_Connection
	accepted_connections: [4]Worker_Connection
	expired_timers: [5]transport.Timer_Expiration
	worker := Static_Worker {
		poller                = poller,
		listener              = listener,
		shutdown_state        = &shutdown_state,
		table                 = &table,
		routes                = &routes,
		connection_states     = connection_states[:],
		connection_buffers    = connection_buffers[:],
		native_events         = native_events[:],
		readiness_events      = readiness_events[:],
		accepted_storage      = accepted_storage[:],
		accepted_connections  = accepted_connections[:],
		timers                = &timers,
		expired_timer_storage = expired_timers[:],
		event_budget          = 1,
		byte_budget           = 64,
		accept_attempt_budget = 4,
		timeout_ms            = 0,
		body_limit            = 1024,
		timeouts              = connections.Connection_Timeouts {
			request_head_ns    = 60_000_000_000,
			response_write_ns  = 60_000_000_000,
			keep_alive_idle_ns = 60_000_000_000,
		},
	}
	initial_turn := run_static_worker_turn(&worker)
	testing.expect_value(t, initial_turn.result, Static_Worker_Result.Ready)
	testing.expect_value(t, initial_turn.accepted_count, 4)

	hot_prefix := "GET /hot HTTP/1.1\r\nHost: example.test\r\nX-Hot-A: "
	second_field := "\r\nX-Hot-B: "
	third_field := "\r\nX-Hot-C: "
	fourth_field := "\r\nX-Hot-D: "
	field_value_length := 7500
	hot_request: [32 * 1024]u8
	hot_request_length := 0
	copy(hot_request[:len(hot_prefix)], transmute([]u8)hot_prefix)
	hot_request_length += len(hot_prefix)
	for i := 0; i < field_value_length; i += 1 {
		hot_request[hot_request_length + i] = 'x'
	}
	hot_request_length += field_value_length
	copy(
		hot_request[hot_request_length:hot_request_length + len(second_field)],
		transmute([]u8)second_field,
	)
	hot_request_length += len(second_field)
	for i := 0; i < field_value_length; i += 1 {
		hot_request[hot_request_length + i] = 'x'
	}
	hot_request_length += field_value_length
	copy(
		hot_request[hot_request_length:hot_request_length + len(third_field)],
		transmute([]u8)third_field,
	)
	hot_request_length += len(third_field)
	for i := 0; i < field_value_length; i += 1 {
		hot_request[hot_request_length + i] = 'x'
	}
	hot_request_length += field_value_length
	copy(
		hot_request[hot_request_length:hot_request_length + len(fourth_field)],
		transmute([]u8)fourth_field,
	)
	hot_request_length += len(fourth_field)
	for i := 0; i < field_value_length; i += 1 {
		hot_request[hot_request_length + i] = 'x'
	}
	hot_request_length += field_value_length
	cold_request := "GET /ready HTTP/1.1\r\nHost: cold.example\r\n\r\n"
	for client in hot_clients {
		hot_written, hot_write_error := linux.write(client, hot_request[:hot_request_length])
		testing.expect_value(t, hot_write_error, linux.Errno.NONE)
		testing.expect_value(t, hot_written, hot_request_length)
	}

	for turn_index := 0; turn_index < 1024; turn_index += 1 {
		turn := run_static_worker_turn(&worker)
		testing.expect_value(t, turn.result, Static_Worker_Result.Ready)
		testing.expect_value(t, turn.processed_count, 1)
	}
	hot_prefix_count := 0
	for state in connection_states {
		if !state.active ||
		   state.phase != .Reading_Request_Head ||
		   state.head_driver.input_length <= 0 {
			continue
		}
		if http1.bytes_equal(state.request_input[:len(hot_prefix)], hot_prefix) {
			hot_prefix_count += 1
			testing.expect_value(t, state.head_driver.input_length < hot_request_length, true)
		}
	}
	testing.expect_value(t, hot_prefix_count, len(hot_clients))

	cold_written, cold_write_error := linux.write(cold_client, transmute([]u8)cold_request)
	testing.expect_value(t, cold_write_error, linux.Errno.NONE)
	testing.expect_value(t, cold_written, len(cold_request))

	cold_served := false
	for turn_index := 0; turn_index < 16; turn_index += 1 {
		turn := run_static_worker_turn(&worker)

		if turn.result == .Error || turn.result == .Invalid {
			testing.expect_value(t, turn.result, Static_Worker_Result.Ready)
			break
		}
		for state in connection_states {
			if state.phase == .Keep_Alive_Idle {
				cold_served = true
			}
		}
		if cold_served {
			break
		}
	}
	testing.expect_value(t, cold_served, true)

	hot_prefix_count = 0
	for state in connection_states {
		if !state.active ||
		   state.phase != .Reading_Request_Head ||
		   state.head_driver.input_length <= 0 {
			continue
		}
		if http1.bytes_equal(state.request_input[:len(hot_prefix)], hot_prefix) {
			hot_prefix_count += 1
			testing.expect_value(t, state.head_driver.input_length < hot_request_length, true)
		}
	}
	testing.expect_value(t, hot_prefix_count, len(hot_clients))

	served_again := run_static_worker_turn(&worker)
	testing.expect_value(t, served_again.result, Static_Worker_Result.Ready)
	testing.expect_value(t, served_again.processed_count, 1)
	response: [256]u8
	response_length, read_error := linux.read(cold_client, response[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(response[:response_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
	)

	for client in hot_clients {
		linux.close(client)
	}
	linux.close(cold_client)
	testing.expect_value(
		t,
		shutdown.request_shutdown(&shutdown_state, &timers, 99, 100, 0),
		shutdown.Result.Force_Connections,
	)
	cleanup_turn := run_static_worker_turn(&worker)
	testing.expect_value(t, cleanup_turn.closed_count, 4)
	testing.expect_value(t, shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, shutdown_state.active_connections, u64(0))
	epoll.close(&poller)
	socket.close_listener(&listener)
}
