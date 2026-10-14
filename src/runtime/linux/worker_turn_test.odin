#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import connections "veylor:runtime/connections"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

@(test)
test_descriptor_limit_pauses_listener_until_owner_resumes_it :: proc(t: ^testing.T) {
	listener, listen_result, listen_error := socket.listen_ipv4([4]u8{127, 0, 0, 1}, 0, 4)
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

	_, register_error := epoll.register(poller, listener.fd, 401, epoll.Interest{readable = true})
	testing.expect_value(t, register_error, i32(0))
	registered := register_error == 0
	paused := false
	result, pause_error := pause_listener_for_descriptor_limit(
		poller,
		listener,
		&registered,
		&paused,
	)
	testing.expect_value(t, result, epoll.Result.Ready)
	testing.expect_value(t, pause_error, i32(0))
	testing.expect_value(t, registered, false)
	testing.expect_value(t, paused, true)

	paused = false
	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(7, 8, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	native_events: [1]linux.EPoll_Event
	readiness_events: [1]transport.Readiness_Event
	accepted_storage: [1]socket.Accepted_Connection
	connection_storage: [1]Worker_Connection
	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	expired_timer_storage: [1]transport.Timer_Expiration
	turn := run_worker_turn(
		poller,
		listener,
		401,
		&registered,
		&paused,
		&table,
		native_events[:],
		readiness_events[:],
		accepted_storage[:],
		connection_storage[:],
		&timers,
		expired_timer_storage[:],
		1,
		1,
		1,
		0,
	)
	testing.expect_value(t, turn.poll_result, epoll.Result.Timeout)
	testing.expect_value(t, registered, true)
	epoll.unregister(poller, listener.fd)
	epoll.close(&poller)
	socket.close_listener(&listener)
}

@(test)
test_worker_turn_bounds_events_rotates_ready_streams_and_assigns_accepted_slots :: proc(
	t: ^testing.T,
) {
	loopback := [4]u8{127, 0, 0, 1}
	listener, listener_result, listener_error := socket.listen_ipv4(loopback, 0, 8)
	testing.expect_value(t, listener_result, socket.Result.Ready)
	testing.expect_value(t, listener_error, i32(0))
	if listener_result != .Ready {
		return
	}

	client_flags: linux.Socket_FD_Flags = {.CLOEXEC}
	client, client_error := linux.socket(.INET, .STREAM, client_flags, .TCP)
	testing.expect_value(t, client_error, linux.Errno.NONE)
	if client_error != .NONE {
		socket.close_listener(&listener)
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
		socket.close_listener(&listener)
		return
	}

	slots: [2]connections.Connection_Slot
	free_stack: [2]u32
	table, table_result := connections.init_connection_table(3, 4, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	poller, poller_result, poller_error := epoll.create()
	testing.expect_value(t, poller_result, epoll.Result.Ready)
	testing.expect_value(t, poller_error, i32(0))
	if table_result != .Ready || poller_result != .Ready {
		if poller_result == .Ready {
			epoll.close(&poller)
		}
		linux.close(client)
		socket.close_listener(&listener)
		return
	}

	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		epoll.close(&poller)
		linux.close(client)
		socket.close_listener(&listener)
		return
	}
	_, poller_error = epoll.register(poller, sockets[0], 202, epoll.Interest{readable = true})
	testing.expect_value(t, poller_error, i32(0))
	other_sockets: [2]linux.Fd
	socket_error = linux.socketpair(.UNIX, .STREAM, .HOPOPT, &other_sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		linux.close(sockets[0])
		linux.close(sockets[1])
		epoll.close(&poller)
		linux.close(client)
		socket.close_listener(&listener)
		return
	}
	_, poller_error = epoll.register(
		poller,
		other_sockets[0],
		203,
		epoll.Interest{readable = true},
	)
	testing.expect_value(t, poller_error, i32(0))

	native_events: [1]linux.EPoll_Event
	readiness_events: [1]transport.Readiness_Event
	accepted_storage: [2]socket.Accepted_Connection
	connection_storage: [2]Worker_Connection
	timer_storage: [2]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	expired_timer_storage: [1]transport.Timer_Expiration
	testing.expect_value(
		t,
		transport.schedule_timer(
			&timers,
			transport.Timer_Expiration{timer_id = 99, deadline_ns = 0},
		),
		transport.Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		transport.schedule_timer(
			&timers,
			transport.Timer_Expiration{timer_id = 100, deadline_ns = 0},
		),
		transport.Timer_Queue_Result.Scheduled,
	)
	listener_registered := false
	descriptor_limit_paused := false
	turn := run_worker_turn(
		poller,
		listener,
		101,
		&listener_registered,
		&descriptor_limit_paused,
		&table,
		native_events[:],
		readiness_events[:],
		accepted_storage[:],
		connection_storage[:],
		&timers,
		expired_timer_storage[:],
		1,
		4096,
		2,
		100,
	)
	testing.expect_value(t, turn.poll_result, epoll.Result.Ready)
	testing.expect_value(t, turn.byte_budget_per_connection, 4096)
	testing.expect_value(t, listener_registered, true)
	testing.expect_value(t, len(turn.stream_events) <= 1, true)
	testing.expect_value(t, len(turn.accepted), 1)
	testing.expect_value(t, len(turn.expired_timers), 1)
	if len(turn.expired_timers) == 1 {
		testing.expect_value(t, turn.expired_timers[0].timer_id, u64(99))
	}
	if len(turn.accepted) == 1 {
		testing.expect_value(t, turn.accepted[0].token.worker_id, u32(3))
		resource_id, lookup_result := connections.lookup_connection(&table, turn.accepted[0].token)
		testing.expect_value(t, lookup_result, connections.Connection_Table_Result.Found)
		testing.expect_value(t, resource_id, u64(turn.accepted[0].fd))
	}
	dummy_token: connections.Connection_Token
	dummy_token, table_result = connections.allocate_connection(&table, 0)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	testing.expect_value(t, table.free_count, 0)

	message := "ready"
	_, socket_error = linux.write(sockets[1], transmute([]u8)message)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	_, socket_error = linux.write(other_sockets[1], transmute([]u8)message)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	next_turn := run_worker_turn(
		poller,
		listener,
		101,
		&listener_registered,
		&descriptor_limit_paused,
		&table,
		native_events[:],
		readiness_events[:],
		accepted_storage[:],
		connection_storage[:],
		&timers,
		expired_timer_storage[:],
		1,
		4096,
		2,
		100,
	)
	testing.expect_value(t, next_turn.poll_result, epoll.Result.Ready)
	testing.expect_value(t, listener_registered, false)
	testing.expect_value(t, len(next_turn.stream_events), 1)
	testing.expect_value(t, len(next_turn.expired_timers), 1)
	if len(next_turn.expired_timers) == 1 {
		testing.expect_value(t, next_turn.expired_timers[0].timer_id, u64(100))
	}
	first_stream_id := u64(0)
	if len(next_turn.stream_events) == 1 {
		first_stream_id = next_turn.stream_events[0].connection_id
		testing.expect_value(t, first_stream_id == 202 || first_stream_id == 203, true)
		testing.expect_value(t, next_turn.stream_events[0].readable, true)
	}
	table_result = connections.release_connection(&table, dummy_token)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Released)

	resumed_turn := run_worker_turn(
		poller,
		listener,
		101,
		&listener_registered,
		&descriptor_limit_paused,
		&table,
		native_events[:],
		readiness_events[:],
		accepted_storage[:],
		connection_storage[:],
		&timers,
		expired_timer_storage[:],
		1,
		4096,
		2,
		0,
	)
	testing.expect_value(t, resumed_turn.poll_result, epoll.Result.Ready)
	testing.expect_value(t, listener_registered, true)
	testing.expect_value(t, len(resumed_turn.stream_events), 1)
	if len(next_turn.stream_events) == 1 && len(resumed_turn.stream_events) == 1 {
		testing.expect_value(
			t,
			first_stream_id != resumed_turn.stream_events[0].connection_id,
			true,
		)
	}

	if len(turn.accepted) == 1 {
		accepted := socket.Accepted_Connection {
			fd   = turn.accepted[0].fd,
			peer = turn.accepted[0].peer,
		}
		socket.close_connection(&accepted)
		connections.release_connection(&table, turn.accepted[0].token)
	}
	linux.close(sockets[0])
	linux.close(sockets[1])
	linux.close(other_sockets[0])
	linux.close(other_sockets[1])
	linux.close(client)
	socket.close_listener(&listener)
	epoll.close(&poller)
}
