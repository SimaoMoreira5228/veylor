#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import connections "veylor:runtime/connections"
import "veylor:transport"
import socket "veylor:transport/linux/socket"

@(test)
test_accept_batch_drains_multiple_concurrent_loopback_clients :: proc(t: ^testing.T) {
	loopback := [4]u8{127, 0, 0, 1}
	listener, listener_result, listener_error := socket.listen_ipv4(loopback, 0, 16)
	testing.expect_value(t, listener_result, socket.Result.Ready)
	testing.expect_value(t, listener_error, i32(0))

	if listener_result != .Ready {
		return
	}

	client_fds: [8]linux.Fd
	client_count := 0
	for i in 0 ..< len(client_fds) {
		flags: linux.Socket_FD_Flags = {.CLOEXEC}
		fd, socket_error := linux.socket(.INET, .STREAM, flags, .TCP)
		testing.expect_value(t, socket_error, linux.Errno.NONE)

		if socket_error != .NONE {
			break
		}
		destination := linux.Sock_Addr_In {
			sin_family = .INET,
			sin_port   = u16be(listener.local_address.port),
			sin_addr   = loopback,
		}
		connect_error := linux.connect(fd, &destination)
		testing.expect_value(t, connect_error, linux.Errno.NONE)

		if connect_error != .NONE {
			linux.close(fd)
			break
		}
		client_fds[client_count] = fd
		client_count += 1
	}
	testing.expect_value(t, client_count, len(client_fds))

	if client_count != len(client_fds) {
		for fd, i in client_fds[:client_count] {
			linux.close(fd)
		}
		socket.close_listener(&listener)
		return
	}

	accepted_storage: [9]socket.Accepted_Connection
	batch := socket.accept_ready(listener, accepted_storage[:], len(client_fds) + 1)
	testing.expect_value(t, batch.result, socket.Accept_Batch_Result.Drained)
	testing.expect_value(t, batch.attempts, len(client_fds) + 1)
	testing.expect_value(t, len(batch.connections), len(client_fds))

	for connection, i in batch.connections {
		testing.expect_value(t, connection.fd >= 0, true)
		testing.expect_value(t, connection.peer.family, transport.Address_Family.IPv4)
		socket.close_connection(&accepted_storage[i])
	}
	for fd in client_fds[:client_count] {
		linux.close(fd)
	}
	socket.close_listener(&listener)
}

@(test)
test_accept_turn_resumes_after_caller_storage_fills :: proc(t: ^testing.T) {
	loopback := [4]u8{127, 0, 0, 1}
	listener, listener_result, listener_error := socket.listen_ipv4(loopback, 0, 8)
	testing.expect_value(t, listener_result, socket.Result.Ready)
	testing.expect_value(t, listener_error, i32(0))

	if listener_result != .Ready {
		return
	}

	client_fds: [2]linux.Fd
	for &fd in client_fds {
		fd = -1
	}
	client_count := 0
	for &fd in client_fds {
		client_fd, socket_error := linux.socket(.INET, .STREAM, {.CLOEXEC}, .TCP)
		if socket_error != .NONE {
			break
		}
		destination := linux.Sock_Addr_In {
			sin_family = .INET,
			sin_port   = u16be(listener.local_address.port),
			sin_addr   = loopback,
		}
		if linux.connect(client_fd, &destination) != .NONE {
			linux.close(client_fd)
			break
		}
		fd = client_fd
		client_count += 1
	}
	testing.expect_value(t, client_count, len(client_fds))

	if client_count != len(client_fds) {
		for fd in client_fds {
			if fd >= 0 {
				linux.close(fd)
			}
		}
		socket.close_listener(&listener)
		return
	}

	slots: [2]connections.Connection_Slot
	free_stack: [2]u32
	table, table_result := connections.init_connection_table(4, 1, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	accepted_storage: [1]socket.Accepted_Connection
	worker_storage: [1]Worker_Connection
	readiness := transport.Readiness_Event {
		connection_id = 51,
		readable      = true,
	}
	first_turn := accept_listener_readiness(
		listener,
		51,
		readiness,
		&table,
		accepted_storage[:],
		worker_storage[:],
		4,
	)
	testing.expect_value(t, first_turn.result, Accept_Turn_Result.Caller_Capacity)
	testing.expect_value(t, len(first_turn.connections), 1)
	testing.expect_value(t, table.free_count, 1)

	if len(first_turn.connections) == 1 {
		socket.close_connection(&accepted_storage[0])
		connections.release_connection(&table, first_turn.connections[0].token)
	}
	second_turn := accept_listener_readiness(
		listener,
		51,
		readiness,
		&table,
		accepted_storage[:],
		worker_storage[:],
		4,
	)
	testing.expect_value(t, second_turn.result, Accept_Turn_Result.Caller_Capacity)
	testing.expect_value(t, len(second_turn.connections), 1)

	if len(second_turn.connections) == 1 {
		socket.close_connection(&accepted_storage[0])
		connections.release_connection(&table, second_turn.connections[0].token)
	}
	third_turn := accept_listener_readiness(
		listener,
		51,
		readiness,
		&table,
		accepted_storage[:],
		worker_storage[:],
		4,
	)
	testing.expect_value(t, third_turn.result, Accept_Turn_Result.Drained)
	testing.expect_value(t, len(third_turn.connections), 0)
	testing.expect_value(t, table.free_count, len(table.slots))
	for fd in client_fds {
		linux.close(fd)
	}
	socket.close_listener(&listener)
}

@(test)
test_accept_turn_assigns_worker_scoped_connection_tokens :: proc(t: ^testing.T) {
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
	table, table_result := connections.init_connection_table(7, 12, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	accepted_storage: [2]socket.Accepted_Connection
	worker_storage: [2]Worker_Connection
	readiness := transport.Readiness_Event {
		connection_id = 91,
		readable      = true,
	}
	turn := accept_listener_readiness(
		listener,
		91,
		readiness,
		&table,
		accepted_storage[:],
		worker_storage[:],
		4,
	)
	testing.expect_value(t, turn.result, Accept_Turn_Result.Drained)
	testing.expect_value(t, len(turn.connections), 1)

	if len(turn.connections) != 1 {
		linux.close(client)
		socket.close_listener(&listener)
		return
	}
	token := turn.connections[0].token
	testing.expect_value(t, token.worker_id, u32(7))
	testing.expect_value(t, token.worker_generation, u64(12))
	resource_id, lookup_result := connections.lookup_connection(&table, token)
	testing.expect_value(t, lookup_result, connections.Connection_Table_Result.Found)
	testing.expect_value(t, resource_id, u64(turn.connections[0].fd))

	first_connection := socket.Accepted_Connection {
		fd   = turn.connections[0].fd,
		peer = turn.connections[0].peer,
	}
	socket.close_connection(&first_connection)
	testing.expect_value(
		t,
		connections.release_connection(&table, token),
		connections.Connection_Table_Result.Released,
	)
	linux.close(client)
	socket.close_listener(&listener)
}

@(test)
test_accept_turn_skips_listener_without_readiness_or_free_slots :: proc(t: ^testing.T) {
	loopback := [4]u8{127, 0, 0, 1}
	listener, listener_result, listener_error := socket.listen_ipv4(loopback, 0, 8)
	testing.expect_value(t, listener_result, socket.Result.Ready)
	testing.expect_value(t, listener_error, i32(0))

	if listener_result != .Ready {
		return
	}

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(3, 5, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token, allocation_result := connections.allocate_connection(&table, 0)
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)
	accepted_storage: [1]socket.Accepted_Connection
	worker_storage: [1]Worker_Connection
	no_readiness := transport.Readiness_Event {
		connection_id = 10,
	}
	turn := accept_listener_readiness(
		listener,
		10,
		no_readiness,
		&table,
		accepted_storage[:],
		worker_storage[:],
		1,
	)
	testing.expect_value(t, turn.result, Accept_Turn_Result.Not_Ready)

	readiness := transport.Readiness_Event {
		connection_id = 10,
		readable      = true,
	}
	turn = accept_listener_readiness(
		listener,
		10,
		readiness,
		&table,
		accepted_storage[:],
		worker_storage[:],
		1,
	)
	testing.expect_value(t, turn.result, Accept_Turn_Result.Table_Full)
	testing.expect_value(t, len(turn.connections), 0)
	connections.release_connection(&table, token)
	socket.close_listener(&listener)
}
