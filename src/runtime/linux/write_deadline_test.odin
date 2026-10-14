#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import connection "veylor:http1/connection"
import connections "veylor:runtime/connections"
import "veylor:transport"
import socket "veylor:transport/linux/socket"

@(test)
test_partial_response_writes_refresh_deadline_then_cancel_on_complete :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(19, 10, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token: connections.Connection_Token
	token, table_result = connections.allocate_connection(&table, 907)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	testing.expect_value(
		t,
		connections.transition_connection(&table, token, .Writing_Response),
		connections.Connection_Table_Result.Transitioned,
	)

	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	timeouts := connections.Connection_Timeouts {
		response_write_ns  = 25,
		keep_alive_idle_ns = 40,
	}
	response_bytes := "abcdefghij"
	driver := connection.begin_connection_write(907, transmute([]u8)response_bytes)

	turn := write_connection_turn_with_deadline(
		907,
		sockets[0],
		&driver,
		4,
		true,
		&table,
		token,
		&timers,
		1501,
		100,
		timeouts,
	)
	testing.expect_value(t, turn.write.result, connection.Connection_Write_Result.Progress)
	testing.expect_value(t, turn.bytes_written, 4)
	testing.expect_value(t, turn.budget_exhausted, true)
	testing.expect_value(
		t,
		turn.deadline_result,
		connections.Connection_Deadline_Schedule_Result.Scheduled,
	)
	testing.expect_value(t, timer_storage[0].timer_id, u64(1501))
	testing.expect_value(t, timer_storage[0].deadline_ns, u64(125))

	turn = write_connection_turn_with_deadline(
		907,
		sockets[0],
		&driver,
		4,
		true,
		&table,
		token,
		&timers,
		1502,
		110,
		timeouts,
	)
	testing.expect_value(t, turn.write.result, connection.Connection_Write_Result.Progress)
	testing.expect_value(t, turn.bytes_written, 4)
	testing.expect_value(t, timer_storage[0].timer_id, u64(1502))
	testing.expect_value(t, timer_storage[0].deadline_ns, u64(135))

	turn = write_connection_turn_with_deadline(
		907,
		sockets[0],
		&driver,
		4,
		true,
		&table,
		token,
		&timers,
		1503,
		120,
		timeouts,
	)
	testing.expect_value(t, turn.write.result, connection.Connection_Write_Result.Complete)
	testing.expect_value(t, turn.bytes_written, 2)
	testing.expect_value(
		t,
		turn.deadline_result,
		connections.Connection_Deadline_Schedule_Result.Scheduled,
	)
	testing.expect_value(t, timers.count, 1)
	testing.expect_value(
		t,
		table.slots[int(token.slot)].deadline_kind,
		connections.Connection_Deadline_Kind.Keep_Alive_Idle,
	)
	testing.expect_value(t, timer_storage[0].deadline_ns, u64(160))
	testing.expect_value(t, table.slots[int(token.slot)].has_deadline, true)

	received: [10]u8
	read_length, read_error := linux.read(sockets[1], received[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(t, read_length, len(response_bytes))
	testing.expect_value(t, string(received[:read_length]), response_bytes)
	testing.expect_value(
		t,
		connections.resume_keep_alive_request(&table, &timers, token),
		connections.Connection_Deadline_Schedule_Result.Ready,
	)
	testing.expect_value(t, timers.count, 0)
	testing.expect_value(
		t,
		connections.transition_connection(&table, token, .Closing),
		connections.Connection_Table_Result.Transitioned,
	)
	connections.release_connection(&table, token)
	linux.close(sockets[0])
	linux.close(sockets[1])
}

@(test)
test_response_write_does_not_send_without_deadline_capacity :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(20, 11, slots[:], free_stack[:])
	token: connections.Connection_Token
	token, table_result = connections.allocate_connection(&table, 908)
	connections.transition_connection(&table, token, .Writing_Response)
	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	transport.schedule_timer(
		&timers,
		transport.Timer_Expiration{timer_id = 1599, deadline_ns = 10},
	)
	timeouts := connections.Connection_Timeouts {
		response_write_ns = 25,
	}
	message := "blocked"
	driver := connection.begin_connection_write(908, transmute([]u8)message)
	turn := write_connection_turn_with_deadline(
		908,
		sockets[0],
		&driver,
		len(message),
		false,
		&table,
		token,
		&timers,
		1600,
		100,
		timeouts,
	)
	testing.expect_value(
		t,
		turn.deadline_result,
		connections.Connection_Deadline_Schedule_Result.Full,
	)
	testing.expect_value(t, turn.bytes_written, 0)
	testing.expect_value(t, driver.has_operation, false)
	read_buffer: [8]u8
	read_result := socket.read_stream(sockets[1], read_buffer[:])
	testing.expect_value(t, read_result.kind, transport.IO_Result_Kind.Would_Block)
	transport.cancel_timer(&timers, 1599)
	connections.release_connection(&table, token)
	linux.close(sockets[0])
	linux.close(sockets[1])
}
