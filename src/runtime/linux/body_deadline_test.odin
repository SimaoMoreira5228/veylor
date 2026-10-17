#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import connection "veylor:http1/connection"
import connections "veylor:runtime/connections"
import "veylor:transport"

@(test)
test_body_progress_refreshes_deadline_and_completion_cancels_it :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(17, 8, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token: connections.Connection_Token
	token, table_result = connections.allocate_connection(&table, 905)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	testing.expect_value(
		t,
		connections.transition_connection(&table, token, .Reading_Request_Body),
		connections.Connection_Table_Result.Transitioned,
	)

	timer_storage: [1]transport.Timer_Expiration
	timer_index_storage: [transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	timer_id := connections.connection_deadline_timer_id(token)
	timeouts := connections.Connection_Timeouts {
		body_progress_ns = 50,
	}
	head := http1.Request_Head {
		content_length     = 10,
		has_content_length = true,
	}
	driver := connection.begin_connection_body(905, head)
	input: [4]u8
	spans: [1]http1.Span

	turn := read_connection_body_turn_with_deadline(
		905,
		sockets[0],
		&driver,
		input[:],
		spans[:],
		{},
		4,
		&table,
		token,
		&timers,
		100,
		timeouts,
	)
	testing.expect_value(t, turn.body.result, connection.Connection_Body_Result.Would_Block)
	testing.expect_value(
		t,
		turn.deadline_result,
		connections.Connection_Deadline_Schedule_Result.Scheduled,
	)
	testing.expect_value(t, timer_storage[0].deadline_ns, u64(150))

	first_chunk := "abcd"
	write_length, write_error := linux.write(sockets[1], transmute([]u8)first_chunk)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(first_chunk))
	turn = read_connection_body_turn_with_deadline(
		905,
		sockets[0],
		&driver,
		input[:],
		spans[:],
		{},
		4,
		&table,
		token,
		&timers,
		120,
		timeouts,
	)
	testing.expect_value(t, turn.body.result, connection.Connection_Body_Result.Data)
	testing.expect_value(t, turn.bytes_read, 4)
	testing.expect_value(
		t,
		turn.deadline_result,
		connections.Connection_Deadline_Schedule_Result.Scheduled,
	)
	testing.expect_value(t, timer_storage[0].timer_id, timer_id)
	testing.expect_value(t, timer_storage[0].deadline_ns, u64(170))
	testing.expect_value(t, string(input[:4]), first_chunk)

	second_chunk := "efgh"
	write_length, write_error = linux.write(sockets[1], transmute([]u8)second_chunk)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(second_chunk))
	turn = read_connection_body_turn_with_deadline(
		905,
		sockets[0],
		&driver,
		input[:],
		spans[:],
		{},
		4,
		&table,
		token,
		&timers,
		140,
		timeouts,
	)
	testing.expect_value(t, turn.body.result, connection.Connection_Body_Result.Data)
	testing.expect_value(t, turn.body.body_complete, false)
	testing.expect_value(t, timer_storage[0].timer_id, timer_id)
	testing.expect_value(t, timer_storage[0].deadline_ns, u64(190))

	last_chunk := "ij"
	write_length, write_error = linux.write(sockets[1], transmute([]u8)last_chunk)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(last_chunk))
	turn = read_connection_body_turn_with_deadline(
		905,
		sockets[0],
		&driver,
		input[:],
		spans[:],
		{},
		4,
		&table,
		token,
		&timers,
		160,
		timeouts,
	)
	testing.expect_value(t, turn.body.result, connection.Connection_Body_Result.Data)
	testing.expect_value(t, turn.body.body_complete, true)
	testing.expect_value(
		t,
		turn.deadline_result,
		connections.Connection_Deadline_Schedule_Result.Cancelled,
	)
	testing.expect_value(t, timers.count, 0)
	testing.expect_value(t, table.slots[int(token.slot)].has_deadline, false)
	testing.expect_value(t, string(input[:2]), last_chunk)

	connections.release_connection(&table, token)
	linux.close(sockets[0])
	linux.close(sockets[1])
}

@(test)
test_body_read_does_not_start_without_deadline_storage :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(18, 9, slots[:], free_stack[:])
	token: connections.Connection_Token
	token, table_result = connections.allocate_connection(&table, 906)
	connections.transition_connection(&table, token, .Reading_Request_Body)
	timer_storage: [1]transport.Timer_Expiration
	timer_index_storage: [transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	transport.schedule_timer(
		&timers,
		transport.Timer_Expiration{timer_id = 1499, deadline_ns = 10},
	)
	head := http1.Request_Head {
		content_length     = 1,
		has_content_length = true,
	}
	driver := connection.begin_connection_body(906, head)
	input: [1]u8
	spans: [1]http1.Span
	turn := read_connection_body_turn_with_deadline(
		906,
		sockets[0],
		&driver,
		input[:],
		spans[:],
		{},
		1,
		&table,
		token,
		&timers,
		100,
		connections.Connection_Timeouts{body_progress_ns = 50},
	)
	testing.expect_value(
		t,
		turn.deadline_result,
		connections.Connection_Deadline_Schedule_Result.Full,
	)
	testing.expect_value(t, turn.bytes_read, 0)
	testing.expect_value(t, driver.has_operation, false)
	transport.cancel_timer(&timers, 1499)
	connections.release_connection(&table, token)
	linux.close(sockets[0])
	linux.close(sockets[1])
}
