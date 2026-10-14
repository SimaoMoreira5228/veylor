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
test_request_head_reads_and_parses_no_more_than_each_turn_budget :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}

	request := "GET /budgeted HTTP/1.1\r\nHost: example.test\r\n\r\nbody"
	write_length, write_error := linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(request))

	if write_error != .NONE || write_length != len(request) {
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}

	input: [128]u8
	headers: [8]http1.Header
	driver := connection.begin_connection_head(73)
	head_event: connection.Connection_Head_Event
	total_bytes_read := 0
	for turn := 0; turn < 16; turn += 1 {
		read_turn := read_connection_head_turn(73, sockets[0], &driver, input[:], headers[:], 8)
		testing.expect_value(t, read_turn.bytes_read <= 8, true)

		if read_turn.budget_exhausted {
			testing.expect_value(t, read_turn.bytes_read, 8)
		}
		total_bytes_read += read_turn.bytes_read
		head_event = read_turn.head

		if head_event.result != .Need_More {
			break
		}
	}
	testing.expect_value(t, head_event.result, connection.Connection_Head_Result.Ready)
	testing.expect_value(t, total_bytes_read <= len(request), true)

	if head_event.result == .Ready {
		testing.expect_value(t, head_event.head.length, len(request) - len("body"))
		testing.expect_value(
			t,
			string(
				input[head_event.head.target.offset:head_event.head.target.offset +
				head_event.head.target.length],
			),
			"/budgeted",
		)
	}
	linux.close(sockets[0])
	linux.close(sockets[1])
}

@(test)
test_request_head_input_capacity_exhaustion_is_reported_without_another_read :: proc(
	t: ^testing.T,
) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}

	request := "GET /large HTTP/1.1\r\nHost: example.test\r\n\r\n"
	linux.write(sockets[1], transmute([]u8)request)
	input: [8]u8
	headers: [4]http1.Header
	driver := connection.begin_connection_head(74)
	first := read_connection_head_turn(74, sockets[0], &driver, input[:], headers[:], len(input))
	testing.expect_value(t, first.head.result, connection.Connection_Head_Result.Too_Large)
	testing.expect_value(t, first.input_full, true)
	testing.expect_value(t, first.bytes_read, len(input))

	second := read_connection_head_turn(74, sockets[0], &driver, input[:], headers[:], len(input))
	testing.expect_value(t, second.head.result, connection.Connection_Head_Result.Too_Large)
	testing.expect_value(t, second.input_full, false)
	testing.expect_value(t, second.bytes_read, 0)
	testing.expect_value(t, driver.state, connection.Connection_Head_State.Failed)
	linux.close(sockets[0])
	linux.close(sockets[1])
}

@(test)
test_request_head_deadline_survives_partial_reads_and_cancels_on_ready :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}

	request := "GET /deadline HTTP/1.1\r\nHost: example.test\r\n\r\n"
	write_length, write_error := linux.write(sockets[1], transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(request))

	if write_error != .NONE || write_length != len(request) {
		linux.close(sockets[0])
		linux.close(sockets[1])
		return
	}

	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(15, 6, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	token: connections.Connection_Token
	token, table_result = connections.allocate_connection(&table, 903)
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)
	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	timeouts := connections.Connection_Timeouts {
		request_head_ns = 50,
	}

	input: [128]u8
	headers: [8]http1.Header
	driver := connection.begin_connection_head(903)
	head_event: connection.Connection_Head_Event
	for turn := 0; turn < 16; turn += 1 {
		read_turn := read_connection_head_turn_with_deadline(
			903,
			sockets[0],
			&driver,
			input[:],
			headers[:],
			8,
			&table,
			token,
			&timers,
			1201,
			100,
			timeouts,
		)
		testing.expect_value(t, read_turn.bytes_read <= 8, true)
		head_event = read_turn.head

		if head_event.result == .Need_More {
			testing.expect_value(t, timers.count, 1)
			testing.expect_value(t, table.slots[int(token.slot)].deadline_timer_id, u64(1201))
			continue
		}
		testing.expect_value(
			t,
			read_turn.deadline_result,
			connections.Connection_Deadline_Schedule_Result.Cancelled,
		)
		break
	}
	testing.expect_value(t, head_event.result, connection.Connection_Head_Result.Ready)
	testing.expect_value(t, timers.count, 0)
	testing.expect_value(t, table.slots[int(token.slot)].has_deadline, false)
	connections.release_connection(&table, token)
	linux.close(sockets[0])
	linux.close(sockets[1])
}

@(test)
test_request_head_deadline_capacity_failure_does_not_read_socket :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}

	message := "GET /blocked HTTP/1.1\r\nHost: example.test\r\n\r\n"
	linux.write(sockets[1], transmute([]u8)message)
	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(16, 7, slots[:], free_stack[:])
	token: connections.Connection_Token
	token, table_result = connections.allocate_connection(&table, 904)
	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	transport.schedule_timer(
		&timers,
		transport.Timer_Expiration{timer_id = 1299, deadline_ns = 50},
	)
	timeouts := connections.Connection_Timeouts {
		request_head_ns = 50,
	}
	input: [128]u8
	headers: [8]http1.Header
	driver := connection.begin_connection_head(904)
	read_turn := read_connection_head_turn_with_deadline(
		904,
		sockets[0],
		&driver,
		input[:],
		headers[:],
		16,
		&table,
		token,
		&timers,
		1202,
		100,
		timeouts,
	)
	testing.expect_value(
		t,
		read_turn.deadline_result,
		connections.Connection_Deadline_Schedule_Result.Full,
	)
	testing.expect_value(t, read_turn.head.result, connection.Connection_Head_Result.Invalid)
	testing.expect_value(t, driver.has_operation, false)
	testing.expect_value(t, timers.count, 1)
	transport.cancel_timer(&timers, 1299)
	connections.release_connection(&table, token)
	linux.close(sockets[0])
	linux.close(sockets[1])
}
