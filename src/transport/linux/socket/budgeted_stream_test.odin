#+build linux
#+test
package socket

import "core:sys/linux"
import "core:testing"
import "veylor:transport"

@(test)
test_stream_byte_budgets_cap_partial_reads_and_writes :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}

	payload := "abcdef"
	write_length, write_error := linux.write(sockets[1], transmute([]u8)payload)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(payload))

	read_budget, budget_valid := begin_stream_byte_budget(4)
	testing.expect_value(t, budget_valid, true)
	read_buffer: [8]u8
	event := read_stream_budgeted(sockets[0], read_buffer[:], &read_budget)
	testing.expect_value(t, event.io.kind, transport.IO_Result_Kind.Progress)
	testing.expect_value(t, event.io.transferred_bytes, 4)
	testing.expect_value(t, event.budget_exhausted, true)
	testing.expect_value(t, string(read_buffer[:4]), "abcd")

	event = read_stream_budgeted(sockets[0], read_buffer[:], &read_budget)
	testing.expect_value(t, event.io.kind, transport.IO_Result_Kind.Would_Block)
	testing.expect_value(t, event.budget_exhausted, true)

	read_budget, budget_valid = begin_stream_byte_budget(4)
	testing.expect_value(t, budget_valid, true)
	event = read_stream_budgeted(sockets[0], read_buffer[:], &read_budget)
	testing.expect_value(t, event.io.kind, transport.IO_Result_Kind.Progress)
	testing.expect_value(t, event.io.transferred_bytes, 2)
	testing.expect_value(t, event.budget_exhausted, false)
	testing.expect_value(t, string(read_buffer[:2]), "ef")

	write_budget: Stream_Byte_Budget
	write_budget, budget_valid = begin_stream_byte_budget(3)
	testing.expect_value(t, budget_valid, true)
	event = write_stream_budgeted(sockets[1], transmute([]u8)payload, &write_budget)
	testing.expect_value(t, event.io.kind, transport.IO_Result_Kind.Progress)
	testing.expect_value(t, event.io.transferred_bytes, 3)
	testing.expect_value(t, event.budget_exhausted, true)
	event = write_stream_budgeted(sockets[1], transmute([]u8)payload[3:], &write_budget)
	testing.expect_value(t, event.io.kind, transport.IO_Result_Kind.Would_Block)
	testing.expect_value(t, event.budget_exhausted, true)

	write_budget, budget_valid = begin_stream_byte_budget(3)
	testing.expect_value(t, budget_valid, true)
	event = write_stream_budgeted(sockets[1], transmute([]u8)payload[3:], &write_budget)
	testing.expect_value(t, event.io.kind, transport.IO_Result_Kind.Progress)
	testing.expect_value(t, event.io.transferred_bytes, 3)
	testing.expect_value(t, event.budget_exhausted, true)

	read_length, read_error := linux.read(sockets[0], read_buffer[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(t, string(read_buffer[:read_length]), payload)
	linux.close(sockets[0])
	linux.close(sockets[1])
}

@(test)
test_stream_byte_budget_rejects_negative_limits :: proc(t: ^testing.T) {
	_, valid := begin_stream_byte_budget(-1)
	testing.expect_value(t, valid, false)
}
