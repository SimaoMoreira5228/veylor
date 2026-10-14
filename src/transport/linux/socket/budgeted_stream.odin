#+build linux
package socket

import "core:sys/linux"
import "veylor:transport"

Stream_Byte_Budget :: struct {
	remaining_bytes: int,
}

Budgeted_IO_Event :: struct {
	io:               transport.IO_Result,
	budget_exhausted: bool,
}

begin_stream_byte_budget :: proc(limit: int) -> (budget: Stream_Byte_Budget, valid: bool) {
	if limit < 0 {
		return {}, false
	}

	return Stream_Byte_Budget{remaining_bytes = limit}, true
}

read_stream_budgeted :: proc(
	fd: linux.Fd,
	buffer: []u8,
	budget: ^Stream_Byte_Budget,
) -> Budgeted_IO_Event {
	if budget.remaining_bytes < 0 {
		return Budgeted_IO_Event {
			io = transport.IO_Result{kind = .Error, error_code = i32(linux.Errno.EINVAL)},
		}
	}

	if budget.remaining_bytes == 0 {
		return Budgeted_IO_Event {
			io = transport.IO_Result{kind = .Would_Block},
			budget_exhausted = true,
		}
	}

	if len(buffer) == 0 {
		return Budgeted_IO_Event{io = transport.IO_Result{kind = .Progress}}
	}

	read_length := min(len(buffer), budget.remaining_bytes)
	result := read_stream(fd, buffer[:read_length])

	if result.kind == .Progress {
		if result.transferred_bytes > read_length {
			return Budgeted_IO_Event {
				io = transport.IO_Result{kind = .Error, error_code = i32(linux.Errno.EIO)},
			}
		}
		budget.remaining_bytes -= result.transferred_bytes
	}

	return Budgeted_IO_Event{io = result, budget_exhausted = budget.remaining_bytes == 0}
}

write_stream_budgeted :: proc(
	fd: linux.Fd,
	buffer: []u8,
	budget: ^Stream_Byte_Budget,
) -> Budgeted_IO_Event {
	if budget.remaining_bytes < 0 {
		return Budgeted_IO_Event {
			io = transport.IO_Result{kind = .Error, error_code = i32(linux.Errno.EINVAL)},
		}
	}

	if budget.remaining_bytes == 0 {
		return Budgeted_IO_Event {
			io = transport.IO_Result{kind = .Would_Block},
			budget_exhausted = true,
		}
	}

	if len(buffer) == 0 {
		return Budgeted_IO_Event{io = transport.IO_Result{kind = .Progress}}
	}

	write_length := min(len(buffer), budget.remaining_bytes)
	result := write_stream(fd, buffer[:write_length])

	if result.kind == .Progress {
		if result.transferred_bytes > write_length {
			return Budgeted_IO_Event {
				io = transport.IO_Result{kind = .Error, error_code = i32(linux.Errno.EIO)},
			}
		}
		budget.remaining_bytes -= result.transferred_bytes
	}

	return Budgeted_IO_Event{io = result, budget_exhausted = budget.remaining_bytes == 0}
}
