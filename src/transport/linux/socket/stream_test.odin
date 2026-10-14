#+build linux
#+test
package socket

import "core:sys/linux"
import "core:testing"
import "veylor:transport"

@(test)
test_nonblocking_stream_preserves_partial_reads_and_empty_queue :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		return
	}

	read_buffer: [3]u8
	result := read_stream(sockets[0], read_buffer[:])
	testing.expect_value(t, result.kind, transport.IO_Result_Kind.Would_Block)

	message := "abcdef"
	result = write_stream(sockets[1], transmute([]u8)message)
	testing.expect_value(t, result.kind, transport.IO_Result_Kind.Progress)
	testing.expect_value(t, result.transferred_bytes, len(message))

	result = read_stream(sockets[0], read_buffer[:])
	testing.expect_value(t, result.kind, transport.IO_Result_Kind.Progress)
	testing.expect_value(t, result.transferred_bytes, 3)
	testing.expect_value(t, string(read_buffer[:]), "abc")

	result = read_stream(sockets[0], read_buffer[:])
	testing.expect_value(t, result.kind, transport.IO_Result_Kind.Progress)
	testing.expect_value(t, result.transferred_bytes, 3)
	testing.expect_value(t, string(read_buffer[:]), "def")

	result = read_stream(sockets[0], read_buffer[:])
	testing.expect_value(t, result.kind, transport.IO_Result_Kind.Would_Block)

	linux.close(sockets[1])
	result = read_stream(sockets[0], read_buffer[:])
	testing.expect_value(t, result.kind, transport.IO_Result_Kind.End_Of_Stream)
	result = write_stream(sockets[0], transmute([]u8)message)
	testing.expect_value(t, result.kind, transport.IO_Result_Kind.Closed)
	linux.close(sockets[0])
}

@(test)
test_nonblocking_stream_reports_closed_descriptors :: proc(t: ^testing.T) {
	buffer: [1]u8
	result := read_stream(-1, buffer[:])
	testing.expect_value(t, result.kind, transport.IO_Result_Kind.Error)
	testing.expect_value(t, result.error_code, i32(linux.Errno.EBADF))
	result = write_stream(-1, buffer[:])
	testing.expect_value(t, result.kind, transport.IO_Result_Kind.Error)
	testing.expect_value(t, result.error_code, i32(linux.Errno.EBADF))
}
