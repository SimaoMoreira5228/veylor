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

@(test)
test_nonblocking_write_resumes_after_a_slow_peer_drains_data :: proc(t: ^testing.T) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}

	payload: [4096]u8
	write_progress := 0
	blocked := false
	for attempt in 0 ..< 2048 {
		result := write_stream(sockets[0], payload[:])

		if result.kind == .Would_Block {
			blocked = true
			break
		}

		if result.kind != .Progress || result.transferred_bytes <= 0 {
			testing.expect_value(t, result.kind, transport.IO_Result_Kind.Progress)
			break
		}
		write_progress += result.transferred_bytes
	}

	testing.expect_value(t, write_progress > 0, true)
	testing.expect_value(t, blocked, true)

	read_buffer: [8192]u8
	read_result := read_stream(sockets[1], read_buffer[:])
	testing.expect_value(t, read_result.kind, transport.IO_Result_Kind.Progress)
	testing.expect_value(t, read_result.transferred_bytes > 0, true)

	resume_message := "resume"
	write_result := write_stream(sockets[0], transmute([]u8)resume_message)
	testing.expect_value(t, write_result.kind, transport.IO_Result_Kind.Progress)
	testing.expect_value(t, write_result.transferred_bytes, len(resume_message))
	linux.close(sockets[0])
	linux.close(sockets[1])
}
