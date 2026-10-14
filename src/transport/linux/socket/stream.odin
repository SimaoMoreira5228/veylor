#+build linux
package socket

import "core:sys/linux"
import "veylor:transport"

read_stream :: proc(fd: linux.Fd, buffer: []u8) -> transport.IO_Result {
	if fd < 0 {
		return transport.IO_Result{kind = .Error, error_code = i32(linux.Errno.EBADF)}
	}

	if len(buffer) == 0 {
		return transport.IO_Result{kind = .Progress}
	}

	transferred, err := linux.recv(fd, buffer, {.DONTWAIT})

	if err != .NONE {
		return socket_io_error(err)
	}

	if transferred == 0 {
		return transport.IO_Result{kind = .End_Of_Stream}
	}

	return transport.IO_Result{kind = .Progress, transferred_bytes = transferred}
}

write_stream :: proc(fd: linux.Fd, buffer: []u8) -> transport.IO_Result {
	if fd < 0 {
		return transport.IO_Result{kind = .Error, error_code = i32(linux.Errno.EBADF)}
	}

	if len(buffer) == 0 {
		return transport.IO_Result{kind = .Progress}
	}

	transferred, err := linux.send(fd, buffer, {.DONTWAIT, .NOSIGNAL})

	if err != .NONE {
		return socket_io_error(err)
	}

	if transferred == 0 {
		return transport.IO_Result{kind = .Would_Block}
	}

	return transport.IO_Result{kind = .Progress, transferred_bytes = transferred}
}

socket_io_error :: proc(err: linux.Errno) -> transport.IO_Result {
	if err == .EAGAIN {
		return transport.IO_Result{kind = .Would_Block}
	}

	if err == .EINTR {
		return transport.IO_Result{kind = .Interrupted}
	}

	if err == .EPIPE || err == .ECONNRESET || err == .ENOTCONN {
		return transport.IO_Result{kind = .Closed}
	}

	return transport.IO_Result{kind = .Error, error_code = i32(err)}
}
