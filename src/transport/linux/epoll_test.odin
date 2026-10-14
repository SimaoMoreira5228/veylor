#+build linux
#+test
package epoll

import "core:sys/linux"
import "core:testing"
import "veylor:transport"

@(test)
test_epoll_reports_readiness_writeability_and_peer_half_close :: proc(t: ^testing.T) {
	poller, result, error_code := create()
	testing.expect_value(t, result, Result.Ready)
	testing.expect_value(t, error_code, i32(0))

	if result != .Ready {
		return
	}

	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		close(&poller)
		return
	}

	result, error_code = register(poller, sockets[0], 77, Interest{readable = true})
	testing.expect_value(t, result, Result.Ready)
	testing.expect_value(t, error_code, i32(0))

	if result != .Ready {
		linux.close(sockets[0])
		linux.close(sockets[1])
		close(&poller)
		return
	}

	message := "ping"
	write_length, write_error := linux.write(sockets[1], transmute([]u8)message)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(message))

	if write_error != .NONE {
		unregister(poller, sockets[0])
		linux.close(sockets[0])
		linux.close(sockets[1])
		close(&poller)
		return
	}

	native_events: [4]linux.EPoll_Event
	ready_events: [4]transport.Readiness_Event
	batch := wait(poller, native_events[:], ready_events[:], 100)
	testing.expect_value(t, batch.result, Result.Ready)
	testing.expect_value(t, len(batch.events), 1)

	if len(batch.events) == 1 {
		testing.expect_value(t, batch.events[0].connection_id, u64(77))
		testing.expect_value(t, batch.events[0].readable, true)
	}

	read_buffer: [4]u8
	read_length, read_error := linux.read(sockets[0], read_buffer[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(t, string(read_buffer[:read_length]), message)

	result, error_code = modify(poller, sockets[0], 77, Interest{writable = true})
	testing.expect_value(t, result, Result.Ready)
	testing.expect_value(t, error_code, i32(0))
	batch = wait(poller, native_events[:], ready_events[:], 100)
	testing.expect_value(t, batch.result, Result.Ready)

	if len(batch.events) == 1 {
		testing.expect_value(t, batch.events[0].writable, true)
	}

	linux.close(sockets[1])
	batch = wait(poller, native_events[:], ready_events[:], 100)
	testing.expect_value(t, batch.result, Result.Ready)

	if len(batch.events) == 1 {
		testing.expect_value(t, batch.events[0].peer_closed, true)
	}

	result, error_code = unregister(poller, sockets[0])
	testing.expect_value(t, result, Result.Ready)
	testing.expect_value(t, error_code, i32(0))
	linux.close(sockets[0])
	result, error_code = close(&poller)
	testing.expect_value(t, result, Result.Ready)
	testing.expect_value(t, error_code, i32(0))
}

@(test)
test_epoll_timeout_uses_caller_storage_without_emitting_events :: proc(t: ^testing.T) {
	poller, result, error_code := create()
	testing.expect_value(t, result, Result.Ready)
	testing.expect_value(t, error_code, i32(0))

	if result != .Ready {
		return
	}

	native_events: [2]linux.EPoll_Event
	ready_events: [2]transport.Readiness_Event
	batch := wait(poller, native_events[:], ready_events[:], 0)
	testing.expect_value(t, batch.result, Result.Timeout)
	testing.expect_value(t, len(batch.events), 0)

	result, error_code = close(&poller)
	testing.expect_value(t, result, Result.Ready)
	testing.expect_value(t, error_code, i32(0))
}
