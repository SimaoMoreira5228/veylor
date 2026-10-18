#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"

@(test)
test_signal_host_wakeup_fires_while_events_queued :: proc(t: ^testing.T) {
	fd, eventfd_error := linux.eventfd(0, {.CLOEXEC, .NONBLOCK})
	testing.expect_value(t, eventfd_error, linux.Errno.NONE)
	if eventfd_error != .NONE {
		return
	}
	defer linux.close(fd)

	storage: [4]host.Host_Event
	queue := host.init_host_queue(storage[:])
	wakeup := Host_Wakeup {
		fd = fd,
	}
	worker := Static_Worker {
		host_queue  = &queue,
		host_wakeup = &wakeup,
	}

	token := connections.Connection_Token {
		worker_id         = 1,
		worker_generation = 1,
		slot              = 0,
		generation        = 1,
	}
	get_method := "GET"
	get_path := "/x"
	testing.expect_value(
		t,
		host.enqueue_request(&queue, token, 7, transmute([]u8)get_method, transmute([]u8)get_path),
		host.Host_Queue_Result.Enqueued,
	)
	signal_host_wakeup_if_queued(&worker)

	buffer: [8]u8
	read_length, read_error := linux.read(fd, buffer[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(t, read_length, len(buffer))

	drained: [1]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&queue, drained[:]).copied, 1)
	signal_host_wakeup_if_queued(&worker)
	_, idle_error := linux.read(fd, buffer[:])
	testing.expect_value(t, idle_error, linux.Errno.EAGAIN)

	worker.host_wakeup = nil
	signal_host_wakeup_if_queued(&worker)
}

@(test)
test_signal_static_worker_notifies_control_fd :: proc(t: ^testing.T) {
	fd, fd_error := linux.eventfd(0, {.CLOEXEC, .NONBLOCK})
	testing.expect_value(t, fd_error, linux.Errno.NONE)
	if fd_error != .NONE {
		return
	}
	defer linux.close(fd)

	worker := Static_Worker {
		control_fd = fd,
	}
	testing.expect(t, signal_static_worker(&worker))

	buffer: [8]u8
	read_length, read_error := linux.read(fd, buffer[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(t, read_length, len(buffer))
	testing.expect_value(t, buffer[0], u8(1))

	testing.expect(t, !signal_static_worker(nil))
	closed := Static_Worker {
		control_fd = -1,
	}
	testing.expect(t, !signal_static_worker(&closed))
}
