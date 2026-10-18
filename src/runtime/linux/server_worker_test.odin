#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"

@(test)
test_signal_host_wakeup_fires_only_on_new_events :: proc(t: ^testing.T) {
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
	signal_host_wakeup_if_queued(&worker, 0)

	buffer: [8]u8
	read_length, read_error := linux.read(fd, buffer[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(t, read_length, len(buffer))

	signal_host_wakeup_if_queued(&worker, 1)
	_, idle_error := linux.read(fd, buffer[:])
	testing.expect_value(t, idle_error, linux.Errno.EAGAIN)

	worker.host_wakeup = nil
	signal_host_wakeup_if_queued(&worker, 0)
}
