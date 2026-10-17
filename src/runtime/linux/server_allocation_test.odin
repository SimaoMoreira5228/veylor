#+build linux
#+test
package linux_runtime

import "core:mem"
import "core:sys/linux"
import "core:testing"
import socket "veylor:transport/linux/socket"

static_worker_request_allocation_client :: proc(t: ^testing.T) {
	instance: Managed_Server_Test_Instance
	loopback := [4]u8{127, 0, 0, 1}
	listener, listen_result, _ := socket.listen_ipv4(loopback, 0, 4)
	testing.expect_value(t, listen_result, socket.Result.Ready)
	if listen_result != .Ready {
		return
	}
	worker_ready := init_managed_server_test_worker(&instance, 51, "/health", "ok", listener, 0)
	testing.expect_value(t, worker_ready, true)
	if !worker_ready {
		cleanup_managed_server_test_instance(&instance)
		return
	}
	client := connect_managed_server_test_client(listener.local_address.port)
	testing.expect_value(t, client >= 0, true)
	if client < 0 {
		cleanup_managed_server_test_instance(&instance)
		return
	}
	keep_alive_request := "GET /health HTTP/1.1\r\nHost: example.test\r\n\r\n"
	close_request := "GET /health HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n"
	keep_alive_response := "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
	close_response := "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok"
	expected_bytes_written := u64(0)
	completed_requests := 0
	for request_index in 0 ..< 64 {
		request := keep_alive_request
		expected_response := keep_alive_response
		if request_index == 63 {
			request = close_request
			expected_response = close_response
		}
		written, write_error := linux.write(client, transmute([]u8)request)
		testing.expect_value(t, write_error, linux.Errno.NONE)
		testing.expect_value(t, written, len(request))
		if write_error != .NONE || written != len(request) {
			break
		}
		expected_bytes_written += u64(len(expected_response))
		for _ in 0 ..< 8 {
			turn := run_static_worker_turn(&instance.worker)
			if turn.result == .Error || turn.result == .Invalid {
				testing.expect_value(t, turn.result, Static_Worker_Result.Ready)
				break
			}
			if instance.worker.metrics.bytes_written == expected_bytes_written {
				break
			}
		}
		testing.expect_value(t, instance.worker.metrics.bytes_written, expected_bytes_written)
		if instance.worker.metrics.bytes_written != expected_bytes_written {
			break
		}
		response: [256]u8
		response_length, read_error := linux.read(client, response[:])
		testing.expect_value(t, read_error, linux.Errno.NONE)
		testing.expect_value(t, string(response[:response_length]), expected_response)
		completed_requests += 1
	}
	testing.expect_value(t, completed_requests, 64)
	linux.close(client)
	for _ in 0 ..< 8 {
		active_connections := false
		for state in instance.connection_states {
			if state.active {
				active_connections = true
				break
			}
		}
		if !active_connections {
			break
		}
		run_static_worker_turn(&instance.worker)
	}
	for state in instance.connection_states {
		testing.expect_value(t, state.active, false)
	}
	testing.expect_value(t, instance.workspace_pool.acquisition_count, u64(64))
	testing.expect_value(t, instance.workspace_pool.release_count, u64(64))
	cleanup_managed_server_test_instance(&instance)
}

static_worker_request_allocation_verifier :: proc(
	t: ^testing.T,
	tracker: ^mem.Tracking_Allocator,
) {
	testing.expect_value(t, tracker.total_allocation_count, i64(0))
	testing.expect_value(t, len(tracker.allocation_map), 0)
}

@(test)
test_static_worker_reuses_bounded_workspaces_without_heap_allocations :: proc(t: ^testing.T) {
	testing.expect_leaks(
		t,
		static_worker_request_allocation_client,
		static_worker_request_allocation_verifier,
	)
}
