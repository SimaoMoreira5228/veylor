#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import connections "veylor:runtime/connections"
import shutdown "veylor:runtime/shutdown"
import socket "veylor:transport/linux/socket"

@(test)
test_static_worker_streams_mounted_files_and_rejects_encoded_traversal :: proc(t: ^testing.T) {
	instance: Managed_Server_Test_Instance
	instance.worker.poller.fd = -1
	instance.worker.listener.fd = -1
	instance.worker.control_fd = -1

	loopback := [4]u8{127, 0, 0, 1}
	listener, listen_result, _ := socket.listen_ipv4(loopback, 0, 4)
	testing.expect_value(t, listen_result, socket.Result.Ready)
	if listen_result != .Ready {
		return
	}

	root_fd, root_error := linux.open("examples/native_static/public", {.CLOEXEC, .DIRECTORY})
	testing.expect_value(t, root_error, linux.Errno.NONE)
	if root_error != .NONE {
		socket.close_listener(&listener)
		return
	}

	mount_prefix := "/assets/"
	file_root: Static_File_Root
	root_ready := init_static_file_root(&file_root, root_fd, transmute([]u8)mount_prefix)
	testing.expect_value(t, root_ready, true)
	if !root_ready {
		linux.close(root_fd)
		socket.close_listener(&listener)
		return
	}

	worker_ready := init_managed_server_test_worker(&instance, 52, "/health", "ok", listener, 0)
	testing.expect_value(t, worker_ready, true)
	if !worker_ready {
		cleanup_managed_server_test_instance(&instance)
		linux.close(root_fd)
		return
	}

	instance.worker.file_root = &file_root
	client := connect_managed_server_test_client(listener.local_address.port)
	testing.expect_value(t, client >= 0, true)
	if client < 0 {
		cleanup_managed_server_test_instance(&instance)
		linux.close(root_fd)
		return
	}

	file_response :=
		"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n" +
		"Content-Length: 12\r\n\r\nVeylor file\n"
	file_request := "GET /assets/hello.txt HTTP/1.1\r\nHost: example.test\r\n\r\n"
	expect_static_file_response(t, &instance.worker, client, file_request, file_response)

	head_response :=
		"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n" +
		"Content-Length: 12\r\n\r\n"
	head_request := "HEAD /assets/hello.txt HTTP/1.1\r\nHost: example.test\r\n\r\n"
	expect_static_file_response(t, &instance.worker, client, head_request, head_response)

	traversal_response := "HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
	traversal_request := "GET /assets/%2e%2e/README.md HTTP/1.1\r\nHost: example.test\r\n\r\n"
	expect_static_file_response(t, &instance.worker, client, traversal_request, traversal_response)
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
	cleanup_managed_server_test_instance(&instance)
	linux.close(root_fd)
}

@(test)
test_forced_shutdown_closes_open_static_file :: proc(t: ^testing.T) {
	instance: Managed_Server_Test_Instance
	instance.worker.poller.fd = -1
	instance.worker.listener.fd = -1
	instance.worker.control_fd = -1

	loopback := [4]u8{127, 0, 0, 1}
	listener, listen_result, _ := socket.listen_ipv4(loopback, 0, 4)
	testing.expect_value(t, listen_result, socket.Result.Ready)
	if listen_result != .Ready {
		return
	}

	if !init_managed_server_test_worker(&instance, 53, "/health", "ok", listener, 0) {
		cleanup_managed_server_test_instance(&instance)
		return
	}

	contents: [STATIC_FILE_TEST_LARGE_SIZE]u8
	for i := 0; i < len(contents); i += 1 {
		contents[i] = u8(i % 251)
	}
	file, file_ready := create_static_file_test_file(contents[:])
	testing.expect_value(t, file_ready, true)
	if !file_ready {
		cleanup_managed_server_test_instance(&instance)
		return
	}
	defer close_static_file(&file)

	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		cleanup_managed_server_test_instance(&instance)
		return
	}
	defer {
		if sockets[0] >= 0 {
			linux.close(sockets[0])
		}
		if sockets[1] >= 0 {
			linux.close(sockets[1])
		}
	}

	send_buffer_size := i32(4096)
	socket_error = linux.setsockopt(sockets[0], .SOCKET, .SNDBUF, &send_buffer_size)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		cleanup_managed_server_test_instance(&instance)
		return
	}
	socket_flags, flags_error := linux.fcntl_getfl(sockets[0], .GETFL)
	testing.expect_value(t, flags_error, linux.Errno.NONE)
	if flags_error != .NONE {
		cleanup_managed_server_test_instance(&instance)
		return
	}
	flags_error = linux.fcntl_setfl(sockets[0], .SETFL, socket_flags + {.NONBLOCK})
	testing.expect_value(t, flags_error, linux.Errno.NONE)
	if flags_error != .NONE {
		cleanup_managed_server_test_instance(&instance)
		return
	}

	token, allocation_result := connections.allocate_connection(
		instance.worker.table,
		u64(sockets[0]),
	)
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)
	if allocation_result != .Ready {
		cleanup_managed_server_test_instance(&instance)
		return
	}

	shutdown_result := shutdown.register_connection(instance.worker.shutdown_state)
	testing.expect_value(t, shutdown_result, shutdown.Result.Accepted)
	if shutdown_result != .Accepted {
		connections.release_connection(instance.worker.table, token)
		cleanup_managed_server_test_instance(&instance)
		return
	}

	state, state_result := init_static_connection(
		Worker_Connection{token = token, fd = sockets[0]},
		instance.worker.workspace_pool,
	)
	testing.expect_value(t, state_result, Static_Connection_Result.Need_More)
	if state_result != .Need_More {
		connections.release_connection(instance.worker.table, token)
		shutdown.close_connection(instance.worker.shutdown_state, instance.worker.timers)
		cleanup_managed_server_test_instance(&instance)
		return
	}

	state.has_file = true
	state.file = file
	state.file_offset = 0
	state.file_remaining = file.size
	state.phase = .Writing_File_Body
	file.fd = -1
	file_turn := write_static_file_turn(
		&state.file,
		state.fd,
		&state.file_offset,
		&state.file_remaining,
		64 * 1024,
	)
	testing.expect_value(t, file_turn.result, Static_File_Write_Result.Progress)
	testing.expect_value(t, state.file_remaining > 0, true)

	instance.connection_states[int(token.slot)] = state
	sockets[0] = -1

	shutdown_result = shutdown.force_shutdown(
		instance.worker.shutdown_state,
		instance.worker.timers,
	)
	testing.expect_value(t, shutdown_result, shutdown.Result.Force_Connections)

	turn := run_static_worker_turn(&instance.worker)
	testing.expect_value(t, turn.result, Static_Worker_Result.Ready)
	testing.expect_value(t, instance.connection_states[int(token.slot)].active, false)
	testing.expect_value(t, instance.worker.shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, instance.worker.shutdown_state.active_connections, u64(0))
	testing.expect_value(
		t,
		instance.worker.metrics.close_reasons[int(Static_Connection_Close_Reason.Shutdown_Forced)],
		u64(1),
	)

	if instance.connection_states[int(token.slot)].active {
		close_static_worker_connection(
			&instance.worker,
			&instance.connection_states[int(token.slot)],
		)
	}

	cleanup_managed_server_test_instance(&instance)
}

expect_static_file_response :: proc(
	t: ^testing.T,
	worker: ^Static_Worker,
	client: linux.Fd,
	request, expected_response: string,
) {
	written, write_error := linux.write(client, transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	if write_error != .NONE || written != len(request) {
		return
	}

	expected_bytes_written := worker.metrics.bytes_written + u64(len(expected_response))
	for _ in 0 ..< 32 {
		turn := run_static_worker_turn(worker)
		if turn.result == .Error || turn.result == .Invalid {
			testing.expect_value(t, turn.result, Static_Worker_Result.Ready)
			return
		}
		if worker.metrics.bytes_written >= expected_bytes_written {
			break
		}
	}

	testing.expect_value(t, worker.metrics.bytes_written, expected_bytes_written)
	if worker.metrics.bytes_written != expected_bytes_written {
		return
	}

	response: [4096]u8
	response_length := 0
	for response_length < len(expected_response) {
		read_length, read_error := linux.read(client, response[response_length:])
		if read_error == .EINTR {
			continue
		}
		testing.expect_value(t, read_error, linux.Errno.NONE)
		if read_error != .NONE || read_length <= 0 {
			break
		}
		response_length += read_length
	}
	testing.expect_value(t, string(response[:response_length]), expected_response)
}
