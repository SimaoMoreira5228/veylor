#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import shutdown "veylor:runtime/shutdown"
import workers "veylor:runtime/workers"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

Managed_Server_Multi_Test_Instance :: struct {
	first:          Managed_Server_Test_Instance,
	second:         Managed_Server_Test_Instance,
	server_workers: [2]^Static_Worker,
	worker_storage: [2]workers.Worker_Thread,
	server:         Managed_Server,
	initialized:    bool,
}

init_managed_server_multi_test_instance :: proc(
	instance: ^Managed_Server_Multi_Test_Instance,
	shutdown_grace_ns: u64,
	first_connection_capacity: int = 32,
	first_workspace_capacity: int = 32,
	second_connection_capacity: int = 32,
	second_workspace_capacity: int = 32,
) -> bool {
	instance.first.worker.poller.fd = -1
	instance.first.worker.listener.fd = -1
	instance.first.worker.control_fd = -1
	instance.second.worker.poller.fd = -1
	instance.second.worker.listener.fd = -1
	instance.second.worker.control_fd = -1
	loopback := [4]u8{127, 0, 0, 1}
	first_listener, first_listen_result, _ := socket.listen_ipv4(loopback, 0, 64, true)
	if first_listen_result != .Ready {
		return false
	}
	first_ready := init_managed_server_test_worker(
		&instance.first,
		31,
		"/multi",
		"first",
		first_listener,
		10,
		first_connection_capacity,
		first_workspace_capacity,
	)
	if !first_ready {
		cleanup_unmanaged_multi_test_instance(instance)
		return false
	}

	second_listener, second_listen_result, _ := socket.listen_ipv4(
		loopback,
		first_listener.local_address.port,
		64,
		true,
	)
	if second_listen_result != .Ready {
		cleanup_unmanaged_multi_test_instance(instance)
		return false
	}

	second_ready := init_managed_server_test_worker(
		&instance.second,
		32,
		"/multi",
		"other",
		second_listener,
		10,
		second_connection_capacity,
		second_workspace_capacity,
	)
	if !second_ready {
		cleanup_unmanaged_multi_test_instance(instance)
		return false
	}

	instance.server_workers[0] = &instance.first.worker
	instance.server_workers[1] = &instance.second.worker
	server_result := init_managed_server(
		&instance.server,
		instance.server_workers[:],
		instance.worker_storage[:],
		shutdown_grace_ns,
	)
	if server_result != .Initialized {
		cleanup_unmanaged_multi_test_instance(instance)
		return false
	}
	instance.initialized = true

	return start_managed_server(&instance.server) == .Started
}

cleanup_unmanaged_multi_test_instance :: proc(instance: ^Managed_Server_Multi_Test_Instance) {
	workers: [2]^Static_Worker
	workers[0] = &instance.first.worker
	workers[1] = &instance.second.worker
	for worker in workers {
		if worker.poller.fd >= 0 {
			epoll.close(&worker.poller)
		}
		if worker.listener.fd >= 0 {
			socket.close_listener(&worker.listener)
		}
	}
}

cleanup_managed_server_multi_test_instance :: proc(instance: ^Managed_Server_Multi_Test_Instance) {
	if instance.initialized {
		if instance.server.phase == .Running {
			request_managed_server_stop(&instance.server)
		}

		if instance.server.phase == .Stop_Requested {
			join_managed_server(&instance.server)
		}

		if instance.server.phase == .Joined || instance.server.phase == .Unstarted {
			destroy_managed_server(&instance.server)
		}
		return
	}
	cleanup_unmanaged_multi_test_instance(instance)
}

@(test)
test_managed_server_gracefully_drains_connections_from_each_worker :: proc(t: ^testing.T) {
	instance: Managed_Server_Multi_Test_Instance
	initialized := init_managed_server_multi_test_instance(&instance, 5_000_000_000)
	testing.expect_value(t, initialized, true)
	if !initialized {
		cleanup_managed_server_multi_test_instance(&instance)
		return
	}

	clients: [32]linux.Fd
	requests_ready := prepare_multi_worker_test_clients(
		instance.first.worker.listener.local_address.port,
		clients[:],
	)
	testing.expect_value(t, requests_ready, true)
	if !requests_ready {
		cleanup_managed_server_multi_test_instance(&instance)
		return
	}

	stop_result := request_managed_server_stop(&instance.server)
	testing.expect_value(t, stop_result, Managed_Server_Result.Stop_Requested)
	for client in clients {
		linux.close(client)
	}
	join_result := join_managed_server(&instance.server)
	testing.expect_value(t, join_result, Managed_Server_Result.Joined)
	expect_multi_worker_shutdown_complete(t, &instance)
	testing.expect_value(
		t,
		destroy_managed_server(&instance.server),
		Managed_Server_Result.Destroyed,
	)
}

@(test)
test_managed_server_force_stops_connections_from_each_worker :: proc(t: ^testing.T) {
	instance: Managed_Server_Multi_Test_Instance
	initialized := init_managed_server_multi_test_instance(&instance, 0)
	testing.expect_value(t, initialized, true)
	if !initialized {
		cleanup_managed_server_multi_test_instance(&instance)
		return
	}

	clients: [32]linux.Fd
	requests_ready := prepare_multi_worker_test_clients(
		instance.first.worker.listener.local_address.port,
		clients[:],
	)
	testing.expect_value(t, requests_ready, true)
	if !requests_ready {
		cleanup_managed_server_multi_test_instance(&instance)
		return
	}

	stop_result := request_managed_server_force_stop(&instance.server)
	testing.expect_value(t, stop_result, Managed_Server_Result.Stop_Requested)
	join_result := join_managed_server(&instance.server)
	testing.expect_value(t, join_result, Managed_Server_Result.Joined)
	expect_multi_worker_shutdown_complete(t, &instance)
	for client in clients {
		response: [1]u8
		response_length, read_error := linux.read(client, response[:])
		testing.expect_value(t, read_error, linux.Errno.NONE)
		testing.expect_value(t, response_length, 0)
		linux.close(client)
	}
	testing.expect_value(
		t,
		destroy_managed_server(&instance.server),
		Managed_Server_Result.Destroyed,
	)
}

@(test)
test_managed_server_worker_error_forces_every_worker_to_stop :: proc(t: ^testing.T) {
	instance: Managed_Server_Multi_Test_Instance
	initialized := init_managed_server_multi_test_instance(&instance, 5_000_000_000)
	testing.expect_value(t, initialized, true)
	if !initialized {
		cleanup_managed_server_multi_test_instance(&instance)
		return
	}

	clients: [32]linux.Fd
	requests_ready := prepare_multi_worker_test_clients(
		instance.first.worker.listener.local_address.port,
		clients[:],
	)
	testing.expect_value(t, requests_ready, true)
	if !requests_ready {
		cleanup_managed_server_multi_test_instance(&instance)
		return
	}

	poller_fd := instance.first.worker.poller.fd
	close_error := linux.close(poller_fd)
	testing.expect_value(t, close_error, linux.Errno.NONE)
	stop_result := request_managed_server_stop(&instance.server)
	testing.expect_value(t, stop_result, Managed_Server_Result.Stop_Requested)
	join_result := join_managed_server(&instance.server)
	testing.expect_value(t, join_result, Managed_Server_Result.Worker_Error)
	testing.expect_value(t, instance.first.worker.managed_result, Static_Worker_Result.Error)
	testing.expect_value(t, instance.second.worker.managed_result, Static_Worker_Result.Ready)
	testing.expect_value(t, instance.first.shutdown_state.active_connections, u64(0))
	testing.expect_value(t, instance.second.shutdown_state.active_connections, u64(0))
	testing.expect_value(t, instance.first.shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, instance.second.shutdown_state.phase, shutdown.Phase.Complete)
	for client in clients {
		linux.close(client)
	}
	instance.first.worker.poller.fd = -1
	instance.first.worker.control_registered = false
	testing.expect_value(
		t,
		destroy_managed_server(&instance.server),
		Managed_Server_Result.Destroyed,
	)
}

@(test)
test_managed_server_workspace_exhaustion_is_worker_local_and_recovers :: proc(t: ^testing.T) {
	instance: Managed_Server_Multi_Test_Instance
	initialized := init_managed_server_multi_test_instance(&instance, 5_000_000_000, 32, 32, 2, 1)
	testing.expect_value(t, initialized, true)
	if !initialized {
		cleanup_managed_server_multi_test_instance(&instance)
		return
	}

	port := instance.first.worker.listener.local_address.port
	other_holder := connect_multi_worker_test_client(port, "other", 64)
	other_probe := connect_multi_worker_test_client(port, "other", 64)
	testing.expect_value(t, other_holder >= 0 && other_probe >= 0, true)
	if other_holder < 0 || other_probe < 0 {
		clients := [?]linux.Fd{other_holder, other_probe}
		for client in clients {
			if client >= 0 {
				linux.close(client)
			}
		}
		cleanup_managed_server_multi_test_instance(&instance)
		return
	}
	other_partial_written := write_partial_multi_worker_request(other_holder)
	testing.expect_value(t, other_partial_written, true)
	wait_for_multi_worker_request_processing()
	testing.expect_value(t, write_multi_worker_request(other_probe), true)
	testing.expect_value(t, read_multi_worker_request_closed(other_probe), true)
	linux.close(other_probe)

	first_worker_client := connect_multi_worker_test_client(port, "first", 128)
	testing.expect_value(t, first_worker_client >= 0, true)
	if first_worker_client >= 0 {
		linux.close(first_worker_client)
	}
	linux.close(other_holder)
	wait_for_multi_worker_request_processing()
	recovered_client := connect_multi_worker_test_client(port, "other", 256)
	testing.expect_value(t, recovered_client >= 0, true)
	if recovered_client >= 0 {
		linux.close(recovered_client)
	}
	stop_result := request_managed_server_stop(&instance.server)
	testing.expect_value(t, stop_result, Managed_Server_Result.Stop_Requested)
	join_result := join_managed_server(&instance.server)
	testing.expect_value(t, join_result, Managed_Server_Result.Joined)
	workspace_exhausted_index := int(Static_Connection_Close_Reason.Workspace_Exhausted)
	workspace_exhausted_count :=
		instance.second.worker.metrics.close_reasons[workspace_exhausted_index]
	testing.expect_value(t, workspace_exhausted_count > 0, true)
	testing.expect_value(t, instance.first.worker.workspace_pool.free_count, 32)
	testing.expect_value(t, instance.second.worker.workspace_pool.free_count, 1)
	testing.expect_value(t, instance.first.shutdown_state.active_connections, u64(0))
	testing.expect_value(t, instance.second.shutdown_state.active_connections, u64(0))
	testing.expect_value(
		t,
		destroy_managed_server(&instance.server),
		Managed_Server_Result.Destroyed,
	)
}

connect_multi_worker_test_client :: proc(
	port: u16,
	expected_body: string,
	attempts: int,
) -> linux.Fd {
	for _ in 0 ..< attempts {
		client := connect_managed_server_test_client(port)
		if client < 0 {
			continue
		}
		if !write_multi_worker_request(client) {
			linux.close(client)
			continue
		}
		response: [256]u8
		response_length, read_error := linux.read(client, response[:])
		response_text := string(response[:response_length])
		response_prefix := "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n"
		if read_error == .NONE &&
		   len(response_text) == len(response_prefix) + len(expected_body) &&
		   response_text[:len(response_prefix)] == response_prefix &&
		   response_text[len(response_prefix):] == expected_body {
			return client
		}
		linux.close(client)
	}
	return -1
}

write_multi_worker_request :: proc(client: linux.Fd) -> bool {
	request := "GET /multi HTTP/1.1\r\nHost: example.test\r\n\r\n"
	written, write_error := linux.write(client, transmute([]u8)request)
	return write_error == .NONE && written == len(request)
}

write_partial_multi_worker_request :: proc(client: linux.Fd) -> bool {
	request := "GET /multi HTTP/1.1\r\nHost: example.test\r\n"
	written, write_error := linux.write(client, transmute([]u8)request)
	return write_error == .NONE && written == len(request)
}

wait_for_multi_worker_request_processing :: proc() {
	sleep := linux.Time_Spec {
		time_sec  = 0,
		time_nsec = 100_000_000,
	}
	for {
		remaining: linux.Time_Spec
		sleep_error := linux.nanosleep(&sleep, &remaining)
		if sleep_error != .EINTR {
			return
		}
		sleep = remaining
	}
}

read_multi_worker_request_closed :: proc(client: linux.Fd) -> bool {
	response: [256]u8
	response_length, read_error := linux.read(client, response[:])
	return (read_error == .NONE && response_length == 0) || read_error == .ECONNRESET
}

prepare_multi_worker_test_clients :: proc(port: u16, clients: []linux.Fd) -> bool {
	if len(clients) == 0 {
		return false
	}
	connected := 0
	for i := 0; i < len(clients); i += 1 {
		client := connect_managed_server_test_client(port)
		if client < 0 {
			break
		}
		clients[i] = client
		connected += 1
	}
	if connected != len(clients) {
		for i := 0; i < connected; i += 1 {
			linux.close(clients[i])
		}
		return false
	}
	request := "GET /multi HTTP/1.1\r\nHost: example.test\r\n\r\n"
	for client in clients {
		written, write_error := linux.write(client, transmute([]u8)request)
		if write_error != .NONE || written != len(request) {
			close_multi_worker_test_clients(clients[:connected])
			return false
		}
	}
	for client in clients {
		response: [256]u8
		response_length, read_error := linux.read(client, response[:])
		response_text := string(response[:response_length])
		first_response := "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst"
		other_response := "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nother"
		if read_error != .NONE {
			close_multi_worker_test_clients(clients[:connected])
			return false
		}
		if response_text != first_response && response_text != other_response {
			close_multi_worker_test_clients(clients[:connected])
			return false
		}
	}
	return true
}

close_multi_worker_test_clients :: proc(clients: []linux.Fd) {
	for client in clients {
		linux.close(client)
	}
}

expect_multi_worker_shutdown_complete :: proc(
	t: ^testing.T,
	instance: ^Managed_Server_Multi_Test_Instance,
) {
	testing.expect_value(t, instance.first.worker.managed_result, Static_Worker_Result.Ready)
	testing.expect_value(t, instance.second.worker.managed_result, Static_Worker_Result.Ready)
	testing.expect_value(t, instance.first.worker.metrics.accepted_connections > 0, true)
	testing.expect_value(t, instance.second.worker.metrics.accepted_connections > 0, true)
	testing.expect_value(t, instance.first.shutdown_state.active_connections, u64(0))
	testing.expect_value(t, instance.second.shutdown_state.active_connections, u64(0))
	testing.expect_value(t, instance.first.shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, instance.second.shutdown_state.phase, shutdown.Phase.Complete)
}
