#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import shutdown "veylor:runtime/shutdown"

@(test)
test_managed_server_force_stop_cancels_a_live_keep_alive_connection :: proc(t: ^testing.T) {
	instance: Managed_Server_Test_Instance
	initialized := init_managed_server_test_instance(
		&instance,
		24,
		"/cancel",
		"cancel",
		10_000_000_000,
	)
	testing.expect_value(t, initialized, true)
	if !initialized {
		cleanup_managed_server_test_instance(&instance)
		return
	}

	client := connect_managed_server_test_client(instance.worker.listener.local_address.port)
	if client < 0 {
		cleanup_managed_server_test_instance(&instance)
		testing.expect_value(t, client >= 0, true)
		return
	}

	request := "GET /cancel HTTP/1.1\r\nHost: cancel.example\r\n\r\n"
	written, write_error := linux.write(client, transmute([]u8)request)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, written, len(request))
	response: [128]u8
	response_length, read_error := linux.read(client, response[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(
		t,
		string(response[:response_length]),
		"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\ncancel",
	)

	stop_result := request_managed_server_stop(&instance.server)
	testing.expect_value(t, stop_result, Managed_Server_Result.Stop_Requested)
	force_result := request_managed_server_force_stop(&instance.server)
	testing.expect_value(t, force_result, Managed_Server_Result.Stop_Requested)
	testing.expect_value(
		t,
		request_managed_server_force_stop(&instance.server),
		Managed_Server_Result.Already_Stopping,
	)
	join_result := join_managed_server(&instance.server)
	testing.expect_value(t, join_result, Managed_Server_Result.Joined)
	testing.expect_value(t, instance.worker.managed_result, Static_Worker_Result.Ready)
	testing.expect_value(t, instance.worker.shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, instance.worker.shutdown_state.active_connections, u64(0))
	testing.expect_value(t, instance.worker.timers.count, 0)

	close_length, close_error := linux.read(client, response[:])
	testing.expect_value(t, close_error, linux.Errno.NONE)
	testing.expect_value(t, close_length, 0)
	linux.close(client)
	testing.expect_value(
		t,
		destroy_managed_server(&instance.server),
		Managed_Server_Result.Destroyed,
	)
}

@(test)
test_managed_server_force_stop_from_running_completes :: proc(t: ^testing.T) {
	instance: Managed_Server_Test_Instance
	initialized := init_managed_server_test_instance(
		&instance,
		25,
		"/cancel-empty",
		"empty",
		10_000_000_000,
	)
	testing.expect_value(t, initialized, true)
	if !initialized {
		cleanup_managed_server_test_instance(&instance)
		return
	}

	force_result := request_managed_server_force_stop(&instance.server)
	testing.expect_value(t, force_result, Managed_Server_Result.Stop_Requested)
	join_result := join_managed_server(&instance.server)
	testing.expect_value(t, join_result, Managed_Server_Result.Joined)
	testing.expect_value(t, instance.worker.managed_result, Static_Worker_Result.Ready)
	testing.expect_value(t, instance.worker.shutdown_state.phase, shutdown.Phase.Complete)
	testing.expect_value(t, instance.worker.shutdown_state.active_connections, u64(0))
	testing.expect_value(t, instance.worker.timers.count, 0)
	testing.expect_value(
		t,
		destroy_managed_server(&instance.server),
		Managed_Server_Result.Destroyed,
	)
}
