#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

@(test)
test_managed_body_stages_allowed_trailers :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	allow := []string{"x-checksum", "x-signature"}
	request := "POST /upload HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nX-Checksum: abc123\r\nX-Signature: s3cr3t\r\n\r\n"

	if !setup_managed_body_fixture(
		t,
		&fixture,
		"POST",
		"/upload",
		request,
		16 * 1024 * 1024,
		allow,
	) {
		close_managed_body_fixture(&fixture)

		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	complete := false
	saw_available := false

	for _ in 0 ..< 5 {
		cmd := host.Host_Command {
			kind      = .Read_Body,
			token     = polled[0].token,
			max_bytes = 1024,
		}
		testing.expect_value(
			t,
			host.enqueue_host_command(&fixture.command_queue, cmd),
			host.Host_Command_Result.Enqueued,
		)
		drain_host_commands(&fixture.worker, 2000)
		notified := host.poll_host_queue(&fixture.host_queue, polled[:])

		if notified.copied == 0 {
			continue
		}
		testing.expect_value(t, notified.copied, 1)
		testing.expect_value(t, polled[0].kind, host.Host_Event_Kind.Body_Available)
		saw_available = true

		if polled[0].body_complete == 1 {
			complete = true
			break
		}
	}
	testing.expect(t, saw_available)
	testing.expect(t, complete)
	testing.expect_value(t, fixture.workspace_storage[0].trailer_count, 2)
	trailer_data := fixture.workspace_storage[0].trailer_data[:]
	first := fixture.workspace_storage[0].trailer_fields[0]
	testing.expect_value(
		t,
		string(trailer_data[first.name_offset:first.name_offset + first.name_length]),
		"X-Checksum",
	)
	testing.expect_value(
		t,
		string(trailer_data[first.value_offset:first.value_offset + first.value_length]),
		"abc123",
	)
	second := fixture.workspace_storage[0].trailer_fields[1]
	testing.expect_value(
		t,
		string(trailer_data[second.name_offset:second.name_offset + second.name_length]),
		"X-Signature",
	)
	testing.expect_value(
		t,
		string(trailer_data[second.value_offset:second.value_offset + second.value_length]),
		"s3cr3t",
	)
	close_managed_body_fixture(&fixture)
}

@(test)
test_managed_body_rejects_disallowed_trailer :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	allow := []string{"x-checksum"}
	request := "POST /upload HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nX-Evil: 1\r\n\r\n"

	if !setup_managed_body_fixture(
		t,
		&fixture,
		"POST",
		"/upload",
		request,
		16 * 1024 * 1024,
		allow,
	) {
		close_managed_body_fixture(&fixture)

		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)

	cmd := host.Host_Command {
		kind      = .Read_Body,
		token     = polled[0].token,
		max_bytes = 1024,
	}
	testing.expect_value(
		t,
		host.enqueue_host_command(&fixture.command_queue, cmd),
		host.Host_Command_Result.Enqueued,
	)
	drain_host_commands(&fixture.worker, 2000)
	testing.expect_value(t, fixture.worker_states[0].phase, Static_Connection_Phase.Failed)
	testing.expect_value(
		t,
		fixture.worker_states[0].close_reason,
		Static_Connection_Close_Reason.Internal_Error,
	)
	close_managed_body_fixture(&fixture)
}
