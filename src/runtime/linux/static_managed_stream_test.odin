#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"

make_managed_stream_begin :: proc(
	token: connections.Connection_Token,
	status: i32,
	reason: string,
) -> host.Host_Command {
	cmd := host.Host_Command {
		kind   = .Response_Begin,
		token  = token,
		status = status,
	}
	copy(cmd.reason[:len(reason)], transmute([]u8)reason)
	cmd.reason_length = u32(len(reason))
	return cmd
}

make_managed_stream_write :: proc(
	token: connections.Connection_Token,
	data: string,
) -> host.Host_Command {
	cmd := host.Host_Command {
		kind  = .Response_Write,
		token = token,
	}
	copy(cmd.body[:len(data)], transmute([]u8)data)
	cmd.body_length = u32(len(data))
	return cmd
}

drain_managed_stream_command :: proc(
	t: ^testing.T,
	fixture: ^Managed_Body_Fixture,
	cmd: host.Host_Command,
) {
	testing.expect_value(
		t,
		host.enqueue_host_command(&fixture.command_queue, cmd),
		host.Host_Command_Result.Enqueued,
	)

	drain_host_commands(&fixture.worker, 2000)
}

expect_managed_writable :: proc(t: ^testing.T, fixture: ^Managed_Body_Fixture) {
	polled: [4]host.Host_Event
	notified := host.poll_host_queue(&fixture.host_queue, polled[:])
	testing.expect_value(t, notified.copied, 1)
	testing.expect_value(t, polled[0].kind, host.Host_Event_Kind.Writable)
}

@(test)
test_encode_stream_chunk_frames_data :: proc(t: ^testing.T) {
	out: [32]u8
	written, ok := encode_stream_chunk(transmute([]u8)string("hello"), out[:])
	testing.expect(t, ok)
	testing.expect_value(t, string(out[:written]), "5\r\nhello\r\n")

	single: [8]u8
	single_written, single_ok := encode_stream_chunk(transmute([]u8)string("x"), single[:])
	testing.expect(t, single_ok)
	testing.expect_value(t, string(single[:single_written]), "1\r\nx\r\n")

	empty: [8]u8
	_, empty_ok := encode_stream_chunk(nil, empty[:])
	testing.expect(t, !empty_ok)

	short: [4]u8
	_, short_ok := encode_stream_chunk(transmute([]u8)string("hello"), short[:])
	testing.expect(t, !short_ok)

	oversize_storage: [host.HOST_MAX_RESPONSE_BODY_BYTES + 1]u8
	oversize_out: [host.HOST_MAX_RESPONSE_BODY_BYTES + 16]u8
	_, oversize_ok := encode_stream_chunk(oversize_storage[:], oversize_out[:])
	testing.expect(t, !oversize_ok)
}

@(test)
test_managed_stream_serves_chunked_body :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "GET /stream HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n"

	if !setup_managed_body_fixture(t, &fixture, "GET", "/stream", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)

		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	stream_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	drain_managed_stream_command(t, &fixture, make_managed_stream_begin(stream_token, 200, "OK"))
	testing.expect_value(
		t,
		fixture.worker_states[0].phase,
		Static_Connection_Phase.Writing_Response,
	)

	turn := drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Response_Progress)
	expect_managed_writable(t, &fixture)

	head_buffer: [256]u8
	testing.expect_value(
		t,
		read_managed_socket_text(t, &fixture, head_buffer[:]),
		"HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n",
	)

	drain_managed_stream_command(t, &fixture, make_managed_stream_write(stream_token, "hello"))
	turn = drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Response_Progress)
	expect_managed_writable(t, &fixture)

	first_buffer: [32]u8
	testing.expect_value(
		t,
		read_managed_socket_text(t, &fixture, first_buffer[:]),
		"5\r\nhello\r\n",
	)

	drain_managed_stream_command(t, &fixture, make_managed_stream_write(stream_token, "world!"))
	turn = drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Response_Progress)
	expect_managed_writable(t, &fixture)

	second_buffer: [32]u8
	testing.expect_value(
		t,
		read_managed_socket_text(t, &fixture, second_buffer[:]),
		"6\r\nworld!\r\n",
	)

	end_cmd := host.Host_Command {
		kind  = .Response_End,
		token = stream_token,
	}
	drain_managed_stream_command(t, &fixture, end_cmd)
	turn = drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Closed)

	terminal_buffer: [16]u8
	testing.expect_value(t, read_managed_socket_text(t, &fixture, terminal_buffer[:]), "0\r\n\r\n")
	close_managed_body_fixture(&fixture)
}

@(test)
test_managed_stream_drops_write_without_room :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "GET /stream HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n"

	if !setup_managed_body_fixture(t, &fixture, "GET", "/stream", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)

		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	stream_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	drain_managed_stream_command(t, &fixture, make_managed_stream_begin(stream_token, 200, "OK"))

	big: [host.HOST_MAX_RESPONSE_BODY_BYTES]u8

	for i := 0; i < len(big); i += 1 {
		big[i] = 'a'
	}
	first := host.Host_Command {
		kind  = .Response_Write,
		token = stream_token,
	}
	copy(first.body[:], big[:])
	first.body_length = u32(len(big))
	drain_managed_stream_command(t, &fixture, first)
	testing.expect_value(
		t,
		fixture.worker_states[0].phase,
		Static_Connection_Phase.Writing_Response,
	)

	second := host.Host_Command {
		kind  = .Response_Write,
		token = stream_token,
	}
	copy(second.body[:], big[:])
	second.body_length = u32(len(big))
	drain_managed_stream_command(t, &fixture, second)
	testing.expect_value(
		t,
		fixture.worker_states[0].phase,
		Static_Connection_Phase.Writing_Response,
	)

	turn := drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Response_Progress)
	expect_managed_writable(t, &fixture)

	head_buffer: [256]u8
	head_text := read_managed_socket_text(t, &fixture, head_buffer[:])
	testing.expect(t, len(head_text) > 0)
	close_managed_body_fixture(&fixture)
}

@(test)
test_managed_stream_begin_rejects_head :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "HEAD /stream HTTP/1.1\r\nHost: example.test\r\n\r\n"

	if !setup_managed_body_fixture(t, &fixture, "HEAD", "/stream", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)

		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	stream_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	drain_managed_stream_command(t, &fixture, make_managed_stream_begin(stream_token, 200, "OK"))
	testing.expect_value(t, fixture.worker_states[0].active, false)
	testing.expect_value(t, fixture.worker.metrics.closed_connections, u64(1))
	close_managed_body_fixture(&fixture)
}

make_managed_stream_trailers :: proc(
	token: connections.Connection_Token,
	name: string,
	value: string,
) -> host.Host_Command {
	cmd := host.Host_Command {
		kind  = .Response_Trailers,
		token = token,
	}
	name_bytes := transmute([]u8)name
	value_bytes := transmute([]u8)value
	used := 0
	copy(cmd.body[used:], name_bytes)
	used += len(name_bytes)
	copy(cmd.body[used:], ": ")
	used += 2
	copy(cmd.body[used:], value_bytes)
	used += len(value_bytes)
	copy(cmd.body[used:], "\r\n")
	used += 2
	cmd.body_length = u32(used)
	return cmd
}

@(test)
test_managed_stream_serves_trailers_then_closes :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "GET /stream HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n"

	if !setup_managed_body_fixture(t, &fixture, "GET", "/stream", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)

		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	stream_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	drain_managed_stream_command(t, &fixture, make_managed_stream_begin(stream_token, 200, "OK"))
	turn := drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Response_Progress)
	expect_managed_writable(t, &fixture)

	head_buffer: [256]u8
	head_text := read_managed_socket_text(t, &fixture, head_buffer[:])
	testing.expect(t, len(head_text) > 0)

	drain_managed_stream_command(t, &fixture, make_managed_stream_write(stream_token, "hello"))
	turn = drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Response_Progress)
	expect_managed_writable(t, &fixture)

	chunk_buffer: [32]u8
	testing.expect_value(
		t,
		read_managed_socket_text(t, &fixture, chunk_buffer[:]),
		"5\r\nhello\r\n",
	)

	drain_managed_stream_command(
		t,
		&fixture,
		make_managed_stream_trailers(stream_token, "X-Checksum", "abc123"),
	)
	turn = drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Closed)

	terminal_buffer: [64]u8
	testing.expect_value(
		t,
		read_managed_socket_text(t, &fixture, terminal_buffer[:]),
		"0\r\nX-Checksum: abc123\r\n\r\n",
	)
	close_managed_body_fixture(&fixture)
}

@(test)
test_managed_stream_rejects_write_and_trailers_after_end :: proc(t: ^testing.T) {
	fixture: Managed_Body_Fixture
	request := "GET /stream HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n"

	if !setup_managed_body_fixture(t, &fixture, "GET", "/stream", request, 16 * 1024 * 1024) {
		close_managed_body_fixture(&fixture)

		return
	}
	polled: [4]host.Host_Event
	testing.expect_value(t, host.poll_host_queue(&fixture.host_queue, polled[:]).copied, 1)
	stream_token := polled[0].token
	fixture.worker.poller = fixture.poller
	fixture.worker.shutdown_state = &fixture.shutdown_state

	drain_managed_stream_command(t, &fixture, make_managed_stream_begin(stream_token, 200, "OK"))
	turn := drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Response_Progress)
	expect_managed_writable(t, &fixture)

	head_buffer: [256]u8
	head_text := read_managed_socket_text(t, &fixture, head_buffer[:])
	testing.expect(t, len(head_text) > 0)

	end_cmd := host.Host_Command {
		kind  = .Response_End,
		token = stream_token,
	}
	drain_managed_stream_command(t, &fixture, end_cmd)
	turn = drive_managed_write_turn(t, &fixture)
	testing.expect_value(t, turn.result, Static_Connection_Result.Closed)

	trailer_cmd := make_managed_stream_trailers(stream_token, "X-Checksum", "abc123")
	drain_managed_stream_command(t, &fixture, trailer_cmd)
	testing.expect_value(t, fixture.worker_states[0].phase, Static_Connection_Phase.Closed)
	close_managed_body_fixture(&fixture)
}
