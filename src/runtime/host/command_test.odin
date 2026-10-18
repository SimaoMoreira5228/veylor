#+test
package host

import "core:testing"
import connections "veylor:runtime/connections"

make_host_command_token :: proc(slot: u32, generation: u64) -> connections.Connection_Token {
	return connections.Connection_Token {
		worker_id = 1,
		worker_generation = 1,
		slot = slot,
		generation = generation,
	}
}

make_read_body_command :: proc(slot: u32, generation: u64, max_bytes: u32) -> Host_Command {
	return Host_Command {
		kind = .Read_Body,
		token = make_host_command_token(slot, generation),
		max_bytes = max_bytes,
	}
}

@(test)
test_host_command_queue_preserves_fifo_order :: proc(t: ^testing.T) {
	storage: [8]Host_Command
	queue := init_host_command_queue(storage[:])

	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_read_body_command(0, 1, 1024)),
		Host_Command_Result.Enqueued,
	)
	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_read_body_command(1, 2, 2048)),
		Host_Command_Result.Enqueued,
	)

	first, first_ok := dequeue_host_command(&queue)
	testing.expect(t, first_ok)
	testing.expect_value(t, first.kind, Host_Command_Kind.Read_Body)
	testing.expect_value(t, first.token, make_host_command_token(0, 1))
	testing.expect_value(t, first.max_bytes, u32(1024))

	second, second_ok := dequeue_host_command(&queue)
	testing.expect(t, second_ok)
	testing.expect_value(t, second.kind, Host_Command_Kind.Read_Body)
	testing.expect_value(t, second.token, make_host_command_token(1, 2))
	testing.expect_value(t, second.max_bytes, u32(2048))

	testing.expect_value(t, pending_host_commands(&queue), 0)
}

@(test)
test_host_command_queue_full_rejects_until_dequeue_frees_a_slot :: proc(t: ^testing.T) {
	storage: [2]Host_Command
	queue := init_host_command_queue(storage[:])

	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_read_body_command(0, 1, 512)),
		Host_Command_Result.Enqueued,
	)
	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_read_body_command(1, 1, 512)),
		Host_Command_Result.Enqueued,
	)
	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_read_body_command(0, 2, 512)),
		Host_Command_Result.Full,
	)

	cmd, ok := dequeue_host_command(&queue)
	testing.expect(t, ok)
	testing.expect_value(t, cmd.token, make_host_command_token(0, 1))

	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_read_body_command(0, 2, 512)),
		Host_Command_Result.Enqueued,
	)
}

@(test)
test_host_command_queue_empty_dequeue_returns_not_ok :: proc(t: ^testing.T) {
	storage: [4]Host_Command
	queue := init_host_command_queue(storage[:])

	_, ok := dequeue_host_command(&queue)
	testing.expect(t, !ok)
	testing.expect_value(t, pending_host_commands(&queue), 0)
}

@(test)
test_host_command_queue_rejects_invalid_args_without_touching_storage :: proc(t: ^testing.T) {
	testing.expect_value(
		t,
		enqueue_host_command(nil, make_read_body_command(0, 1, 512)),
		Host_Command_Result.Invalid,
	)

	_, ok := dequeue_host_command(nil)
	testing.expect(t, !ok)
	testing.expect_value(t, pending_host_commands(nil), 0)

	storage: [4]Host_Command
	sentinel := Host_Command {
		kind      = .Read_Body,
		token     = make_host_command_token(7, 9),
		max_bytes = 4096,
	}
	storage[0] = sentinel
	queue := init_host_command_queue(storage[:])
	queue.storage[0] = sentinel

	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_read_body_command(0, 1, 0)),
		Host_Command_Result.Invalid,
	)
	testing.expect_value(t, queue.storage[0], sentinel)
	testing.expect_value(t, pending_host_commands(&queue), 0)
}

@(test)
test_host_command_queue_pending_tracks_enqueue_and_dequeue :: proc(t: ^testing.T) {
	storage: [4]Host_Command
	queue := init_host_command_queue(storage[:])

	testing.expect_value(t, pending_host_commands(&queue), 0)

	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_read_body_command(0, 1, 128)),
		Host_Command_Result.Enqueued,
	)
	testing.expect_value(t, pending_host_commands(&queue), 1)

	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_read_body_command(1, 1, 256)),
		Host_Command_Result.Enqueued,
	)
	testing.expect_value(t, pending_host_commands(&queue), 2)

	_, ok := dequeue_host_command(&queue)
	testing.expect(t, ok)
	testing.expect_value(t, pending_host_commands(&queue), 1)

	_, ok = dequeue_host_command(&queue)
	testing.expect(t, ok)
	testing.expect_value(t, pending_host_commands(&queue), 0)
}

make_response_start_command :: proc(
	slot: u32,
	generation: u64,
	status: i32,
	reason_length: u32,
	body_length: u32,
) -> Host_Command {
	return Host_Command {
		kind = .Response_Start,
		token = make_host_command_token(slot, generation),
		status = status,
		reason_length = reason_length,
		body_length = body_length,
	}
}

@(test)
test_host_command_queue_validates_response_start_bounds :: proc(t: ^testing.T) {
	storage: [4]Host_Command
	queue := init_host_command_queue(storage[:])

	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_response_start_command(0, 1, 200, 2, 9)),
		Host_Command_Result.Enqueued,
	)
	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_response_start_command(1, 1, 199, 2, 0)),
		Host_Command_Result.Invalid,
	)
	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_response_start_command(1, 1, 200, 0, 0)),
		Host_Command_Result.Invalid,
	)
	testing.expect_value(
		t,
		enqueue_host_command(
			&queue,
			make_response_start_command(1, 1, 200, 2, HOST_MAX_RESPONSE_BODY_BYTES + 1),
		),
		Host_Command_Result.Invalid,
	)
	testing.expect_value(t, pending_host_commands(&queue), 1)
}

make_cancel_command :: proc(slot: u32, generation: u64) -> Host_Command {
	return Host_Command{kind = .Cancel, token = make_host_command_token(slot, generation)}
}

@(test)
test_host_command_queue_accepts_cancel_without_bounds :: proc(t: ^testing.T) {
	storage: [2]Host_Command
	queue := init_host_command_queue(storage[:])

	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_cancel_command(0, 1)),
		Host_Command_Result.Enqueued,
	)
	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_cancel_command(1, 2)),
		Host_Command_Result.Enqueued,
	)
	testing.expect_value(
		t,
		enqueue_host_command(&queue, make_cancel_command(2, 3)),
		Host_Command_Result.Full,
	)

	cmd, ok := dequeue_host_command(&queue)
	testing.expect(t, ok)
	testing.expect_value(t, cmd.kind, Host_Command_Kind.Cancel)
	testing.expect_value(t, cmd.token, make_host_command_token(0, 1))
	testing.expect_value(t, pending_host_commands(&queue), 1)
}
