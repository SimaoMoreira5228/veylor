package host

import "core:sync"
import connections "veylor:runtime/connections"

HOST_COMMAND_QUEUE_DEPTH :: 64
HOST_MAX_RESPONSE_REASON_BYTES :: 64
HOST_MAX_RESPONSE_BODY_BYTES :: 768

Host_Command_Kind :: enum {
	Read_Body,
	Response_Start,
	Response_Begin,
	Response_Write,
	Response_End,
	Cancel,
}

Host_Command :: struct {
	kind:          Host_Command_Kind,
	token:         connections.Connection_Token,
	max_bytes:     u32,
	status:        i32,
	reason_length: u32,
	body_length:   u32,
	reason:        [HOST_MAX_RESPONSE_REASON_BYTES]u8,
	body:          [HOST_MAX_RESPONSE_BODY_BYTES]u8,
}

Host_Command_Result :: enum {
	Enqueued,
	Full,
	Invalid,
}

Host_Command_Queue :: struct {
	storage: []Host_Command,
	head:    int,
	tail:    int,
	count:   int,
	mu:      sync.Mutex,
}

init_host_command_queue :: proc(storage: []Host_Command) -> Host_Command_Queue {
	for i := 0; i < len(storage); i += 1 {
		storage[i] = Host_Command{}
	}

	return Host_Command_Queue{storage = storage, head = 0, tail = 0, count = 0}
}

enqueue_host_command :: proc(
	queue: ^Host_Command_Queue,
	cmd: Host_Command,
) -> Host_Command_Result {
	if queue == nil {
		return .Invalid
	}

	if len(queue.storage) == 0 {
		return .Invalid
	}

	if cmd.kind != .Read_Body &&
	   cmd.kind != .Response_Start &&
	   cmd.kind != .Response_Begin &&
	   cmd.kind != .Response_Write &&
	   cmd.kind != .Response_End &&
	   cmd.kind != .Cancel {
		return .Invalid
	}

	if cmd.kind == .Read_Body && cmd.max_bytes == 0 {
		return .Invalid
	}

	if cmd.kind == .Response_Start {
		if cmd.status < 200 || cmd.status > 599 {
			return .Invalid
		}

		if cmd.reason_length == 0 ||
		   cmd.reason_length > HOST_MAX_RESPONSE_REASON_BYTES ||
		   cmd.body_length > HOST_MAX_RESPONSE_BODY_BYTES {
			return .Invalid
		}
	}

	if cmd.kind == .Response_Begin {
		if cmd.status < 200 || cmd.status > 599 {
			return .Invalid
		}

		if cmd.reason_length == 0 || cmd.reason_length > HOST_MAX_RESPONSE_REASON_BYTES {
			return .Invalid
		}

		if cmd.body_length != 0 {
			return .Invalid
		}
	}

	if cmd.kind == .Response_Write {
		if cmd.body_length == 0 || cmd.body_length > HOST_MAX_RESPONSE_BODY_BYTES {
			return .Invalid
		}
	}

	sync.mutex_lock(&queue.mu)

	if queue.count >= len(queue.storage) {
		sync.mutex_unlock(&queue.mu)

		return .Full
	}

	queue.storage[queue.tail] = cmd
	queue.tail += 1

	if queue.tail >= len(queue.storage) {
		queue.tail = 0
	}

	queue.count += 1
	sync.mutex_unlock(&queue.mu)

	return .Enqueued
}

dequeue_host_command :: proc(queue: ^Host_Command_Queue) -> (cmd: Host_Command, ok: bool) {
	if queue == nil {
		return {}, false
	}

	if len(queue.storage) == 0 {
		return {}, false
	}

	sync.mutex_lock(&queue.mu)

	if queue.count <= 0 {
		sync.mutex_unlock(&queue.mu)

		return {}, false
	}

	cmd = queue.storage[queue.head]
	queue.storage[queue.head] = Host_Command{}
	queue.head += 1

	if queue.head >= len(queue.storage) {
		queue.head = 0
	}

	queue.count -= 1
	sync.mutex_unlock(&queue.mu)

	return cmd, true
}

pending_host_commands :: proc(queue: ^Host_Command_Queue) -> int {
	if queue == nil {
		return 0
	}

	sync.mutex_lock(&queue.mu)
	count := queue.count
	sync.mutex_unlock(&queue.mu)

	return count
}
