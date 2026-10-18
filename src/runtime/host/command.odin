package host

import "core:sync"
import connections "veylor:runtime/connections"

HOST_COMMAND_QUEUE_DEPTH :: 64

Host_Command_Kind :: enum {
	Read_Body,
}

Host_Command :: struct {
	kind:      Host_Command_Kind,
	token:     connections.Connection_Token,
	max_bytes: u32,
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

	if cmd.kind != .Read_Body {
		return .Invalid
	}

	if cmd.max_bytes == 0 {
		return .Invalid
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
