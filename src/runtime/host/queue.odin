package host

import "core:sync"
import connections "veylor:runtime/connections"

HOST_MAX_METHOD_BYTES :: 16
HOST_MAX_PATH_BYTES :: 2048
HOST_QUEUE_DEPTH :: 64

Host_Event_Kind :: enum {
	Request,
	Shutdown,
	Body_Available,
}

Host_Event :: struct {
	token:         connections.Connection_Token,
	route_id:      u64,
	method_length: int,
	path_length:   int,
	method:        [HOST_MAX_METHOD_BYTES]u8,
	path:          [HOST_MAX_PATH_BYTES]u8,
	body_ready:    u32,
	body_complete: u32,
	kind:          Host_Event_Kind,
}

Host_Queue :: struct {
	storage: []Host_Event,
	head:    int,
	tail:    int,
}

Host_Queue_Result :: enum {
	Enqueued,
	Full,
	Invalid,
}

Host_Poll :: struct {
	copied:   int,
	required: int,
}

init_host_queue :: proc(storage: []Host_Event) -> Host_Queue {
	for i := 0; i < len(storage); i += 1 {
		storage[i] = Host_Event{}
	}
	return Host_Queue{storage = storage}
}

pending_host_events :: proc(queue: ^Host_Queue) -> int {
	if queue == nil {
		return 0
	}
	tail := sync.atomic_load_explicit(&queue.tail, .Acquire)
	head := sync.atomic_load_explicit(&queue.head, .Acquire)
	return tail - head
}

enqueue_request :: proc(
	queue: ^Host_Queue,
	token: connections.Connection_Token,
	route_id: u64,
	method: []u8,
	path: []u8,
) -> Host_Queue_Result {
	if queue == nil {
		return .Invalid
	}
	if len(method) == 0 ||
	   len(method) > HOST_MAX_METHOD_BYTES ||
	   len(path) == 0 ||
	   len(path) > HOST_MAX_PATH_BYTES {
		return .Invalid
	}
	return enqueue_event(
		queue,
		Host_Event{token = token, route_id = route_id, kind = .Request},
		method,
		path,
	)
}

enqueue_body_available :: proc(queue: ^Host_Queue, event: Host_Event) -> Host_Queue_Result {
	if event.kind != .Body_Available {
		return .Invalid
	}
	return enqueue_event(queue, event, nil, nil)
}

enqueue_shutdown :: proc(queue: ^Host_Queue) -> Host_Queue_Result {
	if queue == nil {
		return .Invalid
	}
	return enqueue_event(queue, Host_Event{kind = .Shutdown}, nil, nil)
}

enqueue_event :: proc(
	queue: ^Host_Queue,
	event: Host_Event,
	method: []u8,
	path: []u8,
) -> Host_Queue_Result {
	head := sync.atomic_load_explicit(&queue.head, .Acquire)
	tail := queue.tail
	if tail - head >= len(queue.storage) {
		return .Full
	}
	slot := &queue.storage[tail % len(queue.storage)]
	slot^ = event
	slot.method_length = len(method)
	slot.path_length = len(path)
	copy(slot.method[:], method)
	copy(slot.path[:], path)
	sync.atomic_store_explicit(&queue.tail, tail + 1, .Release)
	return .Enqueued
}

peek_host_queue :: proc(queue: ^Host_Queue) -> (event: Host_Event, ok: bool) {
	if queue == nil || len(queue.storage) == 0 {
		return {}, false
	}
	tail := sync.atomic_load_explicit(&queue.tail, .Acquire)
	head := sync.atomic_load_explicit(&queue.head, .Acquire)
	if tail - head <= 0 {
		return {}, false
	}
	return queue.storage[head % len(queue.storage)], true
}

poll_host_queue :: proc(queue: ^Host_Queue, batch: []Host_Event) -> Host_Poll {
	if queue == nil || len(queue.storage) == 0 {
		return Host_Poll{}
	}
	tail := sync.atomic_load_explicit(&queue.tail, .Acquire)
	pending := tail - queue.head
	if pending <= 0 {
		return Host_Poll{}
	}

	required := pending
	copied := min(pending, len(batch))
	for i := 0; i < copied; i += 1 {
		batch[i] = queue.storage[queue.head % len(queue.storage)]
		queue.head += 1
	}
	return Host_Poll{copied = copied, required = required}
}
