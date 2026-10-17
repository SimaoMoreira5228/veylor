#+test
package host

import "core:testing"
import connections "veylor:runtime/connections"

make_host_token :: proc(slot: u32, generation: u64) -> connections.Connection_Token {
	return connections.Connection_Token {
		worker_id = 1,
		worker_generation = 1,
		slot = slot,
		generation = generation,
	}
}

@(test)
test_host_queue_empty_poll_copies_nothing :: proc(t: ^testing.T) {
	storage: [HOST_QUEUE_DEPTH]Host_Event
	queue := init_host_queue(storage[:])
	batch: [4]Host_Event
	poll := poll_host_queue(&queue, batch[:])
	testing.expect_value(t, poll.copied, 0)
	testing.expect_value(t, poll.required, 0)
	testing.expect_value(t, pending_host_events(&queue), 0)
}

@(test)
test_host_queue_preserves_fifo_order_and_bytes :: proc(t: ^testing.T) {
	storage: [HOST_QUEUE_DEPTH]Host_Event
	queue := init_host_queue(storage[:])
	first_method := transmute([]u8)string("GET")
	first_path := transmute([]u8)string("/first")
	second_method := transmute([]u8)string("POST")
	second_path := transmute([]u8)string("/second")
	testing.expect_value(
		t,
		enqueue_request(&queue, make_host_token(0, 1), 11, first_method, first_path),
		Host_Queue_Result.Enqueued,
	)
	testing.expect_value(
		t,
		enqueue_request(&queue, make_host_token(1, 2), 12, second_method, second_path),
		Host_Queue_Result.Enqueued,
	)
	batch: [4]Host_Event
	poll := poll_host_queue(&queue, batch[:])
	testing.expect_value(t, poll.copied, 2)
	testing.expect_value(t, poll.required, 2)
	testing.expect_value(t, batch[0].token, make_host_token(0, 1))
	testing.expect_value(t, batch[0].route_id, u64(11))
	testing.expect_value(t, string(batch[0].method[:batch[0].method_length]), "GET")
	testing.expect_value(t, string(batch[0].path[:batch[0].path_length]), "/first")
	testing.expect_value(t, batch[1].token, make_host_token(1, 2))
	testing.expect_value(t, batch[1].route_id, u64(12))
	testing.expect_value(t, string(batch[1].method[:batch[1].method_length]), "POST")
	testing.expect_value(t, string(batch[1].path[:batch[1].path_length]), "/second")
	testing.expect_value(t, pending_host_events(&queue), 0)
}

@(test)
test_host_queue_full_rejects_until_poll_frees_a_slot :: proc(t: ^testing.T) {
	storage: [2]Host_Event
	queue := init_host_queue(storage[:])
	method := transmute([]u8)string("GET")
	path := transmute([]u8)string("/a")
	testing.expect_value(
		t,
		enqueue_request(&queue, make_host_token(0, 1), 1, method, path),
		Host_Queue_Result.Enqueued,
	)
	testing.expect_value(
		t,
		enqueue_request(&queue, make_host_token(1, 1), 2, method, path),
		Host_Queue_Result.Enqueued,
	)
	testing.expect_value(
		t,
		enqueue_request(&queue, make_host_token(0, 2), 3, method, path),
		Host_Queue_Result.Full,
	)
	batch: [1]Host_Event
	poll := poll_host_queue(&queue, batch[:])
	testing.expect_value(t, poll.copied, 1)
	testing.expect_value(t, poll.required, 2)
	testing.expect_value(t, batch[0].route_id, u64(1))
	testing.expect_value(
		t,
		enqueue_request(&queue, make_host_token(0, 2), 3, method, path),
		Host_Queue_Result.Enqueued,
	)
}

@(test)
test_host_queue_undersized_batch_reports_required_and_keeps_order :: proc(t: ^testing.T) {
	storage: [HOST_QUEUE_DEPTH]Host_Event
	queue := init_host_queue(storage[:])
	method := transmute([]u8)string("GET")
	paths := [?]string{"/a", "/b", "/c"}
	for path, i in paths {
		path_bytes := transmute([]u8)path
		testing.expect_value(
			t,
			enqueue_request(&queue, make_host_token(u32(i), 1), u64(i + 1), method, path_bytes),
			Host_Queue_Result.Enqueued,
		)
	}
	batch: [1]Host_Event
	poll := poll_host_queue(&queue, batch[:])
	testing.expect_value(t, poll.copied, 1)
	testing.expect_value(t, poll.required, 3)
	testing.expect_value(t, string(batch[0].path[:batch[0].path_length]), "/a")
	remaining: [2]Host_Event
	poll = poll_host_queue(&queue, remaining[:])
	testing.expect_value(t, poll.copied, 2)
	testing.expect_value(t, poll.required, 2)
	testing.expect_value(t, string(remaining[0].path[:remaining[0].path_length]), "/b")
	testing.expect_value(t, string(remaining[1].path[:remaining[1].path_length]), "/c")
}

@(test)
test_host_queue_zero_batch_leaves_pending_events_queued :: proc(t: ^testing.T) {
	storage: [HOST_QUEUE_DEPTH]Host_Event
	queue := init_host_queue(storage[:])
	method := transmute([]u8)string("GET")
	path := transmute([]u8)string("/a")
	testing.expect_value(
		t,
		enqueue_request(&queue, make_host_token(0, 1), 7, method, path),
		Host_Queue_Result.Enqueued,
	)
	empty: [0]Host_Event
	poll := poll_host_queue(&queue, empty[:])
	testing.expect_value(t, poll.copied, 0)
	testing.expect_value(t, poll.required, 1)
	testing.expect_value(t, pending_host_events(&queue), 1)
}

@(test)
test_host_queue_shutdown_event_round_trips_with_kind :: proc(t: ^testing.T) {
	storage: [HOST_QUEUE_DEPTH]Host_Event
	queue := init_host_queue(storage[:])
	testing.expect_value(t, enqueue_shutdown(&queue), Host_Queue_Result.Enqueued)
	batch: [2]Host_Event
	poll := poll_host_queue(&queue, batch[:])
	testing.expect_value(t, poll.copied, 1)
	testing.expect_value(t, batch[0].kind, Host_Event_Kind.Shutdown)
}

@(test)
test_host_queue_rejects_oversize_metadata_without_storing :: proc(t: ^testing.T) {
	storage: [HOST_QUEUE_DEPTH]Host_Event
	queue := init_host_queue(storage[:])
	method := transmute([]u8)string("GET")
	oversize_path: [HOST_MAX_PATH_BYTES + 1]u8
	for i := 0; i < len(oversize_path); i += 1 {
		oversize_path[i] = 'a'
	}
	oversize_path[0] = '/'
	testing.expect_value(
		t,
		enqueue_request(&queue, make_host_token(0, 1), 1, method, oversize_path[:]),
		Host_Queue_Result.Invalid,
	)
	testing.expect_value(t, pending_host_events(&queue), 0)
	empty_method: [0]u8
	path := transmute([]u8)string("/a")
	testing.expect_value(
		t,
		enqueue_request(&queue, make_host_token(0, 1), 1, empty_method[:], path),
		Host_Queue_Result.Invalid,
	)
	testing.expect_value(t, pending_host_events(&queue), 0)
}
