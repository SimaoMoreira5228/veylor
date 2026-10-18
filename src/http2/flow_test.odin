#+test
package http2

import "core:testing"

@(test)
test_flow_init_bounds :: proc(t: ^testing.T) {
	window: Flow_Window

	testing.expect_value(t, flow_window_init(&window, FLOW_INITIAL_WINDOW_SIZE), true)
	testing.expect_value(t, window.value, i64(65535))

	testing.expect_value(t, flow_window_init(&window, 0), true)
	testing.expect_value(t, window.value, i64(0))

	testing.expect_value(t, flow_window_init(&window, FLOW_MAX_WINDOW_SIZE), true)
	testing.expect_value(t, window.value, FLOW_MAX_WINDOW_SIZE)

	testing.expect_value(t, flow_window_init(&window, -1), false)
	testing.expect_value(t, flow_window_init(&window, FLOW_MAX_WINDOW_SIZE + 1), false)
}

@(test)
test_flow_take_send_checks :: proc(t: ^testing.T) {
	window: Flow_Window
	testing.expect_value(t, flow_window_init(&window, 100), true)

	testing.expect_value(t, flow_window_take(&window, 40), true)
	testing.expect_value(t, window.value, i64(60))

	testing.expect_value(t, flow_window_take(&window, 60), true)
	testing.expect_value(t, window.value, i64(0))

	testing.expect_value(t, flow_window_take(&window, 1), false)
	testing.expect_value(t, window.value, i64(0))

	testing.expect_value(t, flow_window_take(&window, 0), true)
	testing.expect_value(t, window.value, i64(0))

	testing.expect_value(t, flow_window_take(&window, -1), false)
	testing.expect_value(t, window.value, i64(0))
}

@(test)
test_flow_take_rejects_overdraft :: proc(t: ^testing.T) {
	window: Flow_Window
	testing.expect_value(t, flow_window_init(&window, 10), true)

	testing.expect_value(t, flow_window_take(&window, 11), false)
	testing.expect_value(t, window.value, i64(10))
}

@(test)
test_flow_add_overflow_rejected :: proc(t: ^testing.T) {
	window: Flow_Window
	testing.expect_value(t, flow_window_init(&window, FLOW_MAX_WINDOW_SIZE - 10), true)

	testing.expect_value(t, flow_window_add(&window, 10), true)
	testing.expect_value(t, window.value, FLOW_MAX_WINDOW_SIZE)

	testing.expect_value(t, flow_window_add(&window, 1), false)
	testing.expect_value(t, window.value, FLOW_MAX_WINDOW_SIZE)
}

@(test)
test_flow_add_zero_rejected :: proc(t: ^testing.T) {
	window: Flow_Window
	testing.expect_value(t, flow_window_init(&window, 100), true)

	testing.expect_value(t, flow_window_add(&window, 0), false)
	testing.expect_value(t, window.value, i64(100))
}

@(test)
test_flow_add_from_zero :: proc(t: ^testing.T) {
	window: Flow_Window
	testing.expect_value(t, flow_window_init(&window, 0), true)

	testing.expect_value(t, flow_window_add(&window, 65535), true)
	testing.expect_value(t, window.value, i64(65535))

	testing.expect_value(t, flow_window_take(&window, 65535), true)
	testing.expect_value(t, window.value, i64(0))
}

@(test)
test_flow_increment_valid :: proc(t: ^testing.T) {
	testing.expect_value(t, flow_increment_valid(0), false)
	testing.expect_value(t, flow_increment_valid(1), true)
	testing.expect_value(t, flow_increment_valid(u32(FLOW_MAX_WINDOW_SIZE)), true)
}

@(test)
test_flow_chunk_size_limits :: proc(t: ^testing.T) {
	testing.expect_value(t, flow_chunk_size(65535, 65535, 65535, 16384), u32(16384))
	testing.expect_value(t, flow_chunk_size(100, 65535, 1000, 16384), u32(100))
	testing.expect_value(t, flow_chunk_size(65535, 50, 1000, 16384), u32(50))
	testing.expect_value(t, flow_chunk_size(65535, 65535, 100, 16384), u32(100))
	testing.expect_value(t, flow_chunk_size(10, 10, 5, 16384), u32(5))
}

@(test)
test_flow_chunk_size_empty :: proc(t: ^testing.T) {
	testing.expect_value(t, flow_chunk_size(0, 100, 100, 16384), u32(0))
	testing.expect_value(t, flow_chunk_size(100, 0, 100, 16384), u32(0))
	testing.expect_value(t, flow_chunk_size(-5, 100, 100, 16384), u32(0))
	testing.expect_value(t, flow_chunk_size(100, 100, 0, 16384), u32(0))
	testing.expect_value(t, flow_chunk_size(100, 100, 100, 0), u32(0))
}

@(test)
test_flow_chunk_size_huge_remaining :: proc(t: ^testing.T) {
	testing.expect_value(t, flow_chunk_size(100000, 200000, max(u64), 16777215), u32(100000))
	testing.expect_value(t, flow_chunk_size(FLOW_MAX_WINDOW_SIZE, FLOW_MAX_WINDOW_SIZE, max(u64), 16777215), u32(16777215))
}

@(test)
test_flow_send_recv_round_trip :: proc(t: ^testing.T) {
	conn_send: Flow_Window
	conn_recv: Flow_Window
	stream_send: Flow_Window
	stream_recv: Flow_Window

	testing.expect_value(t, flow_window_init(&conn_send, FLOW_INITIAL_WINDOW_SIZE), true)
	testing.expect_value(t, flow_window_init(&conn_recv, FLOW_INITIAL_WINDOW_SIZE), true)
	testing.expect_value(t, flow_window_init(&stream_send, FLOW_INITIAL_WINDOW_SIZE), true)
	testing.expect_value(t, flow_window_init(&stream_recv, FLOW_INITIAL_WINDOW_SIZE), true)

	chunk := flow_chunk_size(conn_send.value, stream_send.value, 100000, 16384)
	testing.expect_value(t, chunk, u32(16384))

	testing.expect_value(t, flow_window_take(&conn_send, i64(chunk)), true)
	testing.expect_value(t, flow_window_take(&stream_send, i64(chunk)), true)
	testing.expect_value(t, conn_send.value, FLOW_INITIAL_WINDOW_SIZE - 16384)

	testing.expect_value(t, flow_window_take(&conn_recv, i64(chunk)), true)
	testing.expect_value(t, flow_window_take(&stream_recv, i64(chunk)), true)

	testing.expect_value(t, flow_window_add(&conn_send, u32(chunk)), true)
	testing.expect_value(t, flow_window_add(&stream_send, u32(chunk)), true)
	testing.expect_value(t, conn_send.value, FLOW_INITIAL_WINDOW_SIZE)
	testing.expect_value(t, stream_send.value, FLOW_INITIAL_WINDOW_SIZE)
}

@(test)
test_flow_recv_overdraft_rejected :: proc(t: ^testing.T) {
	recv: Flow_Window
	testing.expect_value(t, flow_window_init(&recv, 10), true)

	testing.expect_value(t, flow_window_take(&recv, 11), false)
	testing.expect_value(t, recv.value, i64(10))
}
