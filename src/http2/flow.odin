package http2

FLOW_INITIAL_WINDOW_SIZE :: i64(65535)
FLOW_MAX_WINDOW_SIZE :: i64(0x7fffffff)

Flow_Window :: struct {
	value: i64,
}

flow_window_init :: proc(window: ^Flow_Window, initial: i64) -> bool {
	if initial < 0 || initial > FLOW_MAX_WINDOW_SIZE {
		return false
	}

	window.value = initial

	return true
}

flow_window_take :: proc(window: ^Flow_Window, count: i64) -> bool {
	if count < 0 {
		return false
	}

	if count > window.value {
		return false
	}

	window.value -= count

	return true
}

flow_increment_valid :: proc(increment: u32) -> bool {
	return increment >= 1 && increment <= u32(FLOW_MAX_WINDOW_SIZE)
}

flow_window_add :: proc(window: ^Flow_Window, increment: u32) -> bool {
	if !flow_increment_valid(increment) {
		return false
	}

	if window.value > FLOW_MAX_WINDOW_SIZE - i64(increment) {
		return false
	}

	window.value += i64(increment)

	return true
}

flow_chunk_size :: proc(
	conn_available: i64,
	stream_available: i64,
	remaining: u64,
	max_frame_size: u32,
) -> u32 {
	available := conn_available

	if stream_available < available {
		available = stream_available
	}

	if available <= 0 {
		return 0
	}

	chunk := u64(available)

	if chunk > remaining {
		chunk = remaining
	}

	if chunk > u64(max_frame_size) {
		chunk = u64(max_frame_size)
	}

	return u32(chunk)
}
