package transport

Timer_Queue :: struct {
	storage: []Timer_Expiration,
	count:   int,
}

Timer_Queue_Result :: enum {
	Scheduled,
	Full,
	Duplicate,
	Cancelled,
	Not_Found,
	Expired,
	Not_Due,
}

init_timer_queue :: proc(storage: []Timer_Expiration) -> Timer_Queue {
	return Timer_Queue{storage = storage}
}

schedule_timer :: proc(queue: ^Timer_Queue, timer: Timer_Expiration) -> Timer_Queue_Result {
	for i := 0; i < queue.count; i += 1 {
		if queue.storage[i].timer_id == timer.timer_id {
			return .Duplicate
		}
	}

	if queue.count >= len(queue.storage) {
		return .Full
	}

	index := queue.count
	queue.storage[index] = timer
	queue.count += 1
	sift_timer_up(queue, index)
	return .Scheduled
}

cancel_timer :: proc(queue: ^Timer_Queue, timer_id: u64) -> Timer_Queue_Result {
	for i := 0; i < queue.count; i += 1 {
		if queue.storage[i].timer_id == timer_id {
			remove_timer_at(queue, i)
			return .Cancelled
		}
	}

	return .Not_Found
}

pop_expired_timer :: proc(
	queue: ^Timer_Queue,
	now_ns: u64,
) -> (
	timer: Timer_Expiration,
	result: Timer_Queue_Result,
) {
	if queue.count == 0 || queue.storage[0].deadline_ns > now_ns {
		return {}, .Not_Due
	}
	timer = queue.storage[0]
	remove_timer_at(queue, 0)
	return timer, .Expired
}

timer_wait_timeout_ms :: proc(queue: ^Timer_Queue, now_ns: u64, max_wait_ms: i32) -> i32 {
	if max_wait_ms == 0 {
		return 0
	}

	if queue.count == 0 {
		return max_wait_ms
	}

	deadline_ns := queue.storage[0].deadline_ns

	if deadline_ns <= now_ns {
		return 0
	}

	remaining_ns := deadline_ns - now_ns
	wait_ms := remaining_ns / 1_000_000

	if remaining_ns % 1_000_000 != 0 {
		wait_ms += 1
	}

	if max_wait_ms >= 0 && wait_ms > u64(max_wait_ms) {
		return max_wait_ms
	}

	if wait_ms > u64(0x7fff_ffff) {
		return 0x7fff_ffff
	}
	return i32(wait_ms)
}

sift_timer_up :: proc(queue: ^Timer_Queue, start: int) {
	index := start
	for index > 0 {
		parent := (index - 1) / 2

		if !timer_precedes(queue.storage[index], queue.storage[parent]) {
			return
		}
		temporary := queue.storage[index]
		queue.storage[index] = queue.storage[parent]
		queue.storage[parent] = temporary
		index = parent
	}
}

sift_timer_down :: proc(queue: ^Timer_Queue, start: int) {
	index := start
	for {
		left := index * 2 + 1

		if left >= queue.count {
			return
		}
		first := left
		right := left + 1

		if right < queue.count && timer_precedes(queue.storage[right], queue.storage[left]) {
			first = right
		}

		if !timer_precedes(queue.storage[first], queue.storage[index]) {
			return
		}
		temporary := queue.storage[index]
		queue.storage[index] = queue.storage[first]
		queue.storage[first] = temporary
		index = first
	}
}

remove_timer_at :: proc(queue: ^Timer_Queue, index: int) {
	last := queue.count - 1
	queue.count = last

	if index == last {
		queue.storage[last] = Timer_Expiration{}
		return
	}
	queue.storage[index] = queue.storage[last]
	queue.storage[last] = Timer_Expiration{}

	if index > 0 && timer_precedes(queue.storage[index], queue.storage[(index - 1) / 2]) {
		sift_timer_up(queue, index)
		return
	}
	sift_timer_down(queue, index)
}

timer_precedes :: proc(left, right: Timer_Expiration) -> bool {
	if left.deadline_ns != right.deadline_ns {
		return left.deadline_ns < right.deadline_ns
	}
	return left.timer_id < right.timer_id
}
