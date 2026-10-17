package transport

Timer_Queue :: struct {
	storage:             []Timer_Expiration,
	index_storage:       []u32,
	count:               int,
	deleted_index_count: int,
	initialized:         bool,
}

Timer_Queue_Result :: enum {
	Scheduled,
	Full,
	Duplicate,
	Cancelled,
	Not_Found,
	Expired,
	Not_Due,
	Invalid,
}

TIMER_QUEUE_INDEX_STORAGE_FACTOR :: 3
timer_index_empty :: u32(0xffff_ffff)
timer_index_deleted :: u32(0xffff_fffe)

init_timer_queue :: proc(storage: []Timer_Expiration, index_storage: []u32) -> Timer_Queue {
	if u64(len(storage)) > 0x7fff_ffff {
		return {}
	}

	if len(storage) > len(index_storage) / TIMER_QUEUE_INDEX_STORAGE_FACTOR {
		return {}
	}
	index_slice := index_storage[:len(storage) * TIMER_QUEUE_INDEX_STORAGE_FACTOR]
	hash_capacity := len(storage) * 2

	for i := 0; i < hash_capacity; i += 1 {
		index_slice[i] = timer_index_empty
	}

	for i := hash_capacity; i < len(index_slice); i += 1 {
		index_slice[i] = timer_index_empty
	}

	return Timer_Queue {
		storage = storage,
		index_storage = index_slice,
		initialized = true,
	}
}

valid_timer_queue :: proc(queue: ^Timer_Queue) -> bool {
	if queue == nil || !queue.initialized {
		return false
	}

	if u64(len(queue.storage)) > 0x7fff_ffff {
		return false
	}

	if len(queue.storage) > len(queue.index_storage) / TIMER_QUEUE_INDEX_STORAGE_FACTOR {
		return false
	}

	if queue.count < 0 || queue.count > len(queue.storage) {
		return false
	}

	if queue.deleted_index_count < 0 {
		return false
	}

	if queue.deleted_index_count > len(queue.storage) * 2 {
		return false
	}

	return true
}

schedule_timer :: proc(queue: ^Timer_Queue, timer: Timer_Expiration) -> Timer_Queue_Result {
	if !valid_timer_queue(queue) {
		return .Invalid
	}

	if !timer_index_prepare(queue) {
		return .Invalid
	}

	existing_index, bucket, index_valid := timer_index_lookup(queue, timer.timer_id)
	if !index_valid {
		return .Invalid
	}

	if existing_index >= 0 {
		return .Duplicate
	}

	if queue.count >= len(queue.storage) {
		return .Full
	}

	if bucket < 0 {
		return .Invalid
	}

	index := queue.count
	queue.storage[index] = timer
	queue.count += 1
	timer_index_insert_at(queue, bucket, index)
	sift_timer_up(queue, index)
	return .Scheduled
}

cancel_timer :: proc(queue: ^Timer_Queue, timer_id: u64) -> Timer_Queue_Result {
	if !valid_timer_queue(queue) {
		return .Invalid
	}

	index, _, index_valid := timer_index_lookup(queue, timer_id)
	if !index_valid {
		return .Invalid
	}

	if index < 0 {
		return .Not_Found
	}

	remove_timer_at(queue, index)
	return .Cancelled
}

pop_expired_timer :: proc(
	queue: ^Timer_Queue,
	now_ns: u64,
) -> (
	timer: Timer_Expiration,
	result: Timer_Queue_Result,
) {
	if !valid_timer_queue(queue) {
		return {}, .Invalid
	}

	if queue.count == 0 || queue.storage[0].deadline_ns > now_ns {
		return {}, .Not_Due
	}
	timer = queue.storage[0]
	remove_timer_at(queue, 0)
	return timer, .Expired
}

timer_wait_timeout_ms :: proc(queue: ^Timer_Queue, now_ns: u64, max_wait_ms: i32) -> i32 {
	if !valid_timer_queue(queue) || max_wait_ms == 0 {
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

timer_index_prepare :: proc(queue: ^Timer_Queue) -> bool {
	hash_capacity := len(queue.storage) * 2
	deleted_limit := max(1, hash_capacity / 4)
	if queue.deleted_index_count < deleted_limit {
		return true
	}

	return timer_index_rebuild(queue)
}

timer_index_lookup :: proc(
	queue: ^Timer_Queue,
	timer_id: u64,
) -> (
	index: int,
	bucket: int,
	valid: bool,
) {
	hash_capacity := len(queue.storage) * 2
	if hash_capacity == 0 {
		return -1, -1, true
	}

	first_deleted := -1
	start := int(timer_id % u64(hash_capacity))
	hash := queue.index_storage[:hash_capacity]

	for probe := 0; probe < hash_capacity; probe += 1 {
		bucket = (start + probe) % hash_capacity
		heap_index := hash[bucket]

		if heap_index == timer_index_empty {
			if first_deleted >= 0 {
				return -1, first_deleted, true
			}
			return -1, bucket, true
		}

		if heap_index == timer_index_deleted {
			if first_deleted < 0 {
				first_deleted = bucket
			}
			continue
		}

		if u64(heap_index) >= u64(queue.count) {
			return -1, -1, false
		}

		index = int(heap_index)
		if queue.storage[index].timer_id == timer_id {
			return index, bucket, true
		}
	}

	return -1, first_deleted, true
}

timer_index_insert_at :: proc(queue: ^Timer_Queue, bucket, heap_index: int) {
	hash_capacity := len(queue.storage) * 2
	hash := queue.index_storage[:hash_capacity]
	if hash[bucket] == timer_index_deleted {
		queue.deleted_index_count -= 1
	}

	hash[bucket] = u32(heap_index)
	queue.index_storage[hash_capacity + heap_index] = u32(bucket)
}

timer_index_rebuild :: proc(queue: ^Timer_Queue) -> bool {
	hash_capacity := len(queue.storage) * 2
	hash := queue.index_storage[:hash_capacity]
	inverse := queue.index_storage[hash_capacity:]

	for i := 0; i < len(hash); i += 1 {
		hash[i] = timer_index_empty
	}

	for i := 0; i < len(inverse); i += 1 {
		inverse[i] = timer_index_empty
	}
	queue.deleted_index_count = 0

	for heap_index := 0; heap_index < queue.count; heap_index += 1 {
		timer_id := queue.storage[heap_index].timer_id
		start := int(timer_id % u64(hash_capacity))
		bucket := -1

		for probe := 0; probe < hash_capacity; probe += 1 {
			candidate := (start + probe) % hash_capacity
			if hash[candidate] == timer_index_empty {
				bucket = candidate
				break
			}
		}

		if bucket < 0 {
			return false
		}

		timer_index_insert_at(queue, bucket, heap_index)
	}
	return true
}

sift_timer_up :: proc(queue: ^Timer_Queue, start: int) {
	index := start
	for index > 0 {
		parent := (index - 1) / 2

		if !timer_precedes(queue.storage[index], queue.storage[parent]) {
			return
		}
		swap_timer_entries(queue, index, parent)
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
		swap_timer_entries(queue, index, first)
		index = first
	}
}

swap_timer_entries :: proc(queue: ^Timer_Queue, left, right: int) {
	hash_capacity := len(queue.storage) * 2
	inverse_start := hash_capacity
	hash := queue.index_storage[:hash_capacity]
	left_bucket := int(queue.index_storage[inverse_start + left])
	right_bucket := int(queue.index_storage[inverse_start + right])

	temporary := queue.storage[left]
	queue.storage[left] = queue.storage[right]
	queue.storage[right] = temporary

	hash[left_bucket] = u32(right)
	hash[right_bucket] = u32(left)
	queue.index_storage[inverse_start + left] = u32(right_bucket)
	queue.index_storage[inverse_start + right] = u32(left_bucket)
}

remove_timer_at :: proc(queue: ^Timer_Queue, index: int) {
	last := queue.count - 1
	hash_capacity := len(queue.storage) * 2
	inverse_start := hash_capacity
	hash := queue.index_storage[:hash_capacity]
	bucket := int(queue.index_storage[inverse_start + index])
	hash[bucket] = timer_index_deleted
	queue.index_storage[inverse_start + index] = timer_index_empty
	queue.deleted_index_count += 1
	queue.count = last

	if index == last {
		queue.storage[last] = Timer_Expiration{}
		return
	}

	last_bucket := int(queue.index_storage[inverse_start + last])
	queue.storage[index] = queue.storage[last]
	queue.storage[last] = Timer_Expiration{}
	hash[last_bucket] = u32(index)
	queue.index_storage[inverse_start + index] = u32(last_bucket)
	queue.index_storage[inverse_start + last] = timer_index_empty

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
