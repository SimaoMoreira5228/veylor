#+test
package transport

import "core:testing"

@(test)
test_timer_queue_expires_in_deadline_then_id_order :: proc(t: ^testing.T) {
	storage: [4]Timer_Expiration
	index_storage: [4 * TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	queue := init_timer_queue(storage[:], index_storage[:])
	testing.expect_value(
		t,
		schedule_timer(
			&queue,
			Timer_Expiration {
				timer_id = 4,
				deadline_ns = 20,
			},
		),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		schedule_timer(
			&queue,
			Timer_Expiration {
				timer_id = 3,
				deadline_ns = 10,
			},
		),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		schedule_timer(
			&queue,
			Timer_Expiration {
				timer_id = 2,
				deadline_ns = 10,
			},
		),
		Timer_Queue_Result.Scheduled,
	)

	timer, result := pop_expired_timer(&queue, 9)
	testing.expect_value(t, result, Timer_Queue_Result.Not_Due)
	testing.expect_value(t, queue.count, 3)

	timer, result = pop_expired_timer(&queue, 10)
	testing.expect_value(t, result, Timer_Queue_Result.Expired)
	testing.expect_value(t, timer.timer_id, u64(2))
	timer, result = pop_expired_timer(&queue, 10)
	testing.expect_value(t, result, Timer_Queue_Result.Expired)
	testing.expect_value(t, timer.timer_id, u64(3))
	timer, result = pop_expired_timer(&queue, 19)
	testing.expect_value(t, result, Timer_Queue_Result.Not_Due)
	testing.expect_value(t, queue.count, 1)
}

@(test)
test_timer_queue_rejects_duplicate_and_capacity_overflow :: proc(t: ^testing.T) {
	storage: [1]Timer_Expiration
	index_storage: [TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	queue := init_timer_queue(storage[:], index_storage[:])
	timer := Timer_Expiration {
		timer_id    = 1,
		deadline_ns = 10,
	}

	testing.expect_value(t, schedule_timer(&queue, timer), Timer_Queue_Result.Scheduled)
	testing.expect_value(t, schedule_timer(&queue, timer), Timer_Queue_Result.Duplicate)
	testing.expect_value(
		t,
		schedule_timer(
			&queue,
			Timer_Expiration {
				timer_id = 2,
				deadline_ns = 20,
			},
		),
		Timer_Queue_Result.Full,
	)

	testing.expect_value(t, queue.count, 1)
}

@(test)
test_timer_queue_cancels_root_and_interior_entries :: proc(t: ^testing.T) {
	storage: [5]Timer_Expiration
	index_storage: [5 * TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	queue := init_timer_queue(storage[:], index_storage[:])
	testing.expect_value(
		t,
		schedule_timer(
			&queue,
			Timer_Expiration {
				timer_id = 4,
				deadline_ns = 40,
			},
		),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		schedule_timer(
			&queue,
			Timer_Expiration {
				timer_id = 1,
				deadline_ns = 10,
			},
		),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		schedule_timer(
			&queue,
			Timer_Expiration {
				timer_id = 3,
				deadline_ns = 30,
			},
		),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		schedule_timer(
			&queue,
			Timer_Expiration {
				timer_id = 2,
				deadline_ns = 20,
			},
		),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(t, cancel_timer(&queue, 1), Timer_Queue_Result.Cancelled)
	testing.expect_value(t, cancel_timer(&queue, 3), Timer_Queue_Result.Cancelled)
	testing.expect_value(t, cancel_timer(&queue, 3), Timer_Queue_Result.Not_Found)
	testing.expect_value(t, queue.count, 2)

	timer, result := pop_expired_timer(&queue, 40)
	testing.expect_value(t, result, Timer_Queue_Result.Expired)
	testing.expect_value(t, timer.timer_id, u64(2))
	timer, result = pop_expired_timer(&queue, 40)
	testing.expect_value(t, result, Timer_Queue_Result.Expired)
	testing.expect_value(t, timer.timer_id, u64(4))
}

@(test)
test_timer_wait_timeout_rounds_up_and_respects_worker_cap :: proc(t: ^testing.T) {
	storage: [2]Timer_Expiration
	index_storage: [2 * TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	queue := init_timer_queue(storage[:], index_storage[:])
	testing.expect_value(t, timer_wait_timeout_ms(&queue, 100, 50), i32(50))

	schedule_timer(
		&queue,
		Timer_Expiration {
			timer_id = 1,
			deadline_ns = 1_000_002,
		},
	)

	testing.expect_value(t, timer_wait_timeout_ms(&queue, 1, 50), i32(2))
	testing.expect_value(t, timer_wait_timeout_ms(&queue, 1, 1), i32(1))
	testing.expect_value(t, timer_wait_timeout_ms(&queue, 1, -1), i32(2))
	testing.expect_value(t, timer_wait_timeout_ms(&queue, 1_000_002, -1), i32(0))
}

@(test)
test_timer_wait_timeout_saturates_at_epoll_integer_limit :: proc(t: ^testing.T) {
	storage: [1]Timer_Expiration
	index_storage: [TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	queue := init_timer_queue(storage[:], index_storage[:])
	schedule_timer(
		&queue,
		Timer_Expiration {
			timer_id = 1,
			deadline_ns = 0xffff_ffff_ffff_ffff,
		},
	)
	testing.expect_value(t, timer_wait_timeout_ms(&queue, 0, -1), i32(0x7fff_ffff))
}

@(test)
test_timer_queue_rejects_incomplete_index_storage :: proc(t: ^testing.T) {
	storage: [2]Timer_Expiration
	index_storage: [5]u32
	queue := init_timer_queue(storage[:], index_storage[:])
	testing.expect_value(t, valid_timer_queue(&queue), false)
	testing.expect_value(
		t,
		schedule_timer(
			&queue,
			Timer_Expiration {
				timer_id = 1,
				deadline_ns = 1,
			},
		),
		Timer_Queue_Result.Invalid,
	)
	testing.expect_value(t, cancel_timer(&queue, 1), Timer_Queue_Result.Invalid)
	_, result := pop_expired_timer(&queue, 1)
	testing.expect_value(t, result, Timer_Queue_Result.Invalid)
}

@(test)
test_timer_index_survives_heap_swaps_reuse_and_rebuild :: proc(t: ^testing.T) {
	storage: [8]Timer_Expiration
	index_storage: [8 * TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	queue := init_timer_queue(storage[:], index_storage[:])
	deadlines := [?]u64{80, 20, 60, 40, 10, 70, 30, 50}

	for i := 0; i < len(deadlines); i += 1 {
		testing.expect_value(
			t,
			schedule_timer(
				&queue,
				Timer_Expiration {
					timer_id = u64(i + 1),
					deadline_ns = deadlines[i],
				},
			),
			Timer_Queue_Result.Scheduled,
		)
	}

	removed_timer_ids := [?]u64{2, 4, 6, 8}
	for timer_id in removed_timer_ids {
		testing.expect_value(t, cancel_timer(&queue, timer_id), Timer_Queue_Result.Cancelled)
	}

	added_timer_ids := [?]u64{17, 25, 33, 41}
	added_deadlines := [?]u64{35, 25, 15, 5}
	for i := 0; i < len(added_timer_ids); i += 1 {
		testing.expect_value(
			t,
			schedule_timer(
				&queue,
				Timer_Expiration {
					timer_id = added_timer_ids[i],
					deadline_ns = added_deadlines[i],
				},
			),
			Timer_Queue_Result.Scheduled,
		)
	}

	remaining_timer_ids := [?]u64{1, 3, 5, 7, 17, 25, 33, 41}
	for timer_id in remaining_timer_ids {
		testing.expect_value(t, cancel_timer(&queue, timer_id), Timer_Queue_Result.Cancelled)
	}

	testing.expect_value(t, queue.count, 0)
	testing.expect_value(
		t,
		schedule_timer(
			&queue,
			Timer_Expiration {
				timer_id = 2,
				deadline_ns = 5,
			},
		),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(t, cancel_timer(&queue, 2), Timer_Queue_Result.Cancelled)
	testing.expect_value(t, cancel_timer(&queue, 2), Timer_Queue_Result.Not_Found)
}
