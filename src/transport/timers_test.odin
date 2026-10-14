#+test
package transport

import "core:testing"

@(test)
test_timer_queue_expires_in_deadline_then_id_order :: proc(t: ^testing.T) {
	storage: [4]Timer_Expiration
	queue := init_timer_queue(storage[:])
	testing.expect_value(
		t,
		schedule_timer(&queue, Timer_Expiration{timer_id = 4, deadline_ns = 20}),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		schedule_timer(&queue, Timer_Expiration{timer_id = 3, deadline_ns = 10}),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		schedule_timer(&queue, Timer_Expiration{timer_id = 2, deadline_ns = 10}),
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
	queue := init_timer_queue(storage[:])
	timer := Timer_Expiration {
		timer_id    = 1,
		deadline_ns = 10,
	}
	testing.expect_value(t, schedule_timer(&queue, timer), Timer_Queue_Result.Scheduled)
	testing.expect_value(t, schedule_timer(&queue, timer), Timer_Queue_Result.Duplicate)
	testing.expect_value(
		t,
		schedule_timer(&queue, Timer_Expiration{timer_id = 2, deadline_ns = 20}),
		Timer_Queue_Result.Full,
	)
	testing.expect_value(t, queue.count, 1)
}

@(test)
test_timer_queue_cancels_root_and_interior_entries :: proc(t: ^testing.T) {
	storage: [5]Timer_Expiration
	queue := init_timer_queue(storage[:])
	testing.expect_value(
		t,
		schedule_timer(&queue, Timer_Expiration{timer_id = 4, deadline_ns = 40}),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		schedule_timer(&queue, Timer_Expiration{timer_id = 1, deadline_ns = 10}),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		schedule_timer(&queue, Timer_Expiration{timer_id = 3, deadline_ns = 30}),
		Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(
		t,
		schedule_timer(&queue, Timer_Expiration{timer_id = 2, deadline_ns = 20}),
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
