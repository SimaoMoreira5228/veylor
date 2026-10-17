#+test
package shutdown

import "core:testing"
import "veylor:transport"

@(test)
test_shutdown_deadline_is_monotonic_bounded_and_cancellable :: proc(t: ^testing.T) {
	storage: [1]transport.Timer_Expiration
	index_storage: [transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(storage[:], index_storage[:])
	timer, result := schedule_deadline(&timers, 1, 100, 50)
	testing.expect_value(t, result, Deadline_Result.Scheduled)
	testing.expect_value(t, timer.deadline_ns, u64(150))

	_, result = schedule_deadline(&timers, 1, 200, 50)
	testing.expect_value(t, result, Deadline_Result.Duplicate)
	_, result = schedule_deadline(&timers, 2, 200, 50)
	testing.expect_value(t, result, Deadline_Result.Full)

	_, timer_result := transport.pop_expired_timer(&timers, 149)
	testing.expect_value(t, timer_result, transport.Timer_Queue_Result.Not_Due)
	expired: transport.Timer_Expiration
	expired, timer_result = transport.pop_expired_timer(&timers, 150)
	testing.expect_value(t, timer_result, transport.Timer_Queue_Result.Expired)
	testing.expect_value(t, expired.timer_id, u64(1))

	_, result = schedule_deadline(&timers, 3, 300, 25)
	testing.expect_value(t, result, Deadline_Result.Scheduled)
	testing.expect_value(t, cancel_deadline(&timers, 3), Deadline_Result.Cancelled)
	testing.expect_value(t, timers.count, 0)
	testing.expect_value(t, cancel_deadline(&timers, 3), Deadline_Result.Not_Found)
}

@(test)
test_shutdown_deadline_rejects_zero_and_overflowing_timeouts :: proc(t: ^testing.T) {
	storage: [1]transport.Timer_Expiration
	index_storage: [transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(storage[:], index_storage[:])
	_, result := schedule_deadline(&timers, 1, 100, 0)
	testing.expect_value(t, result, Deadline_Result.Invalid)
	_, result = schedule_deadline(&timers, 2, 0xffff_ffff_ffff_ffff, 1)
	testing.expect_value(t, result, Deadline_Result.Overflow)
	testing.expect_value(t, timers.count, 0)
}
