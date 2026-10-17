#+build linux
#+test
package linux_runtime

import "core:testing"
import "veylor:transport"

@(test)
test_listener_descriptor_limit_retries_after_bounded_backoff :: proc(t: ^testing.T) {
	timer_storage: [1]transport.Timer_Expiration
	timer_index_storage: [transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	worker := Static_Worker {
		timers                  = &timers,
		descriptor_limit_paused = true,
	}
	now_ns := u64(100)
	testing.expect_value(t, schedule_static_worker_listener_retry(&worker, now_ns), true)
	testing.expect_value(t, worker.descriptor_retry_scheduled, true)
	testing.expect_value(t, timers.count, 1)
	testing.expect_value(t, timer_storage[0].timer_id, STATIC_LISTENER_RETRY_TIMER_ID)
	testing.expect_value(t, timer_storage[0].deadline_ns, now_ns + STATIC_LISTENER_RETRY_DELAY_NS)
	testing.expect_value(t, schedule_static_worker_listener_retry(&worker, now_ns), true)
	testing.expect_value(t, timers.count, 1)

	timer, timer_result := transport.pop_expired_timer(
		&timers,
		now_ns + STATIC_LISTENER_RETRY_DELAY_NS - 1,
	)
	testing.expect_value(t, timer_result, transport.Timer_Queue_Result.Not_Due)
	testing.expect_value(t, timers.count, 1)
	timer, timer_result = transport.pop_expired_timer(
		&timers,
		now_ns + STATIC_LISTENER_RETRY_DELAY_NS,
	)
	testing.expect_value(t, timer_result, transport.Timer_Queue_Result.Expired)
	expired_timers := [?]transport.Timer_Expiration{timer}
	turn := Static_Worker_Turn {
		result = .Ready,
	}
	process_static_worker_expirations(&worker, expired_timers[:], &turn)
	testing.expect_value(t, worker.descriptor_retry_scheduled, false)
	testing.expect_value(t, worker.descriptor_limit_paused, false)
	testing.expect_value(t, turn.result, Static_Worker_Result.Ready)
}

@(test)
test_listener_retry_rejects_full_timer_storage_and_deadline_overflow :: proc(t: ^testing.T) {
	full_storage: [1]transport.Timer_Expiration
	full_index_storage: [transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	full_timers := transport.init_timer_queue(full_storage[:], full_index_storage[:])
	testing.expect_value(
		t,
		transport.schedule_timer(
			&full_timers,
			transport.Timer_Expiration{timer_id = 41, deadline_ns = 100},
		),
		transport.Timer_Queue_Result.Scheduled,
	)
	full_worker := Static_Worker {
		timers                  = &full_timers,
		descriptor_limit_paused = true,
	}
	testing.expect_value(t, schedule_static_worker_listener_retry(&full_worker, 100), false)
	testing.expect_value(t, full_worker.descriptor_retry_scheduled, false)
	testing.expect_value(t, full_timers.count, 1)

	overflow_storage: [1]transport.Timer_Expiration
	overflow_index_storage: [transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	overflow_timers := transport.init_timer_queue(overflow_storage[:], overflow_index_storage[:])
	overflow_worker := Static_Worker {
		timers                  = &overflow_timers,
		descriptor_limit_paused = true,
	}
	testing.expect_value(
		t,
		schedule_static_worker_listener_retry(&overflow_worker, 0xffff_ffff_ffff_ffff),
		false,
	)
	testing.expect_value(t, overflow_worker.descriptor_retry_scheduled, false)
	testing.expect_value(t, overflow_timers.count, 0)
}
