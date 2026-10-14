#+test
package connections

import "core:testing"
import "veylor:transport"

@(test)
test_phase_deadline_scheduling_cancellation_and_expiration :: proc(t: ^testing.T) {
	slots: [1]Connection_Slot
	free_stack: [1]u32
	table, table_result := init_connection_table(13, 4, slots[:], free_stack[:])
	testing.expect_value(t, table_result, Connection_Table_Result.Ready)
	token: Connection_Token
	token, table_result = allocate_connection(&table, 901)
	testing.expect_value(t, table_result, Connection_Table_Result.Ready)

	timer_storage: [2]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	timeouts := Connection_Timeouts {
		request_head_ns    = 10,
		body_progress_ns   = 20,
		response_write_ns  = 30,
		keep_alive_idle_ns = 40,
	}

	scheduled := schedule_connection_phase_deadline(&table, &timers, token, 1001, 100, timeouts)
	testing.expect_value(t, scheduled.result, Connection_Deadline_Schedule_Result.Scheduled)
	testing.expect_value(t, scheduled.timer.deadline_ns, u64(110))
	testing.expect_value(t, timers.count, 1)

	scheduled_again := schedule_connection_phase_deadline(
		&table,
		&timers,
		token,
		1002,
		100,
		timeouts,
	)
	testing.expect_value(
		t,
		scheduled_again.result,
		Connection_Deadline_Schedule_Result.Connection_Already_Timed,
	)
	testing.expect_value(t, timers.count, 1)
	testing.expect_value(
		t,
		cancel_connection_phase_deadline(&table, &timers, token),
		Connection_Deadline_Schedule_Result.Cancelled,
	)
	testing.expect_value(t, timers.count, 0)

	scheduled = schedule_connection_phase_deadline(&table, &timers, token, 1003, 200, timeouts)
	testing.expect_value(t, scheduled.result, Connection_Deadline_Schedule_Result.Scheduled)
	expired_timer, timer_result := transport.pop_expired_timer(&timers, 210)
	testing.expect_value(t, timer_result, transport.Timer_Queue_Result.Expired)
	completed: Connection_Deadline
	completed, table_result = expire_connection_deadline(&table, expired_timer.timer_id)
	testing.expect_value(t, table_result, Connection_Table_Result.Deadline_Expired)
	testing.expect_value(t, completed.token, token)
	testing.expect_value(t, completed.kind, Connection_Deadline_Kind.Request_Head)

	testing.expect_value(
		t,
		transition_connection(&table, token, .Reading_Request_Body),
		Connection_Table_Result.Transitioned,
	)
	scheduled = schedule_connection_phase_deadline(&table, &timers, token, 1004, 300, timeouts)
	testing.expect_value(t, scheduled.result, Connection_Deadline_Schedule_Result.Scheduled)
	testing.expect_value(t, scheduled.timer.deadline_ns, u64(320))
	testing.expect_value(
		t,
		cancel_connection_phase_deadline(&table, &timers, token),
		Connection_Deadline_Schedule_Result.Cancelled,
	)

	testing.expect_value(
		t,
		transition_connection(&table, token, .Awaiting_Handler),
		Connection_Table_Result.Transitioned,
	)
	testing.expect_value(
		t,
		transition_connection(&table, token, .Writing_Response),
		Connection_Table_Result.Transitioned,
	)
	scheduled = schedule_connection_phase_deadline(&table, &timers, token, 1005, 400, timeouts)
	testing.expect_value(t, scheduled.result, Connection_Deadline_Schedule_Result.Scheduled)
	testing.expect_value(t, scheduled.timer.deadline_ns, u64(430))
	testing.expect_value(
		t,
		cancel_connection_phase_deadline(&table, &timers, token),
		Connection_Deadline_Schedule_Result.Cancelled,
	)

	testing.expect_value(
		t,
		transition_connection(&table, token, .Keep_Alive_Idle),
		Connection_Table_Result.Transitioned,
	)
	scheduled = schedule_connection_phase_deadline(&table, &timers, token, 1006, 500, timeouts)
	testing.expect_value(t, scheduled.result, Connection_Deadline_Schedule_Result.Scheduled)
	testing.expect_value(t, scheduled.timer.deadline_ns, u64(540))
	testing.expect_value(
		t,
		release_connection(&table, token),
		Connection_Table_Result.Invalid_Transition,
	)
	testing.expect_value(
		t,
		cancel_connection_phase_deadline(&table, &timers, token),
		Connection_Deadline_Schedule_Result.Cancelled,
	)
	testing.expect_value(t, release_connection(&table, token), Connection_Table_Result.Released)
	testing.expect_value(t, timers.count, 0)
}

@(test)
test_phase_deadline_rejects_zero_overflow_full_and_stale_requests :: proc(t: ^testing.T) {
	slots: [1]Connection_Slot
	free_stack: [1]u32
	table, table_result := init_connection_table(14, 5, slots[:], free_stack[:])
	testing.expect_value(t, table_result, Connection_Table_Result.Ready)
	token: Connection_Token
	token, table_result = allocate_connection(&table, 902)
	testing.expect_value(t, table_result, Connection_Table_Result.Ready)

	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	zero_timeouts := Connection_Timeouts{}
	result := schedule_connection_phase_deadline(&table, &timers, token, 1101, 0, zero_timeouts)
	testing.expect_value(t, result.result, Connection_Deadline_Schedule_Result.Invalid_Timeout)

	timeouts := Connection_Timeouts {
		request_head_ns = 2,
	}
	result = schedule_connection_phase_deadline(
		&table,
		&timers,
		token,
		1102,
		0xffff_ffff_ffff_fffe,
		timeouts,
	)
	testing.expect_value(t, result.result, Connection_Deadline_Schedule_Result.Deadline_Overflow)

	testing.expect_value(
		t,
		transport.schedule_timer(
			&timers,
			transport.Timer_Expiration{timer_id = 1199, deadline_ns = 50},
		),
		transport.Timer_Queue_Result.Scheduled,
	)
	result = schedule_connection_phase_deadline(&table, &timers, token, 1103, 10, timeouts)
	testing.expect_value(t, result.result, Connection_Deadline_Schedule_Result.Full)
	testing.expect_value(t, table.slots[0].has_deadline, false)

	transport.cancel_timer(&timers, 1199)
	result = schedule_connection_phase_deadline(&table, &timers, token, 1104, 10, timeouts)
	testing.expect_value(t, result.result, Connection_Deadline_Schedule_Result.Scheduled)
	cancel_result := cancel_connection_phase_deadline(&table, &timers, token)
	testing.expect_value(t, cancel_result, Connection_Deadline_Schedule_Result.Cancelled)
	testing.expect_value(
		t,
		cancel_connection_phase_deadline(&table, &timers, token),
		Connection_Deadline_Schedule_Result.Not_Found,
	)
	testing.expect_value(t, release_connection(&table, token), Connection_Table_Result.Released)
	result = schedule_connection_phase_deadline(&table, &timers, token, 1105, 10, timeouts)
	testing.expect_value(t, result.result, Connection_Deadline_Schedule_Result.Stale_Connection)
}

@(test)
test_keep_alive_idle_deadline_cancels_when_next_request_starts :: proc(t: ^testing.T) {
	slots: [1]Connection_Slot
	free_stack: [1]u32
	table, table_result := init_connection_table(21, 12, slots[:], free_stack[:])
	testing.expect_value(t, table_result, Connection_Table_Result.Ready)
	token: Connection_Token
	token, table_result = allocate_connection(&table, 909)
	testing.expect_value(t, table_result, Connection_Table_Result.Ready)
	testing.expect_value(
		t,
		transition_connection(&table, token, .Writing_Response),
		Connection_Table_Result.Transitioned,
	)

	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	timeouts := Connection_Timeouts {
		request_head_ns    = 100,
		keep_alive_idle_ns = 10,
	}
	idle := start_keep_alive_idle_deadline(&table, &timers, token, 1701, 100, timeouts)
	testing.expect_value(t, idle.result, Connection_Deadline_Schedule_Result.Scheduled)
	testing.expect_value(t, idle.timer.deadline_ns, u64(110))
	phase, phase_result := connection_phase(&table, token)
	testing.expect_value(t, phase_result, Connection_Table_Result.Found)
	testing.expect_value(t, phase, Connection_Phase.Keep_Alive_Idle)

	testing.expect_value(
		t,
		resume_keep_alive_request(&table, &timers, token),
		Connection_Deadline_Schedule_Result.Ready,
	)
	testing.expect_value(t, timers.count, 0)
	phase, phase_result = connection_phase(&table, token)
	testing.expect_value(t, phase_result, Connection_Table_Result.Found)
	testing.expect_value(t, phase, Connection_Phase.Reading_Request_Head)

	testing.expect_value(
		t,
		transition_connection(&table, token, .Reading_Request_Body),
		Connection_Table_Result.Transitioned,
	)
	testing.expect_value(
		t,
		transition_connection(&table, token, .Awaiting_Handler),
		Connection_Table_Result.Transitioned,
	)
	testing.expect_value(
		t,
		transition_connection(&table, token, .Writing_Response),
		Connection_Table_Result.Transitioned,
	)
	idle = start_keep_alive_idle_deadline(&table, &timers, token, 1702, 200, timeouts)
	testing.expect_value(t, idle.result, Connection_Deadline_Schedule_Result.Scheduled)
	expired_timer, timer_result := transport.pop_expired_timer(&timers, 210)
	testing.expect_value(t, timer_result, transport.Timer_Queue_Result.Expired)
	deadline: Connection_Deadline
	deadline, table_result = expire_connection_deadline(&table, expired_timer.timer_id)
	testing.expect_value(t, table_result, Connection_Table_Result.Deadline_Expired)
	testing.expect_value(t, deadline.kind, Connection_Deadline_Kind.Keep_Alive_Idle)
	testing.expect_value(t, deadline.token, token)
	testing.expect_value(t, release_connection(&table, token), Connection_Table_Result.Released)
}
