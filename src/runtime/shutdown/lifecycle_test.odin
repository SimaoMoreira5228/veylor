#+test
package shutdown

import "core:testing"
import "veylor:transport"

@(test)
test_shutdown_stops_admission_drains_until_deadline_then_completes :: proc(t: ^testing.T) {
	state: State
	testing.expect_value(t, register_connection(&state), Result.Accepted)
	testing.expect_value(t, register_connection(&state), Result.Accepted)

	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])

	testing.expect_value(t, request_shutdown(&state, &timers, 71, 100, 50), Result.Draining)
	testing.expect_value(t, state.phase, Phase.Draining)
	testing.expect_value(t, state.active_connections, u64(2))
	testing.expect_value(t, timer_storage[0].deadline_ns, u64(150))
	testing.expect_value(t, register_connection(&state), Result.Not_Accepting)

	testing.expect_value(
		t,
		process_deadline(&state, transport.Timer_Expiration{timer_id = 71, deadline_ns = 149}),
		Result.Stale_Expiration,
	)
	testing.expect_value(t, process_deadline(&state, timer_storage[0]), Result.Force_Connections)
	testing.expect_value(t, state.phase, Phase.Forcing)
	testing.expect_value(t, state.has_deadline, false)

	testing.expect_value(t, close_connection(&state, &timers), Result.Connection_Closed)
	testing.expect_value(t, state.active_connections, u64(1))
	testing.expect_value(t, close_connection(&state, &timers), Result.Completed)
	testing.expect_value(t, state.phase, Phase.Complete)
	testing.expect_value(t, destroy(&state), Result.Destroyed)
	testing.expect_value(t, state.phase, Phase.Destroyed)
}

@(test)
test_shutdown_completes_early_and_cancels_deadline :: proc(t: ^testing.T) {
	state: State
	testing.expect_value(t, register_connection(&state), Result.Accepted)
	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	testing.expect_value(t, request_shutdown(&state, &timers, 81, 200, 75), Result.Draining)
	testing.expect_value(t, close_connection(&state, &timers), Result.Completed)
	testing.expect_value(t, timers.count, 0)
	testing.expect_value(t, state.phase, Phase.Complete)
	testing.expect_value(t, process_deadline(&state, timer_storage[0]), Result.Stale_Expiration)
	testing.expect_value(t, destroy(&state), Result.Destroyed)
}

@(test)
test_shutdown_does_not_change_state_when_deadline_cannot_be_scheduled :: proc(t: ^testing.T) {
	state: State
	testing.expect_value(t, register_connection(&state), Result.Accepted)
	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	testing.expect_value(
		t,
		transport.schedule_timer(
			&timers,
			transport.Timer_Expiration{timer_id = 91, deadline_ns = 10},
		),
		transport.Timer_Queue_Result.Scheduled,
	)
	testing.expect_value(t, request_shutdown(&state, &timers, 92, 20, 10), Result.Timer_Full)
	testing.expect_value(t, state.phase, Phase.Running)
	testing.expect_value(t, register_connection(&state), Result.Accepted)

	transport.cancel_timer(&timers, 91)
	testing.expect_value(t, request_shutdown(&state, &timers, 92, 20, 10), Result.Draining)
	testing.expect_value(t, destroy(&state), Result.Busy)
	deadline, timer_result := transport.pop_expired_timer(&timers, 30)
	testing.expect_value(t, timer_result, transport.Timer_Queue_Result.Expired)
	testing.expect_value(t, process_deadline(&state, deadline), Result.Force_Connections)
	testing.expect_value(t, close_connection(&state, &timers), Result.Connection_Closed)
	testing.expect_value(t, close_connection(&state, &timers), Result.Completed)
	testing.expect_value(t, destroy(&state), Result.Destroyed)
}

@(test)
test_zero_grace_shutdown_forces_connections_immediately :: proc(t: ^testing.T) {
	state: State
	testing.expect_value(t, register_connection(&state), Result.Accepted)
	testing.expect_value(t, request_shutdown(nil, nil, 1, 0, 0), Result.Invalid)
	testing.expect_value(t, request_shutdown(&state, nil, 1, 0, 0), Result.Force_Connections)
	testing.expect_value(t, state.phase, Phase.Forcing)
	close_result := close_connection(&state, nil)
	testing.expect_value(t, close_result, Result.Completed)
	testing.expect_value(t, destroy(&state), Result.Destroyed)
}

@(test)
test_force_shutdown_completes_without_connections :: proc(t: ^testing.T) {
	state: State
	testing.expect_value(t, force_shutdown(&state, nil), Result.Completed)
	testing.expect_value(t, state.phase, Phase.Complete)
	testing.expect_value(t, state.active_connections, u64(0))
	testing.expect_value(t, destroy(&state), Result.Destroyed)
}

@(test)
test_force_shutdown_cancels_drain_deadline_before_forcing_connections :: proc(t: ^testing.T) {
	state: State
	testing.expect_value(t, register_connection(&state), Result.Accepted)
	timer_storage: [1]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	testing.expect_value(t, request_shutdown(&state, &timers, 101, 500, 100), Result.Draining)
	testing.expect_value(t, timers.count, 1)
	testing.expect_value(t, force_shutdown(&state, &timers), Result.Force_Connections)
	testing.expect_value(t, state.phase, Phase.Forcing)
	testing.expect_value(t, state.has_deadline, false)
	testing.expect_value(t, timers.count, 0)
	testing.expect_value(t, close_connection(&state, &timers), Result.Completed)
	testing.expect_value(t, destroy(&state), Result.Destroyed)
}
