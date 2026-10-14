package shutdown

import "veylor:transport"

Phase :: enum {
	Running,
	Draining,
	Forcing,
	Complete,
	Destroyed,
}

Result :: enum {
	Accepted,
	Draining,
	Force_Connections,
	Connection_Closed,
	Completed,
	Stale_Expiration,
	Not_Accepting,
	Timer_Full,
	Duplicate_Timer,
	Deadline_Overflow,
	Busy,
	Destroyed,
	Invalid,
}

State :: struct {
	active_connections: u64,
	timer_id:           u64,
	deadline_ns:        u64,
	phase:              Phase,
	has_deadline:       bool,
}

request_shutdown :: proc(
	state: ^State,
	timers: ^transport.Timer_Queue,
	timer_id: u64,
	now_ns: u64,
	grace_ns: u64,
) -> Result {
	if state == nil {
		return .Invalid
	}

	if state.phase != .Running {
		return .Invalid
	}

	if state.active_connections == 0 {
		state.phase = .Complete
		return .Completed
	}

	if grace_ns == 0 {
		state.phase = .Forcing
		return .Force_Connections
	}

	if timers == nil {
		return .Invalid
	}

	timer, timer_result := schedule_deadline(timers, timer_id, now_ns, grace_ns)
	switch timer_result {
	case .Full:
		return .Timer_Full
	case .Duplicate:
		return .Duplicate_Timer
	case .Overflow:
		return .Deadline_Overflow
	case .Invalid:
		return .Invalid
	case .Scheduled:
		state.timer_id = timer.timer_id
		state.deadline_ns = timer.deadline_ns
		state.has_deadline = true
		state.phase = .Draining
		return .Draining
	case .Cancelled, .Not_Found:
		return .Invalid
	}
	return .Invalid
}

register_connection :: proc(state: ^State) -> Result {
	return register_connections(state, 1)
}

register_connections :: proc(state: ^State, count: u64) -> Result {
	if state == nil {
		return .Invalid
	}

	if state.phase != .Running {
		return .Not_Accepting
	}

	if count > 0xffff_ffff_ffff_ffff - state.active_connections {
		return .Invalid
	}
	state.active_connections += count
	return .Accepted
}

close_connection :: proc(state: ^State, timers: ^transport.Timer_Queue) -> Result {
	if state == nil {
		return .Invalid
	}

	if state.active_connections == 0 {
		return .Invalid
	}

	if state.has_deadline {
		if timers == nil {
			return .Invalid
		}

		if timers.count < 0 {
			return .Invalid
		}

		if timers.count > len(timers.storage) {
			return .Invalid
		}
	}
	state.active_connections -= 1

	if state.active_connections != 0 {
		return .Connection_Closed
	}

	if state.phase != .Draining && state.phase != .Forcing {
		return .Connection_Closed
	}

	if state.has_deadline {
		timer_result := cancel_deadline(timers, state.timer_id)

		if timer_result != .Cancelled && timer_result != .Not_Found {
			return .Invalid
		}
		state.has_deadline = false
	}
	state.phase = .Complete
	return .Completed
}

process_deadline :: proc(state: ^State, timer: transport.Timer_Expiration) -> Result {
	if state == nil {
		return .Invalid
	}

	if state.phase != .Draining {
		return .Stale_Expiration
	}

	if !state.has_deadline {
		return .Stale_Expiration
	}

	if timer.timer_id != state.timer_id {
		return .Stale_Expiration
	}

	if timer.deadline_ns != state.deadline_ns {
		return .Stale_Expiration
	}
	state.has_deadline = false
	state.phase = .Forcing
	return .Force_Connections
}

destroy :: proc(state: ^State) -> Result {
	if state == nil {
		return .Invalid
	}

	if state.phase == .Destroyed {
		return .Destroyed
	}

	if state.phase != .Complete {
		return .Busy
	}

	if state.active_connections != 0 {
		return .Busy
	}

	if state.has_deadline {
		return .Busy
	}
	state.phase = .Destroyed
	return .Destroyed
}
