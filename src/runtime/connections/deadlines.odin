package connections

import "veylor:transport"

Connection_Timeouts :: struct {
	request_head_ns:    u64,
	body_progress_ns:   u64,
	response_write_ns:  u64,
	keep_alive_idle_ns: u64,
}

Connection_Deadline_Schedule_Result :: enum {
	Not_Scheduled,
	Scheduled,
	Cancelled,
	Full,
	Duplicate_Timer,
	Connection_Already_Timed,
	Phase_Mismatch,
	Stale_Connection,
	Invalid_Timeout,
	Deadline_Overflow,
	Timer_Missing,
	Not_Found,
	Invalid,
}

Connection_Deadline_Schedule :: struct {
	timer:  transport.Timer_Expiration,
	result: Connection_Deadline_Schedule_Result,
}

schedule_connection_phase_deadline :: proc(
	table: ^Connection_Table,
	timers: ^transport.Timer_Queue,
	token: Connection_Token,
	timer_id: u64,
	now_ns: u64,
	timeouts: Connection_Timeouts,
) -> Connection_Deadline_Schedule {
	if table == nil || timers == nil || timers.count < 0 || timers.count > len(timers.storage) {
		return Connection_Deadline_Schedule{result = .Invalid}
	}
	phase, lookup_result := connection_phase(table, token)
	if lookup_result == .Stale {
		return Connection_Deadline_Schedule{result = .Stale_Connection}
	}
	if lookup_result != .Found {
		return Connection_Deadline_Schedule{result = .Invalid}
	}
	kind, valid := connection_deadline_kind(phase)
	if !valid {
		return Connection_Deadline_Schedule{result = .Phase_Mismatch}
	}
	timeout_ns := connection_timeout_ns(timeouts, kind)
	if timeout_ns == 0 {
		return Connection_Deadline_Schedule{result = .Invalid_Timeout}
	}
	if timeout_ns > 0xffff_ffff_ffff_ffff - now_ns {
		return Connection_Deadline_Schedule{result = .Deadline_Overflow}
	}

	timer := transport.Timer_Expiration {
		timer_id    = timer_id,
		deadline_ns = now_ns + timeout_ns,
	}
	timer_result := transport.schedule_timer(timers, timer)
	if timer_result == .Full {
		return Connection_Deadline_Schedule{result = .Full}
	}
	if timer_result == .Duplicate {
		return Connection_Deadline_Schedule{result = .Duplicate_Timer}
	}
	if timer_result != .Scheduled {
		return Connection_Deadline_Schedule{result = .Invalid}
	}

	bind_result := bind_connection_deadline(table, token, timer_id, kind)
	if bind_result != .Deadline_Set {
		transport.cancel_timer(timers, timer_id)
		return Connection_Deadline_Schedule {
			result = connection_deadline_schedule_result(bind_result),
		}
	}
	return Connection_Deadline_Schedule{timer = timer, result = .Scheduled}
}

cancel_connection_phase_deadline :: proc(
	table: ^Connection_Table,
	timers: ^transport.Timer_Queue,
	token: Connection_Token,
) -> Connection_Deadline_Schedule_Result {
	if table == nil || timers == nil || timers.count < 0 || timers.count > len(timers.storage) {
		return .Invalid
	}
	if token.worker_id != table.worker_id || token.worker_generation != table.worker_generation {
		return .Stale_Connection
	}
	if u64(token.slot) >= u64(len(table.slots)) {
		return .Invalid
	}
	slot := &table.slots[int(token.slot)]
	if slot.state != .Occupied || slot.generation != token.generation {
		return .Stale_Connection
	}
	if !slot.has_deadline {
		return .Not_Found
	}

	timer_id := slot.deadline_timer_id
	if transport.cancel_timer(timers, timer_id) != .Cancelled {
		return .Timer_Missing
	}
	if clear_connection_deadline(table, token, timer_id) != .Deadline_Cleared {
		return .Invalid
	}
	return .Cancelled
}

connection_timeout_ns :: proc(
	timeouts: Connection_Timeouts,
	kind: Connection_Deadline_Kind,
) -> u64 {
	switch kind {
	case .Request_Head:
		return timeouts.request_head_ns
	case .Body_Progress:
		return timeouts.body_progress_ns
	case .Response_Write:
		return timeouts.response_write_ns
	case .Keep_Alive_Idle:
		return timeouts.keep_alive_idle_ns
	}
	return 0
}

connection_deadline_schedule_result :: proc(
	result: Connection_Table_Result,
) -> Connection_Deadline_Schedule_Result {
	switch result {
	case .Invalid_Transition:
		return .Connection_Already_Timed
	case .Stale:
		return .Stale_Connection
	case .Duplicate_Deadline:
		return .Duplicate_Timer
	case .Invalid:
		return .Invalid
	case .Deadline_Set:
		return .Scheduled
	case .Ready,
	     .Full,
	     .Found,
	     .Released,
	     .Retired,
	     .Transitioned,
	     .Deadline_Cleared,
	     .Deadline_Expired,
	     .Not_Found:
		return .Invalid
	}
	return .Invalid
}
