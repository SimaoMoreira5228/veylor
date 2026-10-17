package shutdown

import "veylor:transport"

Deadline_Result :: enum {
	Scheduled,
	Cancelled,
	Full,
	Duplicate,
	Not_Found,
	Invalid,
	Overflow,
}

schedule_deadline :: proc(
	timers: ^transport.Timer_Queue,
	timer_id: u64,
	now_ns: u64,
	timeout_ns: u64,
) -> (
	timer: transport.Timer_Expiration,
	result: Deadline_Result,
) {
	if timers == nil {
		return {}, .Invalid
	}

	if !transport.valid_timer_queue(timers) {
		return {}, .Invalid
	}

	if timeout_ns == 0 {
		return {}, .Invalid
	}

	if timeout_ns > 0xffff_ffff_ffff_ffff - now_ns {
		return {}, .Overflow
	}

	timer = transport.Timer_Expiration {
		timer_id    = timer_id,
		deadline_ns = now_ns + timeout_ns,
	}
	switch transport.schedule_timer(timers, timer) {
	case .Scheduled:
		return timer, .Scheduled
	case .Full:
		return {}, .Full
	case .Duplicate:
		return {}, .Duplicate
	case .Cancelled, .Not_Found, .Expired, .Not_Due, .Invalid:
		return {}, .Invalid
	}
	return {}, .Invalid
}

cancel_deadline :: proc(timers: ^transport.Timer_Queue, timer_id: u64) -> Deadline_Result {
	if timers == nil {
		return .Invalid
	}

	if !transport.valid_timer_queue(timers) {
		return .Invalid
	}
	switch transport.cancel_timer(timers, timer_id) {
	case .Cancelled:
		return .Cancelled
	case .Not_Found:
		return .Not_Found
	case .Scheduled, .Full, .Duplicate, .Expired, .Not_Due, .Invalid:
		return .Invalid
	}
	return .Invalid
}
