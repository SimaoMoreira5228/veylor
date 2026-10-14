#+build linux
package linux_runtime

import "core:sys/linux"
import http1 "veylor:http1"
import connection "veylor:http1/connection"
import connections "veylor:runtime/connections"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

Worker_Turn :: struct {
	stream_events:              []transport.Readiness_Event,
	accepted:                   []Worker_Connection,
	expired_timers:             []transport.Timer_Expiration,
	byte_budget_per_connection: int,
	poll_result:                epoll.Result,
	accept_result:              Accept_Turn_Result,
	error_code:                 i32,
}

Head_Read_Turn :: struct {
	head:             connection.Connection_Head_Event,
	deadline_result:  connections.Connection_Deadline_Schedule_Result,
	bytes_read:       int,
	budget_exhausted: bool,
	input_full:       bool,
}

read_connection_head_turn :: proc(
	connection_id: u64,
	fd: linux.Fd,
	driver: ^connection.Connection_Head_Driver,
	input: []u8,
	headers: []http1.Header,
	byte_budget: int,
) -> Head_Read_Turn {
	if driver.state == .Ready {
		return Head_Read_Turn {
			head = connection.Connection_Head_Event {
				head = http1.request_head(&driver.parser, headers),
				result = .Ready,
			},
		}
	}
	if driver.state != .Reading {
		return Head_Read_Turn{head = connection.connection_head_event(driver)}
	}
	if byte_budget <= 0 || driver.input_length < 0 || driver.input_length > len(input) {
		return Head_Read_Turn{head = connection.Connection_Head_Event{result = .Invalid}}
	}
	if driver.input_length == len(input) {
		return Head_Read_Turn {
			head = connection.reject_connection_head(driver, .Too_Large),
			input_full = true,
		}
	}

	budget, valid := socket.begin_stream_byte_budget(byte_budget)
	if !valid {
		return Head_Read_Turn{head = connection.Connection_Head_Event{result = .Invalid}}
	}
	read := socket.read_stream_budgeted(fd, input[driver.input_length:], &budget)
	input_length := driver.input_length
	if read.io.kind == .Progress {
		input_length += read.io.transferred_bytes
	}
	completion := transport.IO_Completion {
		connection_id = connection_id,
		operation_id  = driver.operation_id + 1,
		operation     = .Read,
		result        = read.io,
	}
	head := connection.consume_read_completion(driver, completion, input[:input_length], headers)
	if input_length == len(input) && head.result == .Need_More {
		head = connection.reject_connection_head(driver, .Too_Large)
		return Head_Read_Turn {
			head = head,
			bytes_read = read.io.transferred_bytes,
			budget_exhausted = read.budget_exhausted,
			input_full = true,
		}
	}
	return Head_Read_Turn {
		head = head,
		bytes_read = read.io.transferred_bytes,
		budget_exhausted = read.budget_exhausted,
	}
}

read_connection_head_turn_with_deadline :: proc(
	connection_id: u64,
	fd: linux.Fd,
	driver: ^connection.Connection_Head_Driver,
	input: []u8,
	headers: []http1.Header,
	byte_budget: int,
	table: ^connections.Connection_Table,
	token: connections.Connection_Token,
	timers: ^transport.Timer_Queue,
	timer_id: u64,
	now_ns: u64,
	timeouts: connections.Connection_Timeouts,
) -> Head_Read_Turn {
	if driver.state == .Reading && !driver.has_operation {
		schedule := connections.schedule_connection_phase_deadline(
			table,
			timers,
			token,
			timer_id,
			now_ns,
			timeouts,
		)
		if schedule.result != .Scheduled {
			return Head_Read_Turn {
				head = connection.Connection_Head_Event{result = .Invalid},
				deadline_result = schedule.result,
			}
		}
	}

	turn := read_connection_head_turn(connection_id, fd, driver, input, headers, byte_budget)
	if turn.head.result == .Need_More ||
	   turn.head.result == .Would_Block ||
	   turn.head.result == .Interrupted {
		return turn
	}

	deadline_result := connections.cancel_connection_phase_deadline(table, timers, token)
	if deadline_result != .Cancelled && deadline_result != .Not_Found {
		turn.deadline_result = deadline_result
		return turn
	}
	turn.deadline_result = .Cancelled
	return turn
}

run_worker_turn :: proc(
	poller: epoll.Epoll,
	listener: socket.Listener,
	listener_id: u64,
	listener_registered: ^bool,
	descriptor_limit_paused: ^bool,
	table: ^connections.Connection_Table,
	native_events: []linux.EPoll_Event,
	readiness_events: []transport.Readiness_Event,
	accepted_storage: []socket.Accepted_Connection,
	connection_storage: []Worker_Connection,
	timers: ^transport.Timer_Queue,
	expired_timer_storage: []transport.Timer_Expiration,
	event_budget: int,
	byte_budget_per_connection: int,
	accept_attempt_budget: int,
	timeout_ms: i32,
) -> Worker_Turn {
	if listener_registered == nil ||
	   descriptor_limit_paused == nil ||
	   timers == nil ||
	   event_budget <= 0 ||
	   byte_budget_per_connection <= 0 ||
	   accept_attempt_budget <= 0 ||
	   table.free_count < 0 ||
	   table.free_count > len(table.slots) ||
	   table.free_count > len(table.free_stack) ||
	   timers.count < 0 ||
	   timers.count > len(timers.storage) ||
	   len(expired_timer_storage) == 0 {
		return Worker_Turn{poll_result = .Invalid, accept_result = .Invalid}
	}
	capacity := min(event_budget, len(native_events), len(readiness_events))
	if capacity == 0 {
		return Worker_Turn{poll_result = .Invalid, accept_result = .Invalid}
	}
	if (table.free_count == 0 || descriptor_limit_paused^) && listener_registered^ {
		result, error_code := epoll.unregister(poller, listener.fd)
		if result != .Ready {
			return Worker_Turn {
				poll_result = result,
				accept_result = .Table_Full,
				error_code = error_code,
			}
		}
		listener_registered^ = false
	} else if table.free_count > 0 && !descriptor_limit_paused^ && !listener_registered^ {
		result, error_code := epoll.register(
			poller,
			listener.fd,
			listener_id,
			epoll.Interest{readable = true},
		)
		if result != .Ready {
			return Worker_Turn {
				poll_result = result,
				accept_result = .Error,
				error_code = error_code,
			}
		}
		listener_registered^ = true
	}
	now, clock_error := linux.clock_gettime(.MONOTONIC)
	if clock_error != .NONE {
		return Worker_Turn {
			poll_result = .System_Error,
			accept_result = .Error,
			error_code = i32(clock_error),
		}
	}
	now_ns := u64(now.time_sec) * 1_000_000_000 + u64(now.time_nsec)
	wait_timeout_ms := transport.timer_wait_timeout_ms(timers, now_ns, timeout_ms)
	batch := epoll.wait(
		poller,
		native_events[:capacity],
		readiness_events[:capacity],
		wait_timeout_ms,
	)
	turn := Worker_Turn {
		byte_budget_per_connection = byte_budget_per_connection,
		poll_result                = batch.result,
		accept_result              = .Not_Ready,
	}
	if batch.result != .Ready {
		turn.error_code = batch.error_code
		if batch.result == .Timeout {
			now, clock_error = linux.clock_gettime(.MONOTONIC)
			if clock_error != .NONE {
				turn.poll_result = .System_Error
				turn.error_code = i32(clock_error)
				return turn
			}
			now_ns = u64(now.time_sec) * 1_000_000_000 + u64(now.time_nsec)
			turn.expired_timers = expire_worker_timers_at(timers, expired_timer_storage, now_ns)
		}
		return turn
	}
	now, clock_error = linux.clock_gettime(.MONOTONIC)
	if clock_error != .NONE {
		turn.poll_result = .System_Error
		turn.error_code = i32(clock_error)
		return turn
	}
	now_ns = u64(now.time_sec) * 1_000_000_000 + u64(now.time_nsec)
	turn.expired_timers = expire_worker_timers_at(timers, expired_timer_storage, now_ns)

	accepted_count := 0
	stream_event_count := 0
	listener_seen := false
	for i := 0; i < len(batch.events); i += 1 {
		event := batch.events[i]
		if event.connection_id == listener_id {
			if listener_seen {
				turn.accept_result = .Invalid
				continue
			}
			listener_seen = true
			accept_turn := accept_listener_readiness(
				listener,
				listener_id,
				event,
				table,
				accepted_storage[accepted_count:],
				connection_storage[accepted_count:],
				accept_attempt_budget,
			)
			accepted_count += len(accept_turn.connections)
			turn.accept_result = accept_turn.result
			turn.error_code = accept_turn.error_code
			if accept_turn.result == .Descriptor_Limit {
				pause_result, pause_error := pause_listener_for_descriptor_limit(
					poller,
					listener,
					listener_registered,
					descriptor_limit_paused,
				)
				if pause_result != .Ready {
					turn.poll_result = pause_result
					turn.error_code = pause_error
					return turn
				}
			}
			continue
		}
		readiness_events[stream_event_count] = event
		stream_event_count += 1
	}
	if (table.free_count == 0 || descriptor_limit_paused^) && listener_registered^ {
		result, error_code := epoll.unregister(poller, listener.fd)
		if result != .Ready {
			turn.poll_result = .System_Error
			turn.error_code = error_code
			return turn
		}
		listener_registered^ = false
	}
	turn.stream_events = readiness_events[:stream_event_count]
	turn.accepted = connection_storage[:accepted_count]
	return turn
}
pause_listener_for_descriptor_limit :: proc(
	poller: epoll.Epoll,
	listener: socket.Listener,
	listener_registered: ^bool,
	descriptor_limit_paused: ^bool,
) -> (
	result: epoll.Result,
	error_code: i32,
) {
	if listener_registered == nil || descriptor_limit_paused == nil {
		return .Invalid, 0
	}
	if listener_registered^ {
		result, error_code = epoll.unregister(poller, listener.fd)
		if result != .Ready {
			return
		}
		listener_registered^ = false
	}
	descriptor_limit_paused^ = true
	return .Ready, 0
}

expire_worker_timers_at :: proc(
	timers: ^transport.Timer_Queue,
	expired_storage: []transport.Timer_Expiration,
	now_ns: u64,
) -> []transport.Timer_Expiration {
	expired_count := 0
	for expired_count < len(expired_storage) {
		timer, result := transport.pop_expired_timer(timers, now_ns)
		if result != .Expired {
			break
		}
		expired_storage[expired_count] = timer
		expired_count += 1
	}
	return expired_storage[:expired_count]
}
