#+build linux
package linux_runtime

import "core:sys/linux"
import connections "veylor:runtime/connections"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

Worker_Turn :: struct {
	stream_events: []transport.Readiness_Event,
	accepted:      []Worker_Connection,
	poll_result:   epoll.Result,
	accept_result: Accept_Turn_Result,
	error_code:    i32,
}

run_worker_turn :: proc(
	poller: epoll.Epoll,
	listener: socket.Listener,
	listener_id: u64,
	listener_registered: ^bool,
	table: ^connections.Connection_Table,
	native_events: []linux.EPoll_Event,
	readiness_events: []transport.Readiness_Event,
	accepted_storage: []socket.Accepted_Connection,
	connection_storage: []Worker_Connection,
	event_budget: int,
	accept_attempt_budget: int,
	timeout_ms: i32,
) -> Worker_Turn {
	if listener_registered == nil ||
	   event_budget <= 0 ||
	   accept_attempt_budget <= 0 ||
	   table.free_count < 0 ||
	   table.free_count > len(table.slots) ||
	   table.free_count > len(table.free_stack) {
		return Worker_Turn{poll_result = .Invalid, accept_result = .Invalid}
	}
	capacity := min(event_budget, len(native_events), len(readiness_events))
	if capacity == 0 {
		return Worker_Turn{poll_result = .Invalid, accept_result = .Invalid}
	}
	if table.free_count == 0 && listener_registered^ {
		result, error_code := epoll.unregister(poller, listener.fd)
		if result != .Ready {
			return Worker_Turn {
				poll_result = result,
				accept_result = .Table_Full,
				error_code = error_code,
			}
		}
		listener_registered^ = false
	} else if table.free_count > 0 && !listener_registered^ {
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
	batch := epoll.wait(poller, native_events[:capacity], readiness_events[:capacity], timeout_ms)
	turn := Worker_Turn {
		poll_result   = batch.result,
		accept_result = .Not_Ready,
	}
	if batch.result != .Ready {
		turn.error_code = batch.error_code
		return turn
	}

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
			continue
		}
		readiness_events[stream_event_count] = event
		stream_event_count += 1
	}
	if table.free_count == 0 && listener_registered^ {
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
