#+build linux
package epoll

import "core:sys/linux"
import "veylor:transport"

Epoll :: struct {
	fd: linux.Fd,
}

Interest :: struct {
	readable: bool,
	writable: bool,
}

Result :: enum {
	Ready,
	Timeout,
	Interrupted,
	Invalid,
	System_Error,
}

Batch :: struct {
	events:     []transport.Readiness_Event,
	result:     Result,
	error_code: i32,
}

create :: proc() -> (poller: Epoll, result: Result, error_code: i32) {
	fd, err := linux.epoll_create1({.FDCLOEXEC})

	if err != .NONE {
		return {}, .System_Error, i32(err)
	}

	return Epoll{fd = fd}, .Ready, 0
}

close :: proc(poller: ^Epoll) -> (result: Result, error_code: i32) {
	if poller.fd < 0 {
		return .Invalid, 0
	}

	err := linux.close(poller.fd)

	if err != .NONE {
		return .System_Error, i32(err)
	}

	poller.fd = -1
	return .Ready, 0
}

register :: proc(
	poller: Epoll,
	fd: linux.Fd,
	connection_id: u64,
	interest: Interest,
) -> (
	result: Result,
	error_code: i32,
) {
	return epoll_control(poller, .ADD, fd, connection_id, interest)
}

modify :: proc(
	poller: Epoll,
	fd: linux.Fd,
	connection_id: u64,
	interest: Interest,
) -> (
	result: Result,
	error_code: i32,
) {
	return epoll_control(poller, .MOD, fd, connection_id, interest)
}

unregister :: proc(poller: Epoll, fd: linux.Fd) -> (result: Result, error_code: i32) {
	if poller.fd < 0 || fd < 0 {
		return .Invalid, 0
	}

	err := linux.epoll_ctl(poller.fd, .DEL, fd, nil)

	if err != .NONE {
		return .System_Error, i32(err)
	}

	return .Ready, 0
}

wait :: proc(
	poller: Epoll,
	native_events: []linux.EPoll_Event,
	ready_events: []transport.Readiness_Event,
	timeout_ms: i32,
) -> Batch {
	if poller.fd < 0 || timeout_ms < -1 || len(native_events) == 0 || len(ready_events) == 0 {
		return Batch{result = .Invalid}
	}

	capacity := min(len(native_events), len(ready_events))
	count, err := linux.epoll_wait(poller.fd, &native_events[0], i32(capacity), timeout_ms)

	if err == .EINTR {
		return Batch{result = .Interrupted}
	}

	if err != .NONE {
		return Batch{result = .System_Error, error_code = i32(err)}
	}

	if count == 0 {
		return Batch{result = .Timeout}
	}

	for i := 0; i < int(count); i += 1 {
		native := native_events[i]
		ready_events[i] = transport.Readiness_Event {
			connection_id = native.data.u64,
			readable      = .IN in
				native.events || .PRI in native.events || .RDHUP in native.events,
			writable      = .OUT in native.events,
			peer_closed   = .HUP in native.events || .RDHUP in native.events,
			error         = .ERR in native.events,
		}
	}

	return Batch{events = ready_events[:count], result = .Ready}
}

epoll_control :: proc(
	poller: Epoll,
	op: linux.EPoll_Ctl_Opcode,
	fd: linux.Fd,
	connection_id: u64,
	interest: Interest,
) -> (
	result: Result,
	error_code: i32,
) {
	if poller.fd < 0 || fd < 0 || (!interest.readable && !interest.writable) {
		return .Invalid, 0
	}

	events: linux.EPoll_Event_Set

	if interest.readable {
		events += {.IN, .RDHUP}
	}

	if interest.writable {
		events += {.OUT}
	}

	data: linux.EPoll_Data
	data.u64 = connection_id
	event := linux.EPoll_Event {
		events = events,
		data   = data,
	}

	err := linux.epoll_ctl(poller.fd, op, fd, &event)

	if err != .NONE {
		return .System_Error, i32(err)
	}

	return .Ready, 0
}
