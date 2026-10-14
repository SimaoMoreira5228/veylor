#+build linux
package linux_runtime

import "core:sys/linux"
import "veylor:runtime/connections"
import "veylor:transport"
import socket "veylor:transport/linux/socket"

Accept_Turn_Result :: enum {
	Not_Ready,
	Drained,
	Budget_Exhausted,
	Caller_Capacity,
	Table_Full,
	Listener_Failed,
	Descriptor_Limit,
	Error,
	Invalid,
}

Worker_Connection :: struct {
	token: connections.Connection_Token,
	fd:    linux.Fd,
	peer:  transport.Peer_Address,
}

Accept_Turn :: struct {
	connections: []Worker_Connection,
	attempts:    int,
	result:      Accept_Turn_Result,
	error_code:  i32,
}

accept_listener_readiness :: proc(
	listener: socket.Listener,
	listener_id: u64,
	readiness: transport.Readiness_Event,
	table: ^connections.Connection_Table,
	accepted_storage: []socket.Accepted_Connection,
	connection_storage: []Worker_Connection,
	max_attempts: int,
) -> Accept_Turn {
	if listener.fd < 0 {
		return Accept_Turn{result = .Invalid}
	}

	if readiness.connection_id != listener_id {
		return Accept_Turn{result = .Invalid}
	}

	if max_attempts <= 0 {
		return Accept_Turn{result = .Invalid}
	}

	if table.free_count < 0 {
		return Accept_Turn{result = .Invalid}
	}

	if table.free_count > len(table.slots) {
		return Accept_Turn{result = .Invalid}
	}

	if table.free_count > len(table.free_stack) {
		return Accept_Turn{result = .Invalid}
	}

	if readiness.error || readiness.peer_closed {
		return Accept_Turn{result = .Listener_Failed}
	}

	if !readiness.readable {
		return Accept_Turn{result = .Not_Ready}
	}

	if table.free_count == 0 {
		return Accept_Turn{result = .Table_Full}
	}

	capacity := min(len(accepted_storage), len(connection_storage), table.free_count)

	if capacity == 0 {
		return Accept_Turn{result = .Caller_Capacity}
	}

	batch := socket.accept_ready(listener, accepted_storage[:capacity], max_attempts)
	connection_count := 0
	for i := 0; i < len(batch.connections); i += 1 {
		accepted := batch.connections[i]
		token, allocation_result := connections.allocate_connection(table, u64(accepted.fd))

		if allocation_result != .Ready {
			socket.close_connection(&accepted)
			close_unclaimed_connections(batch.connections[i + 1:])
			return Accept_Turn {
				connections = connection_storage[:connection_count],
				attempts = batch.attempts,
				result = .Error,
			}
		}
		connection_storage[connection_count] = Worker_Connection {
			token = token,
			fd    = accepted.fd,
			peer  = accepted.peer,
		}
		connection_count += 1
	}

	result := accept_turn_result(batch.result)

	if table.free_count == 0 {
		if result == .Drained {
			result = .Table_Full
		}

		if result == .Budget_Exhausted {
			result = .Table_Full
		}

		if result == .Caller_Capacity {
			result = .Table_Full
		}
	}
	return Accept_Turn {
		connections = connection_storage[:connection_count],
		attempts = batch.attempts,
		result = result,
		error_code = batch.error_code,
	}
}

close_unclaimed_connections :: proc(unclaimed: []socket.Accepted_Connection) {
	for i := 0; i < len(unclaimed); i += 1 {
		socket.close_connection(&unclaimed[i])
	}
}

accept_turn_result :: proc(result: socket.Accept_Batch_Result) -> Accept_Turn_Result {
	switch result {
	case .Drained:
		return .Drained
	case .Budget_Exhausted:
		return .Budget_Exhausted
	case .Storage_Full:
		return .Caller_Capacity
	case .Descriptor_Limit:
		return .Descriptor_Limit
	case .Error:
		return .Error
	case .Invalid:
		return .Invalid
	}
	return .Invalid
}
