#+build linux
package linux_runtime

import connections "veylor:runtime/connections"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import socket "veylor:transport/linux/socket"

Worker_Connection_Release :: struct {
	deadline_result: connections.Connection_Deadline_Schedule_Result,
	shutdown_result: shutdown.Result,
	table_result:    connections.Connection_Table_Result,
	socket_result:   socket.Result,
	socket_error:    i32,
}

release_worker_connection :: proc(
	connection: ^Worker_Connection,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
	shutdown_state: ^shutdown.State,
) -> Worker_Connection_Release {
	if connection == nil || table == nil || timers == nil || shutdown_state == nil {
		return Worker_Connection_Release {
			deadline_result = .Invalid,
			shutdown_result = .Invalid,
			table_result = .Invalid,
			socket_result = .Invalid,
		}
	}

	_, lookup_result := connections.lookup_connection(table, connection.token)

	if lookup_result != .Found || connection.fd < 0 {
		return Worker_Connection_Release {
			deadline_result = .Stale_Connection,
			shutdown_result = .Invalid,
			table_result = lookup_result,
			socket_result = .Invalid,
		}
	}

	deadline_result := connections.cancel_connection_phase_deadline(
		table,
		timers,
		connection.token,
	)

	if deadline_result != .Cancelled && deadline_result != .Not_Found {
		return Worker_Connection_Release {
			deadline_result = deadline_result,
			shutdown_result = .Invalid,
			table_result = .Invalid,
			socket_result = .Invalid,
		}
	}

	accepted := socket.Accepted_Connection {
		fd   = connection.fd,
		peer = connection.peer,
	}
	socket_result, socket_error := socket.close_connection(&accepted)
	connection.fd = -1
	table_result := connections.release_connection(table, connection.token)

	if table_result != .Released && table_result != .Retired {
		return Worker_Connection_Release {
			deadline_result = deadline_result,
			shutdown_result = .Invalid,
			table_result = table_result,
			socket_result = socket_result,
			socket_error = socket_error,
		}
	}
	shutdown_result := shutdown.close_connection(shutdown_state, timers)
	return Worker_Connection_Release {
		deadline_result = deadline_result,
		shutdown_result = shutdown_result,
		table_result = table_result,
		socket_result = socket_result,
		socket_error = socket_error,
	}
}

force_worker_connections :: proc(
	worker_connections: []Worker_Connection,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
	shutdown_state: ^shutdown.State,
) -> (
	closed_count: int,
	result: shutdown.Result,
) {
	if table == nil || timers == nil || shutdown_state == nil {
		return 0, .Invalid
	}

	if shutdown_state.phase != .Forcing {
		return 0, .Invalid
	}

	for i := 0; i < len(worker_connections); i += 1 {
		connection := &worker_connections[i]

		if connection.fd == -1 {
			continue
		}

		if connection.fd < 0 {
			return closed_count, .Invalid
		}

		release := release_worker_connection(connection, table, timers, shutdown_state)

		if release.table_result != .Released && release.table_result != .Retired {
			return closed_count, .Invalid
		}
		closed_count += 1

		if release.socket_result != .Ready {
			return closed_count, .Invalid
		}

		if release.shutdown_result != .Connection_Closed && release.shutdown_result != .Completed {
			return closed_count, .Invalid
		}
	}

	if shutdown_state.active_connections != 0 || shutdown_state.phase != .Complete {
		return closed_count, .Busy
	}
	return closed_count, .Completed
}

discard_untracked_worker_connections :: proc(
	worker_connections: []Worker_Connection,
	table: ^connections.Connection_Table,
) {
	for i := 0; i < len(worker_connections); i += 1 {
		connection := &worker_connections[i]
		accepted := socket.Accepted_Connection {
			fd   = connection.fd,
			peer = connection.peer,
		}
		socket.close_connection(&accepted)
		connections.release_connection(table, connection.token)
		connection.fd = -1
	}
}
