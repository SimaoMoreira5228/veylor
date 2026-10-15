#+build linux
package linux_runtime

import http1 "veylor:http1"
import connection "veylor:http1/connection"
import response "veylor:http1/response"
import router "veylor:router"
import connections "veylor:runtime/connections"
import "veylor:transport"
import epoll "veylor:transport/linux"

Static_Connection_Phase :: enum {
	Reading_Request_Head,
	Writing_Response,
	Keep_Alive_Idle,
	Closed,
	Failed,
}

Static_Connection_Result :: enum {
	Need_More,
	Response_Progress,
	Keep_Alive,
	Closed,
	Response_Buffer_Small,
	Transport_Error,
	Invalid,
}

Static_Connection :: struct {
	worker:          Worker_Connection,
	event_id:        u64,
	head_driver:     connection.Connection_Head_Driver,
	write_driver:    connection.Connection_Write_Driver,
	request_input:   []u8,
	headers:         []http1.Header,
	response:        []u8,
	phase:           Static_Connection_Phase,
	response_length: int,
	keep_alive:      bool,
	active:          bool,
	registered:      bool,
}

Static_Connection_Turn :: struct {
	bytes_read:    int,
	bytes_written: int,
	result:        Static_Connection_Result,
	error_code:    i32,
}

init_static_connection :: proc(
	worker: Worker_Connection,
	request_input: []u8,
	headers: []http1.Header,
	response_storage: []u8,
) -> (
	state: Static_Connection,
	result: Static_Connection_Result,
) {
	if worker.fd < 0 ||
	   len(request_input) == 0 ||
	   len(headers) == 0 ||
	   len(response_storage) == 0 {
		return {}, .Invalid
	}
	connection_id := static_connection_event_id(worker.token)
	state = Static_Connection {
		worker        = worker,
		event_id      = connection_id,
		head_driver   = connection.begin_connection_head(connection_id),
		request_input = request_input,
		headers       = headers,
		response      = response_storage,
		phase         = .Reading_Request_Head,
		active        = true,
	}
	return state, .Need_More
}

register_static_connection :: proc(
	poller: epoll.Epoll,
	state: ^Static_Connection,
) -> (
	result: epoll.Result,
	error_code: i32,
) {
	if state == nil || state.worker.fd < 0 || state.phase != .Reading_Request_Head {
		return .Invalid, 0
	}
	result, error_code = epoll.register(
		poller,
		state.worker.fd,
		state.event_id,
		epoll.Interest{readable = true},
	)

	if result == .Ready {
		state.registered = true
	}
	return
}

static_connection_turn :: proc(
	poller: epoll.Epoll,
	state: ^Static_Connection,
	readiness: transport.Readiness_Event,
	routes: ^router.Route_Table,
	body_limit: u64,
	byte_budget: int,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
	timeouts: connections.Connection_Timeouts,
	now_ns: u64,
) -> Static_Connection_Turn {
	if state == nil || routes == nil || table == nil || timers == nil {
		return Static_Connection_Turn{result = .Invalid}
	}
	if state.worker.fd < 0 || readiness.connection_id != state.event_id || byte_budget <= 0 {
		return Static_Connection_Turn{result = .Invalid}
	}
	resource_id, lookup_result := connections.lookup_connection(table, state.worker.token)

	if lookup_result != .Found || resource_id != u64(state.worker.fd) {
		state.phase = .Failed
		return Static_Connection_Turn{result = .Invalid}
	}
	if readiness.error {
		state.phase = .Failed
		return Static_Connection_Turn{result = .Transport_Error}
	}
	if readiness.peer_closed && !readiness.readable && state.phase != .Writing_Response {
		state.phase = .Closed
		return Static_Connection_Turn{result = .Closed}
	}

	if state.phase == .Keep_Alive_Idle {
		if !readiness.readable {
			return Static_Connection_Turn{result = .Keep_Alive}
		}
		resume_result := connections.resume_keep_alive_request(table, timers, state.worker.token)

		if resume_result != .Ready {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Invalid}
		}
		state.head_driver = connection.begin_connection_head(state.event_id)
		state.phase = .Reading_Request_Head
	}

	if state.phase == .Reading_Request_Head {
		if !readiness.readable && !readiness.peer_closed {
			return Static_Connection_Turn{result = .Need_More}
		}
		read_turn := read_connection_head_turn_with_deadline(
			state.event_id,
			state.worker.fd,
			&state.head_driver,
			state.request_input,
			state.headers,
			byte_budget,
			table,
			state.worker.token,
			timers,
			state.event_id,
			now_ns,
			timeouts,
		)
		turn := Static_Connection_Turn {
			bytes_read = read_turn.bytes_read,
		}

		if read_turn.head.result == .Need_More ||
		   read_turn.head.result == .Would_Block ||
		   read_turn.head.result == .Interrupted {
			turn.result = .Need_More
			return turn
		}
		if read_turn.head.result != .Ready {
			write_error_response := static_error_response_for_head(state, read_turn.head.result)

			if write_error_response.result != .Response_Progress {
				state.phase = .Failed
				return write_error_response
			}
			return register_static_response_write(poller, state, table)
		}
		if connections.transition_connection(table, state.worker.token, .Writing_Response) !=
		   .Transitioned {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Invalid}
		}
		state.phase = .Writing_Response

		response_event := router.encode_static_route_response(
			routes,
			read_turn.head.head,
			state.request_input[:state.head_driver.input_length],
			state.response,
			body_limit,
		)

		if response_event.result == .Route_Not_Found {
			error_response := encode_static_error_response(state.response, 404, "Not Found")

			if error_response.result != .Written {
				state.phase = .Failed
				return Static_Connection_Turn{result = .Response_Buffer_Small}
			}
			state.response_length = error_response.written
			state.keep_alive = false
		} else if response_event.result == .Buffer_Small {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Response_Buffer_Small}
		} else if response_event.result != .Written {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Invalid}
		} else {
			state.response_length = response_event.written
			state.keep_alive = response_event.keep_alive
		}
		return register_static_response_write(poller, state, table)
	}

	if state.phase == .Writing_Response {
		if !readiness.writable {
			return Static_Connection_Turn{result = .Response_Progress}
		}
		write_turn := write_connection_turn_with_deadline(
			state.event_id,
			state.worker.fd,
			&state.write_driver,
			byte_budget,
			state.keep_alive,
			table,
			state.worker.token,
			timers,
			state.event_id,
			now_ns,
			timeouts,
		)
		turn := Static_Connection_Turn {
			bytes_written = write_turn.bytes_written,
		}

		if write_turn.write.result == .Complete {
			if state.keep_alive {
				epoll_result, error_code := epoll.modify(
					poller,
					state.worker.fd,
					state.event_id,
					epoll.Interest{readable = true},
				)

				if epoll_result != .Ready {
					state.phase = .Failed
					return Static_Connection_Turn {
						result = .Transport_Error,
						error_code = error_code,
					}
				}
				state.phase = .Keep_Alive_Idle
				turn.result = .Keep_Alive
				return turn
			}
			state.phase = .Closed
			turn.result = .Closed
			return turn
		}

		if write_turn.write.result == .Progress ||
		   write_turn.write.result == .Would_Block ||
		   write_turn.write.result == .Interrupted {
			turn.result = .Response_Progress
			return turn
		}
		state.phase = .Failed
		turn.result = .Transport_Error
		turn.error_code = write_turn.write.error_code
		return turn
	}
	if state.phase == .Closed {
		return Static_Connection_Turn{result = .Closed}
	}
	return Static_Connection_Turn{result = .Invalid}
}

register_static_response_write :: proc(
	poller: epoll.Epoll,
	state: ^Static_Connection,
	table: ^connections.Connection_Table,
) -> Static_Connection_Turn {
	state.write_driver = connection.begin_connection_write(
		state.event_id,
		state.response[:state.response_length],
	)
	if state.write_driver.state != .Pending {
		state.phase = .Failed
		return Static_Connection_Turn{result = .Invalid}
	}
	if state.phase != .Writing_Response {
		if connections.transition_connection(table, state.worker.token, .Writing_Response) !=
		   .Transitioned {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Invalid}
		}
	}
	result, error_code := epoll.modify(
		poller,
		state.worker.fd,
		state.event_id,
		epoll.Interest{writable = true},
	)

	if result != .Ready {
		state.phase = .Failed
		return Static_Connection_Turn{result = .Transport_Error, error_code = error_code}
	}
	state.phase = .Writing_Response
	return Static_Connection_Turn{result = .Response_Progress}
}

static_connection_event_id :: proc(token: connections.Connection_Token) -> u64 {
	return u64(token.slot) + 1
}

static_error_response_for_head :: proc(
	state: ^Static_Connection,
	result: connection.Connection_Head_Result,
) -> Static_Connection_Turn {
	status: u16 = 400
	reason := "Bad Request"

	switch result {
	case .Request_Line_Too_Large:
		status = 414
		reason = "URI Too Long"
	case .Header_Section_Too_Large, .Header_Capacity, .Too_Large:
		status = 431
		reason = "Request Header Fields Too Large"
	case .Expectation_Failed:
		status = 417
		reason = "Expectation Failed"
	case .Invalid, .Buffer_Changed:
	case .Need_More,
	     .Ready,
	     .Would_Block,
	     .Interrupted,
	     .Peer_Closed,
	     .Cancelled,
	     .Transport_Error:
		return Static_Connection_Turn{result = .Invalid}
	}
	error_response := encode_static_error_response(state.response, status, reason)

	if error_response.result == .Buffer_Small {
		return Static_Connection_Turn{result = .Response_Buffer_Small}
	}
	if error_response.result != .Written {
		return Static_Connection_Turn{result = .Invalid}
	}
	state.response_length = error_response.written
	state.keep_alive = false
	return Static_Connection_Turn{result = .Response_Progress}
}

encode_static_error_response :: proc(
	output: []u8,
	status: u16,
	reason: string,
) -> response.Response_Head_Event {
	method := "GET"
	return response.encode_response_head(
		transmute([]u8)method,
		status,
		transmute([]u8)reason,
		[]response.Response_Header{},
		.Fixed_Length,
		0,
		output,
		.Close,
	)
}
