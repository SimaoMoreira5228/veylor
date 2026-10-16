#+build linux
package linux_runtime

import "core:sys/linux"
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
	Capacity_Limit,
	Transport_Error,
	Invalid,
}

Static_Connection_Workspace :: struct {
	head_driver:   connection.Connection_Head_Driver,
	request_input: []u8,
	headers:       []http1.Header,
	response:      []u8,
}

Static_Workspace_Pool :: struct {
	storage:             []Static_Connection_Workspace,
	free_stack:          []u32,
	free_count:          int,
	active_count:        int,
	high_water_mark:     int,
	acquisition_count:   u64,
	release_count:       u64,
	exhaustion_count:    u64,
	initialized:         bool,
}

Static_Connection :: struct {
	token:           connections.Connection_Token,
	fd:              linux.Fd,
	workspace_pool:  ^Static_Workspace_Pool,
	workspace_slot:  int,
	write_driver:    connection.Connection_Write_Driver,
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

init_static_workspace_pool :: proc(
	storage: []Static_Connection_Workspace,
	free_stack: []u32,
) -> (pool: Static_Workspace_Pool, result: bool) {
	if len(storage) == 0 || len(free_stack) < len(storage) {
		return {}, false
	}
	for i := 0; i < len(storage); i += 1 {
		if len(storage[i].request_input) == 0 ||
		   len(storage[i].headers) == 0 ||
		   len(storage[i].response) == 0 {
			return {}, false
		}
		free_stack[i] = u32(len(storage) - i - 1)
	}
	return Static_Workspace_Pool {
		storage     = storage,
		free_stack  = free_stack,
		free_count  = len(storage),
		initialized = true,
	}, true
}

init_static_connection :: proc(
	worker: Worker_Connection,
	workspace_pool: ^Static_Workspace_Pool,
) -> (
	state: Static_Connection,
	result: Static_Connection_Result,
) {
	if worker.fd < 0 || !valid_static_workspace_pool(workspace_pool) {
		return {}, .Invalid
	}
	state = Static_Connection {
		token          = worker.token,
		fd             = worker.fd,
		workspace_pool = workspace_pool,
		workspace_slot = -1,
		phase          = .Reading_Request_Head,
		active         = true,
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
	if state == nil || state.fd < 0 || state.phase != .Reading_Request_Head {
		return .Invalid, 0
	}
	result, error_code = epoll.register(
		poller,
		state.fd,
		static_connection_event_id(state.token),
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
	if state.fd < 0 ||
	   readiness.connection_id != static_connection_event_id(state.token) ||
	   byte_budget <= 0 {
		return Static_Connection_Turn{result = .Invalid}
	}
	resource_id, lookup_result := connections.lookup_connection(table, state.token)

	if lookup_result != .Found || resource_id != u64(state.fd) {
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
		resume_result := connections.resume_keep_alive_request(table, timers, state.token)

		if resume_result != .Ready {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Invalid}
		}
		state.phase = .Reading_Request_Head
	}

	if state.phase == .Reading_Request_Head {
		if !readiness.readable && !readiness.peer_closed {
			return Static_Connection_Turn{result = .Need_More}
		}
		workspace, acquired := acquire_static_workspace(state)

		if !acquired {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Capacity_Limit}
		}
		read_turn := read_connection_head_turn_with_deadline(
			static_connection_event_id(state.token),
			state.fd,
			&workspace.head_driver,
			workspace.request_input,
			workspace.headers,
			byte_budget,
			table,
			state.token,
			timers,
			static_connection_event_id(state.token),
			now_ns,
			timeouts,
		)
		input_length := workspace.head_driver.input_length
		turn := Static_Connection_Turn {
			bytes_read = read_turn.bytes_read,
		}
		if read_turn.deadline_result == .Full {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Capacity_Limit}
		}

		if read_turn.head.result == .Need_More ||
		   read_turn.head.result == .Would_Block ||
		   read_turn.head.result == .Interrupted {
			if input_length == 0 {
				if read_turn.head.result == .Would_Block ||
				   read_turn.head.result == .Interrupted {
					cancel_result := connections.cancel_connection_phase_deadline(
						table,
						timers,
						state.token,
					)
					if cancel_result != .Cancelled && cancel_result != .Not_Found {
						state.phase = .Failed
						return Static_Connection_Turn{result = .Invalid}
					}
				}
				if !release_static_workspace(state) {
					state.phase = .Failed
					return Static_Connection_Turn{result = .Invalid}
				}
			}
			turn.result = .Need_More
			return turn
		}
		if read_turn.head.result != .Ready {
			write_error_response := static_error_response_for_head(
				state,
				workspace.response,
				read_turn.head.result,
			)

			if write_error_response.result != .Response_Progress {
				state.phase = .Failed
				return write_error_response
			}
			return register_static_response_write(poller, state, table)
		}
		if connections.transition_connection(table, state.token, .Writing_Response) !=
		   .Transitioned {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Invalid}
		}
		state.phase = .Writing_Response

		response_event := router.encode_static_route_response(
			routes,
			read_turn.head.head,
			workspace.request_input[:input_length],
			workspace.response,
			body_limit,
		)

		if response_event.result == .Route_Not_Found {
			error_response := encode_static_error_response(
				workspace.response,
				404,
				"Not Found",
			)

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
			static_connection_event_id(state.token),
			state.fd,
			&state.write_driver,
			byte_budget,
			state.keep_alive,
			table,
			state.token,
			timers,
			static_connection_event_id(state.token),
			now_ns,
			timeouts,
		)
		turn := Static_Connection_Turn {
			bytes_written = write_turn.bytes_written,
		}
		if write_turn.deadline_result == .Full {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Capacity_Limit}
		}

		if write_turn.write.result == .Complete {
			if state.keep_alive {
				epoll_result, error_code := epoll.modify(
					poller,
					state.fd,
					static_connection_event_id(state.token),
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
				if !release_static_workspace(state) {
					state.phase = .Failed
					return Static_Connection_Turn{result = .Invalid}
				}
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

acquire_static_workspace :: proc(
	state: ^Static_Connection,
) -> (workspace: ^Static_Connection_Workspace, acquired: bool) {
	if state == nil || !valid_static_workspace_pool(state.workspace_pool) {
		return nil, false
	}
	pool := state.workspace_pool

	if state.workspace_slot >= 0 {
		if state.workspace_slot >= len(pool.storage) {
			return nil, false
		}
		return &pool.storage[state.workspace_slot], true
	}
	if state.workspace_slot != -1 {
		return nil, false
	}
	if pool.free_count == 0 {
		pool.exhaustion_count += 1
		return nil, false
	}
	stack_index := pool.free_count - 1
	slot := pool.free_stack[stack_index]
	if u64(slot) >= u64(len(pool.storage)) {
		return nil, false
	}
	pool.free_count = stack_index
	pool.active_count += 1
	pool.acquisition_count += 1
	pool.high_water_mark = max(pool.high_water_mark, pool.active_count)
	state.workspace_slot = int(slot)
	workspace = &pool.storage[int(slot)]
	workspace.head_driver = connection.begin_connection_head(
		static_connection_event_id(state.token),
	)
	return workspace, true
}

release_static_workspace :: proc(state: ^Static_Connection) -> bool {
	if state == nil {
		return false
	}
	if state.workspace_slot == -1 {
		return true
	}
	if !valid_static_workspace_pool(state.workspace_pool) {
		return false
	}
	pool := state.workspace_pool
	if state.workspace_slot < 0 || state.workspace_slot >= len(pool.storage) {
		return false
	}
	if pool.free_count >= len(pool.storage) {
		return false
	}
	if pool.active_count <= 0 {
		return false
	}
	slot := state.workspace_slot
	reset_static_workspace_head(&pool.storage[slot])
	pool.free_stack[pool.free_count] = u32(slot)
	pool.free_count += 1
	pool.active_count -= 1
	pool.release_count += 1
	state.workspace_slot = -1
	state.write_driver = {}
	return true
}

reset_static_workspace_head :: proc(workspace: ^Static_Connection_Workspace) {
	workspace.head_driver.parser.input_base = nil
	workspace.head_driver.parser.header_base = nil
}

valid_static_workspace_pool :: proc(pool: ^Static_Workspace_Pool) -> bool {
	if pool == nil || !pool.initialized || len(pool.storage) == 0 {
		return false
	}
	if len(pool.free_stack) < len(pool.storage) ||
	   pool.free_count < 0 ||
	   pool.free_count > len(pool.storage) ||
	   pool.active_count < 0 ||
	   pool.active_count > len(pool.storage) ||
	   pool.free_count + pool.active_count != len(pool.storage) ||
	   pool.high_water_mark < pool.active_count ||
	   pool.high_water_mark > len(pool.storage) {
		return false
	}
	return true
}

register_static_response_write :: proc(
	poller: epoll.Epoll,
	state: ^Static_Connection,
	table: ^connections.Connection_Table,
) -> Static_Connection_Turn {
	if state == nil || !valid_static_workspace_pool(state.workspace_pool) ||
	   state.workspace_slot < 0 || state.workspace_slot >= len(state.workspace_pool.storage) {
		return Static_Connection_Turn{result = .Invalid}
	}
	workspace := &state.workspace_pool.storage[state.workspace_slot]
	state.write_driver = connection.begin_connection_write(
		static_connection_event_id(state.token),
		workspace.response[:state.response_length],
	)
	if state.write_driver.state != .Pending {
		state.phase = .Failed
		return Static_Connection_Turn{result = .Invalid}
	}
	if state.phase != .Writing_Response {
		if connections.transition_connection(table, state.token, .Writing_Response) !=
		   .Transitioned {
			state.phase = .Failed
			return Static_Connection_Turn{result = .Invalid}
		}
	}
	result, error_code := epoll.modify(
		poller,
		state.fd,
		static_connection_event_id(state.token),
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
	output: []u8,
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
	error_response := encode_static_error_response(output, status, reason)

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
