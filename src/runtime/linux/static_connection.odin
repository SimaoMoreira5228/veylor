#+build linux
package linux_runtime

import "core:sys/linux"
import http1 "veylor:http1"
import body "veylor:http1/body"
import connection "veylor:http1/connection"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

Static_Connection_Phase :: enum {
	Reading_Request_Head,
	Writing_Response,
	Writing_File_Body,
	Awaiting_Host,
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

Static_Connection_Close_Reason :: enum u8 {
	Unknown,
	Normal,
	Peer_Closed,
	Request_Rejected,
	Route_Not_Found,
	Workspace_Exhausted,
	Deadline_Capacity,
	Response_Buffer_Limit,
	Transport_Error,
	Timed_Out,
	Host_Queue_Full,
	Shutdown_Forced,
	Initialization_Failed,
	Internal_Error,
	Count,
}

STAGED_BODY_CAPACITY :: 2048

Static_Connection_Workspace :: struct {
	head_driver:      connection.Connection_Head_Driver,
	body_driver:      connection.Connection_Body_Driver,
	managed_route_id: u64,
	request_input:    []u8,
	headers:          []http1.Header,
	response:         []u8,
	staged_body:      [STAGED_BODY_CAPACITY]u8,
	body_start:       int,
	body_valid:       int,
	staged_length:    int,
}

Static_Workspace_Pool :: struct {
	storage:           []Static_Connection_Workspace,
	free_stack:        []u32,
	free_count:        int,
	active_count:      int,
	high_water_mark:   int,
	acquisition_count: u64,
	release_count:     u64,
	exhaustion_count:  u64,
	initialized:       bool,
}

Static_Connection :: struct {
	token:           connections.Connection_Token,
	fd:              linux.Fd,
	workspace_pool:  ^Static_Workspace_Pool,
	file_root:       ^Static_File_Root,
	host_queue:      ^host.Host_Queue,
	native_routes:   ^router.Native_Table,
	workspace_slot:  int,
	write_driver:    connection.Connection_Write_Driver,
	file:            Static_File,
	file_offset:     i64,
	file_remaining:  u64,
	has_file:        bool,
	phase:           Static_Connection_Phase,
	response_length: int,
	keep_alive:      bool,
	active:          bool,
	registered:      bool,
	idle_counted:    bool,
	close_reason:    Static_Connection_Close_Reason,
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
) -> (
	pool: Static_Workspace_Pool,
	result: bool,
) {
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
			storage = storage,
			free_stack = free_stack,
			free_count = len(storage),
			initialized = true,
		},
		true
}

init_static_connection :: proc(
	worker: Worker_Connection,
	workspace_pool: ^Static_Workspace_Pool,
	file_root: ^Static_File_Root = nil,
	native_routes: ^router.Native_Table = nil,
	host_queue: ^host.Host_Queue = nil,
) -> (
	state: Static_Connection,
	result: Static_Connection_Result,
) {
	if worker.fd < 0 ||
	   !valid_static_workspace_pool(workspace_pool) ||
	   (file_root != nil && !valid_static_file_root(file_root)) {
		return {}, .Invalid
	}
	state = Static_Connection {
		token = worker.token,
		fd = worker.fd,
		workspace_pool = workspace_pool,
		file_root = file_root,
		native_routes = native_routes,
		host_queue = host_queue,
		workspace_slot = -1,
		file = Static_File{fd = -1},
		phase = .Reading_Request_Head,
		active = true,
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
		state.close_reason = .Internal_Error
		return Static_Connection_Turn{result = .Invalid}
	}
	resource_id, lookup_result := connections.lookup_connection(table, state.token)

	if lookup_result != .Found || resource_id != u64(state.fd) {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Static_Connection_Turn{result = .Invalid}
	}
	if readiness.error {
		state.close_reason = .Transport_Error
		state.phase = .Failed
		return Static_Connection_Turn{result = .Transport_Error}
	}
	if readiness.peer_closed && !readiness.readable && state.phase != .Writing_Response {
		state.close_reason = .Peer_Closed
		state.phase = .Closed
		return Static_Connection_Turn{result = .Closed}
	}

	if state.phase == .Awaiting_Host {
		if readiness.error {
			state.close_reason = .Transport_Error
			state.phase = .Failed
			return Static_Connection_Turn{result = .Transport_Error}
		}
		if readiness.peer_closed {
			state.close_reason = .Peer_Closed
			state.phase = .Closed
			return Static_Connection_Turn{result = .Closed}
		}
		return Static_Connection_Turn{result = .Need_More}
	}

	if state.phase == .Keep_Alive_Idle {
		if !readiness.readable {
			return Static_Connection_Turn{result = .Keep_Alive}
		}
		resume_result := connections.resume_keep_alive_request(table, timers, state.token)

		if resume_result != .Ready {
			state.close_reason = .Internal_Error
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
			state.close_reason = .Workspace_Exhausted
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
			now_ns,
			timeouts,
		)
		input_length := workspace.head_driver.input_length
		turn := Static_Connection_Turn {
			bytes_read = read_turn.bytes_read,
		}
		if read_turn.deadline_result == .Full {
			state.close_reason = .Deadline_Capacity
			state.phase = .Failed
			return Static_Connection_Turn {
				bytes_read = read_turn.bytes_read,
				result = .Capacity_Limit,
			}
		}

		if read_turn.head.result == .Need_More ||
		   read_turn.head.result == .Would_Block ||
		   read_turn.head.result == .Interrupted {
			if input_length == 0 {
				if read_turn.head.result == .Would_Block || read_turn.head.result == .Interrupted {
					cancel_result := connections.cancel_connection_phase_deadline(
						table,
						timers,
						state.token,
					)
					if cancel_result != .Cancelled && cancel_result != .Not_Found {
						state.close_reason = .Internal_Error
						state.phase = .Failed
						return Static_Connection_Turn{result = .Invalid}
					}
				}
				if !release_static_workspace(state) {
					state.close_reason = .Internal_Error
					state.phase = .Failed
					return Static_Connection_Turn{result = .Invalid}
				}
			}
			turn.result = .Need_More
			return turn
		}
		if read_turn.head.result == .Peer_Closed {
			state.close_reason = .Peer_Closed
			state.phase = .Closed
			turn.result = .Closed
			return turn
		}
		if read_turn.head.result != .Ready {
			write_error_response := static_error_response_for_head(
				state,
				workspace.response,
				read_turn.head.result,
			)
			write_error_response.bytes_read = turn.bytes_read

			if write_error_response.result != .Response_Progress {
				state.close_reason = static_connection_close_reason(write_error_response.result)
				state.phase = .Failed
				return write_error_response
			}
			write_turn := register_static_response_write(poller, state, table)
			write_turn.bytes_read += turn.bytes_read
			return write_turn
		}
		if state.native_routes != nil {
			native_event := dispatch_native_route_response(
				state.native_routes,
				read_turn.head.head,
				workspace.request_input[:input_length],
				workspace.response,
				body_limit,
			)
			if native_event.result == .Responded {
				state.response_length = native_event.written
				state.keep_alive = native_event.keep_alive
				if !native_event.keep_alive {
					state.close_reason = .Normal
				}
				write_turn := register_static_response_write(poller, state, table)
				write_turn.bytes_read += turn.bytes_read
				return write_turn
			}
			if native_event.result == .Buffer_Small {
				state.close_reason = .Response_Buffer_Limit
				state.phase = .Failed
				return Static_Connection_Turn{result = .Response_Buffer_Small}
			}
			if native_event.result == .Invalid {
				state.close_reason = .Internal_Error
				state.phase = .Failed
				return Static_Connection_Turn{result = .Invalid}
			}
		}

		response_event := router.encode_static_route_response(
			routes,
			read_turn.head.head,
			workspace.request_input[:input_length],
			workspace.response,
			body_limit,
		)

		if response_event.result == .Managed {
			managed_turn := park_static_connection_for_host(
				poller,
				state,
				read_turn.head.head,
				routes,
				workspace,
				input_length,
				turn.bytes_read,
				body_limit,
				table,
			)
			return managed_turn
		}

		if connections.transition_connection(table, state.token, .Writing_Response) !=
		   .Transitioned {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			turn.result = .Invalid
			return turn
		}
		state.phase = .Writing_Response

		file_response_ready := false
		if response_event.result == .Route_Not_Found && state.file_root != nil {
			file_event := encode_static_file_response(
				state.file_root,
				read_turn.head.head,
				workspace.request_input[:input_length],
				workspace.response,
				body_limit,
			)
			switch file_event.result {
			case .Encoded:
				state.response_length = file_event.response_length
				state.keep_alive = file_event.keep_alive
				state.file = file_event.file
				state.has_file = file_event.stream_body
				if file_event.stream_body {
					state.file_offset = 0
					state.file_remaining = file_event.file.size
				}
				if !file_event.keep_alive {
					state.close_reason = .Normal
				}
				file_response_ready = true

			case .Body_Too_Large:
				file_error := encode_static_file_failure_response(
					state,
					workspace.response,
					413,
					"Content Too Large",
					.Request_Rejected,
				)
				file_error.bytes_read = turn.bytes_read
				if file_error.result != .Response_Progress {
					return file_error
				}
				file_response_ready = true

			case .System_Error:
				file_error := encode_static_file_failure_response(
					state,
					workspace.response,
					500,
					"Internal Server Error",
					.Internal_Error,
				)
				file_error.bytes_read = turn.bytes_read
				if file_error.result != .Response_Progress {
					return file_error
				}
				file_response_ready = true

			case .Buffer_Small:
				state.close_reason = .Response_Buffer_Limit
				state.phase = .Failed
				return Static_Connection_Turn{result = .Response_Buffer_Small}

			case .Invalid:
				state.close_reason = .Internal_Error
				state.phase = .Failed
				return Static_Connection_Turn{result = .Invalid}

			case .Not_Mounted, .Not_Found:
			}
		}
		if file_response_ready {
			write_turn := register_static_response_write(poller, state, table)
			write_turn.bytes_read += turn.bytes_read
			return write_turn
		}

		if response_event.result == .Route_Not_Found {
			state.close_reason = .Route_Not_Found
			error_response := encode_static_error_response(workspace.response, 404, "Not Found")

			if error_response.result != .Written {
				state.close_reason = .Response_Buffer_Limit
				state.phase = .Failed
				turn.result = .Response_Buffer_Small
				return turn
			}
			state.response_length = error_response.written
			state.keep_alive = false
		} else if response_event.result == .Buffer_Small {
			state.close_reason = .Response_Buffer_Limit
			state.phase = .Failed
			turn.result = .Response_Buffer_Small
			return turn
		} else if response_event.result != .Written {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			turn.result = .Invalid
			return turn
		} else {
			state.response_length = response_event.written
			state.keep_alive = response_event.keep_alive
			if !response_event.keep_alive {
				state.close_reason = .Normal
			}
		}
		write_turn := register_static_response_write(poller, state, table)
		write_turn.bytes_read += turn.bytes_read
		return write_turn
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
			state.keep_alive && !state.has_file,
			table,
			state.token,
			timers,
			now_ns,
			timeouts,
		)
		turn := Static_Connection_Turn {
			bytes_written = write_turn.bytes_written,
		}
		if write_turn.deadline_result == .Full {
			state.close_reason = .Deadline_Capacity
			state.phase = .Failed
			return Static_Connection_Turn{result = .Capacity_Limit}
		}

		if write_turn.write.result == .Complete {
			if state.has_file {
				file_turn := begin_static_file_body(state, table, timers, timeouts, now_ns)
				file_turn.bytes_written = write_turn.bytes_written
				return file_turn
			}
			if state.keep_alive {
				epoll_result, error_code := epoll.modify(
					poller,
					state.fd,
					static_connection_event_id(state.token),
					epoll.Interest{readable = true},
				)

				if epoll_result != .Ready {
					state.close_reason = .Transport_Error
					state.phase = .Failed
					return Static_Connection_Turn {
						result = .Transport_Error,
						error_code = error_code,
					}
				}
				state.phase = .Keep_Alive_Idle
				if !release_static_workspace(state) {
					state.close_reason = .Internal_Error
					state.phase = .Failed
					return Static_Connection_Turn{result = .Invalid}
				}
				turn.result = .Keep_Alive
				return turn
			}
			state.phase = .Closed
			if state.close_reason == .Unknown {
				state.close_reason = .Normal
			}
			turn.result = .Closed
			return turn
		}

		if write_turn.write.result == .Progress ||
		   write_turn.write.result == .Would_Block ||
		   write_turn.write.result == .Interrupted {
			turn.result = .Response_Progress
			return turn
		}
		state.close_reason = .Transport_Error
		state.phase = .Failed
		turn.result = .Transport_Error
		turn.error_code = write_turn.write.error_code
		return turn
	}
	if state.phase == .Writing_File_Body {
		return static_file_body_turn(
			poller,
			state,
			readiness,
			byte_budget,
			table,
			timers,
			timeouts,
			now_ns,
		)
	}
	if state.phase == .Closed {
		return Static_Connection_Turn{result = .Closed}
	}
	return Static_Connection_Turn{result = .Invalid}
}

acquire_static_workspace :: proc(
	state: ^Static_Connection,
) -> (
	workspace: ^Static_Connection_Workspace,
	acquired: bool,
) {
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
	workspace.body_driver = {}
	workspace.managed_route_id = 0
	workspace.body_start = 0
	workspace.body_valid = 0
	workspace.staged_length = 0
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
	reset_static_workspace(&pool.storage[slot])
	pool.free_stack[pool.free_count] = u32(slot)
	pool.free_count += 1
	pool.active_count -= 1
	pool.release_count += 1
	state.workspace_slot = -1
	state.write_driver = {}
	return true
}

reset_static_workspace :: proc(workspace: ^Static_Connection_Workspace) {
	workspace.head_driver.parser.input_base = nil
	workspace.head_driver.parser.header_base = nil
	workspace.body_driver = {}
	workspace.managed_route_id = 0
	workspace.body_start = 0
	workspace.body_valid = 0
	workspace.staged_length = 0
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

park_static_connection_for_host :: proc(
	poller: epoll.Epoll,
	state: ^Static_Connection,
	head: http1.Request_Head,
	routes: ^router.Route_Table,
	workspace: ^Static_Connection_Workspace,
	input_length: int,
	bytes_read: int,
	body_limit: u64,
	table: ^connections.Connection_Table,
) -> Static_Connection_Turn {
	if state.host_queue == nil {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	input := workspace.request_input[:input_length]
	method := input[head.method.offset:head.method.offset + head.method.length]
	target := input[head.target.offset:head.target.offset + head.target.length]
	path, _, path_result := router.request_path(head.target_form, method, target)
	if path_result != .Found {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	if head.has_content_length && head.content_length > body_limit {
		oversize := encode_static_error_response(workspace.response, 413, "Content Too Large")
		if oversize.result != .Written {
			state.close_reason = .Response_Buffer_Limit
			state.phase = .Failed
			return Static_Connection_Turn{bytes_read = bytes_read, result = .Response_Buffer_Small}
		}
		state.response_length = oversize.written
		state.keep_alive = false
		state.close_reason = .Request_Rejected
		state.phase = .Writing_Response
		write_turn := register_static_response_write(poller, state, table)
		write_turn.bytes_read += bytes_read
		return write_turn
	}
	managed_route, managed_result := router.find_static_request_route(routes, head, input)
	if managed_result != .Found || managed_route.kind != .Managed {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	workspace.managed_route_id = managed_route.route_id
	enqueue_result := host.enqueue_request(
		state.host_queue,
		state.token,
		managed_route.route_id,
		method,
		path,
	)
	if enqueue_result == .Full {
		state.close_reason = .Host_Queue_Full
		state.phase = .Failed
		return Static_Connection_Turn{bytes_read = bytes_read, result = .Capacity_Limit}
	}
	if enqueue_result != .Enqueued {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	body_expected := (head.has_content_length && head.content_length > 0) || head.is_chunked
	if body_expected {
		if connections.transition_connection(table, state.token, .Reading_Request_Body) !=
		   .Transitioned {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
		}
		workspace.body_driver = connection.begin_connection_body(
			static_connection_event_id(state.token),
			head,
		)
		workspace.body_start = head.length
		workspace.body_valid = input_length
		state.phase = .Awaiting_Host
		return Static_Connection_Turn{bytes_read = bytes_read, result = .Need_More}
	}
	if connections.transition_connection(table, state.token, .Awaiting_Handler) != .Transitioned {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	if !release_static_workspace(state) {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Static_Connection_Turn{bytes_read = bytes_read, result = .Invalid}
	}
	state.phase = .Awaiting_Host
	return Static_Connection_Turn{bytes_read = bytes_read, result = .Need_More}
}

Managed_Read_Outcome :: enum {
	Notified,
	Stalled,
	Closed,
	Dropped,
}

Managed_Read_Turn :: struct {
	outcome:    Managed_Read_Outcome,
	bytes_read: int,
}

execute_managed_read_body :: proc(
	state: ^Static_Connection,
	cmd: host.Host_Command,
	byte_budget: int,
	table: ^connections.Connection_Table,
	timers: ^transport.Timer_Queue,
	timeouts: connections.Connection_Timeouts,
	now_ns: u64,
) -> Managed_Read_Turn {
	if cmd.kind != .Read_Body {
		return Managed_Read_Turn{outcome = .Dropped}
	}
	if cmd.max_bytes == 0 {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	resource_id, lookup_result := connections.lookup_connection(table, cmd.token)
	if lookup_result != .Found || resource_id != u64(state.fd) || cmd.token != state.token {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	phase, phase_result := connections.connection_phase(table, cmd.token)
	if phase_result != .Found || phase != .Reading_Request_Body {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	if state.workspace_slot < 0 {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Managed_Read_Turn{outcome = .Closed}
	}

	workspace := &state.workspace_pool.storage[state.workspace_slot]
	if workspace.body_driver.state != .Reading {
		return Managed_Read_Turn{outcome = .Dropped}
	}

	schedule := connections.schedule_connection_phase_deadline(
		table,
		timers,
		state.token,
		now_ns,
		timeouts,
	)
	if schedule.result != .Scheduled &&
	   schedule.result != .Duplicate_Timer &&
	   schedule.result != .Connection_Already_Timed {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		if schedule.result == .Full {
			state.close_reason = .Deadline_Capacity
		}
		return Managed_Read_Turn{outcome = .Closed}
	}

	staged_before := workspace.staged_length
	staged_free := len(workspace.staged_body) - staged_before
	if staged_free < 0 {
		staged_free = 0
	}
	max_command_bytes := int(cmd.max_bytes)
	body_event: connection.Connection_Body_Event
	bytes_read := 0
	if workspace.body_valid > workspace.body_start {
		feed_length := workspace.body_valid - workspace.body_start
		if feed_length > staged_free {
			feed_length = staged_free
		}
		if feed_length > max_command_bytes {
			feed_length = max_command_bytes
		}
		if feed_length <= 0 {
			return Managed_Read_Turn{outcome = .Stalled}
		}
		spans: [64]http1.Span
		body_event = connection.consume_buffered_body(
			&workspace.body_driver,
			workspace.request_input[workspace.body_start:workspace.body_start + feed_length],
			spans[:],
			body.Chunked_Trailer_Context{},
		)
		workspace.body_start += body_event.consumed
		stage_managed_body_spans(
			state,
			workspace,
			body_event,
			workspace.body_start - body_event.consumed,
		)
		if body_event.result == .Too_Large {
			state.close_reason = .Request_Rejected
			state.phase = .Failed
			return Managed_Read_Turn{outcome = .Closed}
		}
		if body_event.result == .Invalid {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			return Managed_Read_Turn{outcome = .Closed}
		}
		if workspace.body_driver.state == .Failed || workspace.body_driver.state == .Cancelled {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			return Managed_Read_Turn{outcome = .Closed}
		}
	} else {
		input_free := len(workspace.request_input) - workspace.body_valid
		if input_free < 0 {
			input_free = 0
		}
		read_cap := byte_budget
		if staged_free < read_cap {
			read_cap = staged_free
		}
		if input_free < read_cap {
			read_cap = input_free
		}
		if max_command_bytes < read_cap {
			read_cap = max_command_bytes
		}
		if read_cap <= 0 {
			return Managed_Read_Turn{outcome = .Stalled}
		}
		budget, budget_valid := socket.begin_stream_byte_budget(read_cap)
		if !budget_valid {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			return Managed_Read_Turn{outcome = .Closed}
		}
		read := socket.read_stream_budgeted(
			state.fd,
			workspace.request_input[workspace.body_valid:],
			&budget,
		)
		if read.io.kind != .Progress {
			if read.io.kind == .Would_Block || read.io.kind == .Interrupted {
				return Managed_Read_Turn{outcome = .Stalled}
			}
			if read.io.kind == .End_Of_Stream || read.io.kind == .Closed {
				state.close_reason = .Peer_Closed
			} else {
				state.close_reason = .Transport_Error
			}
			state.phase = .Failed
			return Managed_Read_Turn{outcome = .Closed}
		}
		workspace.body_valid += read.io.transferred_bytes
		bytes_read = read.io.transferred_bytes
		feed := workspace.request_input[workspace.body_valid - bytes_read:workspace.body_valid]
		spans: [64]http1.Span
		completion := transport.IO_Completion {
			connection_id = static_connection_event_id(state.token),
			operation_id  = workspace.body_driver.operation_id + 1,
			operation     = .Read,
			result        = read.io,
		}
		body_event = connection.consume_body_completion(
			&workspace.body_driver,
			completion,
			feed,
			spans[:],
			body.Chunked_Trailer_Context{},
		)
		if body_event.result == .Invalid || body_event.result == .Span_Capacity {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
		}
		if body_event.result == .Too_Large {
			state.close_reason = .Request_Rejected
			state.phase = .Failed
			return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
		}
		workspace.body_start += body_event.consumed
		stage_managed_body_spans(state, workspace, body_event, workspace.body_valid - bytes_read)
		if workspace.body_driver.state == .Failed || workspace.body_driver.state == .Cancelled {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
		}
	}

	terminal := body_event.body_complete || workspace.body_driver.state != .Reading
	if terminal {
		cancel_result := connections.cancel_connection_phase_deadline(table, timers, state.token)
		if cancel_result != .Cancelled && cancel_result != .Not_Found {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
		}
		if connections.transition_connection(table, state.token, .Awaiting_Handler) !=
		   .Transitioned {
			state.close_reason = .Internal_Error
			state.phase = .Failed
			return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
		}
		return enqueue_managed_body_event(state)
	}
	if workspace.staged_length == staged_before {
		return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Stalled}
	}
	refresh := connections.cancel_connection_phase_deadline(table, timers, state.token)
	if refresh != .Cancelled {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
	}
	reschedule := connections.schedule_connection_phase_deadline(
		table,
		timers,
		state.token,
		now_ns,
		timeouts,
	)
	if reschedule.result != .Scheduled {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		if reschedule.result == .Full {
			state.close_reason = .Deadline_Capacity
		}
		return Managed_Read_Turn{bytes_read = bytes_read, outcome = .Closed}
	}
	return enqueue_managed_body_event(state)
}

stage_managed_body_spans :: proc(
	state: ^Static_Connection,
	workspace: ^Static_Connection_Workspace,
	event: connection.Connection_Body_Event,
	feed_base: int,
) {
	for span in event.spans {
		if span.length <= 0 {
			continue
		}
		if workspace.staged_length + span.length > len(workspace.staged_body) {
			continue
		}
		copy(
			workspace.staged_body[workspace.staged_length:],
			workspace.request_input[feed_base + span.offset:feed_base + span.offset + span.length],
		)
		workspace.staged_length += span.length
	}
}


enqueue_managed_body_event :: proc(state: ^Static_Connection) -> Managed_Read_Turn {
	if state.host_queue == nil || state.workspace_pool == nil {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Managed_Read_Turn{outcome = .Closed}
	}
	if state.workspace_slot < 0 || state.workspace_slot >= len(state.workspace_pool.storage) {
		state.close_reason = .Internal_Error
		state.phase = .Failed
		return Managed_Read_Turn{outcome = .Closed}
	}
	workspace := &state.workspace_pool.storage[state.workspace_slot]
	event := host.Host_Event {
		token      = state.token,
		route_id   = workspace.managed_route_id,
		body_ready = u32(workspace.staged_length),
		kind       = .Body_Available,
	}
	if host.enqueue_body_available(state.host_queue, event) != .Enqueued {
		state.close_reason = .Host_Queue_Full
		state.phase = .Failed
		return Managed_Read_Turn{outcome = .Closed}
	}
	return Managed_Read_Turn{outcome = .Notified}
}

register_static_response_write :: proc(
	poller: epoll.Epoll,
	state: ^Static_Connection,
	table: ^connections.Connection_Table,
) -> Static_Connection_Turn {
	if state == nil ||
	   !valid_static_workspace_pool(state.workspace_pool) ||
	   state.workspace_slot < 0 ||
	   state.workspace_slot >= len(state.workspace_pool.storage) {
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
