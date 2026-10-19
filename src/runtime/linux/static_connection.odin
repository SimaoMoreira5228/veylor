#+build linux
package linux_runtime

import connection "veylor:http1/connection"
import router "veylor:router"
import connections "veylor:runtime/connections"
import "veylor:transport"
import epoll "veylor:transport/linux"

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

		read_turn: Head_Read_Turn
		primed := workspace.body_valid

		if primed > 0 {
			workspace.body_start = 0
			workspace.body_valid = 0
			primed_completion := transport.IO_Completion {
				connection_id = static_connection_event_id(state.token),
				operation_id = workspace.head_driver.operation_id + 1,
				operation = .Read,
				result = transport.IO_Result{kind = .Progress, transferred_bytes = primed},
			}
			primed_head := connection.consume_read_completion(
				&workspace.head_driver,
				primed_completion,
				workspace.request_input[:primed],
				workspace.headers,
			)

			if primed_head.result == .Ready {
				read_turn = Head_Read_Turn {
					head = primed_head,
				}
			} else if primed_head.result == .Need_More {
				schedule := connections.schedule_connection_phase_deadline(
					table,
					timers,
					state.token,
					now_ns,
					timeouts,
				)

				if schedule.result != .Scheduled {
					read_turn = Head_Read_Turn {
						head = connection.Connection_Head_Event{result = .Invalid},
						deadline_result = schedule.result,
					}
				} else {
					read_turn = read_connection_head_turn_with_deadline(
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
				}
			} else {
				read_turn = Head_Read_Turn {
					head = primed_head,
				}
			}
		} else {
			read_turn = read_connection_head_turn_with_deadline(
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
		}

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
				timers,
				timeouts,
				now_ns,
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
			stream_turn := continue_managed_stream(state, poller, table)

			if stream_turn == .Error {
				state.close_reason = .Internal_Error
				state.phase = .Failed

				return Static_Connection_Turn{result = .Invalid}
			}

			if stream_turn == .Progress {
				progress := Static_Connection_Turn {
					bytes_written = write_turn.bytes_written,
				}
				progress.result = .Response_Progress

				return progress
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
				kept := preserve_keep_alive_tail(state)

				if kept < 0 {
					state.close_reason = .Internal_Error
					state.phase = .Failed

					return Static_Connection_Turn{result = .Invalid}
				}

				if kept > 0 {
					resume := connections.resume_keep_alive_request(table, timers, state.token)

					if resume != .Ready {
						state.close_reason = .Internal_Error
						state.phase = .Failed

						return Static_Connection_Turn{result = .Invalid}
					}

					state.phase = .Reading_Request_Head
					head_readiness := readiness
					head_readiness.readable = true
					head_readiness.writable = false
					head_turn := static_connection_turn(
						poller,
						state,
						head_readiness,
						routes,
						body_limit,
						byte_budget,
						table,
						timers,
						timeouts,
						now_ns,
					)
					head_turn.bytes_written += turn.bytes_written

					return head_turn
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

preserve_keep_alive_tail :: proc(state: ^Static_Connection) -> int {
	if state.workspace_pool == nil || state.workspace_slot < 0 {
		return -1
	}

	if state.workspace_slot >= len(state.workspace_pool.storage) {
		return -1
	}

	workspace := &state.workspace_pool.storage[state.workspace_slot]
	kept := workspace.body_valid - workspace.body_start

	if kept < 0 {
		kept = 0
	}

	if kept > len(workspace.request_input) {
		kept = len(workspace.request_input)
	}

	if kept > 0 {
		copy(
			workspace.request_input,
			workspace.request_input[workspace.body_start:workspace.body_valid],
		)
		// NOTE: retains the slot across keep-alive so pipelined bytes survive;
		// staging is reset without touching the preserved input.
		workspace.head_driver = connection.begin_connection_head(
			static_connection_event_id(state.token),
		)
		workspace.body_driver = {}
		workspace.managed_route_id = 0
		workspace.body_start = 0
		workspace.body_valid = kept
		workspace.staged_length = 0
		workspace.body_complete = false
		workspace.trailer_head = nil
		workspace.trailer_allow_count = 0
		workspace.trailer_count = 0
		workspace.trailer_data_used = 0
		workspace.streaming = false
		workspace.stream_ended = false
		workspace.stream_terminal = false
		workspace.stream_writable = false

		return kept
	}

	if !release_static_workspace(state) {
		return -1
	}

	return 0
}
