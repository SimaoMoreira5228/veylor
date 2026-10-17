#+build linux
package linux_runtime

import connection "veylor:http1/connection"
import response "veylor:http1/response"

static_error_response_for_head :: proc(
	state: ^Static_Connection,
	output: []u8,
	result: connection.Connection_Head_Result,
) -> Static_Connection_Turn {
	status: u16 = 400
	reason := "Bad Request"
	state.close_reason = .Request_Rejected

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
	case .Need_More, .Ready, .Would_Block, .Interrupted:
		state.close_reason = .Internal_Error
		return Static_Connection_Turn{result = .Invalid}
	case .Peer_Closed:
		state.close_reason = .Peer_Closed
		return Static_Connection_Turn{result = .Invalid}
	case .Cancelled:
		state.close_reason = .Shutdown_Forced
		return Static_Connection_Turn{result = .Invalid}
	case .Transport_Error:
		state.close_reason = .Transport_Error
		return Static_Connection_Turn{result = .Invalid}
	}
	error_response := encode_static_error_response(output, status, reason)

	if error_response.result == .Buffer_Small {
		state.close_reason = .Response_Buffer_Limit
		return Static_Connection_Turn{result = .Response_Buffer_Small}
	}
	if error_response.result != .Written {
		state.close_reason = .Internal_Error
		return Static_Connection_Turn{result = .Invalid}
	}
	state.response_length = error_response.written
	state.keep_alive = false
	return Static_Connection_Turn{result = .Response_Progress}
}

encode_static_file_failure_response :: proc(
	state: ^Static_Connection,
	output: []u8,
	status: u16,
	reason: string,
	close_reason: Static_Connection_Close_Reason,
) -> Static_Connection_Turn {
	state.close_reason = close_reason
	error_response := encode_static_error_response(output, status, reason)
	if error_response.result == .Buffer_Small {
		state.close_reason = .Response_Buffer_Limit
		state.phase = .Failed
		return Static_Connection_Turn{result = .Response_Buffer_Small}
	}
	if error_response.result != .Written {
		state.close_reason = .Internal_Error
		state.phase = .Failed
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
