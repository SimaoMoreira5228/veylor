package connection

import http1 "veylor:http1"
import "veylor:transport"

Connection_Head_State :: enum {
	Reading,
	Ready,
	Closed,
	Cancelled,
	Failed,
}

Connection_Head_Result :: enum {
	Need_More,
	Ready,
	Would_Block,
	Interrupted,
	Peer_Closed,
	Cancelled,
	Transport_Error,
	Invalid,
	Too_Large,
	Request_Line_Too_Large,
	Header_Section_Too_Large,
	Header_Capacity,
	Expectation_Failed,
	Buffer_Changed,
}

Connection_Head_Event :: struct {
	head:       http1.Request_Head,
	result:     Connection_Head_Result,
	error_code: i32,
}

Connection_Head_Driver :: struct {
	connection_id: u64,
	operation_id:  u64,
	input_length:  int,
	parser:        http1.Parser,
	state:         Connection_Head_State,
	result:        Connection_Head_Result,
	error_code:    i32,
	has_operation: bool,
}

begin_connection_head :: proc(connection_id: u64) -> Connection_Head_Driver {
	return Connection_Head_Driver{connection_id = connection_id, state = .Reading}
}

consume_read_completion :: proc(
	driver: ^Connection_Head_Driver,
	completion: transport.IO_Completion,
	input: []u8,
	headers: []http1.Header,
	limits := http1.DEFAULT_LIMITS,
) -> Connection_Head_Event {
	if driver.state == .Ready {
		return Connection_Head_Event {
			head = http1.request_head(&driver.parser, headers),
			result = .Ready,
		}
	}

	if driver.state != .Reading {
		return connection_head_event(driver)
	}

	if completion.connection_id != driver.connection_id {
		return reject_connection_head(driver, .Invalid)
	}

	if completion.operation != transport.Transport_Operation.Read {
		return reject_connection_head(driver, .Invalid)
	}

	if driver.has_operation && completion.operation_id <= driver.operation_id {
		return reject_connection_head(driver, .Invalid)
	}
	driver.operation_id = completion.operation_id
	driver.has_operation = true

	if completion.result.kind != .Progress && len(input) != driver.input_length {
		return reject_connection_head(driver, .Invalid)
	}

	if !valid_transport_result(completion.result) {
		return reject_connection_head(driver, .Invalid)
	}

	switch completion.result.kind {
	case .Would_Block:
		return Connection_Head_Event{result = .Would_Block}
	case .Interrupted:
		return Connection_Head_Event{result = .Interrupted}
	case .End_Of_Stream:
		if driver.input_length != 0 {
			return reject_connection_head(driver, .Invalid)
		}

		driver.state = .Closed
		driver.result = .Peer_Closed
		return connection_head_event(driver)
	case .Cancelled:
		driver.state = .Cancelled
		driver.result = .Cancelled
		return connection_head_event(driver)
	case .Error:
		driver.state = .Failed
		driver.result = .Transport_Error
		driver.error_code = completion.result.error_code
		return connection_head_event(driver)
	case .Closed:
		return reject_connection_head(driver, .Invalid)
	case .Progress:
		if completion.result.transferred_bytes == 0 {
			return reject_connection_head(driver, .Invalid)
		}

		if len(input) < driver.input_length {
			return reject_connection_head(driver, .Invalid)
		}

		if len(input) - driver.input_length != completion.result.transferred_bytes {
			return reject_connection_head(driver, .Invalid)
		}
	}

	head, parse_result := http1.parse_request_head(&driver.parser, input, headers, limits)
	driver.input_length = len(input)

	if parse_result == .Need_More {
		return Connection_Head_Event{result = .Need_More}
	}

	if parse_result == .Ready {
		driver.state = .Ready
		driver.result = .Ready
		return Connection_Head_Event{head = head, result = .Ready}
	}
	driver.state = .Failed
	driver.result = connection_head_parse_result(parse_result)
	return connection_head_event(driver)
}

connection_head_parse_result :: proc(result: http1.Parse_Result) -> Connection_Head_Result {
	switch result {
	case .Invalid:
		return .Invalid
	case .Request_Line_Too_Large:
		return .Request_Line_Too_Large
	case .Header_Section_Too_Large:
		return .Header_Section_Too_Large
	case .Header_Capacity:
		return .Header_Capacity
	case .Expectation_Failed:
		return .Expectation_Failed
	case .Buffer_Changed:
		return .Buffer_Changed
	case .Need_More:
		return .Need_More
	case .Ready:
		return .Ready
	}
	return .Invalid
}

connection_head_event :: proc(driver: ^Connection_Head_Driver) -> Connection_Head_Event {
	if driver.state == .Ready {
		return Connection_Head_Event{result = .Ready, error_code = driver.error_code}
	}
	return Connection_Head_Event{result = driver.result, error_code = driver.error_code}
}

reject_connection_head :: proc(
	driver: ^Connection_Head_Driver,
	result: Connection_Head_Result,
) -> Connection_Head_Event {
	driver.state = .Failed
	driver.result = result
	return connection_head_event(driver)
}
