package connection

import response "veylor:http1/response"
import "veylor:transport"

Connection_Write_State :: enum {
	Pending,
	Complete,
	Closed,
	Cancelled,
	Failed,
}

Connection_Write_Result :: enum {
	Progress,
	Would_Block,
	Complete,
	Peer_Closed,
	Cancelled,
	Transport_Error,
	Invalid,
}

Connection_Write_Event :: struct {
	accepted_bytes: int,
	result:         Connection_Write_Result,
	error_code:     i32,
}

Connection_Write_Driver :: struct {
	connection_id: u64,
	operation_id:  u64,
	cursor:        response.Response_Write_Cursor,
	state:         Connection_Write_State,
	result:        Connection_Write_Result,
	error_code:    i32,
	has_operation: bool,
}

begin_connection_write :: proc(connection_id: u64, bytes: []u8) -> Connection_Write_Driver {
	cursor := response.begin_response_write(bytes)
	state := Connection_Write_State.Pending
	result := Connection_Write_Result.Progress
	if cursor.state == .Complete {
		state = .Complete
		result = .Complete
	}
	return Connection_Write_Driver {
		connection_id = connection_id,
		cursor = cursor,
		state = state,
		result = result,
	}
}

connection_write_pending :: proc(driver: ^Connection_Write_Driver) -> []u8 {
	if driver.state != .Pending {
		return nil
	}
	return response.response_write_pending(&driver.cursor)
}

consume_write_completion :: proc(
	driver: ^Connection_Write_Driver,
	completion: transport.IO_Completion,
) -> Connection_Write_Event {
	if driver.state != .Pending {
		return connection_write_event(driver)
	}
	if completion.connection_id != driver.connection_id ||
	   completion.operation != transport.Transport_Operation.Write ||
	   !valid_transport_result(completion.result) {
		return fail_connection_write(driver, .Invalid)
	}
	if driver.has_operation && completion.operation_id <= driver.operation_id {
		return fail_connection_write(driver, .Invalid)
	}
	driver.operation_id = completion.operation_id
	driver.has_operation = true

	switch completion.result.kind {
	case .Would_Block:
		driver.result = .Would_Block
		return connection_write_event(driver)
	case .End_Of_Stream:
		return fail_connection_write(driver, .Invalid)
	case .Closed:
		driver.state = .Closed
		driver.result = .Peer_Closed
		return connection_write_event(driver)
	case .Cancelled:
		driver.state = .Cancelled
		driver.result = .Cancelled
		return connection_write_event(driver)
	case .Error:
		driver.state = .Failed
		driver.result = .Transport_Error
		driver.error_code = completion.result.error_code
		return connection_write_event(driver)
	case .Progress:
		pending := connection_write_pending(driver)
		if completion.result.transferred_bytes == 0 ||
		   completion.result.transferred_bytes > len(pending) {
			return fail_connection_write(driver, .Invalid)
		}
		write_result := response.advance_response_write(
			&driver.cursor,
			completion.result.transferred_bytes,
		)
		if write_result == .Invalid {
			return fail_connection_write(driver, .Invalid)
		}
		driver.result = .Progress
		if write_result == .Complete {
			driver.state = .Complete
			driver.result = .Complete
		}
		return connection_write_event_with_count(driver, completion.result.transferred_bytes)
	}
	return fail_connection_write(driver, .Invalid)
}

connection_write_event :: proc(driver: ^Connection_Write_Driver) -> Connection_Write_Event {
	return Connection_Write_Event{result = driver.result, error_code = driver.error_code}
}

connection_write_event_with_count :: proc(
	driver: ^Connection_Write_Driver,
	accepted_bytes: int,
) -> Connection_Write_Event {
	return Connection_Write_Event {
		accepted_bytes = accepted_bytes,
		result = driver.result,
		error_code = driver.error_code,
	}
}

fail_connection_write :: proc(
	driver: ^Connection_Write_Driver,
	result: Connection_Write_Result,
) -> Connection_Write_Event {
	driver.state = .Failed
	driver.result = result
	return connection_write_event(driver)
}
