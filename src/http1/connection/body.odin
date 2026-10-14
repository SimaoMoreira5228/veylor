package connection

import http1 "veylor:http1"
import body "veylor:http1/body"
import "veylor:transport"

Connection_Body_State :: enum {
	Reading,
	Complete,
	Cancelled,
	Failed,
}

Connection_Body_Result :: enum {
	Data,
	Trailer,
	Need_More,
	Would_Block,
	Interrupted,
	Complete,
	Peer_Closed,
	Cancelled,
	Transport_Error,
	Invalid,
	Too_Large,
	Span_Capacity,
}

Connection_Body_Event :: struct {
	spans:         []http1.Span,
	consumed:      int,
	result:        Connection_Body_Result,
	body_complete: bool,
	error_code:    i32,
	trailer_name:  []u8,
	trailer_value: []u8,
}

Connection_Body_Driver :: struct {
	connection_id:      u64,
	operation_id:       u64,
	content_length:     u64,
	has_content_length: bool,
	is_chunked:         bool,
	fixed:              body.Fixed_Body_Decoder,
	chunked:            body.Chunked_Decoder,
	state:              Connection_Body_State,
	result:             Connection_Body_Result,
	error_code:         i32,
	has_operation:      bool,
}

begin_connection_body :: proc(
	connection_id: u64,
	head: http1.Request_Head,
) -> Connection_Body_Driver {
	driver := Connection_Body_Driver {
		connection_id      = connection_id,
		content_length     = head.content_length,
		has_content_length = head.has_content_length,
		is_chunked         = head.is_chunked,
		state              = .Reading,
	}

	if !head.is_chunked && !head.has_content_length {
		driver.state = .Complete
	}

	if !head.is_chunked && head.has_content_length && head.content_length == 0 {
		driver.state = .Complete
	}
	return driver
}

consume_body_completion :: proc(
	driver: ^Connection_Body_Driver,
	completion: transport.IO_Completion,
	input: []u8,
	spans: []http1.Span,
	trailer_context: body.Chunked_Trailer_Context,
	body_limit := body.DEFAULT_FIXED_BODY_LIMIT,
	chunked_limits := body.DEFAULT_CHUNKED_LIMITS,
) -> Connection_Body_Event {
	if driver.state == .Complete {
		return Connection_Body_Event{result = .Complete, body_complete = true}
	}

	if driver.state != .Reading {
		return connection_body_event(driver)
	}

	if completion.connection_id != driver.connection_id {
		return fail_connection_body(driver, .Invalid)
	}

	if completion.operation != transport.Transport_Operation.Read {
		return fail_connection_body(driver, .Invalid)
	}

	if !valid_transport_result(completion.result) {
		return fail_connection_body(driver, .Invalid)
	}

	if driver.has_operation && completion.operation_id <= driver.operation_id {
		return fail_connection_body(driver, .Invalid)
	}
	driver.operation_id = completion.operation_id
	driver.has_operation = true

	if completion.result.kind == .Progress {
		if completion.result.transferred_bytes == 0 {
			return fail_connection_body(driver, .Invalid)
		}

		if completion.result.transferred_bytes != len(input) {
			return fail_connection_body(driver, .Invalid)
		}
	} else if len(input) != 0 {
		return fail_connection_body(driver, .Invalid)
	}

	switch completion.result.kind {
	case .Would_Block:
		return Connection_Body_Event{result = .Would_Block}
	case .Interrupted:
		return Connection_Body_Event{result = .Interrupted}
	case .End_Of_Stream:
		return fail_connection_body(driver, .Invalid)
	case .Cancelled:
		driver.state = .Cancelled
		driver.result = .Cancelled
		return connection_body_event(driver)
	case .Error:
		driver.state = .Failed
		driver.result = .Transport_Error
		driver.error_code = completion.result.error_code
		return connection_body_event(driver)
	case .Closed:
		return fail_connection_body(driver, .Invalid)
	case .Progress:
		return consume_buffered_body(
			driver,
			input,
			spans,
			trailer_context,
			body_limit,
			chunked_limits,
		)
	}
	return fail_connection_body(driver, .Invalid)
}

consume_buffered_body :: proc(
	driver: ^Connection_Body_Driver,
	input: []u8,
	spans: []http1.Span,
	trailer_context: body.Chunked_Trailer_Context,
	body_limit := body.DEFAULT_FIXED_BODY_LIMIT,
	chunked_limits := body.DEFAULT_CHUNKED_LIMITS,
) -> Connection_Body_Event {
	if driver.state == .Complete {
		return Connection_Body_Event{result = .Complete, body_complete = true}
	}

	if driver.state != .Reading {
		return connection_body_event(driver)
	}

	if driver.is_chunked {
		return consume_chunked_body(driver, input, spans, trailer_context, chunked_limits)
	}
	return consume_fixed_body(driver, input, spans, body_limit)
}

consume_fixed_body :: proc(
	driver: ^Connection_Body_Driver,
	input: []u8,
	spans: []http1.Span,
	body_limit: u64,
) -> Connection_Body_Event {
	if len(input) > 0 && len(spans) == 0 {
		return Connection_Body_Event{result = .Span_Capacity}
	}

	event := body.read_fixed_body(&driver.fixed, input, driver.content_length, body_limit)
	switch event.result {
	case .Data:
		spans[0] = event.data
		complete := driver.fixed.state == .Complete

		if complete {
			driver.state = .Complete
		}
		return Connection_Body_Event {
			spans = spans[:1],
			consumed = event.consumed,
			result = .Data,
			body_complete = complete,
		}
	case .Need_More:
		return Connection_Body_Event{consumed = event.consumed, result = .Need_More}
	case .Complete:
		driver.state = .Complete
		return Connection_Body_Event{result = .Complete, body_complete = true}
	case .Too_Large:
		return fail_connection_body(driver, .Too_Large)
	case .Invalid:
		return fail_connection_body(driver, .Invalid)
	}
	return fail_connection_body(driver, .Invalid)
}

consume_chunked_body :: proc(
	driver: ^Connection_Body_Driver,
	input: []u8,
	spans: []http1.Span,
	trailer_context: body.Chunked_Trailer_Context,
	limits: body.Chunked_Limits,
) -> Connection_Body_Event {
	event := body.decode_chunked_body(&driver.chunked, input, spans, trailer_context, limits)

	switch event.result {
	case .Data:
		return Connection_Body_Event {
			spans = spans[:event.span_count],
			consumed = event.consumed,
			result = .Data,
		}
	case .Trailer:
		return Connection_Body_Event {
			spans = spans[:event.span_count],
			consumed = event.consumed,
			result = .Trailer,
			trailer_name = event.trailer_name,
			trailer_value = event.trailer_value,
		}
	case .Need_More:
		return Connection_Body_Event{consumed = event.consumed, result = .Need_More}
	case .Complete:
		driver.state = .Complete
		return Connection_Body_Event {
			spans = spans[:event.span_count],
			consumed = event.consumed,
			result = .Complete,
			body_complete = true,
		}
	case .Too_Large:
		return fail_connection_body(driver, .Too_Large)
	case .Invalid:
		return fail_connection_body(driver, .Invalid)
	case .Span_Capacity:
		return Connection_Body_Event {
			spans = spans[:event.span_count],
			consumed = event.consumed,
			result = .Span_Capacity,
		}
	}
	return fail_connection_body(driver, .Invalid)
}

connection_body_event :: proc(driver: ^Connection_Body_Driver) -> Connection_Body_Event {
	return Connection_Body_Event {
		result = driver.result,
		body_complete = driver.state == .Complete,
		error_code = driver.error_code,
	}
}

fail_connection_body :: proc(
	driver: ^Connection_Body_Driver,
	result: Connection_Body_Result,
) -> Connection_Body_Event {
	driver.state = .Failed
	driver.result = result
	return connection_body_event(driver)
}
