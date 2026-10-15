#+test
package connection

import "core:testing"
import http1 "veylor:http1"
import "veylor:transport"

@(test)
test_connection_head_driver_resumes_partial_reads_and_preserves_read_ahead :: proc(t: ^testing.T) {
	request := "GET / HTTP/1.1\r\nHost: example.com\r\n\r\nbody"
	input: [128]u8
	headers: [4]http1.Header
	driver := begin_connection_head(12)
	first := "GET / HT"
	fake := transport.Fake_Stream {
		input          = transmute([]u8)request,
		max_read_bytes = len(first),
	}
	first_result := transport.fake_stream_read(&fake, input[:])
	first_completion := transport.IO_Completion {
		connection_id = 12,
		operation_id  = 1,
		operation     = .Read,
		result        = first_result,
	}
	event := consume_read_completion(
		&driver,
		first_completion,
		input[:first_result.transferred_bytes],
		headers[:],
	)
	testing.expect_value(t, event.result, Connection_Head_Result.Need_More)

	fake.max_read_bytes = len(request)
	second_result := transport.fake_stream_read(&fake, input[len(first):])
	completion := transport.IO_Completion {
		connection_id = 12,
		operation_id  = 2,
		operation     = .Read,
		result        = second_result,
	}
	event = consume_read_completion(&driver, completion, input[:len(request)], headers[:])
	testing.expect_value(t, event.result, Connection_Head_Result.Ready)
	testing.expect_value(t, event.head.length, len(request) - len("body"))
	testing.expect_value(t, string(input[event.head.length:len(request)]), "body")
}

@(test)
test_connection_head_driver_keeps_backpressure_and_detects_truncated_eof :: proc(t: ^testing.T) {
	input: [64]u8
	headers: [4]http1.Header
	driver := begin_connection_head(21)
	fake := transport.Fake_Stream {
		read_blocked = true,
	}
	blocked := transport.IO_Completion {
		connection_id = 21,
		operation     = .Read,
		result        = transport.fake_stream_read(&fake, input[:]),
	}
	event := consume_read_completion(&driver, blocked, input[:0], headers[:])
	testing.expect_value(t, event.result, Connection_Head_Result.Would_Block)
	testing.expect_value(t, driver.state, Connection_Head_State.Reading)

	partial := "GET / HTTP/1.1\r\nHost:"
	fake.read_blocked = false
	fake.input = transmute([]u8)partial
	fake.max_read_bytes = len(partial)
	read_result := transport.fake_stream_read(&fake, input[:])
	progress := transport.IO_Completion {
		connection_id = 21,
		operation_id  = 2,
		operation     = .Read,
		result        = read_result,
	}
	event = consume_read_completion(&driver, progress, input[:len(partial)], headers[:])
	testing.expect_value(t, event.result, Connection_Head_Result.Need_More)

	eof := transport.IO_Completion {
		connection_id = 21,
		operation_id  = 3,
		operation     = .Read,
		result        = transport.fake_stream_read(&fake, input[:len(partial)]),
	}
	event = consume_read_completion(&driver, eof, input[:len(partial)], headers[:])
	testing.expect_value(t, event.result, Connection_Head_Result.Invalid)
	testing.expect_value(t, driver.state, Connection_Head_State.Failed)
}

@(test)
test_connection_head_driver_rejects_wrong_or_inconsistent_completions :: proc(t: ^testing.T) {
	input: [64]u8
	headers: [4]http1.Header
	driver := begin_connection_head(32)
	wrong_connection := transport.IO_Completion {
		connection_id = 33,
		operation = .Read,
		result = transport.IO_Result{kind = .Would_Block},
	}
	event := consume_read_completion(&driver, wrong_connection, input[:0], headers[:])
	testing.expect_value(t, event.result, Connection_Head_Result.Invalid)

	driver = begin_connection_head(32)
	wrong_operation := transport.IO_Completion {
		connection_id = 32,
		operation = .Write,
		result = transport.IO_Result{kind = .Would_Block},
	}
	event = consume_read_completion(&driver, wrong_operation, input[:0], headers[:])
	testing.expect_value(t, event.result, Connection_Head_Result.Invalid)

	driver = begin_connection_head(32)
	wrong_count := transport.IO_Completion {
		connection_id = 32,
		operation = .Read,
		result = transport.IO_Result{kind = .Progress, transferred_bytes = 1},
	}
	event = consume_read_completion(&driver, wrong_count, input[:0], headers[:])
	testing.expect_value(t, event.result, Connection_Head_Result.Invalid)

	driver = begin_connection_head(32)
	input[0] = 'x'
	blocked_with_unreported_input := transport.IO_Completion {
		connection_id = 32,
		operation = .Read,
		result = transport.IO_Result{kind = .Would_Block},
	}
	event = consume_read_completion(&driver, blocked_with_unreported_input, input[:1], headers[:])
	testing.expect_value(t, event.result, Connection_Head_Result.Invalid)

	driver = begin_connection_head(32)
	blocked_with_id := transport.IO_Completion {
		connection_id = 32,
		operation_id = 9,
		operation = .Read,
		result = transport.IO_Result{kind = .Would_Block},
	}
	event = consume_read_completion(&driver, blocked_with_id, input[:0], headers[:])
	testing.expect_value(t, event.result, Connection_Head_Result.Would_Block)
	event = consume_read_completion(&driver, blocked_with_id, input[:0], headers[:])
	testing.expect_value(t, event.result, Connection_Head_Result.Invalid)
}

@(test)
test_connection_head_preserves_request_and_header_limit_results :: proc(t: ^testing.T) {
	request_line := "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n"
	request_line_input := transmute([]u8)request_line
	request_line_headers: [2]http1.Header
	request_line_driver := begin_connection_head(42)
	request_line_completion := transport.IO_Completion {
		connection_id = 42,
		operation_id = 1,
		operation = .Read,
		result = transport.IO_Result {
			kind = .Progress,
			transferred_bytes = len(request_line_input),
		},
	}
	request_line_limits := http1.DEFAULT_LIMITS
	request_line_limits.request_line_bytes = len("GET / HTTP/1.1") - 1
	request_line_event := consume_read_completion(
		&request_line_driver,
		request_line_completion,
		request_line_input,
		request_line_headers[:],
		request_line_limits,
	)
	testing.expect_value(
		t,
		request_line_event.result,
		Connection_Head_Result.Request_Line_Too_Large,
	)

	header_input := transmute([]u8)request_line
	header_storage: [2]http1.Header
	header_driver := begin_connection_head(43)
	header_completion := transport.IO_Completion {
		connection_id = 43,
		operation_id = 1,
		operation = .Read,
		result = transport.IO_Result {
			kind = .Progress,
			transferred_bytes = len(header_input),
		},
	}
	header_limits := http1.DEFAULT_LIMITS
	header_limits.header_bytes = 1
	header_event := consume_read_completion(
		&header_driver,
		header_completion,
		header_input,
		header_storage[:],
		header_limits,
	)
	testing.expect_value(
		t,
		header_event.result,
		Connection_Head_Result.Header_Section_Too_Large,
	)
}
