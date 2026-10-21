#+test
package response

import "core:testing"

@(test)
test_response_write_cursor_resumes_encoded_head_without_losing_bytes :: proc(t: ^testing.T) {
	method := "GET"
	reason := "OK"
	encoded_head: [128]u8
	head := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		3,
		encoded_head[:],
	)
	testing.expect_value(t, head.result, Response_Head_Result.Written)
	cursor := begin_response_write(encoded_head[:head.written])
	first_write := 7
	result := advance_response_write(&cursor, first_write)
	testing.expect_value(t, result, Response_Write_State.Pending)
	testing.expect_value(
		t,
		string(response_write_pending(&cursor)),
		string(encoded_head[first_write:head.written]),
	)
	result = advance_response_write(&cursor, head.written - first_write)
	testing.expect_value(t, result, Response_Write_State.Complete)
}

@(test)
test_response_write_cursor_rejects_invalid_transport_counts :: proc(t: ^testing.T) {
	body := "data"
	cursor := begin_response_write(transmute([]u8)body)

	result := advance_response_write(&cursor, -1)
	testing.expect_value(t, result, Response_Write_State.Invalid)
	testing.expect_value(t, len(response_write_pending(&cursor)), 0)

	cursor = begin_response_write(transmute([]u8)body)
	result = advance_response_write(&cursor, len(body) + 1)
	testing.expect_value(t, result, Response_Write_State.Invalid)
	testing.expect_value(t, len(response_write_pending(&cursor)), 0)

	cursor = begin_response_write(transmute([]u8)body)
	result = advance_response_write(&cursor, 3)
	testing.expect_value(t, result, Response_Write_State.Pending)
	result = advance_response_write(&cursor, 2)
	testing.expect_value(t, result, Response_Write_State.Invalid)
}

@(test)
test_head_response_suppresses_fixed_body_but_keeps_get_length :: proc(t: ^testing.T) {
	method := "HEAD"
	reason := "OK"
	output: [128]u8
	head := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		7,
		output[:],
	)
	testing.expect_value(t, head.result, Response_Head_Result.Written)
	testing.expect_value(t, head.body_allowed, false)
	testing.expect_value(
		t,
		string(output[:head.written]),
		"HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\n",
	)
}
