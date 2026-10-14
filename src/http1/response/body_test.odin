#+test
package response

import "core:testing"
import "veylor:http1"

@(test)
test_fixed_response_body_tracks_partial_writes :: proc(t: ^testing.T) {
	writer, result := begin_fixed_response_body(8, true)
	testing.expect_value(t, result, Response_Write_State.Pending)
	first_body := "abc"
	result = queue_fixed_response_chunk(&writer, transmute([]u8)first_body)
	testing.expect_value(t, result, Response_Write_State.Pending)
	testing.expect_value(t, string(fixed_response_body_pending(&writer)), first_body)

	result = advance_fixed_response_body(&writer, 2)
	testing.expect_value(t, result, Response_Write_State.Pending)
	testing.expect_value(t, string(fixed_response_body_pending(&writer)), "c")

	result = advance_fixed_response_body(&writer, 1)
	testing.expect_value(t, result, Response_Write_State.Pending)
	testing.expect_value(t, len(fixed_response_body_pending(&writer)), 0)
	second_body := "defgh"
	result = queue_fixed_response_chunk(&writer, transmute([]u8)second_body)
	testing.expect_value(t, result, Response_Write_State.Pending)
	result = finish_fixed_response_body(&writer)
	testing.expect_value(t, result, Response_Write_State.Pending)
	result = advance_fixed_response_body(&writer, 0)
	testing.expect_value(t, result, Response_Write_State.Pending)
	result = advance_fixed_response_body(&writer, 5)
	testing.expect_value(t, result, Response_Write_State.Complete)
	testing.expect_value(t, len(fixed_response_body_pending(&writer)), 0)
}

@(test)
test_fixed_response_body_rejects_chunk_overrun :: proc(t: ^testing.T) {
	body := "short"
	writer, result := begin_fixed_response_body(4, true)
	testing.expect_value(t, result, Response_Write_State.Pending)
	result = queue_fixed_response_chunk(&writer, transmute([]u8)body)
	testing.expect_value(t, result, Response_Write_State.Invalid)
}

@(test)
test_fixed_response_body_ignores_empty_chunks :: proc(t: ^testing.T) {
	writer, result := begin_fixed_response_body(2, true)
	empty: []u8
	result = queue_fixed_response_chunk(&writer, empty)
	testing.expect_value(t, result, Response_Write_State.Pending)
	testing.expect_value(t, writer.queued_bytes, u64(0))
	testing.expect_value(t, len(fixed_response_body_pending(&writer)), 0)

	body := "ok"
	result = queue_fixed_response_chunk(&writer, transmute([]u8)body)
	testing.expect_value(t, result, Response_Write_State.Pending)
	result = advance_fixed_response_body(&writer, 2)
	testing.expect_value(t, result, Response_Write_State.Complete)
}

@(test)
test_fixed_response_body_rejects_short_completion :: proc(t: ^testing.T) {
	body := "ab"
	writer, result := begin_fixed_response_body(5, true)
	testing.expect_value(t, result, Response_Write_State.Pending)
	result = queue_fixed_response_chunk(&writer, transmute([]u8)body)
	testing.expect_value(t, result, Response_Write_State.Pending)
	result = finish_fixed_response_body(&writer)
	testing.expect_value(t, result, Response_Write_State.Pending)
	result = advance_fixed_response_body(&writer, 2)
	testing.expect_value(t, result, Response_Write_State.Invalid)
}

@(test)
test_fixed_response_body_suppression_and_empty_body_complete_immediately :: proc(t: ^testing.T) {
	writer, result := begin_fixed_response_body(7, false)
	testing.expect_value(t, result, Response_Write_State.Complete)
	testing.expect_value(t, len(fixed_response_body_pending(&writer)), 0)

	writer, result = begin_fixed_response_body(0, true)
	testing.expect_value(t, result, Response_Write_State.Complete)
}

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

	body := "ignored"
	writer, result := begin_fixed_response_body(7, head.body_allowed)
	testing.expect_value(t, result, Response_Write_State.Complete)
	testing.expect_value(
		t,
		queue_fixed_response_chunk(&writer, transmute([]u8)body),
		Response_Write_State.Invalid,
	)
	testing.expect_value(t, len(fixed_response_body_pending(&writer)), 0)
}

@(test)
test_response_write_cursor_resumes_chunk_frame_without_losing_bytes :: proc(t: ^testing.T) {
	body := "Wiki"
	chunk_buffer: [16]u8
	chunk := encode_response_chunk(transmute([]u8)body, chunk_buffer[:])
	testing.expect_value(t, chunk.result, Response_Body_Result.Written)
	cursor := begin_response_write(chunk_buffer[:chunk.written])
	result := advance_response_write(&cursor, 3)
	testing.expect_value(t, result, Response_Write_State.Pending)
	testing.expect_value(t, string(response_write_pending(&cursor)), "Wiki\r\n")
	result = advance_response_write(&cursor, 6)
	testing.expect_value(t, result, Response_Write_State.Complete)
}

@(test)
test_response_chunk_encodes_hex_length_and_data :: proc(t: ^testing.T) {
	body := "Wiki"
	input := transmute([]u8)body
	output: [16]u8
	event := encode_response_chunk(input, output[:])
	testing.expect_value(t, event.result, Response_Body_Result.Written)
	testing.expect_value(t, string(output[:event.written]), "4\r\nWiki\r\n")
}

@(test)
test_response_chunk_handles_large_length_without_heap_scratch :: proc(t: ^testing.T) {
	input: [256]u8
	output: [264]u8
	event := encode_response_chunk(input[:], output[:])
	testing.expect_value(t, event.result, Response_Body_Result.Written)
	testing.expect_value(t, string(output[:5]), "100\r\n")
	testing.expect_value(t, string(output[event.written - 2:event.written]), "\r\n")
}

@(test)
test_response_chunk_rejects_empty_and_preserves_output_on_short_buffer :: proc(t: ^testing.T) {
	output: [8]u8
	for i in 0 ..< len(output) {
		output[i] = 0xa5
	}
	empty: []u8
	event := encode_response_chunk(empty, output[:])
	testing.expect_value(t, event.result, Response_Body_Result.Invalid)

	body := "Wiki"
	event = encode_response_chunk(transmute([]u8)body, output[:])
	testing.expect_value(t, event.result, Response_Body_Result.Buffer_Small)
	for value in output {
		testing.expect_value(t, value, u8(0xa5))
	}
}

@(test)
test_response_final_chunk_encodes_safe_trailers :: proc(t: ^testing.T) {
	name := "Digest"
	value := "sha-256=abc"
	trailers := [?]Response_Header{{name = transmute([]u8)name, value = transmute([]u8)value}}
	output: [128]u8
	event := encode_response_final_chunk(trailers[:], output[:])
	testing.expect_value(t, event.result, Response_Body_Result.Written)
	testing.expect_value(t, string(output[:event.written]), "0\r\nDigest: sha-256=abc\r\n\r\n")
}

@(test)
test_response_final_chunk_rejects_forbidden_trailers :: proc(t: ^testing.T) {
	name := "Content-Type"
	value := "text/plain"
	trailers := [?]Response_Header{{name = transmute([]u8)name, value = transmute([]u8)value}}
	output: [128]u8
	event := encode_response_final_chunk(trailers[:], output[:])
	testing.expect_value(t, event.result, Response_Body_Result.Invalid)
}

@(test)
test_response_final_chunk_checks_capacity_before_writing :: proc(t: ^testing.T) {
	name := "Digest"
	value := "sha-256=abc"
	trailers := [?]Response_Header{{name = transmute([]u8)name, value = transmute([]u8)value}}
	output: [8]u8
	for i in 0 ..< len(output) {
		output[i] = 0xa5
	}
	event := encode_response_final_chunk(trailers[:], output[:])
	testing.expect_value(t, event.result, Response_Body_Result.Buffer_Small)
	for byte in output {
		testing.expect_value(t, byte, u8(0xa5))
	}
}
