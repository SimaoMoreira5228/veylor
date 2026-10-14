#+test
package body

import "core:testing"

@(test)
test_fixed_body_streams_across_fragments :: proc(t: ^testing.T) {
	decoder: Fixed_Body_Decoder
	first_raw := "ab"
	first := transmute([]u8)first_raw
	event := read_fixed_body(&decoder, first, 5)
	testing.expect_value(t, event.result, Fixed_Body_Result.Data)
	testing.expect_value(t, event.consumed, 2)
	testing.expect_value(
		t,
		string(first[event.data.offset:event.data.offset + event.data.length]),
		"ab",
	)

	second_raw := "cde"
	second := transmute([]u8)second_raw
	event = read_fixed_body(&decoder, second, 5)
	testing.expect_value(t, event.result, Fixed_Body_Result.Data)
	testing.expect_value(t, event.consumed, 3)

	event = read_fixed_body(&decoder, nil, 5)
	testing.expect_value(t, event.result, Fixed_Body_Result.Complete)
}

@(test)
test_fixed_body_leaves_pipelined_bytes_unconsumed :: proc(t: ^testing.T) {
	decoder: Fixed_Body_Decoder
	raw := "bodyGET /next HTTP/1.1\r\n"
	input := transmute([]u8)raw
	event := read_fixed_body(&decoder, input, 4, 4)
	testing.expect_value(t, event.result, Fixed_Body_Result.Data)
	testing.expect_value(t, event.consumed, 4)
	testing.expect_value(
		t,
		string(input[event.data.offset:event.data.offset + event.data.length]),
		"body",
	)
	testing.expect_value(t, string(input[event.consumed:]), "GET /next HTTP/1.1\r\n")
}

@(test)
test_fixed_body_rejects_declared_length_over_limit_before_consuming :: proc(t: ^testing.T) {
	decoder: Fixed_Body_Decoder
	raw := "x"
	input := transmute([]u8)raw
	event := read_fixed_body(&decoder, input, 5, 4)
	testing.expect_value(t, event.result, Fixed_Body_Result.Too_Large)
	testing.expect_value(t, event.consumed, 0)
}

@(test)
test_fixed_body_rejects_length_changes_between_calls :: proc(t: ^testing.T) {
	decoder: Fixed_Body_Decoder
	raw := "ab"
	input := transmute([]u8)raw
	first := read_fixed_body(&decoder, input, 4)
	testing.expect_value(t, first.result, Fixed_Body_Result.Data)

	second := read_fixed_body(&decoder, input, 5)
	testing.expect_value(t, second.result, Fixed_Body_Result.Invalid)
	third := read_fixed_body(&decoder, input, 4)
	testing.expect_value(t, third.result, Fixed_Body_Result.Invalid)
}

@(test)
test_fixed_body_completes_empty_content_without_consuming_input :: proc(t: ^testing.T) {
	decoder: Fixed_Body_Decoder
	raw := "next"
	input := transmute([]u8)raw
	event := read_fixed_body(&decoder, input, 0)
	testing.expect_value(t, event.result, Fixed_Body_Result.Complete)
	testing.expect_value(t, event.consumed, 0)
}
