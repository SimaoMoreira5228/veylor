#+test
package http1

import "core:testing"

test_span_is_within_input :: proc(span: Span, input_length: int) -> bool {
	if span.offset < 0 || span.length < 0 || span.offset > input_length {
		return false
	}
	return span.length <= input_length - span.offset
}

@(test)
test_request_head_buffer_lifetime_invariants :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	bytes := transmute([]u8)raw
	first_header_end := len("GET / HTTP/1.1\r\nHost: example.com\r\n")
	original: [64]u8
	rebased: [64]u8
	header_a: [2]Header
	header_b: [2]Header
	parser: Parser
	for i := 0; i < len(bytes); i += 1 {
		original[i] = bytes[i]
		rebased[i] = bytes[i]
	}
	_, result := parse_request_head(&parser, original[:first_header_end], header_a[:])
	testing.expect_value(t, result, Parse_Result.Need_More)
	_, result = parse_request_head(&parser, rebased[:len(bytes)], header_a[:])
	testing.expect_value(t, result, Parse_Result.Buffer_Changed)
	_, result = parse_request_head(&parser, original[:first_header_end], header_a[:])
	testing.expect_value(t, result, Parse_Result.Buffer_Changed)

	reset(&parser)
	_, result = parse_request_head(&parser, original[:len(bytes) - 1], header_a[:])
	testing.expect_value(t, result, Parse_Result.Need_More)
	_, result = parse_request_head(&parser, original[:len(bytes) - 2], header_a[:])
	testing.expect_value(t, result, Parse_Result.Buffer_Changed)
}

@(test)
test_empty_input_does_not_bind_a_buffer_base :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.0\r\n\r\n"
	bytes := transmute([]u8)raw
	buffer: [64]u8
	empty_storage: [64]u8
	for i := 0; i < len(bytes); i += 1 {
		buffer[i] = bytes[i]
	}
	headers: [1]Header
	parser: Parser
	empty_nil: []u8
	_, result := parse_request_head(&parser, empty_nil, headers[:])
	testing.expect_value(t, result, Parse_Result.Need_More)
	_, result = parse_request_head(&parser, buffer[:len(bytes)], headers[:])
	testing.expect_value(t, result, Parse_Result.Ready)

	reset(&parser)
	_, result = parse_request_head(&parser, empty_storage[:0], headers[:])
	testing.expect_value(t, result, Parse_Result.Need_More)
	_, result = parse_request_head(&parser, buffer[:len(bytes)], headers[:])
	testing.expect_value(t, result, Parse_Result.Ready)
}


@(test)
test_request_head_binds_header_storage_when_first_field_is_parsed :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	bytes := transmute([]u8)raw
	request_line_end := len("GET / HTTP/1.1\r\n")
	buffer: [64]u8
	header_a: [2]Header
	header_b: [2]Header
	parser: Parser
	for i := 0; i < len(bytes); i += 1 {
		buffer[i] = bytes[i]
	}
	_, result := parse_request_head(&parser, buffer[:request_line_end], header_a[:])
	testing.expect_value(t, result, Parse_Result.Need_More)
	_, result = parse_request_head(&parser, buffer[:len(bytes)], header_b[:])
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, header_a[0].name.length, 0)
	testing.expect_value(t, header_b[0].name.length, len("Host"))
	_, result = parse_request_head(&parser, buffer[:len(bytes)], header_b[:])
	testing.expect_value(t, result, Parse_Result.Ready)

	reset(&parser)
	first_header_end := len("GET / HTTP/1.1\r\nHost: example.com\r\n")
	_, result = parse_request_head(&parser, buffer[:first_header_end], header_a[:])
	testing.expect_value(t, result, Parse_Result.Need_More)
	_, result = parse_request_head(&parser, buffer[:len(bytes)], header_b[:])
	testing.expect_value(t, result, Parse_Result.Buffer_Changed)

	reset(&parser)
	new_raw := "GET / HTTP/1.0\r\n\r\n"
	new_input := transmute([]u8)new_raw
	_, result = parse_request_head(&parser, new_input, header_b[:])
	testing.expect_value(t, result, Parse_Result.Ready)
}

@(test)
test_request_head_waits_for_complete_terminator :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	bytes := transmute([]u8)raw
	buffer: [64]u8
	headers: [2]Header
	parser: Parser
	head: Request_Head
	result: Parse_Result
	for i := 0; i < len(bytes); i += 1 {
		buffer[i] = bytes[i]
		head, result = parse_request_head(&parser, buffer[:i + 1], headers[:])

		if i + 1 < len(bytes) {
			testing.expect_value(t, result, Parse_Result.Need_More)
		}
	}
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, head.headers[0].name.offset, 16)
	testing.expect_value(t, head.length, len(bytes))
}

@(test)
test_request_head_ignores_bounded_leading_empty_lines_at_every_split :: proc(t: ^testing.T) {
	raw :=
		"\r\n\r\n\r\n\r\n\r\n\r\n\r\n" +
		"GET http://target.example/items HTTP/1.1\r\nHost: other.example\r\n\r\n"
	bytes := transmute([]u8)raw
	for split := 1; split < len(bytes); split += 1 {
		input: [128]u8
		for i := 0; i < len(bytes); i += 1 {
			input[i] = bytes[i]
		}
		headers: [2]Header
		parser: Parser
		_, result := parse_request_head(&parser, input[:split], headers[:])
		testing.expect_value(t, result, Parse_Result.Need_More)

		head: Request_Head
		head, result = parse_request_head(&parser, input[:len(bytes)], headers[:])
		testing.expect_value(t, result, Parse_Result.Ready)
		testing.expect_value(t, head.length, len(bytes))
		testing.expect_value(t, head.method.offset, 14)
		testing.expect_value(t, head.target.offset, 18)
		testing.expect_value(t, head.authority.offset, 25)
		testing.expect_value(t, head.authority.length, len("target.example"))
		testing.expect_value(
			t,
			string(input[head.authority.offset:head.authority.offset + head.authority.length]),
			"target.example",
		)
	}
}

@(test)
test_request_head_bounds_leading_empty_lines_by_request_line_limit :: proc(t: ^testing.T) {
	valid := "\r\n\r\n\r\n\r\n\r\n\r\n\r\n" + "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	invalid := "\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n" + "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	limits := DEFAULT_LIMITS
	limits.request_line_bytes = len("GET / HTTP/1.1")
	valid_input := transmute([]u8)valid
	valid_headers: [2]Header
	valid_parser: Parser
	_, result := parse_request_head(&valid_parser, valid_input, valid_headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Ready)

	invalid_input := transmute([]u8)invalid
	invalid_headers: [2]Header
	invalid_parser: Parser
	_, result = parse_request_head(&invalid_parser, invalid_input, invalid_headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Request_Line_Too_Large)
}

@(test)
test_request_head_preserves_empty_authority_after_leading_empty_line :: proc(t: ^testing.T) {
	raw := "\r\nGET / HTTP/1.0\r\n\r\n"
	input := transmute([]u8)raw
	headers: [1]Header
	parser: Parser
	head, result := parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, head.authority, Span{})
}

@(test)
test_request_head_rejects_partial_fields_over_field_limits :: proc(t: ^testing.T) {
	name_raw := "GET / HTTP/1.1\r\nHost: example.com\r\nLong-Field"
	name_input := transmute([]u8)name_raw
	name_headers: [4]Header
	name_parser: Parser
	name_limits := DEFAULT_LIMITS
	name_limits.field_name_bytes = len("Long")
	_, result := parse_request_head(&name_parser, name_input, name_headers[:], name_limits)
	testing.expect_value(t, result, Parse_Result.Header_Section_Too_Large)

	value_raw := "GET / HTTP/1.1\r\nHost: example.com\r\nX: value-too-long"
	value_input := transmute([]u8)value_raw
	value_headers: [4]Header
	value_parser: Parser
	value_limits := DEFAULT_LIMITS
	value_limits.field_value_bytes = len("value")
	_, result = parse_request_head(&value_parser, value_input, value_headers[:], value_limits)
	testing.expect_value(t, result, Parse_Result.Header_Section_Too_Large)

	name_split_raw := "GET / HTTP/1.1\r\nHost: example.com\r\nLonger"
	name_split_input: [128]u8
	for c, i in name_split_raw {
		name_split_input[i] = u8(c)
	}
	name_split_headers: [4]Header
	name_split_parser: Parser
	name_split_limits := DEFAULT_LIMITS
	name_split_limits.field_name_bytes = len("Long")
	name_split := len("GET / HTTP/1.1\r\nHost: example.com\r\nLong")
	_, result = parse_request_head(
		&name_split_parser,
		name_split_input[:name_split],
		name_split_headers[:],
		name_split_limits,
	)
	testing.expect_value(t, result, Parse_Result.Need_More)
	_, result = parse_request_head(
		&name_split_parser,
		name_split_input[:len(name_split_raw)],
		name_split_headers[:],
		name_split_limits,
	)
	testing.expect_value(t, result, Parse_Result.Header_Section_Too_Large)

	value_split_raw := "GET / HTTP/1.1\r\nHost: example.com\r\nX: abcdefghijkl"
	value_split_input: [128]u8
	for c, i in value_split_raw {
		value_split_input[i] = u8(c)
	}
	value_split_headers: [4]Header
	value_split_parser: Parser
	value_split_limits := DEFAULT_LIMITS
	value_split_limits.field_value_bytes = len(" example.com")
	value_split := len("GET / HTTP/1.1\r\nHost: example.com\r\nX: abcdef")
	_, result = parse_request_head(
		&value_split_parser,
		value_split_input[:value_split],
		value_split_headers[:],
		value_split_limits,
	)
	testing.expect_value(t, result, Parse_Result.Need_More)
	_, result = parse_request_head(
		&value_split_parser,
		value_split_input[:len(value_split_raw)],
		value_split_headers[:],
		value_split_limits,
	)
	testing.expect_value(t, result, Parse_Result.Header_Section_Too_Large)
}

@(test)
test_request_head_spans_match_one_shot_at_every_split :: proc(t: ^testing.T) {
	raw :=
		"POST /upload?mode=fast HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"X-Tag: first\r\n" +
		"x-tag: second\r\n" +
		"Content-Length: 3\r\n\r\nabc"
	bytes := transmute([]u8)raw
	input: [256]u8
	for value, i in bytes {
		input[i] = value
	}
	full_input := input[:len(bytes)]

	wanted_headers: [8]Header
	wanted_parser: Parser
	wanted, result := parse_request_head(&wanted_parser, full_input, wanted_headers[:])
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, test_span_is_within_input(wanted.method, len(full_input)), true)
	testing.expect_value(t, test_span_is_within_input(wanted.target, len(full_input)), true)
	testing.expect_value(t, test_span_is_within_input(wanted.authority, len(full_input)), true)
	testing.expect_value(t, test_span_is_within_input(wanted.version, len(full_input)), true)
	for header in wanted.headers {
		testing.expect_value(t, test_span_is_within_input(header.name, len(full_input)), true)
		testing.expect_value(t, test_span_is_within_input(header.value, len(full_input)), true)
	}

	parser: Parser
	headers: [8]Header
	for split := 0; split <= len(full_input); split += 1 {
		reset(&parser)
		_, first_result := parse_request_head(&parser, input[:split], headers[:])

		if split < wanted.length {
			testing.expect_value(t, first_result, Parse_Result.Need_More)
		} else {
			testing.expect_value(t, first_result, Parse_Result.Ready)
		}

		head, second_result := parse_request_head(&parser, full_input, headers[:])
		testing.expect_value(t, second_result, Parse_Result.Ready)
		testing.expect_value(t, head.method, wanted.method)
		testing.expect_value(t, head.target, wanted.target)
		testing.expect_value(t, head.target_form, wanted.target_form)
		testing.expect_value(t, head.authority, wanted.authority)
		testing.expect_value(t, head.version, wanted.version)
		testing.expect_value(t, head.length, wanted.length)
		testing.expect_value(t, head.content_length, wanted.content_length)
		testing.expect_value(t, head.has_content_length, wanted.has_content_length)
		testing.expect_value(t, len(head.headers), len(wanted.headers))
		testing.expect_value(t, test_span_is_within_input(head.method, len(full_input)), true)
		testing.expect_value(t, test_span_is_within_input(head.target, len(full_input)), true)
		testing.expect_value(t, test_span_is_within_input(head.authority, len(full_input)), true)
		testing.expect_value(t, test_span_is_within_input(head.version, len(full_input)), true)
		for header_index in 0 ..< len(wanted.headers) {
			testing.expect_value(
				t,
				test_span_is_within_input(head.headers[header_index].name, len(full_input)),
				true,
			)
			testing.expect_value(
				t,
				test_span_is_within_input(head.headers[header_index].value, len(full_input)),
				true,
			)
			testing.expect_value(
				t,
				head.headers[header_index].name,
				wanted.headers[header_index].name,
			)
			testing.expect_value(
				t,
				head.headers[header_index].value,
				wanted.headers[header_index].value,
			)
		}
	}
}

@(test)
test_request_head_returns_spans_without_copying :: proc(t: ^testing.T) {
	raw := "GET /health HTTP/1.1\r\nHost: example.com\r\nAccept: */*\r\n\r\nbody"
	input := transmute([]u8)raw
	headers: [4]Header
	parser: Parser
	head, result := parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(
		t,
		string(input[head.method.offset:head.method.offset + head.method.length]),
		"GET",
	)
	testing.expect_value(
		t,
		string(input[head.target.offset:head.target.offset + head.target.length]),
		"/health",
	)
	testing.expect_value(t, head.headers[1].value.length, 3)
	testing.expect_value(t, head.length, len(input) - 4)
}

@(test)
test_request_head_preserves_duplicate_field_order_and_bytes :: proc(t: ^testing.T) {
	raw :=
		"GET / HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"X-Value: first\r\n" +
		"x-value: second\r\n\r\n"
	input := transmute([]u8)raw
	headers: [4]Header
	parser: Parser
	head, result := parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, len(head.headers), 3)
	testing.expect_value(
		t,
		string(
			input[head.headers[1].name.offset:head.headers[1].name.offset +
			head.headers[1].name.length],
		),
		"X-Value",
	)
	testing.expect_value(
		t,
		string(
			input[head.headers[1].value.offset:head.headers[1].value.offset +
			head.headers[1].value.length],
		),
		"first",
	)
	testing.expect_value(
		t,
		string(
			input[head.headers[2].name.offset:head.headers[2].name.offset +
			head.headers[2].name.length],
		),
		"x-value",
	)
	testing.expect_value(
		t,
		string(
			input[head.headers[2].value.offset:head.headers[2].value.offset +
			head.headers[2].value.length],
		),
		"second",
	)
}

@(test)
test_request_head_rejects_invalid_line_endings :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\n\n"
	input := transmute([]u8)raw
	headers: [4]Header
	parser: Parser
	_, result := parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, result, Parse_Result.Invalid)
}

@(test)
test_request_head_rejects_ambiguous_field_syntax :: proc(t: ^testing.T) {
	invalid_requests := [?]string {
		"GET / HTTP/1.1\r\nHost: example.com\r\n folded: value\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: example.com\r\nBad Name: value\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: example.com\r\nX-Value: before\x01after\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: example.com\r\nX-Value: before\x7fafter\r\n\r\n",
		"GET / HTTP/1.1\rHost: example.com\r\n\r\n",
		"GET\t/ HTTP/1.1\r\nHost: example.com\r\n\r\n",
		"GET / HTTP/1.2\r\nHost: example.com\r\n\r\n",
	}
	for raw in invalid_requests {
		input := transmute([]u8)raw
		headers: [4]Header
		parser: Parser
		_, result := parse_request_head(&parser, input, headers[:])
		testing.expect_value(t, result, Parse_Result.Invalid)
	}
}

@(test)
test_request_head_framing_error_is_terminal_before_pipelined_request :: proc(t: ^testing.T) {
	raw :=
		"POST /first HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"Content-Length: 1\r\n" +
		"Transfer-Encoding: chunked\r\n\r\n" +
		"GET /next HTTP/1.1\r\nHost: example.com\r\n\r\n"
	input := transmute([]u8)raw
	headers: [8]Header
	parser: Parser
	_, result := parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, result, Parse_Result.Invalid)
	testing.expect_value(t, parser.state, Parser_State.Error)

	_, result = parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, result, Parse_Result.Invalid)
	testing.expect_value(t, parser.state, Parser_State.Error)
}

@(test)
test_request_head_preserves_horizontal_tab_in_field_value :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\r\nHost: example.com\r\nX-Value: a\tb\r\n\r\n"
	input := transmute([]u8)raw
	headers: [2]Header
	parser: Parser
	head, result := parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(
		t,
		string(
			input[head.headers[1].value.offset:head.headers[1].value.offset +
			head.headers[1].value.length],
		),
		"a\tb",
	)
}

@(test)
test_request_head_enforces_fixed_header_capacity :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\r\nA: b\r\nB: c\r\n\r\n"
	input := transmute([]u8)raw
	headers: [1]Header
	parser: Parser
	_, result := parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, result, Parse_Result.Header_Capacity)
}

@(test)
test_request_head_enforces_header_count_at_exact_boundary :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\r\nHost: example.com\r\nX-Test: yes\r\n\r\n"
	input := transmute([]u8)raw
	headers: [2]Header
	limits := DEFAULT_LIMITS
	limits.header_count = 2
	parser: Parser
	head, result := parse_request_head(&parser, input, headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, head.length, len(input))
	testing.expect_value(t, len(head.headers), 2)

	reset(&parser)
	limits.header_count = 1
	_, result = parse_request_head(&parser, input, headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Header_Section_Too_Large)
}

@(test)
test_request_head_enforces_header_byte_limit :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\r\nHost: a\r\n\r\n"
	input := transmute([]u8)raw
	headers: [4]Header
	parser: Parser
	limits := DEFAULT_LIMITS
	limits.header_bytes = 11
	_, result := parse_request_head(&parser, input, headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Ready)

	reset(&parser)
	limits.header_bytes = 10
	_, result = parse_request_head(&parser, input, headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Header_Section_Too_Large)
}

@(test)
test_request_head_enforces_field_name_and_value_limits :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\r\nHost: a\r\nX-Test: value\r\n\r\n"
	input := transmute([]u8)raw
	headers: [4]Header
	limits := DEFAULT_LIMITS
	limits.field_name_bytes = len("X-Test")
	limits.field_value_bytes = len(" value")
	parser: Parser
	_, result := parse_request_head(&parser, input, headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Ready)

	reset(&parser)
	limits.field_name_bytes -= 1
	_, result = parse_request_head(&parser, input, headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Header_Section_Too_Large)

	reset(&parser)
	limits.field_name_bytes = len("X-Test")
	limits.field_value_bytes -= 1
	_, result = parse_request_head(&parser, input, headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Header_Section_Too_Large)

	padded_raw := "GET / HTTP/1.1\r\nHost: a\r\nX-Test:  y \r\n\r\n"
	padded_input := transmute([]u8)padded_raw
	reset(&parser)
	limits.field_name_bytes = len("X-Test")
	limits.field_value_bytes = 3
	_, result = parse_request_head(&parser, padded_input, headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Header_Section_Too_Large)
}

@(test)
test_content_length_values_are_unambiguous :: proc(t: ^testing.T) {
	valid_raw := "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5, 5\r\ncontent-length: 005\r\n\r\n"
	valid_input := transmute([]u8)valid_raw
	valid_headers: [8]Header
	valid_parser: Parser
	valid_head, result := parse_request_head(&valid_parser, valid_input, valid_headers[:])
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, valid_head.has_content_length, true)
	testing.expect_value(t, valid_head.content_length, u64(5))

	max_raw := "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 18446744073709551615\r\n\r\n"
	max_input := transmute([]u8)max_raw
	max_headers: [4]Header
	max_parser: Parser
	max_head, max_result := parse_request_head(&max_parser, max_input, max_headers[:])
	testing.expect_value(t, max_result, Parse_Result.Ready)
	testing.expect_value(t, max_head.content_length, MAX_CONTENT_LENGTH)

	invalid_requests := [?]string {
		"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5, 6\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5,\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length:\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: +5\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 18446744073709551616\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n",
	}
	for raw in invalid_requests {
		input := transmute([]u8)raw
		headers: [8]Header
		parser: Parser
		_, result = parse_request_head(&parser, input, headers[:])
		testing.expect_value(t, result, Parse_Result.Invalid)
	}
}

@(test)
test_transfer_encoding_accepts_only_chunked :: proc(t: ^testing.T) {
	valid_raw := "POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: Chunked\r\n\r\n"
	valid_input := transmute([]u8)valid_raw
	valid_headers: [4]Header
	valid_parser: Parser
	valid_head, valid_result := parse_request_head(&valid_parser, valid_input, valid_headers[:])
	testing.expect_value(t, valid_result, Parse_Result.Ready)
	testing.expect_value(t, valid_head.is_chunked, true)
	testing.expect_value(t, valid_head.has_content_length, false)

	invalid_requests := [?]string {
		"POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding:\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: gzip\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked, gzip\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked, chunked\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked;foo=bar\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n",
		"POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n",
	}
	for raw in invalid_requests {
		input := transmute([]u8)raw
		headers: [8]Header
		parser: Parser
		_, result := parse_request_head(&parser, input, headers[:])
		testing.expect_value(t, result, Parse_Result.Invalid)
	}
}

@(test)
test_request_line_limit_accepts_a_split_trailing_cr :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	input := transmute([]u8)raw
	headers: [1]Header
	parser: Parser
	limits := DEFAULT_LIMITS
	limits.request_line_bytes = len("GET / HTTP/1.1")

	_, result := parse_request_head(&parser, input[:len("GET / HTTP/1.1") + 1], headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Need_More)

	_, result = parse_request_head(&parser, input, headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Ready)

	reset(&parser)
	limits.request_line_bytes -= 1
	_, result = parse_request_head(&parser, input, headers[:], limits)
	testing.expect_value(t, result, Parse_Result.Request_Line_Too_Large)
}
