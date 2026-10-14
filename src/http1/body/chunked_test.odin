#+test
package body

import "core:testing"
import "veylor:http1"

decode_chunked_body_test :: proc(
	decoder: ^Chunked_Decoder,
	input: []u8,
	spans: []http1.Span,
	limits := DEFAULT_CHUNKED_LIMITS,
) -> Chunked_Event {
	permitted_trailer_names := [?]string{"X-Note", "X-Tag", "X-Other", "X", "Y", "Digest"}
	trailer_context := Chunked_Trailer_Context {
		permitted_trailer_names = permitted_trailer_names[:],
	}
	return decode_chunked_body(decoder, input, spans, trailer_context, limits)
}

@(test)
test_chunked_body_streams_bytewise :: proc(t: ^testing.T) {
	raw := "4;foo=bar\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Note: ok\r\n\r\n"
	input := transmute([]u8)raw
	decoder: Chunked_Decoder
	spans: [4]http1.Span
	complete := false
	trailer_seen := false
	for i := 0; i < len(input); i += 1 {
		fragment := input[i:i + 1]
		event := decode_chunked_body_test(&decoder, fragment, spans[:])
		testing.expect_value(t, event.consumed, 1)

		if event.span_count > 0 {
			testing.expect_value(
				t,
				string(fragment[spans[0].offset:spans[0].offset + spans[0].length]),
				string(fragment),
			)
		}

		if event.result == .Trailer {
			trailer_seen = true
			testing.expect_value(t, string(event.trailer_name), "X-Note")
			testing.expect_value(t, string(event.trailer_value), "ok")
		}

		if event.result == .Complete {
			testing.expect_value(t, i, len(input) - 1)
			complete = true
		}
	}
	testing.expect_value(t, trailer_seen, true)
	testing.expect_value(t, complete, true)
}

@(test)
test_chunked_body_returns_data_and_trailer_events :: proc(t: ^testing.T) {
	raw := "1\r\na\r\n0\r\nX-Tag: value\r\nX-Other: second\r\n\r\n"
	input := transmute([]u8)raw
	decoder: Chunked_Decoder
	spans: [2]http1.Span
	event := decode_chunked_body_test(&decoder, input, spans[:])
	testing.expect_value(t, event.result, Chunked_Result.Trailer)
	testing.expect_value(t, event.span_count, 1)
	testing.expect_value(t, string(input[spans[0].offset:spans[0].offset + spans[0].length]), "a")
	testing.expect_value(t, string(event.trailer_name), "X-Tag")
	testing.expect_value(t, string(event.trailer_value), "value")
	first_trailer_consumed := event.consumed
	event = decode_chunked_body_test(&decoder, input[first_trailer_consumed:], spans[:])
	testing.expect_value(t, event.result, Chunked_Result.Trailer)
	testing.expect_value(t, string(event.trailer_name), "X-Other")
	testing.expect_value(t, string(event.trailer_value), "second")
	second_trailer_consumed := event.consumed
	event = decode_chunked_body_test(
		&decoder,
		input[first_trailer_consumed + second_trailer_consumed:],
		spans[:],
	)
	testing.expect_value(t, event.result, Chunked_Result.Complete)
	testing.expect_value(t, event.consumed, 2)
}

@(test)
test_chunked_body_rejects_malformed_and_forbidden_trailers :: proc(t: ^testing.T) {
	invalid := [?]string {
		"1\na",
		"z\r\n",
		"1\r\naX\r\n",
		"0\r\nHost: x\r\n\r\n",
		"0\r\nContent-Length: 1\r\n\r\n",
		"0\r\nTransfer-Encoding: chunked\r\n\r\n",
		"0\r\nTrailer: x\r\n\r\n",
		"0\r\nConnection: close\r\n\r\n",
		"0\r\nAuthorization: secret\r\n\r\n",
		"0\r\nContent-Type: text/plain\r\n\r\n",
	}
	for raw in invalid {
		decoder: Chunked_Decoder
		spans: [4]http1.Span
		event := decode_chunked_body_test(&decoder, transmute([]u8)raw, spans[:])
		testing.expect_value(t, event.result, Chunked_Result.Invalid)
	}
}

@(test)
test_chunked_framing_error_cannot_resume_at_following_bytes :: proc(t: ^testing.T) {
	raw := "z\r\n0\r\n\r\nGET /next HTTP/1.1\r\nHost: example.com\r\n\r\n"
	input := transmute([]u8)raw
	decoder: Chunked_Decoder
	spans: [2]http1.Span
	event := decode_chunked_body_test(&decoder, input, spans[:])
	testing.expect_value(t, event.result, Chunked_Result.Invalid)
	testing.expect_value(t, decoder.state, Chunked_State.Error)

	event = decode_chunked_body_test(&decoder, input[event.consumed:], spans[:])
	testing.expect_value(t, event.result, Chunked_Result.Invalid)
	testing.expect_value(t, event.consumed, 0)
	testing.expect_value(t, decoder.state, Chunked_State.Error)
}

@(test)
test_chunked_body_rejects_connection_nominated_trailers :: proc(t: ^testing.T) {
	head_raw :=
		"POST /upload HTTP/1.1\r\n" + "Host: example.com\r\n" + "Connection: X-Private\r\n\r\n"
	head_input := transmute([]u8)head_raw
	headers: [4]http1.Header
	parser: http1.Parser
	head, head_result := http1.parse_request_head(&parser, head_input, headers[:])
	testing.expect_value(t, head_result, http1.Parse_Result.Ready)
	permitted_trailer_names := [?]string{"X-Private", "Digest"}
	trailer_context := Chunked_Trailer_Context {
		headers                 = head.headers,
		head_input              = head_input,
		permitted_trailer_names = permitted_trailer_names[:],
	}

	body_raw := "0\r\nx-private: secret\r\n\r\n"
	body_input := transmute([]u8)body_raw
	decoder: Chunked_Decoder
	spans: [1]http1.Span
	event := decode_chunked_body(&decoder, body_input, spans[:], trailer_context)
	testing.expect_value(t, event.result, Chunked_Result.Invalid)

	safe_body_raw := "0\r\nDigest: sha-256=abc\r\n\r\n"
	safe_body_input := transmute([]u8)safe_body_raw
	safe_decoder: Chunked_Decoder
	event = decode_chunked_body(&safe_decoder, safe_body_input, spans[:], trailer_context)
	testing.expect_value(t, event.result, Chunked_Result.Trailer)
	testing.expect_value(t, string(event.trailer_name), "Digest")
}

@(test)
test_chunked_body_requires_explicit_trailer_allowlist :: proc(t: ^testing.T) {
	raw := "0\r\nDigest: sha-256=abc\r\n\r\n"
	input := transmute([]u8)raw
	decoder: Chunked_Decoder
	spans: [1]http1.Span
	event := decode_chunked_body(&decoder, input, spans[:], Chunked_Trailer_Context{})
	testing.expect_value(t, event.result, Chunked_Result.Invalid)
}

@(test)
test_chunked_body_enforces_quota_limits :: proc(t: ^testing.T) {
	quota_case :: proc(t: ^testing.T, raw: string, limits: Chunked_Limits) {
		decoder: Chunked_Decoder
		spans: [4]http1.Span
		input := transmute([]u8)raw
		consumed := 0
		event := decode_chunked_body_test(&decoder, input, spans[:], limits)
		for event.result == .Trailer {
			consumed += event.consumed
			event = decode_chunked_body_test(&decoder, input[consumed:], spans[:], limits)
		}
		testing.expect_value(t, event.result, Chunked_Result.Too_Large)
	}

	limits := DEFAULT_CHUNKED_LIMITS
	limits.chunk_line_bytes = 1
	quota_case(t, "10\r\n", limits)
	limits = DEFAULT_CHUNKED_LIMITS
	limits.total_body_bytes = 2
	quota_case(t, "3\r\nabc\r\n0\r\n\r\n", limits)
	limits = DEFAULT_CHUNKED_LIMITS
	limits.extension_bytes = 3
	quota_case(t, "1;foo=bar\r\na\r\n0\r\n\r\n", limits)
	quota_case(t, "1;f=x\r\na\r\n0\r\n\r\n", limits)
	limits.extension_bytes = 4
	quota_case(t, "1;a=1;b=2\r\na\r\n0\r\n\r\n", limits)
	limits = DEFAULT_CHUNKED_LIMITS
	limits.trailer_bytes = 5
	quota_case(t, "0\r\nX: y\r\n\r\n", limits)
	limits = DEFAULT_CHUNKED_LIMITS
	limits.trailer_count = 1
	quota_case(t, "0\r\nX: y\r\nY: z\r\n\r\n", limits)
	overflow_raw := "10000000000000000\r\n"
	overflow := transmute([]u8)overflow_raw
	decoder: Chunked_Decoder
	spans: [4]http1.Span
	event := decode_chunked_body_test(&decoder, overflow, spans[:])
	testing.expect_value(t, event.result, Chunked_Result.Too_Large)
}

@(test)
test_chunked_trailer_quota_check_does_not_overflow :: proc(t: ^testing.T) {
	limits := DEFAULT_CHUNKED_LIMITS
	limits.trailer_bytes = int(0x7fff_ffff_ffff_ffff)
	decoder := Chunked_Decoder {
		state              = .Trailer_Line,
		trailer_bytes_seen = limits.trailer_bytes,
	}
	permitted_trailer_names := [?]string{"X"}
	trailer_context := Chunked_Trailer_Context {
		permitted_trailer_names = permitted_trailer_names[:],
	}
	raw := "X: y\r\n"
	event := decode_chunked_body(&decoder, transmute([]u8)raw, nil, trailer_context, limits)
	testing.expect_value(t, event.result, Chunked_Result.Too_Large)
	testing.expect_value(t, event.consumed, 0)
}

@(test)
test_chunked_body_accepts_exact_quota_boundaries :: proc(t: ^testing.T) {
	limits := DEFAULT_CHUNKED_LIMITS
	limits.chunk_line_bytes = 5
	limits.extension_bytes = 4
	limits.total_body_bytes = 1
	limits.trailer_bytes = 8
	limits.trailer_count = 1
	raw := "1;f=x\r\na\r\n0\r\nX: y\r\n\r\n"
	input := transmute([]u8)raw
	decoder: Chunked_Decoder
	spans: [2]http1.Span
	event := decode_chunked_body_test(&decoder, input, spans[:], limits)
	testing.expect_value(t, event.result, Chunked_Result.Trailer)
	event = decode_chunked_body_test(&decoder, input[event.consumed:], spans[:], limits)
	testing.expect_value(t, event.result, Chunked_Result.Complete)
	testing.expect_value(t, event.consumed, 2)

	limits.chunk_line_bytes = 6
	limits.extension_bytes = 5
	raw = "1 ;f=x\r\na\r\n0\r\n\r\n"
	input = transmute([]u8)raw
	decoder = Chunked_Decoder{}
	event = decode_chunked_body_test(&decoder, input, spans[:], limits)
	testing.expect_value(t, event.result, Chunked_Result.Complete)
	testing.expect_value(t, event.consumed, len(input))

	limits.extension_bytes = 4
	decoder = Chunked_Decoder{}
	event = decode_chunked_body_test(&decoder, input, spans[:], limits)
	testing.expect_value(t, event.result, Chunked_Result.Too_Large)

	limits.chunk_line_bytes = 16
	limits.extension_bytes = 8
	raw = "1;a=1;b=2\r\na\r\n0\r\n\r\n"
	input = transmute([]u8)raw
	decoder = Chunked_Decoder{}
	event = decode_chunked_body_test(&decoder, input, spans[:], limits)
	testing.expect_value(t, event.result, Chunked_Result.Complete)
	testing.expect_value(t, event.consumed, len(input))

	limits.extension_bytes = 7
	decoder = Chunked_Decoder{}
	event = decode_chunked_body_test(&decoder, input, spans[:], limits)
	testing.expect_value(t, event.result, Chunked_Result.Too_Large)

}

@(test)
test_chunked_consumption_leaves_pipelined_bytes :: proc(t: ^testing.T) {
	raw := "0\r\n\r\nGET /next HTTP/1.1\r\n"
	input := transmute([]u8)raw
	decoder: Chunked_Decoder
	spans: [2]http1.Span
	event := decode_chunked_body_test(&decoder, input, spans[:])
	testing.expect_value(t, event.result, Chunked_Result.Complete)
	testing.expect_value(t, event.consumed, len("0\r\n\r\n"))
	testing.expect_value(t, string(input[event.consumed:]), "GET /next HTTP/1.1\r\n")
}

@(test)
test_chunked_span_capacity_can_be_retried :: proc(t: ^testing.T) {
	raw := "1\r\na\r\n1\r\nb\r\n0\r\n\r\n"
	input := transmute([]u8)raw
	decoder: Chunked_Decoder
	no_spans: [0]http1.Span
	event := decode_chunked_body_test(&decoder, input, no_spans[:])
	testing.expect_value(t, event.span_count, 0)
	testing.expect_value(t, event.consumed, 3)
	testing.expect_value(t, event.result, Chunked_Result.Span_Capacity)
	testing.expect_value(t, decoder.state, Chunked_State.Data)
	spans: [2]http1.Span
	event = decode_chunked_body_test(&decoder, input[event.consumed:], spans[:])
	testing.expect_value(t, event.span_count, 2)
	testing.expect_value(t, event.result, Chunked_Result.Complete)
	testing.expect_value(t, event.consumed, len(input) - 3)
}

@(test)
test_chunked_span_capacity_preserves_prior_spans :: proc(t: ^testing.T) {
	raw := "1\r\na\r\n1\r\nb\r\n0\r\n\r\n"
	input := transmute([]u8)raw
	decoder: Chunked_Decoder
	first_spans: [1]http1.Span
	event := decode_chunked_body_test(&decoder, input, first_spans[:])
	testing.expect_value(t, event.span_count, 1)
	testing.expect_value(t, event.result, Chunked_Result.Span_Capacity)
	testing.expect_value(
		t,
		string(input[first_spans[0].offset:first_spans[0].offset + first_spans[0].length]),
		"a",
	)

	second_spans: [1]http1.Span
	second_input_start := event.consumed
	event = decode_chunked_body_test(&decoder, input[second_input_start:], second_spans[:])
	testing.expect_value(t, event.span_count, 1)
	testing.expect_value(t, event.result, Chunked_Result.Complete)
	testing.expect_value(
		t,
		string(
			input[second_input_start +
			second_spans[0].offset:second_input_start +
			second_spans[0].offset +
			second_spans[0].length],
		),
		"b",
	)
}

@(test)
test_chunk_extensions_accept_bws_and_quoted_values :: proc(t: ^testing.T) {
	valid := [?]string{"1 ; foo = bar\r\na\r\n0\r\n\r\n", "1;foo=\"a b\\\"c\"\r\na\r\n0\r\n\r\n"}
	for raw in valid {
		decoder: Chunked_Decoder
		spans: [2]http1.Span
		event := decode_chunked_body_test(&decoder, transmute([]u8)raw, spans[:])
		testing.expect_value(t, event.result, Chunked_Result.Complete)
	}

	invalid := [?]string{"1 ; =x\r\na\r\n", "1;foo=\"unterminated\r\na\r\n", "1;foo=bar \r\na\r\n"}
	for raw in invalid {
		decoder: Chunked_Decoder
		spans: [2]http1.Span
		event := decode_chunked_body_test(&decoder, transmute([]u8)raw, spans[:])
		testing.expect_value(t, event.result, Chunked_Result.Invalid)
	}
}

@(test)
test_chunk_extension_limit_is_enforced_as_fragments_arrive :: proc(t: ^testing.T) {
	limits := DEFAULT_CHUNKED_LIMITS
	limits.extension_bytes = 2
	decoder: Chunked_Decoder
	spans: [1]http1.Span
	raw := "1;f=x"
	fragment := transmute([]u8)raw
	event := decode_chunked_body_test(&decoder, fragment, spans[:], limits)
	testing.expect_value(t, event.result, Chunked_Result.Too_Large)
	testing.expect_value(t, event.consumed, 3)
	testing.expect_value(t, decoder.extension_bytes_seen, 2)
}
