#+test
package http1

import "core:testing"

@(test)
test_http11_recognizes_100_continue_expectations :: proc(t: ^testing.T) {
	testing.expect_value(
		t,
		expectation_request(
			"POST / HTTP/1.1\r\nHost: example.com\r\nExpect: 100-continue\r\n\r\n",
		),
		Parse_Result.Ready,
	)
	head, result := parse_expectation_request(
		"POST / HTTP/1.1\r\nHost: example.com\r\nExpect: 100-Continue, 100-continue\r\n\r\n",
	)
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, head.expect_continue, true)

	head, result = parse_expectation_request(
		"POST / HTTP/1.1\r\nHost: example.com\r\nExpect: 100-continue,\r\n\r\n",
	)
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, head.expect_continue, true)

	head, result = parse_expectation_request(
		"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 0\r\nExpect: 100-continue\r\n\r\n",
	)
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, head.expect_continue, true)

	head, result = parse_expectation_request("GET / HTTP/1.0\r\nExpect: unknown\r\n\r\n")
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, head.expect_continue, false)
	head, result = parse_expectation_request(
		"GET / HTTP/1.1\r\nHost: example.com\r\nExpect:\r\n\r\n",
	)
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, head.expect_continue, false)
}

@(test)
test_http11_reports_unsupported_and_malformed_expectations :: proc(t: ^testing.T) {
	unsupported := [?]string {
		"POST / HTTP/1.1\r\nHost: example.com\r\nExpect: fancy\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nExpect: 100-continue; mode=fast\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nExpect: 100-continue;\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nExpect: fancy=token; q=\"yes\"\r\n\r\n",
	}
	for raw in unsupported {
		testing.expect_value(t, expectation_request(raw), Parse_Result.Expectation_Failed)
	}

	malformed := [?]string {
		"POST / HTTP/1.1\r\nHost: example.com\r\nExpect: 100-continue, =x\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: example.com\r\nExpect: fancy=\"unterminated\r\n\r\n",
	}
	for raw in malformed {
		testing.expect_value(t, expectation_request(raw), Parse_Result.Invalid)
	}
}

expectation_request :: proc(raw: string) -> Parse_Result {
	_, result := parse_expectation_request(raw)
	return result
}

parse_expectation_request :: proc(raw: string) -> (head: Request_Head, result: Parse_Result) {
	input := transmute([]u8)raw
	headers: [8]Header
	parser: Parser
	return parse_request_head(&parser, input, headers[:])
}
