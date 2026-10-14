#+test
package http1

import "core:testing"

@(test)
test_request_target_forms :: proc(t: ^testing.T) {
	testing.expect_value(
		t,
		request_line_result("GET /a%20b?q=x HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("GET /a\\b HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(t, request_line_result("GET /a{b HTTP/1.1\r\n\r\n"), Parse_Result.Invalid)
	testing.expect_value(
		t,
		request_line_result("GET /bad%2 HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("GET /bad% HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("GET /path?token=% HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("GET /path#fragment HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("GET http://user%20name:p%40ss@example.com/a HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("GET http://%zz@example.com/a HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("GET http://user%@example.com/a HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("GET http://example%/a HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("GET http:///path HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("GET http://example.com/a#fragment HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("GET http://example.com/% HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_head_result(
			"GET http://example.com:999999999999999999999999999999999999999999999999999999/ " +
			"HTTP/1.1\r\nHost:\r\n\r\n",
		),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT example.com:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT user@example.com:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [2001:db8::1]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [::]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [1::]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [::1]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [1:2:3:4:5:6:7:8]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [::ffff:192.0.2.1]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [v1.a]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [Vf.a-b!$&'()*+,;=.:]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [:::]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [v.a]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [v1.]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [v1.a%20]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT [::ffff:192.0.2.999]:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT example.com:abc HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT example.com:65535 HTTP/1.1\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT example.com:65536 HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_line_result("CONNECT example.com:999999999999999999999 HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(t, request_line_result("OPTIONS * HTTP/1.1\r\n\r\n"), Parse_Result.Ready)
	testing.expect_value(t, request_line_result("GET * HTTP/1.1\r\n\r\n"), Parse_Result.Invalid)
	testing.expect_value(
		t,
		request_line_result("POST example.com:443 HTTP/1.1\r\n\r\n"),
		Parse_Result.Invalid,
	)
}

@(test)
test_request_head_retains_request_target_form :: proc(t: ^testing.T) {
	form, result := request_line_form("GET /items?id=2 HTTP/1.1\r\n\r\n")
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, form, Request_Target_Form.Origin_Form)

	form, result = request_line_form("GET http://example.com/items HTTP/1.1\r\n\r\n")
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, form, Request_Target_Form.Absolute_Form)

	form, result = request_line_form("CONNECT example.com:443 HTTP/1.1\r\n\r\n")
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, form, Request_Target_Form.Authority_Form)

	form, result = request_line_form("OPTIONS * HTTP/1.1\r\n\r\n")
	testing.expect_value(t, result, Parse_Result.Ready)
	testing.expect_value(t, form, Request_Target_Form.Asterisk_Form)
}

@(test)
test_request_head_selects_raw_authority_by_target_form :: proc(t: ^testing.T) {
	expect_authority :: proc(raw: string, expected: string, t: ^testing.T) {
		input := transmute([]u8)raw
		headers: [4]Header
		parser: Parser
		head, result := parse_request_head(&parser, input, headers[:])
		testing.expect_value(t, result, Parse_Result.Ready)
		testing.expect_value(
			t,
			string(input[head.authority.offset:head.authority.offset + head.authority.length]),
			expected,
		)
	}

	expect_authority("GET /items HTTP/1.1\r\nHost: origin.example\r\n\r\n", "origin.example", t)
	expect_authority(
		"GET http://target.example/items HTTP/1.1\r\nHost: ignored.example\r\n\r\n",
		"target.example",
		t,
	)
	expect_authority(
		"GET http://user%20name:p%40ss@target.example:8443/items HTTP/1.1\r\n" +
		"Host: ignored.example\r\n\r\n",
		"target.example:8443",
		t,
	)
	expect_authority(
		"CONNECT target.example:443 HTTP/1.1\r\nHost: target.example\r\n\r\n",
		"target.example:443",
		t,
	)
	expect_authority("OPTIONS * HTTP/1.1\r\nHost: origin.example\r\n\r\n", "origin.example", t)
}

@(test)
test_http11_requires_one_valid_host_field :: proc(t: ^testing.T) {
	testing.expect_value(t, request_head_result("GET / HTTP/1.1\r\n\r\n"), Parse_Result.Invalid)
	testing.expect_value(
		t,
		request_head_result("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_head_result("GET / HTTP/1.1\r\nhOsT: example.com\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_head_result("GET / HTTP/1.1\r\nHost: example.com\r\nHost: other\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_head_result("GET / HTTP/1.1\r\nHost: example.com/path\r\n\r\n"),
		Parse_Result.Invalid,
	)
	testing.expect_value(
		t,
		request_head_result("GET / HTTP/1.1\r\nHost:\r\n\r\n"),
		Parse_Result.Ready,
	)
	testing.expect_value(
		t,
		request_head_result(
			"GET http://target.example/path HTTP/1.1\r\nHost: other.example\r\n\r\n",
		),
		Parse_Result.Ready,
	)
	testing.expect_value(t, request_head_result("GET / HTTP/1.0\r\n\r\n"), Parse_Result.Ready)
}

request_line_result :: proc(raw: string) -> Parse_Result {
	_, result := request_line_form(raw)
	return result
}

request_line_form :: proc(raw: string) -> (form: Request_Target_Form, result: Parse_Result) {
	request_length := len(raw) - 2
	host := "Host: example.com\r\n\r\n"
	buffer: [256]u8
	for i := 0; i < request_length; i += 1 {
		buffer[i] = raw[i]
	}
	for c, i in transmute([]u8)host {
		buffer[request_length + i] = c
	}
	head, parsed_result := request_head_bytes_result(buffer[:request_length + len(host)])
	return head.target_form, parsed_result
}

request_head_result :: proc(raw: string) -> Parse_Result {
	_, result := request_head_bytes_result(transmute([]u8)raw)
	return result
}

request_head_bytes_result :: proc(input: []u8) -> (head: Request_Head, result: Parse_Result) {
	headers: [8]Header
	parser: Parser
	return parse_request_head(&parser, input, headers[:])
}
