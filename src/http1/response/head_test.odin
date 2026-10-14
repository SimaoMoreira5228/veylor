#+test
package response

import "core:testing"
import "veylor:http1"

@(test)
test_response_head_encodes_fixed_length :: proc(t: ^testing.T) {
	method := "GET"
	server_name := "Veylor"
	content_type := "text/plain"
	server_header := "Server"
	content_type_header := "Content-Type"
	reason := "OK"
	headers := [?]Response_Header {
		{name = transmute([]u8)server_header, value = transmute([]u8)server_name},
		{name = transmute([]u8)content_type_header, value = transmute([]u8)content_type},
	}
	output: [256]u8
	event := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		headers[:],
		.Fixed_Length,
		123,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Written)
	testing.expect_value(t, event.body_allowed, true)
	testing.expect_value(
		t,
		string(output[:event.written]),
		"HTTP/1.1 200 OK\r\n" +
		"Server: Veylor\r\n" +
		"Content-Type: text/plain\r\n" +
		"Content-Length: 123\r\n\r\n",
	)
}

@(test)
test_response_head_selects_chunked_framing :: proc(t: ^testing.T) {
	method := "GET"
	reason := "OK"
	output: [128]u8
	event := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Chunked,
		0,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Written)
	testing.expect_value(
		t,
		string(output[:event.written]),
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
	)
	event = encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Chunked,
		1,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Invalid)

}

@(test)
test_response_head_formats_maximum_u64_content_length :: proc(t: ^testing.T) {
	method := "GET"
	reason := "OK"
	output: [128]u8
	event := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		0xffffffffffffffff,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Written)
	testing.expect_value(
		t,
		string(output[:event.written]),
		"HTTP/1.1 200 OK\r\nContent-Length: 18446744073709551615\r\n\r\n",
	)
}

@(test)
test_response_head_checks_capacity_before_writing :: proc(t: ^testing.T) {
	method := "GET"
	output: [16]u8
	reason := "OK"
	for i in 0 ..< len(output) {
		output[i] = 0xa5
	}
	event := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		0,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Buffer_Small)
	for value in output {
		testing.expect_value(t, value, u8(0xa5))
	}
}

@(test)
test_response_head_omits_content_length_for_1xx_and_204 :: proc(t: ^testing.T) {
	method := "GET"
	reason := "Status"
	output: [128]u8
	cases := [?]struct {
		status:         u16,
		content_length: u64,
		wire:           string,
	} {
		{status = 100, wire = "HTTP/1.1 100 Status\r\n\r\n"},
		{status = 204, wire = "HTTP/1.1 204 Status\r\n\r\n"},
		{status = 205, wire = "HTTP/1.1 205 Status\r\nContent-Length: 0\r\n\r\n"},
		{
			status = 304,
			content_length = 17,
			wire = "HTTP/1.1 304 Status\r\nContent-Length: 17\r\n\r\n",
		},
	}
	for test_case in cases {
		event := encode_response_head(
			transmute([]u8)method,
			test_case.status,
			transmute([]u8)reason,
			nil,
			.Fixed_Length,
			test_case.content_length,
			output[:],
		)
		testing.expect_value(t, event.result, Response_Head_Result.Written)
		testing.expect_value(t, event.body_allowed, false)
		testing.expect_value(t, string(output[:event.written]), test_case.wire)
	}
	event := encode_response_head(
		transmute([]u8)method,
		204,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		1,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Invalid)
	event = encode_response_head(
		transmute([]u8)method,
		205,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		1,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Invalid)
}

@(test)
test_response_head_rejects_injection_and_caller_framing :: proc(t: ^testing.T) {
	method := "GET"
	output: [128]u8
	bad_value := "safe\r\nX-Injected: yes"
	value_header := "X-Value"
	transfer_header := "Transfer-Encoding"
	chunked := "chunked"
	reason := "OK"
	header := [?]Response_Header {
		{name = transmute([]u8)value_header, value = transmute([]u8)bad_value},
	}
	event := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		header[:],
		.Fixed_Length,
		0,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Invalid)

	framing_header := [?]Response_Header {
		{name = transmute([]u8)transfer_header, value = transmute([]u8)chunked},
	}
	event = encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		framing_header[:],
		.Fixed_Length,
		0,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Invalid)

	length_header := "Content-Length"
	framing_header[0] = Response_Header {
		name  = transmute([]u8)length_header,
		value = transmute([]u8)chunked,
	}
	event = encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		framing_header[:],
		.Fixed_Length,
		0,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Invalid)
	connection_header := "Connection"
	close_value := "close"
	framing_header[0] = Response_Header {
		name  = transmute([]u8)connection_header,
		value = transmute([]u8)close_value,
	}
	event = encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		framing_header[:],
		.Fixed_Length,
		0,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Invalid)
}

@(test)
test_successful_connect_response_starts_tunnel_without_http_body_framing :: proc(t: ^testing.T) {
	method := "CONNECT"
	reason := "Connection Established"
	output: [128]u8
	event := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		0,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Written)
	testing.expect_value(t, event.body_allowed, false)
	testing.expect_value(
		t,
		string(output[:event.written]),
		"HTTP/1.1 200 Connection Established\r\n\r\n",
	)

	event = encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		1,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Invalid)
}

@(test)
test_response_head_encodes_connection_policy_without_conflicts :: proc(t: ^testing.T) {
	method := "GET"
	reason := "OK"
	output: [128]u8
	close_event := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		0,
		output[:],
		.Close,
	)
	testing.expect_value(t, close_event.result, Response_Head_Result.Written)
	testing.expect_value(
		t,
		string(output[:close_event.written]),
		"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
	)

	keep_alive_event := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		0,
		output[:],
		.Keep_Alive,
	)
	testing.expect_value(t, keep_alive_event.result, Response_Head_Result.Written)
	testing.expect_value(
		t,
		string(output[:keep_alive_event.written]),
		"HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n",
	)
}

@(test)
test_close_delimited_response_requires_close_and_omits_length_headers :: proc(t: ^testing.T) {
	method := "GET"
	reason := "OK"
	output: [128]u8
	event := encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Close_Delimited,
		0,
		output[:],
		.Close,
	)
	testing.expect_value(t, event.result, Response_Head_Result.Written)
	testing.expect_value(t, event.body_allowed, true)
	testing.expect_value(
		t,
		string(output[:event.written]),
		"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n",
	)

	event = encode_response_head(
		transmute([]u8)method,
		200,
		transmute([]u8)reason,
		nil,
		.Close_Delimited,
		0,
		output[:],
	)
	testing.expect_value(t, event.result, Response_Head_Result.Invalid)
}
