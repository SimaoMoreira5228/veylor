#+test
package response

import "core:testing"
import "veylor:http1"

@(test)
test_connection_options_parse_case_insensitive_tokens :: proc(t: ^testing.T) {
	raw := " CLOSE , keep-alive\t, Upgrade "
	options, valid := http1.parse_connection_options(transmute([]u8)raw)
	testing.expect_value(t, valid, true)
	testing.expect_value(t, options.close, true)
	testing.expect_value(t, options.keep_alive, true)
	testing.expect_value(t, options.upgrade, true)
}

@(test)
test_connection_options_reject_malformed_lists :: proc(t: ^testing.T) {
	invalid_values := [?]string {
		"",
		" ",
		",close",
		"close,",
		"close, ",
		"close,,upgrade",
		"close keep-alive",
		"close;timeout=5",
	}
	for raw in invalid_values {
		_, valid := http1.parse_connection_options(transmute([]u8)raw)
		testing.expect_value(t, valid, false)
	}
}

@(test)
test_request_head_merges_connection_options_across_fields :: proc(t: ^testing.T) {
	raw :=
		"GET / HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"Connection: keep-alive, Upgrade\r\n" +
		"Connection: close\r\n\r\n"
	input := transmute([]u8)raw
	headers: [4]http1.Header
	parser: http1.Parser
	head, result := http1.parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, result, http1.Parse_Result.Ready)
	testing.expect_value(t, head.connection.close, true)
	testing.expect_value(t, head.connection.keep_alive, true)
	testing.expect_value(t, head.connection.upgrade, true)
}

@(test)
test_request_head_rejects_malformed_connection_options :: proc(t: ^testing.T) {
	raw := "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close, \r\n\r\n"
	input := transmute([]u8)raw
	headers: [2]http1.Header
	parser: http1.Parser
	_, result := http1.parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, result, http1.Parse_Result.Invalid)
}

@(test)
test_request_connection_policy_obeys_version_and_framing :: proc(t: ^testing.T) {
	head: http1.Request_Head
	version_11 := "HTTP/1.1"
	version_10 := "HTTP/1.0"
	testing.expect_value(
		t,
		request_connection_policy(head, transmute([]u8)version_11, true, true, false),
		Connection_Policy.Keep_Alive,
	)
	testing.expect_value(
		t,
		request_connection_policy(head, transmute([]u8)version_10, true, true, false),
		Connection_Policy.Close,
	)

	head.connection.keep_alive = true
	testing.expect_value(
		t,
		request_connection_policy(head, transmute([]u8)version_10, true, true, false),
		Connection_Policy.Keep_Alive,
	)
	head.connection.close = true
	testing.expect_value(
		t,
		request_connection_policy(head, transmute([]u8)version_11, true, true, false),
		Connection_Policy.Close,
	)
	head.connection.close = false
	testing.expect_value(
		t,
		request_connection_policy(head, transmute([]u8)version_11, false, true, false),
		Connection_Policy.Close,
	)
	testing.expect_value(
		t,
		request_connection_policy(head, transmute([]u8)version_11, true, false, false),
		Connection_Policy.Close,
	)
	testing.expect_value(
		t,
		request_connection_policy(head, transmute([]u8)version_11, true, true, true),
		Connection_Policy.Close,
	)
}

@(test)
test_response_connection_header_follows_version_and_tunnel_status :: proc(t: ^testing.T) {
	get_method := "GET"
	connect_method := "CONNECT"
	testing.expect_value(
		t,
		response_connection_header(transmute([]u8)get_method, 200, Connection_Policy.Close, true),
		Response_Connection_Header.Close,
	)
	testing.expect_value(
		t,
		response_connection_header(transmute([]u8)get_method, 200, .Keep_Alive, true),
		Response_Connection_Header.Omit,
	)
	testing.expect_value(
		t,
		response_connection_header(transmute([]u8)get_method, 200, .Keep_Alive, false),
		Response_Connection_Header.Keep_Alive,
	)
	testing.expect_value(
		t,
		response_connection_header(transmute([]u8)get_method, 100, .Close, true),
		Response_Connection_Header.Omit,
	)
	testing.expect_value(
		t,
		response_connection_header(transmute([]u8)connect_method, 200, .Close, true),
		Response_Connection_Header.Omit,
	)
}
