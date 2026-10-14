#+test
package router

import "core:testing"
import http1 "veylor:http1"
import body "veylor:http1/body"

test_bytes :: proc(value: string) -> []u8 {
	return transmute([]u8)value
}

write_test_route_path :: proc(path: []u8, value: int) {
	path[0] = '/'
	path[1] = 'r'
	path[2] = u8('0' + value / 1000)
	path[3] = u8('0' + value / 100 % 10)
	path[4] = u8('0' + value / 10 % 10)
	path[5] = u8('0' + value % 10)
}

@(test)
test_static_routes_freeze_and_match_method_path_and_queryless_path :: proc(t: ^testing.T) {
	routes: [3]Static_Route
	table := init_route_table(routes[:])
	response := Static_Response {
		status = 200,
		reason = test_bytes("OK"),
		body   = test_bytes("health"),
	}
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET"), test_bytes("/health"), response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET"), test_bytes("/items"), response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("POST"), test_bytes("/items"), response),
		Route_Table_Result.Added,
	)
	_, result := find_static_route(&table, test_bytes("GET"), test_bytes("/health"))
	testing.expect_value(t, result, Route_Table_Result.Not_Frozen)
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Frozen)

	route: Static_Route
	route, result = find_static_route(&table, test_bytes("GET"), test_bytes("/health"))
	testing.expect_value(t, result, Route_Table_Result.Found)
	testing.expect_value(t, route.response.status, u16(200))
	testing.expect_value(t, string(route.response.body), "health")
	route, result = find_static_route(&table, test_bytes("POST"), test_bytes("/items"))
	testing.expect_value(t, result, Route_Table_Result.Found)
	_, result = find_static_route(&table, test_bytes("HEAD"), test_bytes("/health"))
	testing.expect_value(t, result, Route_Table_Result.Not_Found)
	_, result = find_static_route(&table, test_bytes("GET"), test_bytes("/health?full=1"))
	testing.expect_value(t, result, Route_Table_Result.Not_Found)
	_, result = find_static_route(&table, test_bytes("GET"), test_bytes("/missing"))
	testing.expect_value(t, result, Route_Table_Result.Not_Found)
}

@(test)
test_static_routes_reject_duplicate_keys_when_frozen :: proc(t: ^testing.T) {
	routes: [2]Static_Route
	table := init_route_table(routes[:])
	response := Static_Response {
		status = 204,
	}
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET"), test_bytes("/same"), response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET"), test_bytes("/same"), response),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Duplicate)
	testing.expect_value(t, table.state, Route_Table_State.Invalid)
	_, result := find_static_route(&table, test_bytes("GET"), test_bytes("/same"))
	testing.expect_value(t, result, Route_Table_Result.Invalid)
}

@(test)
test_static_route_storage_and_path_inputs_are_bounded :: proc(t: ^testing.T) {
	routes: [1]Static_Route
	table := init_route_table(routes[:])
	response := Static_Response {
		status = 200,
	}
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET"), test_bytes("/ok"), response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET"), test_bytes("/full"), response),
		Route_Table_Result.Full,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET"), test_bytes("/bad?query"), response),
		Route_Table_Result.Invalid,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET "), test_bytes("/bad"), response),
		Route_Table_Result.Invalid,
	)
}

@(test)
test_static_route_index_matches_large_tables :: proc(t: ^testing.T) {
	route_storage := make([]Static_Route, 10_000)
	defer delete(route_storage)
	path_storage: [10_000][6]u8
	method := test_bytes("GET")
	static_response := Static_Response {
		status = 200,
		body   = test_bytes("ok"),
	}
	route_counts := [?]int{8, 100, 10_000}
	for route_count in route_counts {
		table := init_route_table(route_storage[:route_count])
		for i := 0; i < route_count; i += 1 {
			value := route_count - i - 1
			write_test_route_path(path_storage[value][:], value)
			result := register_static_route(
				&table,
				method,
				path_storage[value][:],
				static_response,
			)
			testing.expect_value(t, result, Route_Table_Result.Added)
		}
		testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Frozen)

		for value := 0; value < route_count; value += 1 {
			path: [6]u8
			write_test_route_path(path[:], value)
			_, result := find_static_route(&table, method, path[:])
			testing.expect_value(t, result, Route_Table_Result.Found)
		}
	}
}

@(test)
test_static_request_routes_use_parsed_origin_and_absolute_paths :: proc(t: ^testing.T) {
	routes: [5]Static_Route
	table := init_route_table(routes[:])
	static_response := Static_Response {
		status = 200,
		reason = test_bytes("OK"),
		body   = test_bytes("ok"),
	}
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET"), test_bytes("/items"), static_response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET"), test_bytes("/"), static_response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("OPTIONS"), test_bytes("/"), static_response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("HEAD"), test_bytes("/items"), static_response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("POST"), test_bytes("/items"), static_response),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Frozen)
	body_limit := body.DEFAULT_FIXED_BODY_LIMIT

	origin_raw := "GET /items?mode=full HTTP/1.1\r\nHost: example.com\r\n\r\n"
	origin_input := test_bytes(origin_raw)
	origin_headers: [1]http1.Header
	origin_parser: http1.Parser
	origin_head, parse_result := http1.parse_request_head(
		&origin_parser,
		origin_input,
		origin_headers[:],
	)
	testing.expect_value(t, parse_result, http1.Parse_Result.Ready)
	route, result := find_static_request_route(&table, origin_head, origin_input)
	testing.expect_value(t, result, Route_Table_Result.Found)
	testing.expect_value(t, string(route.response.body), "ok")
	output: [128]u8
	response_event := encode_static_route_response(
		&table,
		origin_head,
		origin_input,
		output[:],
		body_limit,
	)
	testing.expect_value(t, response_event.result, Static_Response_Result.Written)
	testing.expect_value(t, response_event.keep_alive, true)
	testing.expect_value(
		t,
		string(output[:response_event.written]),
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
	)

	absolute_raw :=
		"GET http://user:pass@target.example/items?mode=full HTTP/1.1\r\n" +
		"Host: ignored.example\r\n\r\n"
	absolute_input := test_bytes(absolute_raw)
	absolute_headers: [1]http1.Header
	absolute_parser: http1.Parser
	absolute_head, absolute_parse_result := http1.parse_request_head(
		&absolute_parser,
		absolute_input,
		absolute_headers[:],
	)
	testing.expect_value(t, absolute_parse_result, http1.Parse_Result.Ready)
	route, result = find_static_request_route(&table, absolute_head, absolute_input)
	testing.expect_value(t, result, Route_Table_Result.Found)
	testing.expect_value(t, string(route.response.body), "ok")
	response_event = encode_static_route_response(
		&table,
		absolute_head,
		absolute_input,
		output[:],
		body_limit,
	)
	testing.expect_value(t, response_event.result, Static_Response_Result.Written)
	testing.expect_value(t, response_event.keep_alive, true)

	root_raw := "GET http://target.example?mode=full HTTP/1.1\r\nHost: ignored.example\r\n\r\n"
	root_input := test_bytes(root_raw)
	root_headers: [1]http1.Header
	root_parser: http1.Parser
	root_head, root_parse_result := http1.parse_request_head(
		&root_parser,
		root_input,
		root_headers[:],
	)
	testing.expect_value(t, root_parse_result, http1.Parse_Result.Ready)
	_, result = find_static_request_route(&table, root_head, root_input)
	testing.expect_value(t, result, Route_Table_Result.Found)

	options_raw :=
		"OPTIONS http://target.example?mode=full HTTP/1.1\r\n" + "Host: ignored.example\r\n\r\n"
	options_input := test_bytes(options_raw)
	options_headers: [1]http1.Header
	options_parser: http1.Parser
	options_head, options_parse_result := http1.parse_request_head(
		&options_parser,
		options_input,
		options_headers[:],
	)
	testing.expect_value(t, options_parse_result, http1.Parse_Result.Ready)
	_, result = find_static_request_route(&table, options_head, options_input)
	testing.expect_value(t, result, Route_Table_Result.Not_Found)

	head_raw := "HEAD /items HTTP/1.1\r\nHost: example.com\r\n\r\n"
	head_input := test_bytes(head_raw)
	head_headers: [1]http1.Header
	head_parser: http1.Parser
	head_request, head_parse_result := http1.parse_request_head(
		&head_parser,
		head_input,
		head_headers[:],
	)
	testing.expect_value(t, head_parse_result, http1.Parse_Result.Ready)
	response_event = encode_static_route_response(
		&table,
		head_request,
		head_input,
		output[:],
		body_limit,
	)
	testing.expect_value(t, response_event.result, Static_Response_Result.Written)
	testing.expect_value(t, response_event.keep_alive, true)
	testing.expect_value(
		t,
		string(output[:response_event.written]),
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n",
	)

	post_raw := "POST /items HTTP/1.1\r\nHost: example.com\r\nContent-Length: 4\r\n\r\n"
	post_input := test_bytes(post_raw)
	post_headers: [2]http1.Header
	post_parser: http1.Parser
	post_request, post_parse_result := http1.parse_request_head(
		&post_parser,
		post_input,
		post_headers[:],
	)
	testing.expect_value(t, post_parse_result, http1.Parse_Result.Ready)
	response_event = encode_static_route_response(
		&table,
		post_request,
		post_input,
		output[:],
		body_limit,
	)
	testing.expect_value(t, response_event.result, Static_Response_Result.Written)
	testing.expect_value(t, response_event.keep_alive, false)
	testing.expect_value(
		t,
		string(output[:response_event.written]),
		"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok",
	)

	expect_raw :=
		"POST /items HTTP/1.1\r\nHost: example.com\r\n" +
		"Content-Length: 4\r\nExpect: 100-continue\r\n\r\n"
	expect_input := test_bytes(expect_raw)
	expect_headers: [3]http1.Header
	expect_parser: http1.Parser
	expect_request, expect_parse_result := http1.parse_request_head(
		&expect_parser,
		expect_input,
		expect_headers[:],
	)
	testing.expect_value(t, expect_parse_result, http1.Parse_Result.Ready)
	response_event = encode_static_route_response(
		&table,
		expect_request,
		expect_input,
		output[:],
		4,
	)
	testing.expect_value(t, response_event.result, Static_Response_Result.Written)
	testing.expect_value(t, response_event.keep_alive, false)
	testing.expect_value(
		t,
		string(output[:response_event.written]),
		"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok",
	)

	oversized_raw :=
		"POST /items HTTP/1.1\r\nHost: example.com\r\n" +
		"Content-Length: 5\r\nExpect: 100-continue\r\n\r\n"
	oversized_input := test_bytes(oversized_raw)
	oversized_headers: [3]http1.Header
	oversized_parser: http1.Parser
	oversized_request, oversized_parse_result := http1.parse_request_head(
		&oversized_parser,
		oversized_input,
		oversized_headers[:],
	)
	testing.expect_value(t, oversized_parse_result, http1.Parse_Result.Ready)
	response_event = encode_static_route_response(
		&table,
		oversized_request,
		oversized_input,
		output[:],
		4,
	)
	testing.expect_value(t, response_event.result, Static_Response_Result.Written)
	testing.expect_value(t, response_event.keep_alive, false)
	testing.expect_value(
		t,
		string(output[:response_event.written]),
		"HTTP/1.1 413 Content Too Large\r\nConnection: close\r\n" +
		"Content-Length: 0\r\n\r\n",
	)

	pipelined_raw :=
		"GET /items HTTP/1.1\r\nHost: example.com\r\n\r\n" +
		"GET /items HTTP/1.1\r\nHost: example.com\r\n\r\n"
	pipelined_input := test_bytes(pipelined_raw)
	pipelined_headers: [1]http1.Header
	pipelined_parser: http1.Parser
	pipelined_request, pipelined_parse_result := http1.parse_request_head(
		&pipelined_parser,
		pipelined_input,
		pipelined_headers[:],
	)
	testing.expect_value(t, pipelined_parse_result, http1.Parse_Result.Ready)
	response_event = encode_static_route_response(
		&table,
		pipelined_request,
		pipelined_input,
		output[:],
		body_limit,
	)
	testing.expect_value(t, response_event.result, Static_Response_Result.Written)
	testing.expect_value(t, response_event.keep_alive, false)
	testing.expect_value(
		t,
		string(output[:response_event.written]),
		"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok",
	)
}
