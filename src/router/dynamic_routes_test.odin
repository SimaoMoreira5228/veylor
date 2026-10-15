#+test
package router

import "core:testing"
import http1 "veylor:http1"
import body "veylor:http1/body"

@(test)
test_dynamic_routes_capture_spans_and_prefer_literal_segments :: proc(t: ^testing.T) {
	routes: [5]Static_Route
	index_slots: [10]Route_Index_Slot
	table := init_route_table(routes[:], index_slots[:])
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("GET"),
			test_bytes("/:section/edit"),
			Static_Response {
				status = 200,
				body   = test_bytes("parameter first"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("GET"),
			test_bytes("/users/:id/edit"),
			Static_Response {
				status = 200,
				body   = test_bytes("literal first"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("GET"),
			test_bytes("/users/:id"),
			Static_Response {
				status = 200,
				reason = test_bytes("OK"),
				body   = test_bytes("user"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("GET"),
			test_bytes("/users/edit"),
			Static_Response {
				status = 200,
				body   = test_bytes("exact"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("POST"),
			test_bytes("/users/:id"),
			Static_Response {
				status = 200,
				body   = test_bytes("posted user"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Frozen)

	request_raw := "GET /users/42 HTTP/1.1\r\nHost: example.com\r\n\r\n"
	input := test_bytes(request_raw)
	headers: [1]http1.Header
	parser: http1.Parser
	head, parse_result := http1.parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, parse_result, http1.Parse_Result.Ready)
	capture_storage: [2]Route_Capture
	route, route_id, captures, result := find_static_request_route_with_captures(
		&table,
		head,
		input,
		capture_storage[:],
	)
	testing.expect_value(t, result, Route_Table_Result.Found)
	testing.expect_value(t, string(route.response.body), "user")
	testing.expect_value(t, route_id, route.route_id)
	testing.expect_value(t, len(captures), 1)
	testing.expect_value(t, route_id > 0, true)
	testing.expect_value(
		t,
		string(
			route.path[captures[0].name.offset:captures[0].name.offset + captures[0].name.length],
		),
		"id",
	)
	testing.expect_value(
		t,
		string(
			input[captures[0].value.offset:captures[0].value.offset + captures[0].value.length],
		),
		"42",
	)
	no_capture_storage: []Route_Capture
	capacity_route, capacity_route_id, _, capture_capacity_result :=
		find_static_request_route_with_captures(
			&table,
			head,
			input,
			no_capture_storage,
		)
	testing.expect_value(t, capture_capacity_result, Route_Table_Result.Capture_Capacity)
	testing.expect_value(t, string(capacity_route.response.body), "user")
	testing.expect_value(t, capacity_route_id, route_id)
	wrapper_route, wrapper_result := find_static_request_route(&table, head, input)
	testing.expect_value(t, wrapper_result, Route_Table_Result.Found)
	testing.expect_value(t, string(wrapper_route.response.body), "user")

	absolute_raw := "GET http://example.com/users/42 HTTP/1.1\r\nHost: example.com\r\n\r\n"
	absolute_input := test_bytes(absolute_raw)
	absolute_headers: [1]http1.Header
	absolute_parser: http1.Parser
	absolute_head, absolute_result := http1.parse_request_head(
		&absolute_parser,
		absolute_input,
		absolute_headers[:],
	)
	testing.expect_value(t, absolute_result, http1.Parse_Result.Ready)
	_, _, absolute_captures, absolute_match_result := find_static_request_route_with_captures(
		&table,
		absolute_head,
		absolute_input,
		capture_storage[:],
	)
	testing.expect_value(t, absolute_match_result, Route_Table_Result.Found)
	testing.expect_value(
		t,
		string(
			absolute_input[absolute_captures[0].value.offset:absolute_captures[0].value.offset +
			absolute_captures[0].value.length],
		),
		"42",
	)

	encoded_path_raw := "GET /users/a%2Fb HTTP/1.1\r\nHost: example.com\r\n\r\n"
	encoded_path_input := test_bytes(encoded_path_raw)
	encoded_path_headers: [1]http1.Header
	encoded_path_parser: http1.Parser
	encoded_path_head, encoded_path_parse_result := http1.parse_request_head(
		&encoded_path_parser,
		encoded_path_input,
		encoded_path_headers[:],
	)
	testing.expect_value(t, encoded_path_parse_result, http1.Parse_Result.Ready)
	_, _, encoded_path_captures, encoded_path_match_result :=
		find_static_request_route_with_captures(
			&table,
			encoded_path_head,
			encoded_path_input,
			capture_storage[:],
		)
	testing.expect_value(t, encoded_path_match_result, Route_Table_Result.Found)
	testing.expect_value(
		t,
		string(
			encoded_path_input[encoded_path_captures[0].value.offset:encoded_path_captures[0].value.offset +
			encoded_path_captures[0].value.length],
		),
		"a%2Fb",
	)

	head_raw := "HEAD /users/42 HTTP/1.1\r\nHost: example.com\r\n\r\n"
	head_input := test_bytes(head_raw)
	head_headers: [1]http1.Header
	head_parser: http1.Parser
	head_request, head_parse_result := http1.parse_request_head(
		&head_parser,
		head_input,
		head_headers[:],
	)
	testing.expect_value(t, head_parse_result, http1.Parse_Result.Ready)
	head_route, _, _, head_match_result := find_static_request_route_with_captures(
		&table,
		head_request,
		head_input,
		capture_storage[:],
	)
	testing.expect_value(t, head_match_result, Route_Table_Result.Found)
	testing.expect_value(t, string(head_route.response.body), "user")
	output: [128]u8
	response_event := encode_static_route_response(
		&table,
		head_request,
		head_input,
		output[:],
		body.DEFAULT_FIXED_BODY_LIMIT,
	)
	testing.expect_value(t, response_event.result, Static_Response_Result.Written)
	testing.expect_value(
		t,
		string(output[:response_event.written]),
		"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n",
	)

	precedence_raw := "GET /users/42/edit HTTP/1.1\r\nHost: example.com\r\n\r\n"
	precedence_input := test_bytes(precedence_raw)
	precedence_headers: [1]http1.Header
	precedence_parser: http1.Parser
	precedence_head, precedence_parse_result := http1.parse_request_head(
		&precedence_parser,
		precedence_input,
		precedence_headers[:],
	)
	testing.expect_value(t, precedence_parse_result, http1.Parse_Result.Ready)
	precedence_route, _, _, precedence_result := find_static_request_route_with_captures(
		&table,
		precedence_head,
		precedence_input,
		capture_storage[:],
	)
	testing.expect_value(t, precedence_result, Route_Table_Result.Found)
	testing.expect_value(t, string(precedence_route.response.body), "literal first")

	exact_raw := "GET /users/edit HTTP/1.1\r\nHost: example.com\r\n\r\n"
	exact_input := test_bytes(exact_raw)
	exact_headers: [1]http1.Header
	exact_parser: http1.Parser
	exact_head, exact_parse_result := http1.parse_request_head(
		&exact_parser,
		exact_input,
		exact_headers[:],
	)
	testing.expect_value(t, exact_parse_result, http1.Parse_Result.Ready)
	exact_route, exact_route_id, _, exact_result := find_static_request_route_with_captures(
		&table,
		exact_head,
		exact_input,
		capture_storage[:],
	)
	testing.expect_value(t, exact_result, Route_Table_Result.Found)
	testing.expect_value(t, string(exact_route.response.body), "exact")
	testing.expect_value(t, exact_route_id != route_id, true)

	post_raw := "POST /users/42 HTTP/1.1\r\nHost: example.com\r\n\r\n"
	post_input := test_bytes(post_raw)
	post_headers: [1]http1.Header
	post_parser: http1.Parser
	post_head, post_parse_result := http1.parse_request_head(
		&post_parser,
		post_input,
		post_headers[:],
	)
	testing.expect_value(t, post_parse_result, http1.Parse_Result.Ready)
	post_route, _, _, post_result := find_static_request_route_with_captures(
		&table,
		post_head,
		post_input,
		capture_storage[:],
	)
	testing.expect_value(t, post_result, Route_Table_Result.Found)
	testing.expect_value(t, string(post_route.response.body), "posted user")
}

@(test)
test_dynamic_route_patterns_reject_duplicate_names_and_ambiguous_shapes :: proc(t: ^testing.T) {
	name_routes: [1]Static_Route
	name_index_slots: [2]Route_Index_Slot
	name_table := init_route_table(name_routes[:], name_index_slots[:])
	testing.expect_value(
		t,
		register_static_route(
			&name_table,
			test_bytes("GET"),
			test_bytes("/:id/:id"),
			Static_Response {
				status = 204,
			},
		),
		Route_Table_Result.Invalid,
	)

	capacity_routes: [1]Static_Route
	capacity_index_slots: [2]Route_Index_Slot
	capacity_table := init_route_table(capacity_routes[:], capacity_index_slots[:])
	testing.expect_value(
		t,
		register_static_route(
			&capacity_table,
			test_bytes("GET"),
			test_bytes("/:a/:b/:c/:d/:e/:f/:g/:h/:i"),
			Static_Response {
				status = 204,
			},
		),
		Route_Table_Result.Invalid,
	)

	wildcard_routes: [1]Static_Route
	wildcard_index_slots: [2]Route_Index_Slot
	wildcard_table := init_route_table(wildcard_routes[:], wildcard_index_slots[:])
	testing.expect_value(
		t,
		register_static_route(
			&wildcard_table,
			test_bytes("GET"),
			test_bytes("/files/*rest/more"),
			Static_Response {
				status = 204,
			},
		),
		Route_Table_Result.Invalid,
	)

	duplicate_routes: [2]Static_Route
	duplicate_index_slots: [4]Route_Index_Slot
	duplicate_table := init_route_table(duplicate_routes[:], duplicate_index_slots[:])
	response := Static_Response {
		status = 204,
	}
	testing.expect_value(
		t,
		register_static_route(
			&duplicate_table,
			test_bytes("GET"),
			test_bytes("/users/:id"),
			response,
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&duplicate_table,
			test_bytes("GET"),
			test_bytes("/users/:name"),
			response,
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&duplicate_table), Route_Table_Result.Duplicate)

	wildcard_duplicates: [2]Static_Route
	wildcard_duplicate_slots: [4]Route_Index_Slot
	wildcard_duplicate_table := init_route_table(
		wildcard_duplicates[:],
		wildcard_duplicate_slots[:],
	)
	wildcard_response := Static_Response {
		status = 204,
	}
	testing.expect_value(
		t,
		register_static_route(
			&wildcard_duplicate_table,
			test_bytes("GET"),
			test_bytes("/files/*rest"),
			wildcard_response,
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&wildcard_duplicate_table,
			test_bytes("GET"),
			test_bytes("/files/*tail"),
			wildcard_response,
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		freeze_route_table(&wildcard_duplicate_table),
		Route_Table_Result.Duplicate,
	)
}

@(test)
test_wildcard_routes_capture_the_remaining_path_after_parameters :: proc(t: ^testing.T) {
	routes: [3]Static_Route
	index_slots: [6]Route_Index_Slot
	table := init_route_table(routes[:], index_slots[:])
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("GET"),
			test_bytes("/*rest"),
			Static_Response {
				status = 200,
				reason = test_bytes("OK"),
				body = test_bytes("fallback"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("GET"),
			test_bytes("/files/:name"),
			Static_Response {
				status = 200,
				reason = test_bytes("OK"),
				body   = test_bytes("file"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("GET"),
			test_bytes("/files/readme"),
			Static_Response {
				status = 200,
				reason = test_bytes("OK"),
				body   = test_bytes("exact"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Frozen)
	captures: [2]Route_Capture

	wildcard_raw := "GET /files/a/b HTTP/1.1\r\nHost: example.com\r\n\r\n"
	wildcard_input := test_bytes(wildcard_raw)
	wildcard_headers: [1]http1.Header
	wildcard_parser: http1.Parser
	wildcard_head, wildcard_parse_result := http1.parse_request_head(
		&wildcard_parser,
		wildcard_input,
		wildcard_headers[:],
	)
	testing.expect_value(t, wildcard_parse_result, http1.Parse_Result.Ready)
	wildcard_route, _, wildcard_captures, wildcard_result :=
		find_static_request_route_with_captures(
			&table,
			wildcard_head,
			wildcard_input,
			captures[:],
		)
	testing.expect_value(t, wildcard_result, Route_Table_Result.Found)
	testing.expect_value(t, string(wildcard_route.response.body), "fallback")
	testing.expect_value(t, len(wildcard_captures), 1)
	testing.expect_value(
		t,
		string(
			wildcard_input[wildcard_captures[0].value.offset:wildcard_captures[0].value.offset +
			wildcard_captures[0].value.length],
		),
		"files/a/b",
	)

	parameter_raw := "GET /files/data HTTP/1.1\r\nHost: example.com\r\n\r\n"
	parameter_input := test_bytes(parameter_raw)
	parameter_headers: [1]http1.Header
	parameter_parser: http1.Parser
	parameter_head, parameter_parse_result := http1.parse_request_head(
		&parameter_parser,
		parameter_input,
		parameter_headers[:],
	)
	testing.expect_value(t, parameter_parse_result, http1.Parse_Result.Ready)
	parameter_route, _, _, parameter_result := find_static_request_route_with_captures(
		&table,
		parameter_head,
		parameter_input,
		captures[:],
	)
	testing.expect_value(t, parameter_result, Route_Table_Result.Found)
	testing.expect_value(t, string(parameter_route.response.body), "file")

	exact_raw := "GET /files/readme HTTP/1.1\r\nHost: example.com\r\n\r\n"
	exact_input := test_bytes(exact_raw)
	exact_headers: [1]http1.Header
	exact_parser: http1.Parser
	exact_head, exact_parse_result := http1.parse_request_head(
		&exact_parser,
		exact_input,
		exact_headers[:],
	)
	testing.expect_value(t, exact_parse_result, http1.Parse_Result.Ready)
	exact_route, _, _, exact_result := find_static_request_route_with_captures(
		&table,
		exact_head,
		exact_input,
		captures[:],
	)
	testing.expect_value(t, exact_result, Route_Table_Result.Found)
	testing.expect_value(t, string(exact_route.response.body), "exact")

	root_raw := "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
	root_input := test_bytes(root_raw)
	root_headers: [1]http1.Header
	root_parser: http1.Parser
	root_head, root_parse_result := http1.parse_request_head(
		&root_parser,
		root_input,
		root_headers[:],
	)
	testing.expect_value(t, root_parse_result, http1.Parse_Result.Ready)
	root_route, _, root_captures, root_result := find_static_request_route_with_captures(
		&table,
		root_head,
		root_input,
		captures[:],
	)
	testing.expect_value(t, root_result, Route_Table_Result.Found)
	testing.expect_value(t, string(root_route.response.body), "fallback")
	testing.expect_value(t, root_captures[0].value.length, 0)

	absolute_root_raw := "GET http://example.com HTTP/1.1\r\nHost: example.com\r\n\r\n"
	absolute_root_input := test_bytes(absolute_root_raw)
	absolute_root_headers: [1]http1.Header
	absolute_root_parser: http1.Parser
	absolute_root_head, absolute_root_parse_result := http1.parse_request_head(
		&absolute_root_parser,
		absolute_root_input,
		absolute_root_headers[:],
	)
	testing.expect_value(t, absolute_root_parse_result, http1.Parse_Result.Ready)
	_, _, absolute_root_captures, absolute_root_result := find_static_request_route_with_captures(
		&table,
		absolute_root_head,
		absolute_root_input,
		captures[:],
	)
	testing.expect_value(t, absolute_root_result, Route_Table_Result.Found)
	testing.expect_value(
		t,
		absolute_root_captures[0].value.offset,
		absolute_root_head.target.offset + absolute_root_head.target.length,
	)
	testing.expect_value(t, absolute_root_captures[0].value.length, 0)
}
