#+test
package router

import "core:mem"
import "core:testing"
import http1 "veylor:http1"
import body "veylor:http1/body"

test_bytes :: proc(value: string) -> []u8 {
	return transmute([]u8)value
}

static_route_allocation_client :: proc(t: ^testing.T) {
	routes: [1]Static_Route
	index_slots: [2]Route_Index_Slot
	table := init_route_table(routes[:], index_slots[:])
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
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Frozen)

	raw := "GET /health HTTP/1.1\r\nHost: example.test\r\n\r\n"
	input := test_bytes(raw)
	for _ in 0 ..< 64 {
		parser: http1.Parser
		headers: [4]http1.Header
		head, parse_result := http1.parse_request_head(&parser, input, headers[:])
		testing.expect_value(t, parse_result, http1.Parse_Result.Ready)
		output: [128]u8
		event := encode_static_route_response(
			&table,
			head,
			input,
			output[:],
			body.DEFAULT_FIXED_BODY_LIMIT,
		)
		testing.expect_value(t, event.result, Static_Response_Result.Written)
		testing.expect_value(t, event.written > 0, true)
	}
}

static_route_allocation_verifier :: proc(t: ^testing.T, tracker: ^mem.Tracking_Allocator) {
	testing.expect_value(t, tracker.total_allocation_count, i64(0))
	testing.expect_value(t, len(tracker.allocation_map), 0)
}

@(test)
test_warm_exact_static_route_path_allocates_no_heap_memory :: proc(t: ^testing.T) {
	testing.expect_leaks(t, static_route_allocation_client, static_route_allocation_verifier)
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
	index_slots: [6]Route_Index_Slot
	table := init_route_table(routes[:], index_slots[:])
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
	index_slots: [4]Route_Index_Slot
	table := init_route_table(routes[:], index_slots[:])
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
test_static_routes_normalize_unreserved_escapes_and_preserve_encoded_slashes :: proc(
	t: ^testing.T,
) {
	routes: [3]Static_Route
	index_slots: [6]Route_Index_Slot
	table := init_route_table(routes[:], index_slots[:])
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("GET"),
			test_bytes("/~user"),
			Static_Response {
				status = 200,
				body   = test_bytes("unreserved"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("GET"),
			test_bytes("/a%2Fb"),
			Static_Response {
				status = 200,
				body   = test_bytes("encoded slash"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&table,
			test_bytes("GET"),
			test_bytes("/a/b"),
			Static_Response {
				status = 200,
				body   = test_bytes("path separator"),
			},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Frozen)

	route, result := find_static_route(&table, test_bytes("GET"), test_bytes("/%7euser"))
	testing.expect_value(t, result, Route_Table_Result.Found)
	testing.expect_value(t, string(route.response.body), "unreserved")

	route, result = find_static_route(&table, test_bytes("GET"), test_bytes("/a%2fb"))
	testing.expect_value(t, result, Route_Table_Result.Found)
	testing.expect_value(t, string(route.response.body), "encoded slash")

	route, result = find_static_route(&table, test_bytes("GET"), test_bytes("/a/b"))
	testing.expect_value(t, result, Route_Table_Result.Found)
	testing.expect_value(t, string(route.response.body), "path separator")

	duplicate_routes: [2]Static_Route
	duplicate_index_slots: [4]Route_Index_Slot
	duplicate_table := init_route_table(duplicate_routes[:], duplicate_index_slots[:])
	response := Static_Response {
		status = 204,
	}
	testing.expect_value(
		t,
		register_static_route(&duplicate_table, test_bytes("GET"), test_bytes("/~user"), response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(
			&duplicate_table,
			test_bytes("GET"),
			test_bytes("/%7Euser"),
			response,
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&duplicate_table), Route_Table_Result.Duplicate)
}

@(test)
test_static_route_storage_and_path_inputs_are_bounded :: proc(t: ^testing.T) {
	routes: [1]Static_Route
	index_slots: [2]Route_Index_Slot
	table := init_route_table(routes[:], index_slots[:])
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
test_route_table_requires_index_slots_only_for_exact_routes :: proc(t: ^testing.T) {
	exact_routes: [1]Static_Route
	no_index_slots: []Route_Index_Slot
	exact_table := init_route_table(exact_routes[:], no_index_slots)
	response := Static_Response {
		status = 204,
	}
	testing.expect_value(
		t,
		register_static_route(&exact_table, test_bytes("GET"), test_bytes("/items"), response),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&exact_table), Route_Table_Result.Index_Capacity)
	testing.expect_value(t, exact_table.state, Route_Table_State.Registering)

	exact_index_slots: [2]Route_Index_Slot
	exact_table.index_slots = exact_index_slots[:]
	testing.expect_value(t, freeze_route_table(&exact_table), Route_Table_Result.Frozen)
	_, result := find_static_route(&exact_table, test_bytes("GET"), test_bytes("/items"))
	testing.expect_value(t, result, Route_Table_Result.Found)

	dynamic_routes: [1]Static_Route
	dynamic_table := init_route_table(dynamic_routes[:], no_index_slots)
	testing.expect_value(
		t,
		register_static_route(&dynamic_table, test_bytes("GET"), test_bytes("/:item"), response),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&dynamic_table), Route_Table_Result.Frozen)
}

@(test)
test_static_route_index_matches_large_tables :: proc(t: ^testing.T) {
	route_storage := make([]Static_Route, 10_000)
	defer delete(route_storage)
	index_slots := make([]Route_Index_Slot, 32_768)
	defer delete(index_slots)
	path_storage: [10_000][6]u8
	method := test_bytes("GET")
	static_response := Static_Response {
		status = 200,
		body   = test_bytes("ok"),
	}

	route_counts := [?]int{8, 100, 10_000}
	for route_count in route_counts {
		table := init_route_table(route_storage[:route_count], index_slots[:])
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
test_route_hash_collision_falls_back_to_bounded_binary_search :: proc(t: ^testing.T) {
	routes: [10]Static_Route
	index_slots: [32]Route_Index_Slot
	table := init_route_table(routes[:], index_slots[:])
	path_storage: [10][6]u8
	method := test_bytes("GET")
	response := Static_Response {
		status = 204,
	}
	candidate_paths: [32][10][6]u8
	candidate_counts: [32]int
	for value := 0; value < 10_000; value += 1 {
		candidate: [6]u8
		write_test_route_path(candidate[:], value)
		bucket := route_hash(method, candidate[:], len(index_slots))

		if candidate_counts[bucket] == len(candidate_paths[bucket]) {
			continue
		}
		copy(candidate_paths[bucket][candidate_counts[bucket]][:], candidate[:])
		candidate_counts[bucket] += 1
	}

	target_bucket := -1
	for bucket_count, bucket_index in candidate_counts {
		if bucket_count == len(path_storage) {
			target_bucket = bucket_index
			break
		}
	}
	testing.expect_value(t, target_bucket >= 0, true)

	for i := 0; i < len(path_storage); i += 1 {
		copy(path_storage[i][:], candidate_paths[target_bucket][i][:])
		result := register_static_route(&table, method, path_storage[i][:], response)
		testing.expect_value(t, result, Route_Table_Result.Added)
	}
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Frozen)

	last_route := table.routes[table.exact_count - 1]
	last_path := last_route.path
	slot_index := route_hash(method, last_path, len(index_slots))
	probes := 0
	for index_slots[slot_index] != u32(last_route.route_id) {
		probes += 1
		slot_index += 1

		if slot_index == len(index_slots) {
			slot_index = 0
		}
	}
	testing.expect_value(t, probes > MAX_ROUTE_HASH_PROBES, true)
	_, result := find_static_route(&table, method, last_path)
	testing.expect_value(t, result, Route_Table_Result.Found)
}

@(test)
test_static_request_routes_use_parsed_origin_and_absolute_paths :: proc(t: ^testing.T) {
	routes: [7]Static_Route
	index_slots: [14]Route_Index_Slot
	table := init_route_table(routes[:], index_slots[:])
	static_response := Static_Response {
		status = 200,
		reason = test_bytes("OK"),
		body   = test_bytes("ok"),
	}
	head_response := Static_Response {
		status = 200,
		reason = test_bytes("OK"),
		body   = test_bytes("head"),
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
		register_static_route(&table, test_bytes("POST"), test_bytes("/items"), static_response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("GET"), test_bytes("/explicit"), static_response),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_static_route(&table, test_bytes("HEAD"), test_bytes("/explicit"), head_response),
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

	explicit_head_raw := "HEAD /explicit HTTP/1.1\r\nHost: example.com\r\n\r\n"
	explicit_head_input := test_bytes(explicit_head_raw)
	explicit_head_headers: [1]http1.Header
	explicit_head_parser: http1.Parser
	explicit_head_request, explicit_head_parse_result := http1.parse_request_head(
		&explicit_head_parser,
		explicit_head_input,
		explicit_head_headers[:],
	)
	testing.expect_value(t, explicit_head_parse_result, http1.Parse_Result.Ready)
	response_event = encode_static_route_response(
		&table,
		explicit_head_request,
		explicit_head_input,
		output[:],
		body_limit,
	)
	testing.expect_value(t, response_event.result, Static_Response_Result.Written)
	testing.expect_value(
		t,
		string(output[:response_event.written]),
		"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n",
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
