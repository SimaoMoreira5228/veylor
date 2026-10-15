package router

import http1 "veylor:http1"

find_static_request_route :: proc(
	table: ^Route_Table,
	head: http1.Request_Head,
	input: []u8,
) -> (
	route: Static_Route,
	result: Route_Table_Result,
) {
	candidate, _, _, match_result := find_static_request_route_with_captures(
		table,
		head,
		input,
		[]Route_Capture{},
	)

	if match_result == .Capture_Capacity {
		return candidate, .Found
	}
	return candidate, match_result
}

find_static_request_route_with_captures :: proc(
	table: ^Route_Table,
	head: http1.Request_Head,
	input: []u8,
	capture_storage: []Route_Capture,
) -> (
	route: Static_Route,
	route_id: u64,
	captures: []Route_Capture,
	result: Route_Table_Result,
) {
	if head.method.offset < 0 {
		return {}, 0, nil, .Invalid
	}

	if head.method.length < 0 {
		return {}, 0, nil, .Invalid
	}

	if head.method.offset > len(input) {
		return {}, 0, nil, .Invalid
	}

	if head.method.length > len(input) - head.method.offset {
		return {}, 0, nil, .Invalid
	}

	if head.target.offset < 0 {
		return {}, 0, nil, .Invalid
	}

	if head.target.length < 0 {
		return {}, 0, nil, .Invalid
	}

	if head.target.offset > len(input) {
		return {}, 0, nil, .Invalid
	}

	if head.target.length > len(input) - head.target.offset {
		return {}, 0, nil, .Invalid
	}

	method := input[head.method.offset:head.method.offset + head.method.length]
	target := input[head.target.offset:head.target.offset + head.target.length]
	path, path_offset, path_result := request_path(head.target_form, method, target)

	if path_result != .Found {
		return {}, 0, nil, path_result
	}

	path_input_offset := -1

	if path_offset >= 0 {
		path_input_offset = head.target.offset + path_offset
	}
	candidate, route_result := find_static_route(table, method, path)

	if route_result == .Found {
		return candidate, candidate.route_id, nil, .Found
	}

	if route_result != .Not_Found {
		return {}, 0, nil, route_result
	}

	dynamic_candidate: Static_Route
	dynamic_captures: []Route_Capture
	dynamic_candidate, dynamic_captures, route_result = find_dynamic_request_route(
		table,
		method,
		path,
		path_input_offset,
		capture_storage,
	)

	if route_result == .Found {
		return dynamic_candidate, dynamic_candidate.route_id, dynamic_captures, .Found
	}

	if route_result == .Capture_Capacity {
		return dynamic_candidate, dynamic_candidate.route_id, nil, .Capture_Capacity
	}

	if route_result != .Not_Found {
		return {}, 0, nil, route_result
	}

	if !http1.bytes_equal(method, "HEAD") {
		return {}, 0, nil, .Not_Found
	}

	get_method: string = "GET"
	method = transmute([]u8)get_method
	candidate, route_result = find_static_route(table, method, path)

	if route_result == .Found {
		return candidate, candidate.route_id, nil, .Found
	}

	if route_result != .Not_Found {
		return {}, 0, nil, route_result
	}
	dynamic_candidate, dynamic_captures, route_result = find_dynamic_request_route(
		table,
		method,
		path,
		path_input_offset,
		capture_storage,
	)

	if route_result != .Found {
		if route_result == .Capture_Capacity {
			return dynamic_candidate, dynamic_candidate.route_id, nil, .Capture_Capacity
		}
		return {}, 0, nil, route_result
	}
	return dynamic_candidate, dynamic_candidate.route_id, dynamic_captures, .Found
}

find_dynamic_request_route :: proc(
	table: ^Route_Table,
	method, path: []u8,
	path_input_offset: int,
	capture_storage: []Route_Capture,
) -> (
	route: Static_Route,
	captures: []Route_Capture,
	result: Route_Table_Result,
) {
	if path_input_offset < 0 {
		return {}, nil, .Not_Found
	}

	method_start := table.exact_count
	method_end := table.count
	for method_start < method_end {
		middle := method_start + (method_end - method_start) / 2
		candidate := table.routes[middle]

		if compare_bytes(candidate.method, method) < 0 {
			method_start = middle + 1
		} else {
			method_end = middle
		}
	}

	for route_index := method_start; route_index < table.count; route_index += 1 {
		candidate := table.routes[route_index]

		if compare_bytes(candidate.method, method) != 0 {
			break
		}

		candidate_captures: [MAX_ROUTE_CAPTURE_COUNT]Route_Capture
		capture_count, matched := match_route_pattern(
			candidate.path,
			path,
			path_input_offset,
			candidate_captures[:],
		)

		if !matched {
			continue
		}

		if capture_count > len(capture_storage) {
			return candidate, nil, .Capture_Capacity
		}
		copy(capture_storage[:capture_count], candidate_captures[:capture_count])
		return candidate, capture_storage[:capture_count], .Found
	}
	return {}, nil, .Not_Found
}

match_route_pattern :: proc(
	pattern, path: []u8,
	path_input_offset: int,
	captures: []Route_Capture,
) -> (
	capture_count: int,
	matched: bool,
) {
	pattern_index := 1
	path_index := 1
	for {
		pattern_end := pattern_index
		for pattern_end < len(pattern) && pattern[pattern_end] != '/' {
			pattern_end += 1
		}

		path_end := path_index
		for path_end < len(path) && path[path_end] != '/' {
			path_end += 1
		}

		dynamic_segment :=
			pattern_index < pattern_end &&
			(pattern[pattern_index] == ':' || pattern[pattern_index] == '*')

		if dynamic_segment {
			wildcard_segment := pattern[pattern_index] == '*'

			if capture_count >= len(captures) {
				return 0, false
			}

			if !wildcard_segment && path_index == path_end {
				return 0, false
			}
			value_length := path_end - path_index

			if wildcard_segment {
				value_length = len(path) - path_index
			}

			captures[capture_count] = Route_Capture {
				name = http1.Span {
					offset = pattern_index + 1,
					length = pattern_end - pattern_index - 1,
				},
				value = http1.Span{offset = path_input_offset + path_index, length = value_length},
			}
			capture_count += 1

			if wildcard_segment {
				return capture_count, true
			}
		} else if compare_encoded_paths(
			   pattern[pattern_index:pattern_end],
			   path[path_index:path_end],
		   ) !=
		   0 {
			return 0, false
		}

		pattern_finished := pattern_end == len(pattern)
		path_finished := path_end == len(path)

		if pattern_finished || path_finished {
			return capture_count, pattern_finished && path_finished
		}
		pattern_index = pattern_end + 1
		path_index = path_end + 1
	}
}

request_path :: proc(
	target_form: http1.Request_Target_Form,
	method, target: []u8,
) -> (
	path: []u8,
	path_offset: int,
	result: Route_Table_Result,
) {
	switch target_form {
	case .Origin_Form:
		path, path_offset = path_before_query(target)
		return path, path_offset, .Found
	case .Absolute_Form:
		colon := -1
		for c, i in target {
			if c == ':' {
				colon = i
				break
			}
		}

		if colon <= 0 {
			return nil, -1, .Invalid
		}

		if len(target) - colon <= 3 {
			return nil, -1, .Invalid
		}

		if target[colon + 1] != '/' {
			return nil, -1, .Invalid
		}

		if target[colon + 2] != '/' {
			return nil, -1, .Invalid
		}
		authority_end := colon + 3
		for authority_end < len(target) &&
		    target[authority_end] != '/' &&
		    target[authority_end] != '?' {
			authority_end += 1
		}

		if authority_end == colon + 3 {
			return nil, -1, .Invalid
		}

		if authority_end == len(target) {
			if http1.bytes_equal(method, "OPTIONS") {
				return nil, -1, .Not_Found
			}
			root_path := transmute([]u8)ROOT_PATH_TEXT
			return root_path, len(target) - 1, .Found
		}

		if target[authority_end] == '?' {
			if http1.bytes_equal(method, "OPTIONS") {
				return nil, -1, .Not_Found
			}
			root_path := transmute([]u8)ROOT_PATH_TEXT
			return root_path, len(target) - 1, .Found
		}

		path, path_offset = path_before_query(target[authority_end:])
		return path, authority_end + path_offset, .Found
	case .Authority_Form, .Asterisk_Form:
		return nil, -1, .Not_Found
	}
	return nil, -1, .Invalid
}

path_before_query :: proc(target: []u8) -> (path: []u8, offset: int) {
	for c, i in target {
		if c == '?' {
			return target[:i], 0
		}
	}
	return target, 0
}
