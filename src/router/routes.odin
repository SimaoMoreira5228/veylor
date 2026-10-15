package router

import http1 "veylor:http1"
import response "veylor:http1/response"

Static_Response :: struct {
	status:  u16,
	reason:  []u8,
	headers: []response.Response_Header,
	body:    []u8,
}

// NOTE: Route keys, response slices, and index storage must remain alive and unchanged until discard.
Static_Route :: struct {
	method:                []u8,
	path:                  []u8,
	response:              Static_Response,
	route_id:              u64,
	dynamic_segment_count: int,
}

MAX_ROUTE_HASH_PROBES :: 8
MAX_ROUTE_INDEX :: u64(0xffff_ffff)

Route_Index_Slot :: u32

ROOT_PATH_TEXT :: string("/")
MAX_ROUTE_CAPTURE_COUNT :: 8

Route_Capture :: struct {
	name:  http1.Span,
	value: http1.Span,
}

Route_Table_State :: enum {
	Registering,
	Frozen,
	Invalid,
}

Route_Table_Result :: enum {
	Added,
	Found,
	Not_Found,
	Not_Frozen,
	Duplicate,
	Capture_Capacity,
	Index_Capacity,
	Full,
	Frozen,
	Invalid,
}

Route_Table :: struct {
	routes:      []Static_Route,
	index_slots: []Route_Index_Slot,
	count:       int,
	exact_count: int,
	state:       Route_Table_State,
}

init_route_table :: proc(storage: []Static_Route, index_slots: []Route_Index_Slot) -> Route_Table {
	for i := 0; i < len(storage); i += 1 {
		storage[i] = Static_Route{}
	}

	for i := 0; i < len(index_slots); i += 1 {
		index_slots[i] = 0
	}
	return Route_Table{routes = storage, index_slots = index_slots}
}

register_static_route :: proc(
	table: ^Route_Table,
	method: []u8,
	path: []u8,
	static_response: Static_Response,
) -> Route_Table_Result {
	if table == nil {
		return .Invalid
	}

	if table.state == .Frozen {
		return .Frozen
	}

	if table.state != .Registering {
		return .Invalid
	}

	if !valid_route_method(method) {
		return .Invalid
	}

	dynamic_segments, pattern_valid := inspect_route_pattern(path)

	if !pattern_valid {
		return .Invalid
	}

	if static_response.status < 200 || static_response.status > 599 {
		return .Invalid
	}

	if table.count < 0 {
		return .Invalid
	}

	if table.count > len(table.routes) {
		return .Invalid
	}

	if table.count == len(table.routes) {
		return .Full
	}

	table.routes[table.count] = Static_Route {
		method                = method,
		path                  = path,
		response              = static_response,
		dynamic_segment_count = dynamic_segments,
	}

	table.count += 1
	return .Added
}

freeze_route_table :: proc(table: ^Route_Table) -> Route_Table_Result {
	if table == nil {
		return .Invalid
	}

	if table.count < 0 {
		return .Invalid
	}

	if table.count > len(table.routes) {
		return .Invalid
	}

	if table.state == .Frozen {
		return .Frozen
	}

	if table.state != .Registering {
		return .Invalid
	}

	exact_count := 0
	for route in table.routes[:table.count] {
		if route.dynamic_segment_count == 0 {
			exact_count += 1
		}
	}

	if exact_count > len(table.index_slots) / 2 ||
	   u64(exact_count) > MAX_ROUTE_INDEX ||
	   u64(len(table.index_slots)) > MAX_ROUTE_INDEX {
		return .Index_Capacity
	}

	table.exact_count = partition_exact_routes(table.routes[:table.count])
	sort_routes(table.routes[:table.exact_count])
	sort_dynamic_routes(table.routes[table.exact_count:table.count])
	for i := 1; i < table.exact_count; i += 1 {
		if compare_route_keys(
			   table.routes[i - 1].method,
			   table.routes[i - 1].path,
			   table.routes[i].method,
			   table.routes[i].path,
		   ) ==
		   0 {
			table.state = .Invalid
			return .Duplicate
		}
	}

	for i := table.exact_count + 1; i < table.count; i += 1 {
		if compare_dynamic_route_keys(table.routes[i - 1], table.routes[i]) == 0 {
			table.state = .Invalid
			return .Duplicate
		}
	}

	for i := 0; i < table.count; i += 1 {
		table.routes[i].route_id = u64(i) + 1
	}

	for i := 0; i < len(table.index_slots); i += 1 {
		table.index_slots[i] = 0
	}

	for i := 0; i < table.exact_count; i += 1 {
		route := table.routes[i]
		slot_index := route_hash(route.method, route.path, len(table.index_slots))

		for table.index_slots[slot_index] != 0 {
			slot_index += 1

			if slot_index == len(table.index_slots) {
				slot_index = 0
			}
		}
		table.index_slots[slot_index] = u32(route.route_id)
	}

	table.state = .Frozen
	return .Frozen
}

find_static_route :: proc(
	table: ^Route_Table,
	method: []u8,
	path: []u8,
) -> (
	route: Static_Route,
	result: Route_Table_Result,
) {
	if table == nil {
		return {}, .Invalid
	}

	if table.count < 0 {
		return {}, .Invalid
	}

	if table.count > len(table.routes) {
		return {}, .Invalid
	}

	if table.exact_count < 0 || table.exact_count > table.count {
		return {}, .Invalid
	}

	if table.state == .Registering {
		return {}, .Not_Frozen
	}

	if table.state != .Frozen {
		return {}, .Invalid
	}

	if !valid_route_method(method) {
		return {}, .Not_Found
	}

	if !valid_route_path(path) {
		return {}, .Not_Found
	}

	if table.exact_count == 0 {
		return {}, .Not_Found
	}

	if table.exact_count > len(table.index_slots) / 2 {
		return {}, .Invalid
	}

	slot_index := route_hash(method, path, len(table.index_slots))
	for probe := 0; probe < MAX_ROUTE_HASH_PROBES; probe += 1 {
		route_id := table.index_slots[slot_index]

		if route_id == 0 {
			return {}, .Not_Found
		}

		if u64(route_id) > u64(table.exact_count) {
			return {}, .Invalid
		}

		candidate := table.routes[int(route_id) - 1]

		if compare_route_keys(candidate.method, candidate.path, method, path) == 0 {
			return candidate, .Found
		}
		slot_index += 1

		if slot_index == len(table.index_slots) {
			slot_index = 0
		}
	}

	low := 0
	high := table.exact_count
	for low < high {
		middle := low + (high - low) / 2
		candidate := table.routes[middle]
		comparison := compare_route_keys(candidate.method, candidate.path, method, path)

		if comparison < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}

	if low >= table.exact_count {
		return {}, .Not_Found
	}

	route = table.routes[low]

	if compare_route_keys(route.method, route.path, method, path) != 0 {
		return {}, .Not_Found
	}
	return route, .Found
}

valid_route_method :: proc(method: []u8) -> bool {
	if len(method) == 0 {
		return false
	}

	for c in method {
		if !http1.is_token(c) {
			return false
		}
	}
	return true
}

valid_route_path :: proc(path: []u8) -> bool {
	if len(path) == 0 {
		return false
	}

	if path[0] != '/' {
		return false
	}

	for c in path {
		if c == '?' {
			return false
		}
	}
	return http1.valid_origin_form(path)
}

inspect_route_pattern :: proc(path: []u8) -> (dynamic_segments: int, valid: bool) {
	if !valid_route_path(path) {
		return 0, false
	}

	segment_start := 1
	for {
		segment_end := segment_start
		for segment_end < len(path) && path[segment_end] != '/' {
			segment_end += 1
		}

		dynamic_segment :=
			segment_start < segment_end &&
			(path[segment_start] == ':' || path[segment_start] == '*')

		if dynamic_segment {
			wildcard_segment := path[segment_start] == '*'
			name_start := segment_start + 1

			if name_start == segment_end || (wildcard_segment && segment_end != len(path)) {
				return 0, false
			}

			for name_index := name_start; name_index < segment_end; name_index += 1 {
				if !http1.is_token(path[name_index]) {
					return 0, false
				}
			}

			if route_parameter_name_seen(path, name_start, segment_end) {
				return 0, false
			}
			dynamic_segments += 1

			if dynamic_segments > MAX_ROUTE_CAPTURE_COUNT {
				return 0, false
			}
		}

		if segment_end == len(path) {
			return dynamic_segments, true
		}
		segment_start = segment_end + 1
	}
}

route_parameter_name_seen :: proc(path: []u8, name_start, name_end: int) -> bool {
	segment_start := 1
	for segment_start < name_start {
		segment_end := segment_start
		for segment_end < name_start && path[segment_end] != '/' {
			segment_end += 1
		}

		if segment_start < segment_end &&
		   (path[segment_start] == ':' || path[segment_start] == '*') {
			previous_name_start := segment_start + 1

			if segment_end - previous_name_start == name_end - name_start {
				if compare_bytes(
					   path[previous_name_start:segment_end],
					   path[name_start:name_end],
				   ) ==
				   0 {
					return true
				}
			}
		}
		segment_start = segment_end + 1
	}
	return false
}

partition_exact_routes :: proc(routes: []Static_Route) -> int {
	exact_count := 0
	for i := 0; i < len(routes); i += 1 {
		if routes[i].dynamic_segment_count != 0 {
			continue
		}

		temporary := routes[exact_count]
		routes[exact_count] = routes[i]
		routes[i] = temporary
		exact_count += 1
	}
	return exact_count
}

compare_dynamic_route_keys :: proc(left, right: Static_Route) -> int {
	method_comparison := compare_bytes(left.method, right.method)

	if method_comparison != 0 {
		return method_comparison
	}

	left_index := 1
	right_index := 1
	for {
		left_end := left_index
		for left_end < len(left.path) && left.path[left_end] != '/' {
			left_end += 1
		}

		right_end := right_index
		for right_end < len(right.path) && right.path[right_end] != '/' {
			right_end += 1
		}

		left_kind := route_segment_kind(left.path, left_index, left_end)
		right_kind := route_segment_kind(right.path, right_index, right_end)

		if left_kind < right_kind {
			return -1
		}

		if left_kind > right_kind {
			return 1
		}

		if left_kind == 0 {
			comparison := compare_encoded_paths(
				left.path[left_index:left_end],
				right.path[right_index:right_end],
			)

			if comparison != 0 {
				return comparison
			}
		}

		left_finished := left_end == len(left.path)
		right_finished := right_end == len(right.path)

		if left_finished || right_finished {
			if left_finished && right_finished {
				return 0
			}

			if left_finished {
				return -1
			}
			return 1
		}
		left_index = left_end + 1
		right_index = right_end + 1
	}
}

route_segment_kind :: proc(path: []u8, start, end: int) -> int {
	if start == end {
		return 0
	}

	if path[start] == ':' {
		return 1
	}

	if path[start] == '*' {
		return 2
	}
	return 0
}

compare_route_keys :: proc(method_a, path_a, method_b, path_b: []u8) -> int {
	method_comparison := compare_bytes(method_a, method_b)

	if method_comparison != 0 {
		return method_comparison
	}
	return compare_encoded_paths(path_a, path_b)
}

Route_Path_Token :: struct {
	value:   u8,
	encoded: bool,
}

compare_encoded_paths :: proc(left, right: []u8) -> int {
	left_index := 0
	right_index := 0

	for left_index < len(left) && right_index < len(right) {
		left_token, next_left_index := route_path_token(left, left_index)
		right_token, next_right_index := route_path_token(right, right_index)
		left_index = next_left_index
		right_index = next_right_index

		if left_token.encoded != right_token.encoded {
			if left_token.encoded {
				return 1
			}
			return -1
		}

		if left_token.value < right_token.value {
			return -1
		}

		if left_token.value > right_token.value {
			return 1
		}
	}

	if left_index < len(left) {
		return 1
	}

	if right_index < len(right) {
		return -1
	}
	return 0
}

route_path_token :: proc(path: []u8, index: int) -> (token: Route_Path_Token, next_index: int) {
	if path[index] != '%' {
		return Route_Path_Token{value = path[index]}, index + 1
	}

	value := route_hex_value(path[index + 1]) * 16 + route_hex_value(path[index + 2])
	return Route_Path_Token{value = value, encoded = !http1.is_uri_unreserved(value)}, index + 3
}

route_hex_value :: proc(c: u8) -> u8 {
	if c >= '0' && c <= '9' {
		return c - '0'
	}

	if c >= 'a' && c <= 'f' {
		return c - 'a' + 10
	}
	return c - 'A' + 10
}

route_hash :: proc(method, path: []u8, slot_count: int) -> int {
	modulus := u64(slot_count)
	hash := u64(5381)

	for c in method {
		hash = (hash * 33 + u64(c)) % modulus
	}
	hash = (hash * 33) % modulus

	path_index := 0
	for path_index < len(path) {
		token, next_path_index := route_path_token(path, path_index)
		encoded := u64(0)

		if token.encoded {
			encoded = 1
		}
		hash = (hash * 33 + encoded) % modulus
		hash = (hash * 33 + u64(token.value)) % modulus
		path_index = next_path_index
	}
	return int(hash)
}

compare_bytes :: proc(left, right: []u8) -> int {
	common_length := min(len(left), len(right))
	for i := 0; i < common_length; i += 1 {
		if left[i] < right[i] {
			return -1
		}

		if left[i] > right[i] {
			return 1
		}
	}

	if len(left) < len(right) {
		return -1
	}

	if len(left) > len(right) {
		return 1
	}
	return 0
}

sort_routes :: proc(routes: []Static_Route) {
	for start := len(routes) / 2; start > 0; {
		start -= 1
		sift_route_heap(routes, start, len(routes))
	}

	for end := len(routes); end > 1; {
		end -= 1
		temporary := routes[0]
		routes[0] = routes[end]
		routes[end] = temporary
		sift_route_heap(routes, 0, end)
	}
}

sort_dynamic_routes :: proc(routes: []Static_Route) {
	for start := len(routes) / 2; start > 0; {
		start -= 1
		sift_dynamic_route_heap(routes, start, len(routes))
	}

	for end := len(routes); end > 1; {
		end -= 1
		temporary := routes[0]
		routes[0] = routes[end]
		routes[end] = temporary
		sift_dynamic_route_heap(routes, 0, end)
	}
}

sift_dynamic_route_heap :: proc(routes: []Static_Route, start, end: int) {
	root := start
	for root < end / 2 {
		child := root * 2 + 1

		if child + 1 < end && compare_dynamic_route_keys(routes[child], routes[child + 1]) < 0 {
			child += 1
		}

		if compare_dynamic_route_keys(routes[root], routes[child]) >= 0 {
			return
		}

		temporary := routes[root]
		routes[root] = routes[child]
		routes[child] = temporary
		root = child
	}
}

sift_route_heap :: proc(routes: []Static_Route, start, end: int) {
	root := start
	for root < end / 2 {
		child := root * 2 + 1

		if child + 1 < end {
			child_route := routes[child + 1]

			if compare_route_keys(
				   routes[child].method,
				   routes[child].path,
				   child_route.method,
				   child_route.path,
			   ) <
			   0 {
				child += 1
			}
		}

		if compare_route_keys(
			   routes[root].method,
			   routes[root].path,
			   routes[child].method,
			   routes[child].path,
		   ) >=
		   0 {
			return
		}

		temporary := routes[root]
		routes[root] = routes[child]
		routes[child] = temporary
		root = child
	}
}
