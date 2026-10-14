package router

import http1 "veylor:http1"
import response "veylor:http1/response"

Static_Response :: struct {
	status:  u16,
	reason:  []u8,
	headers: []response.Response_Header,
	body:    []u8,
}

// NOTE: Route keys and response slices are borrowed until the table is discarded.
Static_Route :: struct {
	method:   []u8,
	path:     []u8,
	response: Static_Response,
}

ROOT_PATH_TEXT :: string("/")

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
	Full,
	Frozen,
	Invalid,
}

Route_Table :: struct {
	routes: []Static_Route,
	count:  int,
	state:  Route_Table_State,
}

init_route_table :: proc(storage: []Static_Route) -> Route_Table {
	for i := 0; i < len(storage); i += 1 {
		storage[i] = Static_Route{}
	}
	return Route_Table{routes = storage}
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

	if !valid_static_route(method, path, static_response) {
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
		method   = method,
		path     = path,
		response = static_response,
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
	sort_routes(table.routes[:table.count])
	for i := 1; i < table.count; i += 1 {
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

	low := 0
	high := table.count
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

	if low >= table.count {
		return {}, .Not_Found
	}

	route = table.routes[low]

	if compare_route_keys(route.method, route.path, method, path) != 0 {
		return {}, .Not_Found
	}
	return route, .Found
}

find_static_request_route :: proc(
	table: ^Route_Table,
	head: http1.Request_Head,
	input: []u8,
) -> (
	route: Static_Route,
	result: Route_Table_Result,
) {
	if head.method.offset < 0 {
		return {}, .Invalid
	}

	if head.method.length < 0 {
		return {}, .Invalid
	}

	if head.method.offset > len(input) {
		return {}, .Invalid
	}

	if head.method.length > len(input) - head.method.offset {
		return {}, .Invalid
	}

	if head.target.offset < 0 {
		return {}, .Invalid
	}

	if head.target.length < 0 {
		return {}, .Invalid
	}

	if head.target.offset > len(input) {
		return {}, .Invalid
	}

	if head.target.length > len(input) - head.target.offset {
		return {}, .Invalid
	}
	method := input[head.method.offset:head.method.offset + head.method.length]
	target := input[head.target.offset:head.target.offset + head.target.length]
	path, path_result := request_path(head.target_form, method, target)

	if path_result != .Found {
		return {}, path_result
	}
	return find_static_route(table, method, path)
}

request_path :: proc(
	target_form: http1.Request_Target_Form,
	method, target: []u8,
) -> (
	path: []u8,
	result: Route_Table_Result,
) {
	switch target_form {
	case .Origin_Form:
		return path_before_query(target), .Found
	case .Absolute_Form:
		colon := -1
		for c, i in target {
			if c == ':' {
				colon = i
				break
			}
		}

		if colon <= 0 {
			return nil, .Invalid
		}

		if len(target) - colon <= 3 {
			return nil, .Invalid
		}

		if target[colon + 1] != '/' {
			return nil, .Invalid
		}

		if target[colon + 2] != '/' {
			return nil, .Invalid
		}
		authority_end := colon + 3
		for authority_end < len(target) &&
		    target[authority_end] != '/' &&
		    target[authority_end] != '?' {
			authority_end += 1
		}

		if authority_end == colon + 3 {
			return nil, .Invalid
		}

		if authority_end == len(target) {
			if http1.bytes_equal(method, "OPTIONS") {
				return nil, .Not_Found
			}
			root_path := transmute([]u8)ROOT_PATH_TEXT
			return root_path, .Found
		}

		if target[authority_end] == '?' {
			if http1.bytes_equal(method, "OPTIONS") {
				return nil, .Not_Found
			}
			root_path := transmute([]u8)ROOT_PATH_TEXT
			return root_path, .Found
		}
		return path_before_query(target[authority_end:]), .Found
	case .Authority_Form, .Asterisk_Form:
		return nil, .Not_Found
	}
	return nil, .Invalid
}

path_before_query :: proc(target: []u8) -> []u8 {
	for c, i in target {
		if c == '?' {
			return target[:i]
		}
	}
	return target
}

valid_static_route :: proc(method, path: []u8, static_response: Static_Response) -> bool {
	if !valid_route_method(method) {
		return false
	}

	if !valid_route_path(path) {
		return false
	}

	if static_response.status < 200 {
		return false
	}
	return static_response.status <= 599
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

compare_route_keys :: proc(method_a, path_a, method_b, path_b: []u8) -> int {
	method_comparison := compare_bytes(method_a, method_b)

	if method_comparison != 0 {
		return method_comparison
	}
	return compare_bytes(path_a, path_b)
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
