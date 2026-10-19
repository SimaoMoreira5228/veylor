package router

import http1 "veylor:http1"

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
