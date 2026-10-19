package router

import http1 "veylor:http1"
import response "veylor:http1/response"

Static_Response :: struct {
	status:  u16,
	reason:  []u8,
	headers: []response.Response_Header,
	body:    []u8,
}

Route_Kind :: enum {
	Static,
	Managed,
}

// NOTE: Route keys, response slices, and index storage must remain alive and unchanged until discard.
Static_Route :: struct {
	method:                []u8,
	path:                  []u8,
	response:              Static_Response,
	route_id:              u64,
	dynamic_segment_count: int,
	kind:                  Route_Kind,
	managed_trailers:      [MAX_MANAGED_TRAILER_NAMES][]u8,
	managed_trailer_count: int,
}

MAX_ROUTE_HASH_PROBES :: 8
MAX_ROUTE_INDEX :: u64(0xffff_ffff)
MAX_MANAGED_TRAILER_NAMES :: 32
MAX_MANAGED_TRAILER_NAME_BYTES :: 128

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
		kind                  = .Static,
	}

	table.count += 1
	return .Added
}

register_managed_route :: proc(
	table: ^Route_Table,
	method: []u8,
	path: []u8,
) -> Route_Table_Result {
	return register_managed_route_with_trailers(table, method, path, nil)
}

register_managed_route_with_trailers :: proc(
	table: ^Route_Table,
	method: []u8,
	path: []u8,
	trailers: [][]u8,
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

	if len(trailers) > MAX_MANAGED_TRAILER_NAMES {
		return .Invalid
	}

	for name in trailers {
		if !valid_trailer_name(name) {
			return .Invalid
		}
	}

	if !valid_route_method(method) {
		return .Invalid
	}

	dynamic_segments, pattern_valid := inspect_route_pattern(path)

	if !pattern_valid {
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

	route := Static_Route {
		method                = method,
		path                  = path,
		dynamic_segment_count = dynamic_segments,
		kind                  = .Managed,
	}
	for i := 0; i < len(trailers); i += 1 {
		route.managed_trailers[i] = trailers[i]
	}
	route.managed_trailer_count = len(trailers)

	table.routes[table.count] = route

	table.count += 1
	return .Added
}

valid_trailer_name :: proc(name: []u8) -> bool {
	if len(name) == 0 || len(name) > MAX_MANAGED_TRAILER_NAME_BYTES {
		return false
	}
	for c in name {
		if !http1.is_token(c) {
			return false
		}
	}
	return true
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
