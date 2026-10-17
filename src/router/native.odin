package router

import http1 "veylor:http1"

Native_Handler_Result :: enum i32 {
	Ok,
	Invalid_Argument,
	Capacity,
	Internal_Error,
}

Native_Request_View :: struct {
	method_data: [^]u8,
	method_len:  u32,
	path_data:   [^]u8,
	path_len:    u32,
}

Native_Response_View :: struct {
	status:      i32,
	reason_data: [^]u8,
	reason_len:  u32,
	body_data:   [^]u8,
	body_len:    u32,
}

Native_Handler :: #type proc "c" (
	user_data: rawptr,
	request: ^Native_Request_View,
	response: ^Native_Response_View,
) -> i32

Native_Route :: struct {
	method:    []u8,
	path:      []u8,
	handler:   Native_Handler,
	user_data: rawptr,
}

Native_Table :: struct {
	routes: []Native_Route,
	count:  int,
}

MAX_NATIVE_ROUTES :: 64

init_native_table :: proc(storage: []Native_Route) -> Native_Table {
	for i := 0; i < len(storage); i += 1 {
		storage[i] = Native_Route{}
	}
	return Native_Table{routes = storage}
}

register_native_route :: proc(
	table: ^Native_Table,
	method: []u8,
	path: []u8,
	handler: Native_Handler,
	user_data: rawptr,
) -> Route_Table_Result {
	if table == nil || handler == nil {
		return .Invalid
	}
	if !valid_route_method(method) || !valid_route_path(path) {
		return .Invalid
	}
	if table.count < 0 || table.count > len(table.routes) {
		return .Invalid
	}
	if table.count == len(table.routes) {
		return .Full
	}
	for i := 0; i < table.count; i += 1 {
		existing := table.routes[i]
		if compare_bytes(existing.method, method) == 0 && compare_bytes(existing.path, path) == 0 {
			return .Duplicate
		}
	}
	table.routes[table.count] = Native_Route {
		method    = method,
		path      = path,
		handler   = handler,
		user_data = user_data,
	}
	table.count += 1
	return .Added
}

find_native_request_route :: proc(
	table: ^Native_Table,
	method: []u8,
	path: []u8,
	path_input_offset: int,
) -> (
	route: Native_Route,
	result: Route_Table_Result,
) {
	if table == nil {
		return {}, .Invalid
	}
	if table.count < 0 || table.count > len(table.routes) {
		return {}, .Invalid
	}
	if !valid_route_method(method) || !valid_route_path(path) {
		return {}, .Not_Found
	}

	candidate, candidate_result := find_native_route(table, method, path)
	if candidate_result == .Found {
		return candidate, .Found
	}
	if candidate_result != .Not_Found {
		return {}, candidate_result
	}

	if path_input_offset >= 0 {
		candidate, candidate_result = find_dynamic_native_route(
			table,
			method,
			path,
			path_input_offset,
		)
		if candidate_result == .Found {
			return candidate, .Found
		}
		if candidate_result != .Not_Found {
			return {}, candidate_result
		}
	}

	if !http1.bytes_equal(method, "HEAD") {
		return {}, .Not_Found
	}
	get_method := transmute([]u8)string("GET")
	candidate, candidate_result = find_native_route(table, get_method, path)
	if candidate_result == .Found {
		return candidate, .Found
	}
	if candidate_result != .Not_Found {
		return {}, candidate_result
	}
	if path_input_offset >= 0 {
		candidate, candidate_result = find_dynamic_native_route(
			table,
			get_method,
			path,
			path_input_offset,
		)
		if candidate_result == .Found {
			return candidate, .Found
		}
		if candidate_result != .Not_Found {
			return {}, candidate_result
		}
	}
	return {}, .Not_Found
}

find_dynamic_native_route :: proc(
	table: ^Native_Table,
	method: []u8,
	path: []u8,
	path_input_offset: int,
) -> (
	route: Native_Route,
	result: Route_Table_Result,
) {
	if path_input_offset < 0 {
		return {}, .Not_Found
	}
	for route_index := 0; route_index < table.count; route_index += 1 {
		candidate := table.routes[route_index]
		if compare_bytes(candidate.method, method) != 0 {
			continue
		}
		captures: [MAX_ROUTE_CAPTURE_COUNT]Route_Capture
		_, matched := match_route_pattern(candidate.path, path, path_input_offset, captures[:])
		if matched {
			return candidate, .Found
		}
	}
	return {}, .Not_Found
}

find_native_route :: proc(
	table: ^Native_Table,
	method: []u8,
	path: []u8,
) -> (
	route: Native_Route,
	result: Route_Table_Result,
) {
	if table == nil {
		return {}, .Invalid
	}
	if table.count < 0 || table.count > len(table.routes) {
		return {}, .Invalid
	}
	if !valid_route_method(method) || !valid_route_path(path) {
		return {}, .Not_Found
	}
	for i := 0; i < table.count; i += 1 {
		candidate := table.routes[i]
		if compare_bytes(candidate.method, method) == 0 &&
		   compare_bytes(candidate.path, path) == 0 {
			return candidate, .Found
		}
	}
	return {}, .Not_Found
}
