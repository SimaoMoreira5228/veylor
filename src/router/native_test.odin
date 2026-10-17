#+test
package router

import "core:testing"

native_test_handler :: proc "c" (
	user_data: rawptr,
	request: ^Native_Request_View,
	response: ^Native_Response_View,
) -> i32 {
	if user_data == nil || request == nil || response == nil {
		return i32(Native_Handler_Result.Invalid_Argument)
	}
	counter := cast(^int)user_data
	counter^ += 1
	response.status = 200
	return i32(Native_Handler_Result.Ok)
}

@(test)
test_native_table_registers_and_finds_exact_routes :: proc(t: ^testing.T) {
	storage: [4]Native_Route
	table := init_native_table(storage[:])
	method := transmute([]u8)string("GET")
	path := transmute([]u8)string("/native")
	counter := 0
	testing.expect_value(
		t,
		register_native_route(&table, method, path, native_test_handler, rawptr(&counter)),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_native_route(&table, method, path, native_test_handler, rawptr(&counter)),
		Route_Table_Result.Duplicate,
	)
	route, result := find_native_route(&table, method, path)
	testing.expect_value(t, result, Route_Table_Result.Found)
	testing.expect_value(t, route.handler != nil, true)
	request := Native_Request_View {
		method_data = raw_data(method),
		method_len  = u32(len(method)),
		path_data   = raw_data(path),
		path_len    = u32(len(path)),
	}
	response_view := Native_Response_View{}
	testing.expect_value(
		t,
		route.handler(rawptr(&counter), &request, &response_view),
		i32(Native_Handler_Result.Ok),
	)
	testing.expect_value(t, counter, 1)
	testing.expect_value(t, response_view.status, i32(200))
}

@(test)
test_native_request_route_matches_dynamic_patterns_and_head_fallback :: proc(t: ^testing.T) {
	storage: [4]Native_Route
	table := init_native_table(storage[:])
	counter := 0
	testing.expect_value(
		t,
		register_native_route(
			&table,
			transmute([]u8)string("GET"),
			transmute([]u8)string("/users/:id"),
			native_test_handler,
			rawptr(&counter),
		),
		Route_Table_Result.Added,
	)
	route, result := find_native_request_route(
		&table,
		transmute([]u8)string("GET"),
		transmute([]u8)string("/users/42"),
		0,
	)
	testing.expect_value(t, result, Route_Table_Result.Found)
	testing.expect_value(t, string(route.path), "/users/:id")
	route, result = find_native_request_route(
		&table,
		transmute([]u8)string("HEAD"),
		transmute([]u8)string("/users/42"),
		0,
	)
	testing.expect_value(t, result, Route_Table_Result.Found)
	_, result = find_native_request_route(
		&table,
		transmute([]u8)string("GET"),
		transmute([]u8)string("/missing"),
		0,
	)
	testing.expect_value(t, result, Route_Table_Result.Not_Found)
}

@(test)
test_native_table_rejects_invalid_handlers_and_missing_routes :: proc(t: ^testing.T) {
	storage: [1]Native_Route
	table := init_native_table(storage[:])
	method := transmute([]u8)string("GET")
	path := transmute([]u8)string("/native")
	testing.expect_value(
		t,
		register_native_route(&table, method, path, nil, nil),
		Route_Table_Result.Invalid,
	)
	_, result := find_native_route(&table, method, transmute([]u8)string("/missing"))
	testing.expect_value(t, result, Route_Table_Result.Not_Found)
}
