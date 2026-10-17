#+test
package router

import "core:testing"
import http1 "veylor:http1"

parse_managed_test_head :: proc(
	raw: string,
	input: []u8,
	headers: []http1.Header,
) -> http1.Request_Head {
	parser: http1.Parser
	head, parse_result := http1.parse_request_head(&parser, input, headers)
	assert(parse_result == .Ready)
	return head
}

@(test)
test_managed_routes_register_and_match_with_kind :: proc(t: ^testing.T) {
	storage: [4]Static_Route
	index_storage: [8]Route_Index_Slot
	table := init_route_table(storage[:], index_storage[:])
	testing.expect_value(
		t,
		register_managed_route(
			&table,
			transmute([]u8)string("GET"),
			transmute([]u8)string("/managed"),
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_managed_route(
			&table,
			transmute([]u8)string("GET"),
			transmute([]u8)string("/users/:id"),
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Frozen)

	raw := "GET /managed HTTP/1.1\r\nHost: example.test\r\n\r\n"
	input := transmute([]u8)raw
	headers: [4]http1.Header
	head := parse_managed_test_head(raw, input, headers[:])
	route, result := find_static_request_route(&table, head, input)
	testing.expect_value(t, result, Route_Table_Result.Found)
	testing.expect_value(t, route.kind, Route_Kind.Managed)

	dynamic_raw := "GET /users/42 HTTP/1.1\r\nHost: example.test\r\n\r\n"
	dynamic_input := transmute([]u8)dynamic_raw
	dynamic_headers: [4]http1.Header
	dynamic_head := parse_managed_test_head(dynamic_raw, dynamic_input, dynamic_headers[:])
	dynamic_route, dynamic_result := find_static_request_route(&table, dynamic_head, dynamic_input)
	testing.expect_value(t, dynamic_result, Route_Table_Result.Found)
	testing.expect_value(t, dynamic_route.kind, Route_Kind.Managed)
}

@(test)
test_managed_and_static_duplicates_conflict_at_freeze :: proc(t: ^testing.T) {
	storage: [2]Static_Route
	index_storage: [4]Route_Index_Slot
	table := init_route_table(storage[:], index_storage[:])
	testing.expect_value(
		t,
		register_static_route(
			&table,
			transmute([]u8)string("GET"),
			transmute([]u8)string("/clash"),
			Static_Response{status = 200},
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(
		t,
		register_managed_route(
			&table,
			transmute([]u8)string("GET"),
			transmute([]u8)string("/clash"),
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Duplicate)
}
