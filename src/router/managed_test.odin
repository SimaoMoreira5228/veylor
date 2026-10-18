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

@(test)
test_managed_route_with_trailers_registers_allowlist :: proc(t: ^testing.T) {
	storage: [4]Static_Route
	index_storage: [8]Route_Index_Slot
	table := init_route_table(storage[:], index_storage[:])
	allowlist := [?]string{"x-checksum", "x-signature"}
	names := make([][]u8, len(allowlist), context.temp_allocator)

	for allowed, i in allowlist {
		names[i] = transmute([]u8)allowed
	}
	testing.expect_value(
		t,
		register_managed_route_with_trailers(
			&table,
			transmute([]u8)string("POST"),
			transmute([]u8)string("/upload"),
			names,
		),
		Route_Table_Result.Added,
	)
	testing.expect_value(t, freeze_route_table(&table), Route_Table_Result.Frozen)
	testing.expect_value(t, table.routes[0].kind, Route_Kind.Managed)
	testing.expect_value(t, table.routes[0].managed_trailer_count, 2)
	testing.expect_value(t, string(table.routes[0].managed_trailers[0]), "x-checksum")
	testing.expect_value(t, string(table.routes[0].managed_trailers[1]), "x-signature")
}

@(test)
test_managed_route_with_trailers_rejects_invalid_allowlist :: proc(t: ^testing.T) {
	storage: [4]Static_Route
	index_storage: [8]Route_Index_Slot
	table := init_route_table(storage[:], index_storage[:])
	oversize: [MAX_MANAGED_TRAILER_NAMES + 1][]u8
	oversize[0] = transmute([]u8)string("x-checksum")
	testing.expect_value(
		t,
		register_managed_route_with_trailers(
			&table,
			transmute([]u8)string("POST"),
			transmute([]u8)string("/upload"),
			oversize[:],
		),
		Route_Table_Result.Invalid,
	)
	empty_name := [?]string{""}
	empty_names := make([][]u8, len(empty_name), context.temp_allocator)

	for allowed, i in empty_name {
		empty_names[i] = transmute([]u8)allowed
	}
	testing.expect_value(
		t,
		register_managed_route_with_trailers(
			&table,
			transmute([]u8)string("POST"),
			transmute([]u8)string("/upload"),
			empty_names,
		),
		Route_Table_Result.Invalid,
	)
	bad_name := [?]string{"bad name"}
	bad_names := make([][]u8, len(bad_name), context.temp_allocator)

	for allowed, i in bad_name {
		bad_names[i] = transmute([]u8)allowed
	}
	testing.expect_value(
		t,
		register_managed_route_with_trailers(
			&table,
			transmute([]u8)string("POST"),
			transmute([]u8)string("/upload"),
			bad_names,
		),
		Route_Table_Result.Invalid,
	)
	testing.expect_value(t, table.count, 0)
}
