#+test
package http2

import "core:testing"

@(test)
test_rfc_c3_requests_without_huffman :: proc(t: ^testing.T) {
	backing: [128]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 4096)

	headers: [16]Hpack_Header

	scratch: [4096]u8

	used := 0

	count := decode_block(t, RFC_C31[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 4)

	expect_decoded_header(t, headers[0], ":method", "GET", .Indexed)
	expect_decoded_header(t, headers[1], ":scheme", "http", .Indexed)
	expect_decoded_header(t, headers[2], ":path", "/", .Indexed)
	expect_decoded_header(t, headers[3], ":authority", "www.example.com", .Incremental)

	expect_table_contents(t, &table, 57, []string{":authority"}, []string{"www.example.com"})

	count = decode_block(t, RFC_C32[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 5)

	expect_decoded_header(t, headers[0], ":method", "GET", .Indexed)
	expect_decoded_header(t, headers[1], ":scheme", "http", .Indexed)
	expect_decoded_header(t, headers[2], ":path", "/", .Indexed)
	expect_decoded_header(t, headers[3], ":authority", "www.example.com", .Indexed)
	expect_decoded_header(t, headers[4], "cache-control", "no-cache", .Incremental)

	expect_table_contents(
		t,
		&table,
		110,
		[]string{"cache-control", ":authority"},
		[]string{"no-cache", "www.example.com"},
	)

	count = decode_block(t, RFC_C33[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 5)

	expect_decoded_header(t, headers[0], ":method", "GET", .Indexed)
	expect_decoded_header(t, headers[1], ":scheme", "https", .Indexed)
	expect_decoded_header(t, headers[2], ":path", "/index.html", .Indexed)
	expect_decoded_header(t, headers[3], ":authority", "www.example.com", .Indexed)
	expect_decoded_header(t, headers[4], "custom-key", "custom-value", .Incremental)

	expect_table_contents(
		t,
		&table,
		164,
		[]string{"custom-key", "cache-control", ":authority"},
		[]string{"custom-value", "no-cache", "www.example.com"},
	)
}

@(test)
test_rfc_c4_requests_with_huffman :: proc(t: ^testing.T) {
	backing: [128]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 4096)

	headers: [16]Hpack_Header

	scratch: [4096]u8

	used := 0

	count := decode_block(t, RFC_C41[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 4)

	expect_decoded_header(t, headers[0], ":method", "GET", .Indexed)
	expect_decoded_header(t, headers[1], ":scheme", "http", .Indexed)
	expect_decoded_header(t, headers[2], ":path", "/", .Indexed)
	expect_decoded_header(t, headers[3], ":authority", "www.example.com", .Incremental)

	expect_table_contents(t, &table, 57, []string{":authority"}, []string{"www.example.com"})

	count = decode_block(t, RFC_C42[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 5)

	expect_decoded_header(t, headers[0], ":method", "GET", .Indexed)
	expect_decoded_header(t, headers[1], ":scheme", "http", .Indexed)
	expect_decoded_header(t, headers[2], ":path", "/", .Indexed)
	expect_decoded_header(t, headers[3], ":authority", "www.example.com", .Indexed)
	expect_decoded_header(t, headers[4], "cache-control", "no-cache", .Incremental)

	expect_table_contents(
		t,
		&table,
		110,
		[]string{"cache-control", ":authority"},
		[]string{"no-cache", "www.example.com"},
	)

	count = decode_block(t, RFC_C43[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 5)

	expect_decoded_header(t, headers[0], ":method", "GET", .Indexed)
	expect_decoded_header(t, headers[1], ":scheme", "https", .Indexed)
	expect_decoded_header(t, headers[2], ":path", "/index.html", .Indexed)
	expect_decoded_header(t, headers[3], ":authority", "www.example.com", .Indexed)
	expect_decoded_header(t, headers[4], "custom-key", "custom-value", .Incremental)

	expect_table_contents(
		t,
		&table,
		164,
		[]string{"custom-key", "cache-control", ":authority"},
		[]string{"custom-value", "no-cache", "www.example.com"},
	)
}

@(test)
test_rfc_c5_responses_without_huffman :: proc(t: ^testing.T) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 256)

	headers: [16]Hpack_Header

	scratch: [4096]u8

	used := 0

	date21 := "Mon, 21 Oct 2013 20:13:21 GMT"

	location := "https://www.example.com"

	count := decode_block(t, RFC_C51[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 4)

	expect_decoded_header(t, headers[0], ":status", "302", .Incremental)
	expect_decoded_header(t, headers[1], "cache-control", "private", .Incremental)
	expect_decoded_header(t, headers[2], "date", date21, .Incremental)
	expect_decoded_header(t, headers[3], "location", location, .Incremental)

	expect_table_contents(
		t,
		&table,
		222,
		[]string{"location", "date", "cache-control", ":status"},
		[]string{location, date21, "private", "302"},
	)

	count = decode_block(t, RFC_C52[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 4)

	expect_decoded_header(t, headers[0], ":status", "307", .Incremental)
	expect_decoded_header(t, headers[1], "cache-control", "private", .Indexed)
	expect_decoded_header(t, headers[2], "date", date21, .Indexed)
	expect_decoded_header(t, headers[3], "location", location, .Indexed)

	expect_table_contents(
		t,
		&table,
		222,
		[]string{":status", "location", "date", "cache-control"},
		[]string{"307", location, date21, "private"},
	)

	count = decode_block(t, RFC_C53[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 6)

	date22 := "Mon, 21 Oct 2013 20:13:22 GMT"

	cookie := "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1"

	expect_decoded_header(t, headers[0], ":status", "200", .Indexed)
	expect_decoded_header(t, headers[1], "cache-control", "private", .Indexed)
	expect_decoded_header(t, headers[2], "date", date22, .Incremental)
	expect_decoded_header(t, headers[3], "location", location, .Indexed)
	expect_decoded_header(t, headers[4], "content-encoding", "gzip", .Incremental)
	expect_decoded_header(t, headers[5], "set-cookie", cookie, .Incremental)

	expect_table_contents(
		t,
		&table,
		215,
		[]string{"set-cookie", "content-encoding", "date"},
		[]string{cookie, "gzip", date22},
	)
}

@(test)
test_rfc_c6_responses_with_huffman :: proc(t: ^testing.T) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 256)

	headers: [16]Hpack_Header

	scratch: [4096]u8

	used := 0

	date21 := "Mon, 21 Oct 2013 20:13:21 GMT"

	location := "https://www.example.com"

	count := decode_block(t, RFC_C61[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 4)

	expect_decoded_header(t, headers[0], ":status", "302", .Incremental)
	expect_decoded_header(t, headers[1], "cache-control", "private", .Incremental)
	expect_decoded_header(t, headers[2], "date", date21, .Incremental)
	expect_decoded_header(t, headers[3], "location", location, .Incremental)

	expect_table_contents(
		t,
		&table,
		222,
		[]string{"location", "date", "cache-control", ":status"},
		[]string{location, date21, "private", "302"},
	)

	count = decode_block(t, RFC_C62[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 4)

	expect_decoded_header(t, headers[0], ":status", "307", .Incremental)
	expect_decoded_header(t, headers[1], "cache-control", "private", .Indexed)
	expect_decoded_header(t, headers[2], "date", date21, .Indexed)
	expect_decoded_header(t, headers[3], "location", location, .Indexed)

	expect_table_contents(
		t,
		&table,
		222,
		[]string{":status", "location", "date", "cache-control"},
		[]string{"307", location, date21, "private"},
	)

	count = decode_block(t, RFC_C63[:], &table, headers[:], scratch[:], &used)

	testing.expect_value(t, count, 6)

	date22 := "Mon, 21 Oct 2013 20:13:22 GMT"

	cookie := "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1"

	expect_decoded_header(t, headers[0], ":status", "200", .Indexed)
	expect_decoded_header(t, headers[1], "cache-control", "private", .Indexed)
	expect_decoded_header(t, headers[2], "date", date22, .Incremental)
	expect_decoded_header(t, headers[3], "location", location, .Indexed)
	expect_decoded_header(t, headers[4], "content-encoding", "gzip", .Incremental)
	expect_decoded_header(t, headers[5], "set-cookie", cookie, .Incremental)

	expect_table_contents(
		t,
		&table,
		215,
		[]string{"set-cookie", "content-encoding", "date"},
		[]string{cookie, "gzip", date22},
	)
}

expect_decoded_header :: proc(
	t: ^testing.T,
	got: Hpack_Header,
	want_name: string,
	want_value: string,
	want_kind: Hpack_Kind,
) {
	testing.expect_value(t, got.name, want_name)
	testing.expect_value(t, got.value, want_value)
	testing.expect_value(t, got.kind, want_kind)
}

expect_table_contents :: proc(
	t: ^testing.T,
	table: ^Hpack_Table,
	want_size: u64,
	want_names: []string,
	want_values: []string,
) {
	testing.expect_value(t, table.size, want_size)
	testing.expect_value(t, table.count, len(want_names))

	for i in 0 ..< len(want_names) {
		entry, ok := hpack_table_get(table, u64(62 + i))

		testing.expect_value(t, ok, true)
		testing.expect_value(t, entry.name, want_names[i])
		testing.expect_value(t, entry.value, want_values[i])
	}
}

decode_block :: proc(
	t: ^testing.T,
	raw: []u8,
	table: ^Hpack_Table,
	headers: []Hpack_Header,
	scratch: []u8,
	used: ^int,
) -> int {
	count, consumed, pinned, ok := decode_header_block(raw, table, headers, scratch[used^:])

	testing.expect_value(t, ok, true)
	testing.expect_value(t, consumed, len(raw))

	used^ += pinned

	return count
}

RFC_C21 := [26]u8{
	0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79,
	0x0d, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64,
	0x65, 0x72,
}

RFC_C22 := [14]u8{
	0x04, 0x0c, 0x2f, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2f, 0x70, 0x61,
	0x74, 0x68,
}

RFC_C23 := [17]u8{
	0x10, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x06, 0x73,
	0x65, 0x63, 0x72, 0x65, 0x74,
}

RFC_C24 := [1]u8{
	0x82,
}

RFC_C31 := [20]u8{
	0x82, 0x86, 0x84, 0x41, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61,
	0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d,
}

RFC_C32 := [14]u8{
	0x82, 0x86, 0x84, 0xbe, 0x58, 0x08, 0x6e, 0x6f, 0x2d, 0x63, 0x61, 0x63,
	0x68, 0x65,
}

RFC_C33 := [29]u8{
	0x82, 0x87, 0x85, 0xbf, 0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d,
	0x2d, 0x6b, 0x65, 0x79, 0x0c, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d,
	0x76, 0x61, 0x6c, 0x75, 0x65,
}

RFC_C41 := [17]u8{
	0x82, 0x86, 0x84, 0x41, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b,
	0xa0, 0xab, 0x90, 0xf4, 0xff,
}

RFC_C42 := [12]u8{
	0x82, 0x86, 0x84, 0xbe, 0x58, 0x86, 0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf,
}

RFC_C43 := [24]u8{
	0x82, 0x87, 0x85, 0xbf, 0x40, 0x88, 0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xa9,
	0x7d, 0x7f, 0x89, 0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xb8, 0xe8, 0xb4, 0xbf,
}

RFC_C51 := [70]u8{
	0x48, 0x03, 0x33, 0x30, 0x32, 0x58, 0x07, 0x70, 0x72, 0x69, 0x76, 0x61,
	0x74, 0x65, 0x61, 0x1d, 0x4d, 0x6f, 0x6e, 0x2c, 0x20, 0x32, 0x31, 0x20,
	0x4f, 0x63, 0x74, 0x20, 0x32, 0x30, 0x31, 0x33, 0x20, 0x32, 0x30, 0x3a,
	0x31, 0x33, 0x3a, 0x32, 0x31, 0x20, 0x47, 0x4d, 0x54, 0x6e, 0x17, 0x68,
	0x74, 0x74, 0x70, 0x73, 0x3a, 0x2f, 0x2f, 0x77, 0x77, 0x77, 0x2e, 0x65,
	0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d,
}

RFC_C52 := [8]u8{
	0x48, 0x03, 0x33, 0x30, 0x37, 0xc1, 0xc0, 0xbf,
}

RFC_C53 := [98]u8{
	0x88, 0xc1, 0x61, 0x1d, 0x4d, 0x6f, 0x6e, 0x2c, 0x20, 0x32, 0x31, 0x20,
	0x4f, 0x63, 0x74, 0x20, 0x32, 0x30, 0x31, 0x33, 0x20, 0x32, 0x30, 0x3a,
	0x31, 0x33, 0x3a, 0x32, 0x32, 0x20, 0x47, 0x4d, 0x54, 0xc0, 0x5a,
	0x04, 0x67, 0x7a, 0x69, 0x70, 0x77, 0x38, 0x66, 0x6f, 0x6f, 0x3d, 0x41,
	0x53, 0x44, 0x4a, 0x4b, 0x48, 0x51, 0x4b, 0x42, 0x5a, 0x58, 0x4f, 0x51,
	0x57, 0x45, 0x4f, 0x50, 0x49, 0x55, 0x41, 0x58, 0x51, 0x57, 0x45, 0x4f,
	0x49, 0x55, 0x3b, 0x20, 0x6d, 0x61, 0x78, 0x2d, 0x61, 0x67, 0x65, 0x3d,
	0x33, 0x36, 0x30, 0x30, 0x3b, 0x20, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x3d, 0x31,
}

RFC_C61 := [54]u8{
	0x48, 0x82, 0x64, 0x02, 0x58, 0x85, 0xae, 0xc3, 0x77, 0x1a, 0x4b, 0x61,
	0x96, 0xd0, 0x7a, 0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20, 0x05,
	0x95, 0x04, 0x0b, 0x81, 0x66, 0xe0, 0x82, 0xa6, 0x2d, 0x1b, 0xff, 0x6e,
	0x91, 0x9d, 0x29, 0xad, 0x17, 0x18, 0x63, 0xc7, 0x8f, 0x0b, 0x97, 0xc8,
	0xe9, 0xae, 0x82, 0xae, 0x43, 0xd3,
}

RFC_C62 := [8]u8{
	0x48, 0x83, 0x64, 0x0e, 0xff, 0xc1, 0xc0, 0xbf,
}

RFC_C63 := [79]u8{
	0x88, 0xc1, 0x61, 0x96, 0xd0, 0x7a, 0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44,
	0xa8, 0x20, 0x05, 0x95, 0x04, 0x0b, 0x81, 0x66, 0xe0, 0x84, 0xa6, 0x2d,
	0x1b, 0xff, 0xc0, 0x5a, 0x83, 0x9b, 0xd9, 0xab, 0x77, 0xad, 0x94, 0xe7,
	0x82, 0x1d, 0xd7, 0xf2, 0xe6, 0xc7, 0xb3, 0x35, 0xdf, 0xdf, 0xcd, 0x5b,
	0x39, 0x60, 0xd5, 0xaf, 0x27, 0x08, 0x7f, 0x36, 0x72, 0xc1, 0xab, 0x27,
	0x0f, 0xb5, 0x29, 0x1f, 0x95, 0x87, 0x31, 0x60, 0x65, 0xc0, 0x03, 0xed,
	0x4e, 0xe5, 0xb1, 0x06, 0x3d, 0x50, 0x07,
}

