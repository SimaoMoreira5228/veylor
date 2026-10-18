#+test
package http2

import "core:testing"

@(test)
test_hpack_static_table_spot_checks :: proc(t: ^testing.T) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 4096)

	expect_static_entry(t, &table, 1, ":authority", "")
	expect_static_entry(t, &table, 2, ":method", "GET")
	expect_static_entry(t, &table, 3, ":method", "POST")
	expect_static_entry(t, &table, 4, ":path", "/")
	expect_static_entry(t, &table, 5, ":path", "/index.html")
	expect_static_entry(t, &table, 6, ":scheme", "http")
	expect_static_entry(t, &table, 7, ":scheme", "https")
	expect_static_entry(t, &table, 8, ":status", "200")
	expect_static_entry(t, &table, 16, "accept-encoding", "gzip, deflate")
	expect_static_entry(t, &table, 24, "cache-control", "")
	expect_static_entry(t, &table, 33, "date", "")
	expect_static_entry(t, &table, 46, "location", "")
	expect_static_entry(t, &table, 55, "set-cookie", "")
	expect_static_entry(t, &table, 61, "www-authenticate", "")

	_, ok := hpack_table_get(&table, 0)

	testing.expect_value(t, ok, false)

	_, ok = hpack_table_get(&table, 62)

	testing.expect_value(t, ok, false)
}

@(test)
test_hpack_table_insert_sizes_and_eviction :: proc(t: ^testing.T) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 256)

	testing.expect_value(t, hpack_table_insert(&table, ":status", "302"), true)
	testing.expect_value(t, hpack_table_insert(&table, "cache-control", "private"), true)
	testing.expect_value(t, hpack_table_insert(&table, "date", "Mon, 21 Oct 2013 20:13:21 GMT"), true)
	testing.expect_value(t, hpack_table_insert(&table, "location", "https://www.example.com"), true)

	testing.expect_value(t, table.size, u64(222))
	testing.expect_value(t, table.count, 4)

	expect_table_entry(t, &table, 62, "location", "https://www.example.com")
	expect_table_entry(t, &table, 63, "date", "Mon, 21 Oct 2013 20:13:21 GMT")
	expect_table_entry(t, &table, 64, "cache-control", "private")
	expect_table_entry(t, &table, 65, ":status", "302")

	_, ok := hpack_table_get(&table, 66)

	testing.expect_value(t, ok, false)

	testing.expect_value(t, hpack_table_insert(&table, ":status", "307"), true)

	testing.expect_value(t, table.size, u64(222))
	testing.expect_value(t, table.count, 4)

	expect_table_entry(t, &table, 62, ":status", "307")
	expect_table_entry(t, &table, 63, "location", "https://www.example.com")
	expect_table_entry(t, &table, 64, "date", "Mon, 21 Oct 2013 20:13:21 GMT")
	expect_table_entry(t, &table, 65, "cache-control", "private")
}

@(test)
test_hpack_table_oversize_entry_empties :: proc(t: ^testing.T) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 256)

	testing.expect_value(t, hpack_table_insert(&table, ":status", "302"), true)

	big_buf: [300]u8

	for i in 0 ..< len(big_buf) {
		big_buf[i] = 'x'
	}

	big_value := string(big_buf[:])

	testing.expect_value(t, hpack_table_insert(&table, "set-cookie", big_value), false)

	testing.expect_value(t, table.count, 0)
	testing.expect_value(t, table.size, u64(0))

	_, ok := hpack_table_get(&table, 62)

	testing.expect_value(t, ok, false)

	testing.expect_value(t, hpack_table_insert(&table, ":status", "307"), true)

	expect_table_entry(t, &table, 62, ":status", "307")
}

@(test)
test_hpack_table_set_max_size :: proc(t: ^testing.T) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 256)

	testing.expect_value(t, hpack_table_insert(&table, ":status", "302"), true)
	testing.expect_value(t, hpack_table_insert(&table, "cache-control", "private"), true)
	testing.expect_value(t, hpack_table_insert(&table, "date", "Mon, 21 Oct 2013 20:13:21 GMT"), true)
	testing.expect_value(t, hpack_table_insert(&table, "location", "https://www.example.com"), true)

	testing.expect_value(t, hpack_table_set_max_size(&table, 100), true)

	testing.expect_value(t, table.size, u64(63))
	testing.expect_value(t, table.count, 1)

	expect_table_entry(t, &table, 62, "location", "https://www.example.com")

	testing.expect_value(t, hpack_table_set_max_size(&table, 300), false)

	testing.expect_value(t, table.size, u64(63))
	testing.expect_value(t, table.count, 1)

	testing.expect_value(t, hpack_table_set_max_size(&table, 0), true)

	testing.expect_value(t, table.size, u64(0))
	testing.expect_value(t, table.count, 0)

	testing.expect_value(t, hpack_table_set_max_size(&table, 256), true)

	testing.expect_value(t, hpack_table_insert(&table, ":status", "200"), true)

	expect_table_entry(t, &table, 62, ":status", "200")
}

@(test)
test_hpack_table_backing_capacity_bound :: proc(t: ^testing.T) {
	backing: [2]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 1 << 20)

	testing.expect_value(t, hpack_table_insert(&table, "a", "1"), true)
	testing.expect_value(t, hpack_table_insert(&table, "b", "2"), true)
	testing.expect_value(t, hpack_table_insert(&table, "c", "3"), true)

	testing.expect_value(t, table.count, 2)

	expect_table_entry(t, &table, 62, "c", "3")
	expect_table_entry(t, &table, 63, "b", "2")

	_, ok := hpack_table_get(&table, 64)

	testing.expect_value(t, ok, false)
}

@(test)
test_hpack_indexed_round_trip :: proc(t: ^testing.T) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 4096)

	testing.expect_value(t, hpack_table_insert(&table, "custom-key", "custom-header"), true)

	enc: [16]u8

	dec: [16]u8

	for index in u64(1) ..= u64(62) {
		written, enc_ok := encode_indexed_header(index, enc[:])

		testing.expect_value(t, enc_ok, true)

		entry, get_ok := hpack_table_get(&table, index)

		testing.expect_value(t, get_ok, true)

		name, value, kind, consumed, pinned, dec_ok := decode_header_field(
			enc[:written],
			&table,
			dec[:],
		)

		testing.expect_value(t, dec_ok, true)
		testing.expect_value(t, consumed, written)
		testing.expect_value(t, pinned, 0)
		testing.expect_value(t, kind, Hpack_Kind.Indexed)
		testing.expect_value(t, name, entry.name)
		testing.expect_value(t, value, entry.value)
	}

	_, enc_ok := encode_indexed_header(0, enc[:])

	testing.expect_value(t, enc_ok, false)

	index_zero := [?]u8{0x80}

	_, _, _, _, _, zero_ok := decode_header_field(index_zero[:], &table, dec[:])

	testing.expect_value(t, zero_ok, false)

	index_past_end := [?]u8{0xff, 0x00}

	_, _, _, _, _, past_ok := decode_header_field(index_past_end[:], &table, dec[:])

	testing.expect_value(t, past_ok, false)
}

@(test)
test_hpack_literal_round_trip_matrix :: proc(t: ^testing.T) {
	kinds := [?]Hpack_Kind{.Incremental, .Without_Indexing, .Never_Indexed}

	for kind in kinds {
		expect_literal_round_trip(t, kind, 24, "cache-control", "no-cache", false, false)
		expect_literal_round_trip(t, kind, 24, "cache-control", "no-cache", true, true)
		expect_literal_round_trip(t, kind, 0, "custom-key", "custom-header", false, false)
		expect_literal_round_trip(t, kind, 0, "custom-key", "custom-header", true, true)
	}
}

@(test)
test_hpack_size_update_round_trip :: proc(t: ^testing.T) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 4096)

	enc: [16]u8

	scratch: [64]u8

	written, enc_ok := encode_table_size_update(256, enc[:])

	testing.expect_value(t, enc_ok, true)
	testing.expect_value(t, written, 3)
	testing.expect_value(t, enc[0], u8(0x3f))
	testing.expect_value(t, enc[1], u8(0xe1))
	testing.expect_value(t, enc[2], u8(0x01))

	_, _, _, _, _, field_ok := decode_header_field(enc[:written], &table, scratch[:])

	testing.expect_value(t, field_ok, false)

	headers: [4]Hpack_Header

	block_scratch: [64]u8

	count, consumed, _, block_ok := decode_header_block(enc[:written], &table, headers[:], block_scratch[:])

	testing.expect_value(t, block_ok, true)
	testing.expect_value(t, consumed, 3)
	testing.expect_value(t, count, 0)
	testing.expect_value(t, table.max_size, u64(256))

	big: [16]u8

	big_written, big_ok := encode_table_size_update(5000, big[:])

	testing.expect_value(t, big_ok, true)

	_, _, _, over_ok := decode_header_block(big[:big_written], &table, headers[:], block_scratch[:])

	testing.expect_value(t, over_ok, false)

	testing.expect_value(t, table.max_size, u64(256))
}

@(test)
test_hpack_literal_rejects_truncation :: proc(t: ^testing.T) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 4096)

	enc: [128]u8

	scratch: [128]u8

	written, enc_ok := encode_literal_header(
		.Incremental,
		0,
		bytes_of("custom-key"),
		bytes_of("custom-header"),
		false,
		false,
		enc[:],
	)

	testing.expect_value(t, enc_ok, true)

	for cut in 0 ..< written {
		_, _, _, _, _, ok := decode_header_field(enc[:cut], &table, scratch[:])

		testing.expect_value(t, ok, false)
	}

	testing.expect_value(t, table.count, 0)

	indexed_written, indexed_ok := encode_literal_header(
		.Without_Indexing,
		24,
		nil,
		bytes_of("no-cache"),
		false,
		false,
		enc[:],
	)

	testing.expect_value(t, indexed_ok, true)

	for cut in 0 ..< indexed_written {
		_, _, _, _, _, ok := decode_header_field(enc[:cut], &table, scratch[:])

		testing.expect_value(t, ok, false)
	}
}

@(test)
test_hpack_incremental_with_evicted_name :: proc(t: ^testing.T) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 128)

	name_scratch: [64]u8

	copy(name_scratch[:], "evict-me")

	name := string(name_scratch[:8])

	testing.expect_value(t, hpack_table_insert(&table, name, "v1"), true)

	expect_table_entry(t, &table, 62, "evict-me", "v1")

	enc: [256]u8

	big_value := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	written, enc_ok := encode_literal_header(
		.Incremental,
		62,
		nil,
		bytes_of(big_value),
		false,
		false,
		enc[:],
	)

	testing.expect_value(t, enc_ok, true)

	field_scratch: [256]u8

	got_name, got_value, kind, consumed, _, dec_ok := decode_header_field(
		enc[:written],
		&table,
		field_scratch[:],
	)

	testing.expect_value(t, dec_ok, true)
	testing.expect_value(t, consumed, written)
	testing.expect_value(t, kind, Hpack_Kind.Incremental)
	testing.expect_value(t, got_name, "evict-me")
	testing.expect_value(t, got_value, big_value)

	expect_table_entry(t, &table, 62, "evict-me", big_value)
	testing.expect_value(t, table.count, 1)
}

@(test)
test_hpack_representation_golden_bytes :: proc(t: ^testing.T) {
	enc: [64]u8

	indexed := [?]u8{0x82}

	written, ok := encode_indexed_header(2, enc[:])

	testing.expect_value(t, ok, true)
	testing.expect_value(t, written, 1)
	testing.expect_value(t, enc[0], indexed[0])

	with_indexing := [?]u8 {
		0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79, 0x0d, 0x63,
		0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72,
	}

	written, ok = encode_literal_header(
		.Incremental,
		0,
		bytes_of("custom-key"),
		bytes_of("custom-header"),
		false,
		false,
		enc[:],
	)

	testing.expect_value(t, ok, true)
	testing.expect_value(t, written, len(with_indexing))
	testing.expect_value(t, string(enc[:written]), string(with_indexing[:]))

	without_indexing := [?]u8 {
		0x04, 0x0c, 0x2f, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2f, 0x70, 0x61, 0x74, 0x68,
	}

	written, ok = encode_literal_header(
		.Without_Indexing,
		4,
		nil,
		bytes_of("/sample/path"),
		false,
		false,
		enc[:],
	)

	testing.expect_value(t, ok, true)
	testing.expect_value(t, written, len(without_indexing))
	testing.expect_value(t, string(enc[:written]), string(without_indexing[:]))

	never_indexed := [?]u8 {
		0x10, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x06, 0x73, 0x65, 0x63,
		0x72, 0x65, 0x74,
	}

	written, ok = encode_literal_header(
		.Never_Indexed,
		0,
		bytes_of("password"),
		bytes_of("secret"),
		false,
		false,
		enc[:],
	)

	testing.expect_value(t, ok, true)
	testing.expect_value(t, written, len(never_indexed))
	testing.expect_value(t, string(enc[:written]), string(never_indexed[:]))

	_, enc_by_index_ok := encode_literal_header(.Indexed, 2, nil, bytes_of("x"), false, false, enc[:])

	testing.expect_value(t, enc_by_index_ok, false)
}

expect_static_entry :: proc(t: ^testing.T, table: ^Hpack_Table, index: u64, name: string, value: string) {
	entry, ok := hpack_table_get(table, index)

	testing.expect_value(t, ok, true)
	testing.expect_value(t, entry.name, name)
	testing.expect_value(t, entry.value, value)
}

expect_table_entry :: proc(t: ^testing.T, table: ^Hpack_Table, index: u64, name: string, value: string) {
	entry, ok := hpack_table_get(table, index)

	testing.expect_value(t, ok, true)
	testing.expect_value(t, entry.name, name)
	testing.expect_value(t, entry.value, value)
}

expect_literal_round_trip :: proc(
	t: ^testing.T,
	kind: Hpack_Kind,
	name_index: u64,
	name: string,
	value: string,
	huffman_name: bool,
	huffman_value: bool,
) {
	backing: [8]Hpack_Entry

	table: Hpack_Table

	hpack_table_init(&table, backing[:], 4096)

	enc: [256]u8

	scratch: [256]u8

	before := table.count

	name_arg: []u8

	if name_index == 0 {
		name_arg = bytes_of(name)
	}

	written, enc_ok := encode_literal_header(
		kind,
		name_index,
		name_arg,
		bytes_of(value),
		huffman_name,
		huffman_value,
		enc[:],
	)

	testing.expect_value(t, enc_ok, true)

	got_name, got_value, got_kind, consumed, pinned, dec_ok := decode_header_field(
		enc[:written],
		&table,
		scratch[:],
	)

	testing.expect_value(t, dec_ok, true)
	testing.expect_value(t, consumed, written)
	testing.expect_value(t, got_kind, kind)
	testing.expect_value(t, got_name, name)
	testing.expect_value(t, got_value, value)

	if name_index == 0 {
		testing.expect_value(t, pinned, len(name) + len(value))
	} else {
		testing.expect_value(t, pinned, len(value))
	}

	if kind == .Incremental {
		testing.expect_value(t, table.count, before + 1)

		expect_table_entry(t, &table, 62, name, value)
	} else {
		testing.expect_value(t, table.count, before)
	}
}
