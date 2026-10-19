package http2

Hpack_Entry :: struct {
	name:  string,
	value: string,
}

Hpack_Table :: struct {
	entries:           []Hpack_Entry,
	count:             int,
	size:              u64,
	max_size:          u64,
	protocol_max_size: u64,
	signalled_max:     u64,
}

Hpack_Kind :: enum u8 {
	Indexed,
	Incremental,
	Without_Indexing,
	Never_Indexed,
}

Hpack_Header :: struct {
	name:  string,
	value: string,
	kind:  Hpack_Kind,
}

hpack_entry_size :: proc(name: string, value: string) -> (size: u64, ok: bool) {
	name_len := u64(len(name))

	value_len := u64(len(value))

	if value_len > max(u64) - u64(32) {
		return 0, false
	}

	room := max(u64) - u64(32) - value_len

	if name_len > room {
		return 0, false
	}

	return name_len + value_len + u64(32), true
}

hpack_table_init :: proc(table: ^Hpack_Table, backing: []Hpack_Entry, protocol_max_size: u64) {
	table.entries = backing

	table.count = 0

	table.size = 0

	table.max_size = protocol_max_size

	table.protocol_max_size = protocol_max_size

	table.signalled_max = protocol_max_size
}

hpack_evict_oldest :: proc(table: ^Hpack_Table) {
	old := table.entries[table.count - 1]

	old_size, _ := hpack_entry_size(old.name, old.value)

	table.size -= old_size

	table.count -= 1
}

hpack_table_set_max_size :: proc(table: ^Hpack_Table, new_max: u64) -> bool {
	if new_max > table.protocol_max_size {
		return false
	}

	table.max_size = new_max

	for table.count > 0 {
		if table.size <= table.max_size {
			break
		}

		hpack_evict_oldest(table)
	}

	return true
}

hpack_table_insert :: proc(table: ^Hpack_Table, name: string, value: string) -> bool {
	entry_size, size_ok := hpack_entry_size(name, value)

	if !size_ok {
		return false
	}

	if len(table.entries) == 0 {
		table.count = 0

		table.size = 0

		return false
	}

	if entry_size > table.max_size {
		table.count = 0

		table.size = 0

		return false
	}

	for table.count > 0 {
		room := table.max_size - table.size

		if entry_size <= room {
			break
		}

		hpack_evict_oldest(table)
	}

	if table.count >= len(table.entries) {
		hpack_evict_oldest(table)
	}

	for i := table.count; i > 0; i -= 1 {
		table.entries[i] = table.entries[i - 1]
	}

	table.entries[0] = Hpack_Entry{name = name, value = value}

	table.count += 1

	table.size += entry_size

	return true
}

hpack_table_get :: proc(table: ^Hpack_Table, index: u64) -> (entry: Hpack_Entry, ok: bool) {
	if index == 0 {
		return {}, false
	}

	static_len := u64(len(HPACK_STATIC_TABLE))

	if index <= static_len {
		return HPACK_STATIC_TABLE[int(index) - 1], true
	}

	dyn := index - static_len - 1

	if dyn >= u64(table.count) {
		return {}, false
	}

	return table.entries[dyn], true
}

encode_indexed_header :: proc(index: u64, out: []u8) -> (written: int, ok: bool) {
	if index == 0 {
		return 0, false
	}

	index_len, int_ok := encode_hpack_int(index, 7, out)

	if !int_ok {
		return 0, false
	}

	out[0] |= 0x80

	return index_len, true
}

encode_table_size_update :: proc(new_max: u64, out: []u8) -> (written: int, ok: bool) {
	update_len, int_ok := encode_hpack_int(new_max, 5, out)

	if !int_ok {
		return 0, false
	}

	out[0] |= 0x20

	return update_len, true
}

encode_literal_header :: proc(
	kind: Hpack_Kind,
	name_index: u64,
	name: []u8,
	value: []u8,
	huffman_name: bool,
	huffman_value: bool,
	out: []u8,
) -> (
	written: int,
	ok: bool,
) {
	prefix_bits := 0

	flag := u8(0)

	switch kind {
	case .Incremental:
		prefix_bits = 6
		flag = 0x40
	case .Without_Indexing:
		prefix_bits = 4
	case .Never_Indexed:
		prefix_bits = 4
		flag = 0x10
	case .Indexed:
		return 0, false
	}

	index_len, index_ok := encode_hpack_int(name_index, prefix_bits, out)

	if !index_ok {
		return 0, false
	}

	out[0] |= flag

	offset := index_len

	if name_index == 0 {
		name_written, name_ok := encode_hpack_string(name, huffman_name, out[offset:])

		if !name_ok {
			return 0, false
		}

		offset += name_written
	}

	value_written, value_ok := encode_hpack_string(value, huffman_value, out[offset:])

	if !value_ok {
		return 0, false
	}

	offset += value_written

	return offset, true
}

decode_literal_field :: proc(
	input: []u8,
	table: ^Hpack_Table,
	scratch: []u8,
	kind: Hpack_Kind,
	prefix_bits: int,
) -> (
	name: string,
	value: string,
	field_kind: Hpack_Kind,
	consumed: int,
	pinned: int,
	ok: bool,
) {
	name_index, index_len, index_ok := decode_hpack_int(input, prefix_bits)

	if !index_ok {
		return "", "", kind, 0, 0, false
	}

	offset := index_len

	if name_index == 0 {
		name_bytes, name_used, name_ok := decode_hpack_string(input[offset:], 7, scratch)

		if !name_ok {
			return "", "", kind, 0, 0, false
		}

		name = string(name_bytes)

		offset += name_used

		pinned += len(name_bytes)
	} else {
		entry, found := hpack_table_get(table, name_index)

		if !found {
			return "", "", kind, 0, 0, false
		}

		name = entry.name
	}

	value_bytes, value_used, value_ok := decode_hpack_string(input[offset:], 7, scratch[pinned:])

	if !value_ok {
		return "", "", kind, 0, 0, false
	}

	value = string(value_bytes)

	offset += value_used

	pinned += len(value_bytes)

	if kind == .Incremental {
		_ = hpack_table_insert(table, name, value)
	}

	return name, value, kind, offset, pinned, true
}

decode_header_field :: proc(
	input: []u8,
	table: ^Hpack_Table,
	scratch: []u8,
) -> (
	name: string,
	value: string,
	kind: Hpack_Kind,
	consumed: int,
	pinned: int,
	ok: bool,
) {
	if len(input) == 0 {
		return "", "", .Indexed, 0, 0, false
	}

	first := input[0]

	if first & 0x80 != 0 {
		index, index_len, index_ok := decode_hpack_int(input, 7)

		if !index_ok {
			return "", "", .Indexed, 0, 0, false
		}

		if index == 0 {
			return "", "", .Indexed, 0, 0, false
		}

		entry, found := hpack_table_get(table, index)

		if !found {
			return "", "", .Indexed, 0, 0, false
		}

		return entry.name, entry.value, .Indexed, index_len, 0, true
	}

	if first & 0xc0 == 0x40 {
		return decode_literal_field(input, table, scratch, .Incremental, 6)
	}

	if first & 0xf0 == 0x10 {
		return decode_literal_field(input, table, scratch, .Never_Indexed, 4)
	}

	if first & 0xf0 == 0x00 {
		return decode_literal_field(input, table, scratch, .Without_Indexing, 4)
	}

	return "", "", .Indexed, 0, 0, false
}

decode_header_block :: proc(
	input: []u8,
	table: ^Hpack_Table,
	headers: []Hpack_Header,
	scratch: []u8,
) -> (
	count: int,
	consumed: int,
	pinned: int,
	ok: bool,
) {
	offset := 0

	for offset < len(input) {
		if count >= len(headers) {
			return 0, 0, 0, false
		}

		if (input[offset] & 0xe0) == 0x20 {
			new_max, update_len, update_ok := decode_hpack_int(input[offset:], 5)

			if !update_ok {
				return 0, 0, 0, false
			}

			if !hpack_table_set_max_size(table, new_max) {
				return 0, 0, 0, false
			}

			offset += update_len

			continue
		}

		name, value, kind, field_len, field_pinned, field_ok := decode_header_field(
			input[offset:],
			table,
			scratch[pinned:],
		)

		if !field_ok {
			return 0, 0, 0, false
		}

		headers[count] = Hpack_Header{name = name, value = value, kind = kind}

		count += 1

		offset += field_len

		pinned += field_pinned
	}

	return count, offset, pinned, true
}

HPACK_STATIC_TABLE := [61]Hpack_Entry{
	{":authority", ""},
	{":method", "GET"},
	{":method", "POST"},
	{":path", "/"},
	{":path", "/index.html"},
	{":scheme", "http"},
	{":scheme", "https"},
	{":status", "200"},
	{":status", "204"},
	{":status", "206"},
	{":status", "304"},
	{":status", "400"},
	{":status", "404"},
	{":status", "500"},
	{"accept-charset", ""},
	{"accept-encoding", "gzip, deflate"},
	{"accept-language", ""},
	{"accept-ranges", ""},
	{"accept", ""},
	{"access-control-allow-origin", ""},
	{"age", ""},
	{"allow", ""},
	{"authorization", ""},
	{"cache-control", ""},
	{"content-disposition", ""},
	{"content-encoding", ""},
	{"content-language", ""},
	{"content-length", ""},
	{"content-location", ""},
	{"content-range", ""},
	{"content-type", ""},
	{"cookie", ""},
	{"date", ""},
	{"etag", ""},
	{"expect", ""},
	{"expires", ""},
	{"from", ""},
	{"host", ""},
	{"if-match", ""},
	{"if-modified-since", ""},
	{"if-none-match", ""},
	{"if-range", ""},
	{"if-unmodified-since", ""},
	{"last-modified", ""},
	{"link", ""},
	{"location", ""},
	{"max-forwards", ""},
	{"proxy-authenticate", ""},
	{"proxy-authorization", ""},
	{"range", ""},
	{"referer", ""},
	{"refresh", ""},
	{"retry-after", ""},
	{"server", ""},
	{"set-cookie", ""},
	{"strict-transport-security", ""},
	{"transfer-encoding", ""},
	{"user-agent", ""},
	{"vary", ""},
	{"via", ""},
	{"www-authenticate", ""},
}
