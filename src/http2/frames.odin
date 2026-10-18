package http2

FLAG_END_STREAM :: u8(0x01)
FLAG_END_HEADERS :: u8(0x04)
FLAG_PADDED :: u8(0x08)
FLAG_PRIORITY :: u8(0x20)
FLAG_ACK :: u8(0x01)

SETTINGS_ACK :: FLAG_ACK
PING_ACK :: FLAG_ACK

PRIORITY_PAYLOAD_LEN :: 5
RST_STREAM_PAYLOAD_LEN :: 4
SETTINGS_ENTRY_LEN :: 6
PING_PAYLOAD_LEN :: 8
WINDOW_UPDATE_PAYLOAD_LEN :: 4
GOAWAY_FIXED_LEN :: 8

MAX_WINDOW_INCREMENT :: u32(0x7fffffff)

SETTINGS_HEADER_TABLE_SIZE :: u16(0x1)
SETTINGS_ENABLE_PUSH :: u16(0x2)
SETTINGS_MAX_CONCURRENT_STREAMS :: u16(0x3)
SETTINGS_INITIAL_WINDOW_SIZE :: u16(0x4)
SETTINGS_MAX_FRAME_SIZE :: u16(0x5)
SETTINGS_MAX_HEADER_LIST_SIZE :: u16(0x6)

SETTINGS_ENABLE_PUSH_MAX :: u32(1)
SETTINGS_INITIAL_WINDOW_SIZE_MAX :: u32(0x7fffffff)
SETTINGS_MAX_FRAME_SIZE_MIN :: u32(16384)
SETTINGS_MAX_FRAME_SIZE_MAX :: u32(16777215)

Error_Code :: enum u32 {
	No_Error            = 0,
	Protocol_Error      = 1,
	Internal_Error      = 2,
	Flow_Control_Error  = 3,
	Settings_Timeout    = 4,
	Stream_Closed       = 5,
	Frame_Size_Error    = 6,
	Refused_Stream      = 7,
	Cancel              = 8,
	Compression_Error   = 9,
	Connect_Error       = 10,
	Enhance_Your_Calm   = 11,
	Inadequate_Security = 12,
	Http_1_1_Required   = 13,
}

Settings_Parameter :: struct {
	id:    u16,
	value: u32,
}

Headers_Priority :: struct {
	exclusive:  bool,
	dependency: u32,
	// NOTE: weight holds the raw wire byte; the effective priority weight is wire value + 1 (1..256).
	weight:     u8,
}

payload_length_matches :: proc(header: Frame_Header, payload: []u8) -> bool {
	return u32(len(payload)) == header.length
}

settings_parameter_valid :: proc(id: u16, value: u32) -> bool {
	switch id {
	case SETTINGS_ENABLE_PUSH:
		return value <= SETTINGS_ENABLE_PUSH_MAX
	case SETTINGS_INITIAL_WINDOW_SIZE:
		return value <= SETTINGS_INITIAL_WINDOW_SIZE_MAX
	case SETTINGS_MAX_FRAME_SIZE:
		return value >= SETTINGS_MAX_FRAME_SIZE_MIN && value <= SETTINGS_MAX_FRAME_SIZE_MAX
	}

	return true
}

decode_data_payload :: proc(
	header: Frame_Header,
	payload: []u8,
) -> (
	data: []u8,
	pad_length: u8,
	ok: bool,
) {
	if header.kind != .Data {
		return nil, 0, false
	}

	if header.stream_id == 0 {
		return nil, 0, false
	}

	if !payload_length_matches(header, payload) {
		return nil, 0, false
	}

	if header.flags & FLAG_PADDED == 0 {
		return payload, 0, true
	}

	if len(payload) == 0 {
		return nil, 0, false
	}

	pad := payload[0]

	if int(pad) + 1 > len(payload) {
		return nil, 0, false
	}

	data_end := len(payload) - int(pad)

	return payload[1:data_end], pad, true
}

encode_data_payload :: proc(data: []u8, pad_length: u8, out: []u8) -> (written: int, ok: bool) {
	total := len(data)

	if pad_length > 0 {
		if len(data) > max(int) - int(pad_length) - 1 {
			return 0, false
		}

		total += int(pad_length) + 1
	}

	if total > len(out) {
		return 0, false
	}

	offset := 0

	if pad_length > 0 {
		out[0] = pad_length

		offset = 1
	}

	for i in 0 ..< len(data) {
		out[offset + i] = data[i]
	}

	offset += len(data)

	for i := offset; i < total; i += 1 {
		out[i] = 0
	}

	return total, true
}

encode_priority_fields :: proc(fields: Headers_Priority, out: []u8) -> bool {
	if len(out) < PRIORITY_PAYLOAD_LEN {
		return false
	}

	if fields.dependency > MAX_STREAM_ID {
		return false
	}

	out[0] = u8(fields.dependency >> 24)
	out[1] = u8(fields.dependency >> 16)
	out[2] = u8(fields.dependency >> 8)
	out[3] = u8(fields.dependency)
	out[4] = fields.weight

	if fields.exclusive {
		out[0] |= 0x80
	}

	return true
}

decode_priority_fields :: proc(
	fields_bytes: []u8,
) -> (
	fields: Headers_Priority,
	ok: bool,
) {
	if len(fields_bytes) != PRIORITY_PAYLOAD_LEN {
		return {}, false
	}

	fields.exclusive = fields_bytes[0] & 0x80 != 0

	fields.dependency =
		u32(fields_bytes[0] & 0x7f) << 24 |
		u32(fields_bytes[1]) << 16 |
		u32(fields_bytes[2]) << 8 |
		u32(fields_bytes[3])

	fields.weight = fields_bytes[4]

	return fields, true
}

decode_priority_payload :: proc(
	header: Frame_Header,
	payload: []u8,
) -> (
	fields: Headers_Priority,
	ok: bool,
) {
	if header.kind != .Priority {
		return {}, false
	}

	if header.stream_id == 0 {
		return {}, false
	}

	if !payload_length_matches(header, payload) {
		return {}, false
	}

	return decode_priority_fields(payload)
}

decode_headers_payload :: proc(
	header: Frame_Header,
	payload: []u8,
) -> (
	fragment: []u8,
	priority: Headers_Priority,
	has_priority: bool,
	pad_length: u8,
	ok: bool,
) {
	if header.kind != .Headers {
		return nil, {}, false, 0, false
	}

	if header.stream_id == 0 {
		return nil, {}, false, 0, false
	}

	if !payload_length_matches(header, payload) {
		return nil, {}, false, 0, false
	}

	offset := 0

	pad := u8(0)

	if header.flags & FLAG_PADDED != 0 {
		if len(payload) == 0 {
			return nil, {}, false, 0, false
		}

		pad = payload[0]

		offset = 1
	}

	fields := Headers_Priority{}

	present := false

	if header.flags & FLAG_PRIORITY != 0 {
		if len(payload) - offset < PRIORITY_PAYLOAD_LEN {
			return nil, {}, false, 0, false
		}

		decoded, fields_ok := decode_priority_fields(payload[offset:offset + PRIORITY_PAYLOAD_LEN])

		if !fields_ok {
			return nil, {}, false, 0, false
		}

		fields = decoded

		offset += PRIORITY_PAYLOAD_LEN

		present = true
	}

	remaining := len(payload) - offset

	if int(pad) > remaining {
		return nil, {}, false, 0, false
	}

	end := len(payload) - int(pad)

	return payload[offset:end], fields, present, pad, true
}

encode_headers_payload :: proc(
	fragment: []u8,
	priority: ^Headers_Priority,
	pad_length: u8,
	out: []u8,
) -> (
	written: int,
	ok: bool,
) {
	total := len(fragment)

	if priority != nil {
		if total > max(int) - PRIORITY_PAYLOAD_LEN {
			return 0, false
		}

		total += PRIORITY_PAYLOAD_LEN
	}

	if pad_length > 0 {
		if total > max(int) - int(pad_length) - 1 {
			return 0, false
		}

		total += int(pad_length) + 1
	}

	if total > len(out) {
		return 0, false
	}

	offset := 0

	if pad_length > 0 {
		out[0] = pad_length

		offset = 1
	}

	if priority != nil {
		if !encode_priority_fields(priority^, out[offset:]) {
			return 0, false
		}

		offset += PRIORITY_PAYLOAD_LEN
	}

	for i in 0 ..< len(fragment) {
		out[offset + i] = fragment[i]
	}

	offset += len(fragment)

	for i := offset; i < total; i += 1 {
		out[i] = 0
	}

	return total, true
}

encode_rst_stream_payload :: proc(code: Error_Code, out: []u8) -> bool {
	if len(out) < RST_STREAM_PAYLOAD_LEN {
		return false
	}

	value := u32(code)

	out[0] = u8(value >> 24)
	out[1] = u8(value >> 16)
	out[2] = u8(value >> 8)
	out[3] = u8(value)

	return true
}

decode_rst_stream_payload :: proc(
	header: Frame_Header,
	payload: []u8,
) -> (
	code: Error_Code,
	ok: bool,
) {
	if header.kind != .Rst_Stream {
		return .No_Error, false
	}

	if header.stream_id == 0 {
		return .No_Error, false
	}

	if !payload_length_matches(header, payload) {
		return .No_Error, false
	}

	if len(payload) != RST_STREAM_PAYLOAD_LEN {
		return .No_Error, false
	}

	value := u32(payload[0]) << 24 | u32(payload[1]) << 16 | u32(payload[2]) << 8 | u32(payload[3])

	return Error_Code(value), true
}

encode_settings_payload :: proc(params: []Settings_Parameter, out: []u8) -> (written: int, ok: bool) {
	if len(params) > max(int) / SETTINGS_ENTRY_LEN {
		return 0, false
	}

	total := len(params) * SETTINGS_ENTRY_LEN

	if total > len(out) {
		return 0, false
	}

	for i in 0 ..< len(params) {
		if !settings_parameter_valid(params[i].id, params[i].value) {
			return 0, false
		}

		base := i * SETTINGS_ENTRY_LEN

		out[base + 0] = u8(params[i].id >> 8)
		out[base + 1] = u8(params[i].id)
		out[base + 2] = u8(params[i].value >> 24)
		out[base + 3] = u8(params[i].value >> 16)
		out[base + 4] = u8(params[i].value >> 8)
		out[base + 5] = u8(params[i].value)
	}

	return total, true
}

decode_settings_payload :: proc(
	header: Frame_Header,
	payload: []u8,
	params_out: []Settings_Parameter,
) -> (
	count: int,
	ok: bool,
) {
	if header.kind != .Settings {
		return 0, false
	}

	if header.stream_id != 0 {
		return 0, false
	}

	if !payload_length_matches(header, payload) {
		return 0, false
	}

	if header.flags & SETTINGS_ACK != 0 {
		if len(payload) != 0 {
			return 0, false
		}

		return 0, true
	}

	if len(payload) % SETTINGS_ENTRY_LEN != 0 {
		return 0, false
	}

	need := len(payload) / SETTINGS_ENTRY_LEN

	if need > len(params_out) {
		return 0, false
	}

	for i in 0 ..< need {
		base := i * SETTINGS_ENTRY_LEN

		id := u16(payload[base]) << 8 | u16(payload[base + 1])

		value :=
			u32(payload[base + 2]) << 24 |
			u32(payload[base + 3]) << 16 |
			u32(payload[base + 4]) << 8 |
			u32(payload[base + 5])

		if !settings_parameter_valid(id, value) {
			return 0, false
		}

		params_out[i] = Settings_Parameter{id = id, value = value}
	}

	return need, true
}

encode_ping_payload :: proc(opaque: [8]u8, out: []u8) -> bool {
	if len(out) < PING_PAYLOAD_LEN {
		return false
	}

	for i in 0 ..< PING_PAYLOAD_LEN {
		out[i] = opaque[i]
	}

	return true
}

decode_ping_payload :: proc(
	header: Frame_Header,
	payload: []u8,
) -> (
	opaque: [8]u8,
	ok: bool,
) {
	if header.kind != .Ping {
		return {}, false
	}

	if header.stream_id != 0 {
		return {}, false
	}

	if !payload_length_matches(header, payload) {
		return {}, false
	}

	if len(payload) != PING_PAYLOAD_LEN {
		return {}, false
	}

	for i in 0 ..< PING_PAYLOAD_LEN {
		opaque[i] = payload[i]
	}

	return opaque, true
}

encode_goaway_payload :: proc(
	last_stream_id: u32,
	code: Error_Code,
	debug_data: []u8,
	out: []u8,
) -> (
	written: int,
	ok: bool,
) {
	if last_stream_id > MAX_STREAM_ID {
		return 0, false
	}

	if len(debug_data) > max(int) - GOAWAY_FIXED_LEN {
		return 0, false
	}

	total := GOAWAY_FIXED_LEN + len(debug_data)

	if total > len(out) {
		return 0, false
	}

	out[0] = u8(last_stream_id >> 24)
	out[1] = u8(last_stream_id >> 16)
	out[2] = u8(last_stream_id >> 8)
	out[3] = u8(last_stream_id)

	value := u32(code)

	out[4] = u8(value >> 24)
	out[5] = u8(value >> 16)
	out[6] = u8(value >> 8)
	out[7] = u8(value)

	for i in 0 ..< len(debug_data) {
		out[GOAWAY_FIXED_LEN + i] = debug_data[i]
	}

	return total, true
}

decode_goaway_payload :: proc(
	header: Frame_Header,
	payload: []u8,
) -> (
	last_stream_id: u32,
	code: Error_Code,
	debug_data: []u8,
	ok: bool,
) {
	if header.kind != .GoAway {
		return 0, .No_Error, nil, false
	}

	if header.stream_id != 0 {
		return 0, .No_Error, nil, false
	}

	if !payload_length_matches(header, payload) {
		return 0, .No_Error, nil, false
	}

	if len(payload) < GOAWAY_FIXED_LEN {
		return 0, .No_Error, nil, false
	}

	last := u32(payload[0]) << 24 | u32(payload[1]) << 16 | u32(payload[2]) << 8 | u32(payload[3])

	// NOTE: the reserved top bit is masked rather than rejected; senders must clear it.
	last &= MAX_STREAM_ID

	value := u32(payload[4]) << 24 | u32(payload[5]) << 16 | u32(payload[6]) << 8 | u32(payload[7])

	return last, Error_Code(value), payload[GOAWAY_FIXED_LEN:], true
}

encode_window_update_payload :: proc(increment: u32, out: []u8) -> bool {
	if increment == 0 || increment > MAX_WINDOW_INCREMENT {
		return false
	}

	if len(out) < WINDOW_UPDATE_PAYLOAD_LEN {
		return false
	}

	out[0] = u8(increment >> 24)
	out[1] = u8(increment >> 16)
	out[2] = u8(increment >> 8)
	out[3] = u8(increment)

	return true
}

decode_window_update_payload :: proc(
	header: Frame_Header,
	payload: []u8,
) -> (
	increment: u32,
	ok: bool,
) {
	if header.kind != .Window_Update {
		return 0, false
	}

	if !payload_length_matches(header, payload) {
		return 0, false
	}

	if len(payload) != WINDOW_UPDATE_PAYLOAD_LEN {
		return 0, false
	}

	// NOTE: the reserved top bit is ignored on receipt per RFC 9113 section 6.9.
	increment =
		u32(payload[0] & 0x7f) << 24 |
		u32(payload[1]) << 16 |
		u32(payload[2]) << 8 |
		u32(payload[3])

	if increment == 0 {
		return 0, false
	}

	return increment, true
}

encode_continuation_payload :: proc(fragment: []u8, out: []u8) -> (written: int, ok: bool) {
	if len(fragment) > len(out) {
		return 0, false
	}

	for i in 0 ..< len(fragment) {
		out[i] = fragment[i]
	}

	return len(fragment), true
}

decode_continuation_payload :: proc(
	header: Frame_Header,
	payload: []u8,
) -> (
	fragment: []u8,
	ok: bool,
) {
	if header.kind != .Continuation {
		return nil, false
	}

	if header.stream_id == 0 {
		return nil, false
	}

	if !payload_length_matches(header, payload) {
		return nil, false
	}

	return payload, true
}
