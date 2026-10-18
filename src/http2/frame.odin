package http2

FRAME_HEADER_LEN :: 9
DEFAULT_MAX_FRAME_SIZE :: u32(16384)
MAX_STREAM_ID :: u32(0x7fffffff)

Frame_Type :: enum u8 {
	Data          = 0,
	Headers       = 1,
	Priority      = 2,
	Rst_Stream    = 3,
	Settings      = 4,
	Push_Promise  = 5,
	Ping          = 6,
	GoAway        = 7,
	Window_Update = 8,
	Continuation  = 9,
	Unknown       = 255,
}

Frame_Header :: struct {
	length:    u32,
	kind:      Frame_Type,
	raw_type:  u8,
	flags:     u8,
	stream_id: u32,
}

frame_type_from_byte :: proc(type_byte: u8) -> (kind: Frame_Type, raw_type: u8) {
	switch type_byte {
	case 0:
		return .Data, type_byte
	case 1:
		return .Headers, type_byte
	case 2:
		return .Priority, type_byte
	case 3:
		return .Rst_Stream, type_byte
	case 4:
		return .Settings, type_byte
	case 5:
		return .Push_Promise, type_byte
	case 6:
		return .Ping, type_byte
	case 7:
		return .GoAway, type_byte
	case 8:
		return .Window_Update, type_byte
	case 9:
		return .Continuation, type_byte
	}

	return .Unknown, type_byte
}

parse_frame_header :: proc(input: []u8) -> (header: Frame_Header, ok: bool) {
	if len(input) < FRAME_HEADER_LEN {
		return {}, false
	}

	length := u32(input[0]) << 16 | u32(input[1]) << 8 | u32(input[2])

	if length > DEFAULT_MAX_FRAME_SIZE {
		return {}, false
	}

	kind, raw_type := frame_type_from_byte(input[3])

	flags := input[4]

	if input[5] & 0x80 != 0 {
		return {}, false
	}

	stream_id := u32(input[5]) << 24 | u32(input[6]) << 16 | u32(input[7]) << 8 | u32(input[8])

	header = Frame_Header {
		length    = length,
		kind      = kind,
		raw_type  = raw_type,
		flags     = flags,
		stream_id = stream_id,
	}

	return header, true
}

encode_frame_header :: proc(header: Frame_Header, out: []u8) -> bool {
	if len(out) < FRAME_HEADER_LEN {
		return false
	}

	if header.length > 0xffffff {
		return false
	}

	if header.stream_id > MAX_STREAM_ID {
		return false
	}

	type_byte := header.raw_type

	if header.kind != .Unknown {
		type_byte = u8(header.kind)
	}

	out[0] = u8(header.length >> 16)
	out[1] = u8(header.length >> 8)
	out[2] = u8(header.length)
	out[3] = type_byte
	out[4] = header.flags
	out[5] = u8(header.stream_id >> 24)
	out[6] = u8(header.stream_id >> 16)
	out[7] = u8(header.stream_id >> 8)
	out[8] = u8(header.stream_id)

	return true
}
