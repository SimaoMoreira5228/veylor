package response

Response_Write_State :: enum {
	Pending,
	Complete,
	Invalid,
}

Response_Write_Cursor :: struct {
	bytes:  []u8,
	offset: int,
	state:  Response_Write_State,
}

// NOTE: The caller keeps the byte slice alive and unchanged until the cursor completes.
begin_response_write :: proc(bytes: []u8) -> Response_Write_Cursor {
	state := Response_Write_State.Pending

	if len(bytes) == 0 {
		state = .Complete
	}
	return Response_Write_Cursor{bytes = bytes, state = state}
}
response_write_pending :: proc(cursor: ^Response_Write_Cursor) -> []u8 {
	if cursor.state != .Pending || cursor.offset < 0 || cursor.offset > len(cursor.bytes) {
		return nil
	}
	return cursor.bytes[cursor.offset:]
}

advance_response_write :: proc(
	cursor: ^Response_Write_Cursor,
	accepted_bytes: int,
) -> Response_Write_State {
	if cursor.state != .Pending {
		cursor.state = .Invalid
		return .Invalid
	}

	if cursor.offset < 0 {
		cursor.state = .Invalid
		return .Invalid
	}

	if cursor.offset > len(cursor.bytes) {
		cursor.state = .Invalid
		return .Invalid
	}

	if accepted_bytes < 0 {
		cursor.state = .Invalid
		return .Invalid
	}

	if accepted_bytes > len(cursor.bytes) - cursor.offset {
		cursor.state = .Invalid
		return .Invalid
	}
	cursor.offset += accepted_bytes

	if cursor.offset == len(cursor.bytes) {
		cursor.state = .Complete
	}
	return cursor.state
}
