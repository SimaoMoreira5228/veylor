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

Fixed_Response_Body_Writer :: struct {
	content_length: u64,
	queued_bytes:   u64,
	written_bytes:  u64,
	current:        Response_Write_Cursor,
	has_current:    bool,
	finish_called:  bool,
	state:          Response_Write_State,
}

// NOTE: The caller keeps the byte slice alive and unchanged until the cursor completes.
begin_response_write :: proc(bytes: []u8) -> Response_Write_Cursor {
	state := Response_Write_State.Pending

	if len(bytes) == 0 {
		state = .Complete
	}
	return Response_Write_Cursor{bytes = bytes, state = state}
}

begin_fixed_response_body :: proc(
	content_length: u64,
	body_allowed: bool,
) -> (
	writer: Fixed_Response_Body_Writer,
	result: Response_Write_State,
) {
	if !body_allowed {
		writer.state = .Complete
		return writer, writer.state
	}
	writer.content_length = content_length
	writer.state = .Pending

	if content_length == 0 {
		writer.state = .Complete
	}
	return writer, writer.state
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

queue_fixed_response_chunk :: proc(
	writer: ^Fixed_Response_Body_Writer,
	bytes: []u8,
) -> Response_Write_State {
	if writer.state != .Pending || writer.finish_called || writer.has_current {
		writer.state = .Invalid
		return .Invalid
	}

	if len(bytes) == 0 {
		return .Pending
	}

	if writer.queued_bytes > writer.content_length {
		writer.state = .Invalid
		return .Invalid
	}

	if u64(len(bytes)) > writer.content_length - writer.queued_bytes {
		writer.state = .Invalid
		return .Invalid
	}
	writer.current = begin_response_write(bytes)
	writer.has_current = true
	writer.queued_bytes += u64(len(bytes))
	return .Pending
}

fixed_response_body_pending :: proc(writer: ^Fixed_Response_Body_Writer) -> []u8 {
	if writer.state != .Pending || !writer.has_current {
		return nil
	}
	return response_write_pending(&writer.current)
}

advance_fixed_response_body :: proc(
	writer: ^Fixed_Response_Body_Writer,
	accepted_bytes: int,
) -> Response_Write_State {
	if writer.state != .Pending || !writer.has_current {
		writer.state = .Invalid
		return .Invalid
	}

	current_result := advance_response_write(&writer.current, accepted_bytes)

	if current_result == .Invalid {
		writer.state = .Invalid
		return .Invalid
	}

	if current_result == .Complete {
		writer.written_bytes += u64(len(writer.current.bytes))
		writer.current = Response_Write_Cursor{}
		writer.has_current = false

		if writer.finish_called {
			finish_fixed_response_body(writer)
		} else if writer.written_bytes == writer.content_length {
			writer.state = .Complete
		}
		return writer.state
	}
	return writer.state
}

finish_fixed_response_body :: proc(writer: ^Fixed_Response_Body_Writer) -> Response_Write_State {
	if writer.state == .Complete || writer.state == .Invalid {
		return writer.state
	}
	writer.finish_called = true

	if writer.has_current {
		return .Pending
	}

	if writer.written_bytes != writer.content_length {
		writer.state = .Invalid
		return .Invalid
	}
	writer.state = .Complete
	return .Complete
}
