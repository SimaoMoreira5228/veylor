package response

import "veylor:http1"

Connection_Policy :: enum {
	Close,
	Keep_Alive,
}

Response_Connection_Header :: enum {
	Omit,
	Close,
	Keep_Alive,
}

Response_Header :: struct {
	name:  []u8,
	value: []u8,
}

Response_Framing :: enum {
	Fixed_Length,
	Chunked,
	Close_Delimited,
}

Response_Head_Result :: enum {
	Written,
	Buffer_Small,
	Invalid,
}

Response_Head_Event :: struct {
	written:      int,
	result:       Response_Head_Result,
	body_allowed: bool,
}

encode_response_head :: proc(
	request_method: []u8,
	status: u16,
	reason: []u8,
	headers: []Response_Header,
	framing: Response_Framing,
	content_length: u64,
	output: []u8,
	connection_header := Response_Connection_Header.Omit,
) -> Response_Head_Event {
	if status < 100 || status > 599 {
		return Response_Head_Event{result = .Invalid}
	}

	if !valid_response_value(reason) {
		return Response_Head_Event{result = .Invalid}
	}

	if !valid_request_method(request_method) {
		return Response_Head_Event{result = .Invalid}
	}

	head_request := http1.bytes_equal(request_method, "HEAD")
	connect_tunnel := http1.bytes_equal(request_method, "CONNECT") && status >= 200 && status < 300
	content_length_allowed := status >= 200 && status != 204 && !connect_tunnel
	body_allowed := !head_request && status >= 200

	if status == 204 {
		body_allowed = false
	}

	if status == 205 {
		body_allowed = false
	}

	if status == 304 {
		body_allowed = false
	}

	if connect_tunnel {
		body_allowed = false
	}

	chunked_allowed := status >= 200

	if status == 204 {
		chunked_allowed = false
	}

	if status == 205 {
		chunked_allowed = false
	}

	if connect_tunnel {
		chunked_allowed = false
	}

	if !body_allowed && !head_request && status != 304 {
		chunked_allowed = false
	}

	valid_framing := framing == .Fixed_Length

	if framing == .Chunked {
		valid_framing = true
	}

	if framing == .Close_Delimited {
		valid_framing = true
	}

	if !valid_framing {
		return Response_Head_Event{result = .Invalid}
	}

	valid_connection_header := connection_header == .Omit

	if connection_header == .Close {
		valid_connection_header = true
	}

	if connection_header == .Keep_Alive {
		valid_connection_header = true
	}

	if !valid_connection_header {
		return Response_Head_Event{result = .Invalid}
	}

	if status < 200 && connection_header != .Omit {
		return Response_Head_Event{result = .Invalid}
	}

	if connect_tunnel && connection_header != .Omit {
		return Response_Head_Event{result = .Invalid}
	}

	if !content_length_allowed && content_length != 0 {
		return Response_Head_Event{result = .Invalid}
	}

	if status == 205 && content_length != 0 {
		return Response_Head_Event{result = .Invalid}
	}

	if framing == .Chunked {
		if !chunked_allowed || content_length != 0 {
			return Response_Head_Event{result = .Invalid}
		}
	}

	if framing == .Close_Delimited {
		if !body_allowed {
			return Response_Head_Event{result = .Invalid}
		}

		if connection_header != .Close {
			return Response_Head_Event{result = .Invalid}
		}

		if content_length != 0 {
			return Response_Head_Event{result = .Invalid}
		}
	}

	for header in headers {
		if !valid_response_header(header) {
			return Response_Head_Event{result = .Invalid}
		}
	}

	status_bytes: [3]u8
	status_bytes[0] = u8('0' + status / 100)
	status_bytes[1] = u8('0' + (status / 10) % 10)
	status_bytes[2] = u8('0' + status % 10)

	length_bytes: [20]u8
	length_start := decimal_u64(content_length, length_bytes[:])
	length_text := length_bytes[length_start:]

	required := 0

	if !reserve_response_bytes(&required, 9, len(output)) {
		return Response_Head_Event{result = .Buffer_Small}
	}

	if !reserve_response_bytes(&required, len(status_bytes), len(output)) {
		return Response_Head_Event{result = .Buffer_Small}
	}

	if !reserve_response_bytes(&required, 1, len(output)) {
		return Response_Head_Event{result = .Buffer_Small}
	}

	if !reserve_response_bytes(&required, len(reason), len(output)) {
		return Response_Head_Event{result = .Buffer_Small}
	}

	if !reserve_response_bytes(&required, 2, len(output)) {
		return Response_Head_Event{result = .Buffer_Small}
	}

	for header in headers {
		if !reserve_response_bytes(&required, len(header.name), len(output)) {
			return Response_Head_Event{result = .Buffer_Small}
		}

		if !reserve_response_bytes(&required, 2, len(output)) {
			return Response_Head_Event{result = .Buffer_Small}
		}

		if !reserve_response_bytes(&required, len(header.value), len(output)) {
			return Response_Head_Event{result = .Buffer_Small}
		}

		if !reserve_response_bytes(&required, 2, len(output)) {
			return Response_Head_Event{result = .Buffer_Small}
		}
	}

	if connection_header == .Close {
		if !reserve_response_bytes(&required, len("Connection: close\r\n"), len(output)) {
			return Response_Head_Event{result = .Buffer_Small}
		}
	}

	if connection_header == .Keep_Alive {
		if !reserve_response_bytes(&required, len("Connection: keep-alive\r\n"), len(output)) {
			return Response_Head_Event{result = .Buffer_Small}
		}
	}

	if framing == .Fixed_Length && content_length_allowed {
		if !reserve_response_bytes(&required, len("Content-Length: "), len(output)) {
			return Response_Head_Event{result = .Buffer_Small}
		}

		if !reserve_response_bytes(&required, len(length_text), len(output)) {
			return Response_Head_Event{result = .Buffer_Small}
		}

		if !reserve_response_bytes(&required, 4, len(output)) {
			return Response_Head_Event{result = .Buffer_Small}
		}
	}

	if framing == .Chunked {
		if !reserve_response_bytes(&required, len("Transfer-Encoding: chunked\r\n"), len(output)) {
			return Response_Head_Event{result = .Buffer_Small}
		}
	}

	position := 0
	position = write_response_string(output, position, "HTTP/1.1 ")
	position = write_response_bytes(output, position, status_bytes[:])
	position = write_response_string(output, position, " ")
	position = write_response_bytes(output, position, reason)
	position = write_response_string(output, position, "\r\n")
	for header in headers {
		position = write_response_bytes(output, position, header.name)
		position = write_response_string(output, position, ": ")
		position = write_response_bytes(output, position, header.value)
		position = write_response_string(output, position, "\r\n")
	}

	if connection_header == .Close {
		position = write_response_string(output, position, "Connection: close\r\n")
	} else if connection_header == .Keep_Alive {
		position = write_response_string(output, position, "Connection: keep-alive\r\n")
	}

	if framing == .Fixed_Length && content_length_allowed {
		position = write_response_string(output, position, "Content-Length: ")
		position = write_response_bytes(output, position, length_text)
		position = write_response_string(output, position, "\r\n")
	} else if framing == .Chunked {
		position = write_response_string(output, position, "Transfer-Encoding: chunked\r\n")
	}
	position = write_response_string(output, position, "\r\n")
	return Response_Head_Event{written = position, result = .Written, body_allowed = body_allowed}
}

response_connection_header :: proc(
	request_method: []u8,
	status: u16,
	policy: Connection_Policy,
	request_is_http11: bool,
) -> Response_Connection_Header {
	if status < 200 || (http1.bytes_equal(request_method, "CONNECT") && status < 300) {
		return .Omit
	}

	if policy == .Close {
		return .Close
	}

	if !request_is_http11 {
		return .Keep_Alive
	}
	return .Omit
}

request_connection_policy :: proc(
	head: http1.Request_Head,
	version: []u8,
	response_is_self_delimited: bool,
	request_body_complete: bool,
	response_close: bool,
) -> Connection_Policy {
	if !response_is_self_delimited {
		return .Close
	}

	if !request_body_complete {
		return .Close
	}

	if response_close {
		return .Close
	}

	if head.connection.close {
		return .Close
	}

	if http1.bytes_equal(version, "HTTP/1.1") {
		return .Keep_Alive
	}

	if http1.bytes_equal(version, "HTTP/1.0") && head.connection.keep_alive {
		return .Keep_Alive
	}
	return .Close
}

valid_request_method :: proc(method: []u8) -> bool {
	if len(method) == 0 {
		return false
	}
	for c in method {
		if !http1.is_token(c) {
			return false
		}
	}
	return true
}

valid_response_header :: proc(header: Response_Header) -> bool {
	if len(header.name) == 0 {
		return false
	}

	if !valid_response_value(header.value) {
		return false
	}

	for c in header.name {
		if !http1.is_token(c) {
			return false
		}
	}

	if http1.ascii_case_insensitive_equal(header.name, "content-length") {
		return false
	}

	if http1.ascii_case_insensitive_equal(header.name, "transfer-encoding") {
		return false
	}

	if http1.ascii_case_insensitive_equal(header.name, "connection") {
		return false
	}
	return true
}

valid_response_value :: proc(value: []u8) -> bool {
	for c in value {
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return false
		}
	}
	return true
}

reserve_response_bytes :: proc(required: ^int, amount, capacity: int) -> bool {
	if amount > capacity - required^ {
		return false
	}
	required^ += amount
	return true
}

write_response_bytes :: proc(output: []u8, position: int, value: []u8) -> int {
	for c, i in value {
		output[position + i] = c
	}
	return position + len(value)
}

write_response_string :: proc(output: []u8, position: int, value: string) -> int {
	for c, i in value {
		output[position + i] = u8(c)
	}
	return position + len(value)
}

decimal_u64 :: proc(value: u64, output: []u8) -> int {
	position := len(output)
	remaining := value
	for {
		position -= 1
		output[position] = u8('0' + remaining % 10)
		remaining /= 10

		if remaining == 0 {
			break
		}
	}
	return position
}
