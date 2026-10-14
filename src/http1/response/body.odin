package response

import "veylor:http1"

Response_Body_Result :: enum {
	Written,
	Buffer_Small,
	Invalid,
}

Response_Body_Event :: struct {
	written: int,
	result:  Response_Body_Result,
}

FORBIDDEN_RESPONSE_TRAILERS :: [?]string {
	"host",
	"trailer",
	"connection",
	"keep-alive",
	"proxy-connection",
	"upgrade",
	"te",
	"authorization",
	"proxy-authorization",
	"expect",
	"content-encoding",
	"content-type",
	"content-range",
}

encode_response_chunk :: proc(data, output: []u8) -> Response_Body_Event {
	if len(data) == 0 {
		return Response_Body_Event{result = .Invalid}
	}
	hex_bytes: [16]u8
	hex_start := hexadecimal_u64(u64(len(data)), hex_bytes[:])
	hex_text := hex_bytes[hex_start:]

	if len(hex_text) > len(output) {
		return Response_Body_Event{result = .Buffer_Small}
	}

	remaining_output := len(output) - len(hex_text)

	if len(data) > remaining_output {
		return Response_Body_Event{result = .Buffer_Small}
	}
	remaining_output -= len(data)

	if 4 > remaining_output {
		return Response_Body_Event{result = .Buffer_Small}
	}

	position := write_response_bytes(output, 0, hex_text)
	position = write_response_string(output, position, "\r\n")
	position = write_response_bytes(output, position, data)
	position = write_response_string(output, position, "\r\n")
	return Response_Body_Event{written = position, result = .Written}
}

encode_response_final_chunk :: proc(
	trailers: []Response_Header,
	output: []u8,
) -> Response_Body_Event {
	for trailer in trailers {
		if !valid_response_header(trailer) {
			return Response_Body_Event{result = .Invalid}
		}

		if !valid_response_trailer(trailer.name) {
			return Response_Body_Event{result = .Invalid}
		}
	}

	required := 3
	for trailer in trailers {
		if !reserve_response_bytes(&required, len(trailer.name), len(output)) {
			return Response_Body_Event{result = .Buffer_Small}
		}

		if !reserve_response_bytes(&required, 2, len(output)) {
			return Response_Body_Event{result = .Buffer_Small}
		}

		if !reserve_response_bytes(&required, len(trailer.value), len(output)) {
			return Response_Body_Event{result = .Buffer_Small}
		}

		if !reserve_response_bytes(&required, 2, len(output)) {
			return Response_Body_Event{result = .Buffer_Small}
		}
	}

	if !reserve_response_bytes(&required, 2, len(output)) {
		return Response_Body_Event{result = .Buffer_Small}
	}

	position := write_response_string(output, 0, "0\r\n")
	for trailer in trailers {
		position = write_response_bytes(output, position, trailer.name)
		position = write_response_string(output, position, ": ")
		position = write_response_bytes(output, position, trailer.value)
		position = write_response_string(output, position, "\r\n")
	}
	position = write_response_string(output, position, "\r\n")
	return Response_Body_Event{written = position, result = .Written}
}

valid_response_trailer :: proc(name: []u8) -> bool {
	for forbidden in FORBIDDEN_RESPONSE_TRAILERS {
		if http1.ascii_case_insensitive_equal(name, forbidden) {
			return false
		}
	}
	return true
}

hexadecimal_u64 :: proc(value: u64, output: []u8) -> int {
	position := len(output)
	remaining := value
	for {
		position -= 1
		digit := remaining % 16

		if digit < 10 {
			output[position] = u8('0' + digit)
		} else {
			output[position] = u8('a' + digit - 10)
		}
		remaining /= 16

		if remaining == 0 {
			break
		}
	}
	return position
}
