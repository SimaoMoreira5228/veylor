package body

import "veylor:http1"

Chunked_Result :: enum {
	Need_More,
	Data,
	Trailer,
	Complete,
	Invalid,
	Too_Large,
	Span_Capacity,
}

Chunked_Event :: struct {
	span_count:    int,
	consumed:      int,
	result:        Chunked_Result,
	trailer_name:  []u8,
	trailer_value: []u8,
}

Chunked_Limits :: struct {
	total_body_bytes: u64,
	chunk_line_bytes: int,
	extension_bytes:  int,
	trailer_bytes:    int,
	trailer_count:    int,
}

Chunked_Trailer_Context :: struct {
	headers:                 []http1.Header,
	head_input:              []u8,
	permitted_trailer_names: []string,
}

DEFAULT_CHUNKED_LIMITS :: Chunked_Limits {
	total_body_bytes = 16 * 1024 * 1024,
	chunk_line_bytes = 1024,
	extension_bytes  = 512,
	trailer_bytes    = 8 * 1024,
	trailer_count    = 32,
}

Chunked_State :: enum {
	Size_Line,
	Data,
	Data_CR,
	Data_LF,
	Trailer_Line,
	Complete,
	Error,
}

Chunked_Decoder :: struct {
	state:                Chunked_State,
	result:               Chunked_Result,
	chunk_remaining:      u64,
	total_body_bytes:     u64,
	line_length:          int,
	line_cr:              bool,
	extension_started:    bool,
	extension_bytes_seen: int,
	trailer_bytes_seen:   int,
	trailer_count_seen:   int,
	scratch:              [1024]u8,
}

// NOTE: Data spans borrow input; trailer slices borrow decoder scratch and expire on the next call.
decode_chunked_body :: proc(
	decoder: ^Chunked_Decoder,
	input: []u8,
	spans: []http1.Span,
	trailer_context: Chunked_Trailer_Context,
	limits := DEFAULT_CHUNKED_LIMITS,
) -> Chunked_Event {
	if decoder.state == .Complete {
		return Chunked_Event{result = .Complete}
	}

	if decoder.state == .Error {
		return Chunked_Event{result = decoder.result}
	}

	span_count := 0
	for i := 0; i < len(input); {
		switch decoder.state {
		case .Size_Line, .Trailer_Line:
			is_trailer := decoder.state == .Trailer_Line
			pending_trailer_bytes := decoder.line_length + 1

			if decoder.line_cr {
				pending_trailer_bytes = decoder.line_length + 2
			}

			if is_trailer {
				if limits.trailer_bytes < 0 {
					return chunked_event(decoder, span_count, i, .Too_Large)
				}

				if decoder.trailer_bytes_seen < 0 {
					return chunked_event(decoder, span_count, i, .Too_Large)
				}

				if decoder.trailer_bytes_seen > limits.trailer_bytes {
					return chunked_event(decoder, span_count, i, .Too_Large)
				}

				remaining_trailer_bytes := limits.trailer_bytes - decoder.trailer_bytes_seen

				if pending_trailer_bytes > remaining_trailer_bytes {
					return chunked_event(decoder, span_count, i, .Too_Large)
				}
			}

			if decoder.line_cr {
				if input[i] != '\n' {
					i += 1
					return chunked_event(decoder, span_count, i, .Invalid)
				}
				i += 1
				decoder.line_cr = false

				if is_trailer {
					decoder.trailer_bytes_seen += decoder.line_length + 2

					if decoder.line_length == 0 {
						decoder.state = .Complete
						return chunked_event(decoder, span_count, i, .Complete)
					}

					if decoder.trailer_bytes_seen > limits.trailer_bytes {
						return chunked_event(decoder, span_count, i, .Too_Large)
					}

					name, value, valid := parse_trailer(decoder.scratch[:decoder.line_length])

					if !valid {
						return chunked_event(decoder, span_count, i, .Invalid)
					}

					if !request_trailer_allowed(name, trailer_context) {
						return chunked_event(decoder, span_count, i, .Invalid)
					}

					decoder.line_length = 0
					decoder.trailer_count_seen += 1

					if decoder.trailer_count_seen > limits.trailer_count {
						return chunked_event(decoder, span_count, i, .Too_Large)
					}

					return Chunked_Event {
						span_count = span_count,
						consumed = i,
						result = .Trailer,
						trailer_name = name,
						trailer_value = value,
					}
				}

				size, valid, too_large := parse_chunk_size(decoder.scratch[:decoder.line_length])

				if too_large {
					return chunked_event(decoder, span_count, i, .Too_Large)
				}

				if !valid {
					return chunked_event(decoder, span_count, i, .Invalid)
				}
				decoder.line_length = 0
				decoder.extension_started = false
				decoder.extension_bytes_seen = 0

				if decoder.total_body_bytes > limits.total_body_bytes {
					return chunked_event(decoder, span_count, i, .Too_Large)
				}

				remaining_body_bytes := limits.total_body_bytes - decoder.total_body_bytes

				if size > remaining_body_bytes {
					return chunked_event(decoder, span_count, i, .Too_Large)
				}

				if size == 0 {
					decoder.state = .Trailer_Line
				} else {
					decoder.chunk_remaining = size
					decoder.state = .Data
				}
				continue
			}

			c := input[i]

			if c == '\r' {
				i += 1
				decoder.line_cr = true
				continue
			}

			if c == '\n' {
				i += 1
				return chunked_event(decoder, span_count, i, .Invalid)
			}

			if decoder.line_length >= limits.chunk_line_bytes {
				return chunked_event(decoder, span_count, i, .Too_Large)
			}

			if decoder.line_length >= len(decoder.scratch) {
				return chunked_event(decoder, span_count, i, .Too_Large)
			}

			if !is_trailer && !decoder.extension_started && !http1.is_hex(c) {
				decoder.extension_started = true
				decoder.extension_bytes_seen = 0
			}

			if !is_trailer && decoder.extension_started {
				if decoder.extension_bytes_seen >= limits.extension_bytes {
					return chunked_event(decoder, span_count, i, .Too_Large)
				}
				decoder.extension_bytes_seen += 1
			}
			i += 1
			decoder.scratch[decoder.line_length] = c
			decoder.line_length += 1
		case .Data:
			n := min(u64(len(input) - i), decoder.chunk_remaining)

			if n > 0 {
				if span_count >= len(spans) {
					return chunked_event(decoder, span_count, i, .Span_Capacity)
				}
				spans[span_count] = http1.Span {
					offset = i,
					length = int(n),
				}
				span_count += 1
				i += int(n)
				decoder.chunk_remaining -= n
				decoder.total_body_bytes += n
			}

			if decoder.chunk_remaining == 0 {
				decoder.state = .Data_CR
			}
		case .Data_CR:
			if input[i] != '\r' {
				i += 1
				return chunked_event(decoder, span_count, i, .Invalid)
			}

			i += 1
			decoder.state = .Data_LF
		case .Data_LF:
			if input[i] != '\n' {
				i += 1
				return chunked_event(decoder, span_count, i, .Invalid)
			}

			i += 1
			decoder.state = .Size_Line
		case .Complete, .Error:
			return chunked_event(decoder, span_count, i, decoder.result)
		}
	}

	if span_count > 0 {
		return chunked_event(decoder, span_count, len(input), .Data)
	}
	return chunked_event(decoder, 0, len(input), .Need_More)
}

request_trailer_allowed :: proc(
	trailer_name: []u8,
	trailer_context: Chunked_Trailer_Context,
) -> bool {
	permitted := false
	for allowed_name in trailer_context.permitted_trailer_names {
		if http1.ascii_case_insensitive_equal(trailer_name, allowed_name) {
			permitted = true
			break
		}
	}

	if !permitted {
		return false
	}

	for header in trailer_context.headers {
		if header.name.offset < 0 || header.name.length < 0 {
			return false
		}

		if header.name.offset > len(trailer_context.head_input) {
			return false
		}

		if header.name.length > len(trailer_context.head_input) - header.name.offset {
			return false
		}

		if header.value.offset < 0 || header.value.length < 0 {
			return false
		}

		if header.value.offset > len(trailer_context.head_input) {
			return false
		}

		if header.value.length > len(trailer_context.head_input) - header.value.offset {
			return false
		}

		name := trailer_context.head_input[header.name.offset:header.name.offset +
		header.name.length]

		if !http1.ascii_case_insensitive_equal(name, "connection") {
			continue
		}

		value := trailer_context.head_input[header.value.offset:header.value.offset +
		header.value.length]
		nominated, valid := http1.connection_option_present(value, trailer_name)

		if !valid {
			return false
		}

		if nominated {
			return false
		}
	}
	return true
}

chunked_event :: proc(
	decoder: ^Chunked_Decoder,
	span_count: int,
	consumed: int,
	result: Chunked_Result,
) -> Chunked_Event {
	if result == .Invalid || result == .Too_Large {
		decoder.state = .Error
		decoder.result = result
	}
	return Chunked_Event{span_count = span_count, consumed = consumed, result = result}
}

parse_chunk_size :: proc(line: []u8) -> (size: u64, valid: bool, too_large: bool) {
	i := 0
	for i < len(line) && http1.is_hex(line[i]) {
		c := line[i]
		digit: u64

		if c >= '0' && c <= '9' {
			digit = u64(c - '0')
		} else if c >= 'a' && c <= 'f' {
			digit = u64(c - 'a' + 10)
		} else if c >= 'A' && c <= 'F' {
			digit = u64(c - 'A' + 10)
		} else {
			return 0, false, false
		}

		if size > (~u64(0) - digit) / 16 {
			return 0, false, true
		}
		size = size * 16 + digit
		i += 1
	}

	if i == len(line) {
		return size, i > 0, false
	}

	if i == 0 {
		return 0, false, false
	}

	for i < len(line) && http1.is_ows(line[i]) {
		i += 1
	}

	if i == len(line) || line[i] != ';' {
		return 0, false, false
	}

	if !valid_extensions(line[i:]) {
		return 0, false, false
	}
	return size, true, false
}

valid_extensions :: proc(bytes: []u8) -> bool {
	i := 0
	for i < len(bytes) {
		i = skip_bws(bytes, i)

		if i == len(bytes) || bytes[i] != ';' {
			return false
		}
		i += 1
		i = skip_bws(bytes, i)
		start := i
		for i < len(bytes) && http1.is_token(bytes[i]) {
			i += 1
		}

		if i == start {
			return false
		}

		name_end := i
		i = skip_bws(bytes, i)

		if i == len(bytes) && i != name_end {
			return false
		}

		if i < len(bytes) && bytes[i] == '=' {
			i += 1
			value_start := i
			i = skip_bws(bytes, i)

			if i == len(bytes) && i != value_start {
				return false
			}

			if i < len(bytes) && bytes[i] == '"' {
				quoted_end, valid := http1.parse_quoted_string(bytes, i)

				if !valid {
					return false
				}
				i = quoted_end
			} else {
				start = i
				for i < len(bytes) && http1.is_token(bytes[i]) {
					i += 1
				}

				if i == start {
					return false
				}
			}

			value_end := i
			i = skip_bws(bytes, i)

			if i == len(bytes) && i != value_end {
				return false
			}
		}

		if i < len(bytes) && bytes[i] != ';' {
			return false
		}
	}
	return true
}

skip_bws :: proc(bytes: []u8, i: int) -> int {
	index := i
	for index < len(bytes) && http1.is_ows(bytes[index]) {
		index += 1
	}
	return index
}

parse_trailer :: proc(line: []u8) -> (name: []u8, value: []u8, valid: bool) {
	colon := -1
	for c, i in line {
		if c == ':' {
			colon = i
			break
		}
	}

	if colon <= 0 {
		return nil, nil, false
	}

	for c in line[:colon] {
		if !http1.is_token(c) {
			return nil, nil, false
		}
	}

	field_name := line[:colon]

	if http1.ascii_case_insensitive_equal(field_name, "host") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "content-length") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "transfer-encoding") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "trailer") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "connection") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "keep-alive") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "proxy-connection") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "upgrade") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "te") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "authorization") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "proxy-authorization") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "expect") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "content-encoding") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "content-type") {
		return nil, nil, false
	}

	if http1.ascii_case_insensitive_equal(field_name, "content-range") {
		return nil, nil, false
	}

	value_start := colon + 1
	value_end := len(line)
	for value_start < value_end && http1.is_ows(line[value_start]) {
		value_start += 1
	}

	for value_end > value_start && http1.is_ows(line[value_end - 1]) {
		value_end -= 1
	}

	for c in line[value_start:value_end] {
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return nil, nil, false
		}
	}

	return field_name, line[value_start:value_end], true
}
