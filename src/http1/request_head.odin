package http1

Span :: struct {
	offset: int,
	length: int,
}

Header :: struct {
	name:  Span,
	value: Span,
}

Request_Head :: struct {
	method:             Span,
	target:             Span,
	target_form:        Request_Target_Form,
	authority:          Span,
	version:            Span,
	headers:            []Header,
	length:             int,
	content_length:     u64,
	has_content_length: bool,
	is_chunked:         bool,
	expect_continue:    bool,
	connection:         Connection_Options,
}

Parse_Result :: enum {
	Need_More,
	Ready,
	Invalid,
	Too_Large,
	Header_Capacity,
	Expectation_Failed,
	Buffer_Changed,
}

Parser_State :: enum {
	Request_Line,
	Headers,
	Ready,
	Error,
}

Parser :: struct {
	state:                     Parser_State,
	result:                    Parse_Result,
	scan_offset:               int,
	line_start:                int,
	header_start:              int,
	header_value_start_offset: int,
	leading_empty_bytes:       int,
	method:                    Span,
	target:                    Span,
	target_form:               Request_Target_Form,
	authority:                 Span,
	version:                   Span,
	header_count:              int,
	host_seen:                 bool,
	transfer_encoding_seen:    bool,
	has_content_length:        bool,
	content_length:            u64,
	expect_continue:           bool,
	connection:                Connection_Options,
	head_length:               int,
	input_base:                rawptr,
	input_length:              int,
	header_base:               rawptr,
}

Limits :: struct {
	request_line_bytes: int,
	header_bytes:       int,
	header_count:       int,
	field_name_bytes:   int,
	field_value_bytes:  int,
}

DEFAULT_LIMITS :: Limits {
	request_line_bytes = 8 * 1024,
	header_bytes       = 32 * 1024,
	header_count       = 100,
	field_name_bytes   = 256,
	field_value_bytes  = 8 * 1024,
}

reset :: proc(parser: ^Parser) {
	parser^ = Parser{}
}

parse_request_head :: proc(
	parser: ^Parser,
	input: []u8,
	headers: []Header,
	limits := DEFAULT_LIMITS,
) -> (
	head: Request_Head,
	result: Parse_Result,
) {
	if parser.state == .Error {
		return {}, parser.result
	}

	if parser.input_base == nil {
		if len(input) > 0 {
			parser.input_base = raw_data(input)
			parser.input_length = len(input)
		}
	} else if parser.input_base != raw_data(input) || len(input) < parser.input_length {
		return fail(parser, .Buffer_Changed)
	} else {
		parser.input_length = len(input)
	}

	if len(headers) < parser.header_count {
		return {}, .Header_Capacity
	}

	if parser.header_count > 0 && parser.header_base != raw_data(headers) {
		return fail(parser, .Buffer_Changed)
	}
	// NOTE: Append at one input base; don't compact or mutate storage while parser views are live.

	if parser.state == .Ready {
		return request_head(parser, headers), .Ready
	}

	for i := parser.scan_offset; i < len(input); i += 1 {
		if parser.state == .Headers && parser.header_value_start_offset == 0 && input[i] == ':' {
			parser.header_value_start_offset = i - parser.line_start + 1
		}

		if input[i] == '\r' {
			if i + 1 < len(input) && input[i + 1] != '\n' {
				return fail(parser, .Invalid)
			}
			continue
		}

		if input[i] != '\n' {
			continue
		}

		if i == parser.line_start || input[i - 1] != '\r' {
			return fail(parser, .Invalid)
		}

		line_end := i - 1
		line_next := i + 1

		if parser.state == .Request_Line {
			if line_end - parser.line_start > limits.request_line_bytes {
				return fail(parser, .Too_Large)
			}

			if line_end == parser.line_start {
				empty_line_bytes := line_next - parser.line_start

				if limits.request_line_bytes < 0 {
					return fail(parser, .Too_Large)
				}

				if parser.leading_empty_bytes > limits.request_line_bytes {
					return fail(parser, .Too_Large)
				}
				remaining_request_line_bytes :=
					limits.request_line_bytes - parser.leading_empty_bytes

				if empty_line_bytes > remaining_request_line_bytes {
					return fail(parser, .Too_Large)
				}
				parser.leading_empty_bytes += empty_line_bytes
			} else {
				method, target, version, target_form, authority, valid := parse_request_line(
					input[parser.line_start:line_end],
				)

				if !valid {
					return fail(parser, .Invalid)
				}
				method.offset += parser.line_start
				target.offset += parser.line_start
				version.offset += parser.line_start

				if authority.length > 0 {
					authority.offset += parser.line_start
				}
				parser.method = method
				parser.target = target
				parser.target_form = target_form
				parser.authority = authority
				parser.version = version
				parser.header_start = line_next
				parser.state = .Headers
			}
		} else {
			if line_next - parser.header_start > limits.header_bytes {
				return fail(parser, .Too_Large)
			}

			if line_end == parser.line_start {
				version := input[parser.version.offset:parser.version.offset +
				parser.version.length]

				if bytes_equal(version, "HTTP/1.1") && !parser.host_seen {
					return fail(parser, .Invalid)
				}
				parser.state = .Ready
				parser.result = .Ready
				parser.head_length = line_next
				return request_head(parser, headers), .Ready
			}

			if parser.header_count >= limits.header_count {
				return fail(parser, .Too_Large)
			}

			if parser.header_count >= len(headers) {
				return fail(parser, .Header_Capacity)
			}
			header, valid := parse_header(input, parser.line_start, line_end)

			if !valid {
				return fail(parser, .Invalid)
			}
			field_value_start := header.name.offset + header.name.length + 1

			if field_value_start > line_end {
				return fail(parser, .Invalid)
			}
			field_value_bytes := line_end - field_value_start

			if header.name.length > limits.field_name_bytes {
				return fail(parser, .Too_Large)
			}

			if field_value_bytes > limits.field_value_bytes {
				return fail(parser, .Too_Large)
			}
			name := input[header.name.offset:header.name.offset + header.name.length]
			value := input[header.value.offset:header.value.offset + header.value.length]

			if ascii_case_insensitive_equal(name, "host") {
				if parser.host_seen {
					return fail(parser, .Invalid)
				}

				if !valid_host_field(value) {
					return fail(parser, .Invalid)
				}
				parser.host_seen = true

				if parser.target_form == .Origin_Form || parser.target_form == .Asterisk_Form {
					parser.authority = header.value
				}
			}

			if ascii_case_insensitive_equal(name, "connection") {
				options, options_valid := parse_connection_options(value)

				if !options_valid {
					return fail(parser, .Invalid)
				}
				parser.connection.close = parser.connection.close || options.close
				parser.connection.keep_alive = parser.connection.keep_alive || options.keep_alive
				parser.connection.upgrade = parser.connection.upgrade || options.upgrade
			}

			if ascii_case_insensitive_equal(name, "content-length") {
				content_length, content_length_valid := parse_content_length(value)

				if !content_length_valid {
					return fail(parser, .Invalid)
				}

				if parser.has_content_length && parser.content_length != content_length {
					return fail(parser, .Invalid)
				}

				if parser.transfer_encoding_seen {
					return fail(parser, .Invalid)
				}
				parser.content_length = content_length
				parser.has_content_length = true
			}

			if ascii_case_insensitive_equal(name, "transfer-encoding") {
				if parser.has_content_length {
					return fail(parser, .Invalid)
				}

				if parser.transfer_encoding_seen {
					return fail(parser, .Invalid)
				}
				version := input[parser.version.offset:parser.version.offset +
				parser.version.length]

				if !bytes_equal(version, "HTTP/1.1") {
					return fail(parser, .Invalid)
				}

				if !ascii_case_insensitive_equal(value, "chunked") {
					return fail(parser, .Invalid)
				}
				parser.transfer_encoding_seen = true
			}

			if ascii_case_insensitive_equal(name, "expect") {
				version := input[parser.version.offset:parser.version.offset +
				parser.version.length]

				if bytes_equal(version, "HTTP/1.1") {
					expect_continue, supported, expectation_valid := parse_expectations(value)

					if !expectation_valid {
						return fail(parser, .Invalid)
					}

					if !supported {
						return fail(parser, .Expectation_Failed)
					}
					parser.expect_continue = parser.expect_continue || expect_continue
				}
			}

			if parser.header_base == nil {
				parser.header_base = raw_data(headers)
			}
			headers[parser.header_count] = header
			parser.header_count += 1
			parser.header_value_start_offset = 0
		}

		parser.scan_offset = line_next
		parser.line_start = line_next
		i = line_next - 1
	}

	if parser.state == .Request_Line {
		request_line_bytes := len(input) - parser.line_start

		if request_line_bytes > 0 && input[len(input) - 1] == '\r' {
			request_line_bytes -= 1
		}

		if request_line_bytes > limits.request_line_bytes {
			return fail(parser, .Too_Large)
		}
	}

	if parser.state == .Headers && len(input) - parser.header_start > limits.header_bytes {
		return fail(parser, .Too_Large)
	}

	if parser.state == .Headers {
		line_end := len(input)

		if line_end > parser.line_start && input[line_end - 1] == '\r' {
			line_end -= 1
		}
		line_length := line_end - parser.line_start

		if limits.field_name_bytes < 0 || limits.field_value_bytes < 0 {
			return fail(parser, .Too_Large)
		}

		if parser.header_value_start_offset == 0 {
			if line_length > limits.field_name_bytes {
				return fail(parser, .Too_Large)
			}
		} else {
			field_name_bytes := parser.header_value_start_offset - 1

			if field_name_bytes > limits.field_name_bytes {
				return fail(parser, .Too_Large)
			}
			field_value_bytes := line_length - parser.header_value_start_offset

			if field_value_bytes > limits.field_value_bytes {
				return fail(parser, .Too_Large)
			}
		}
	}
	parser.scan_offset = len(input)

	if len(input) > 0 && input[len(input) - 1] == '\r' {
		parser.scan_offset -= 1
	}
	return {}, .Need_More
}

ascii_case_insensitive_equal :: proc(bytes: []u8, expected: string) -> bool {
	return ascii_case_insensitive_bytes_equal(bytes, transmute([]u8)expected)
}

ascii_case_insensitive_bytes_equal :: proc(left, right: []u8) -> bool {
	if len(left) != len(right) {
		return false
	}
	for left_byte, i in left {
		lowered_left := left_byte
		lowered_right := right[i]

		if lowered_left >= 'A' && lowered_left <= 'Z' {
			lowered_left += 'a' - 'A'
		}

		if lowered_right >= 'A' && lowered_right <= 'Z' {
			lowered_right += 'a' - 'A'
		}

		if lowered_left != lowered_right {
			return false
		}
	}
	return true
}

request_head :: proc(parser: ^Parser, headers: []Header) -> Request_Head {
	return Request_Head {
		method = parser.method,
		target = parser.target,
		target_form = parser.target_form,
		authority = parser.authority,
		version = parser.version,
		headers = headers[:parser.header_count],
		length = parser.head_length,
		content_length = parser.content_length,
		has_content_length = parser.has_content_length,
		is_chunked = parser.transfer_encoding_seen,
		expect_continue = parser.expect_continue,
		connection = parser.connection,
	}
}

fail :: proc(parser: ^Parser, result: Parse_Result) -> (head: Request_Head, failed: Parse_Result) {
	parser.state = .Error
	parser.result = result
	return {}, result
}

parse_request_line :: proc(
	line: []u8,
) -> (
	method, target, version: Span,
	target_form: Request_Target_Form,
	authority: Span,
	valid: bool,
) {
	first_space := -1
	second_space := -1
	for c, i in line {
		if c == ' ' {
			if first_space < 0 {
				first_space = i
			} else if second_space < 0 {
				second_space = i
			} else {
				return {}, {}, {}, {}, {}, false
			}
		}
	}

	if first_space <= 0 || second_space <= first_space + 1 || second_space == len(line) - 1 {
		return {}, {}, {}, {}, {}, false
	}
	for c in line[:first_space] {
		if !is_token(c) {
			return {}, {}, {}, {}, {}, false
		}
	}
	method_bytes := line[:first_space]
	target_bytes := line[first_space + 1:second_space]
	classified_target_form, target_authority, target_valid := classify_request_target(
		method_bytes,
		target_bytes,
	)

	if !target_valid {
		return {}, {}, {}, {}, {}, false
	}
	target_form = classified_target_form

	if target_authority.length > 0 {
		target_authority.offset += first_space + 1
	}
	authority = target_authority
	version_bytes := line[second_space + 1:]

	if !bytes_equal(version_bytes, "HTTP/1.1") && !bytes_equal(version_bytes, "HTTP/1.0") {
		return {}, {}, {}, {}, {}, false
	}
	return Span{offset = 0, length = first_space},
		Span{offset = first_space + 1, length = second_space - first_space - 1},
		Span{offset = second_space + 1, length = len(line) - second_space - 1},
		target_form,
		authority,
		true
}

parse_header :: proc(input: []u8, start, end: int) -> (header: Header, valid: bool) {
	colon := -1
	for i := start; i < end; i += 1 {
		if input[i] == ':' {
			colon = i
			break
		}
	}

	if colon <= start {
		return {}, false
	}
	for c in input[start:colon] {
		if !is_token(c) {
			return {}, false
		}
	}

	value_start := colon + 1
	value_end := end
	for value_start < value_end && is_ows(input[value_start]) {
		value_start += 1
	}
	for value_end > value_start && is_ows(input[value_end - 1]) {
		value_end -= 1
	}
	for c in input[value_start:value_end] {
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return {}, false
		}
	}
	return Header {
			name = Span{offset = start, length = colon - start},
			value = Span{offset = value_start, length = value_end - value_start},
		},
		true
}

is_token :: proc(c: u8) -> bool {
	return(
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '!' ||
		c == '#' ||
		c == '$' ||
		c == '%' ||
		c == '&' ||
		c == '\'' ||
		c == '*' ||
		c == '+' ||
		c == '-' ||
		c == '.' ||
		c == '^' ||
		c == '_' ||
		c == '`' ||
		c == '|' ||
		c == '~' \
	)
}

is_ows :: proc(c: u8) -> bool {
	return c == ' ' || c == '\t'
}

bytes_equal :: proc(bytes: []u8, expected: string) -> bool {
	if len(bytes) != len(expected) {
		return false
	}
	for c, i in bytes {
		if c != expected[i] {
			return false
		}
	}
	return true
}
