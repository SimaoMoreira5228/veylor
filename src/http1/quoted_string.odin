package http1

parse_quoted_string :: proc(bytes: []u8, start: int) -> (end: int, valid: bool) {
	if start >= len(bytes) || bytes[start] != '"' {
		return start, false
	}

	i := start + 1
	for i < len(bytes) {
		if bytes[i] == '"' {
			return i + 1, true
		}

		if bytes[i] == '\\' {
			i += 1

			if i >= len(bytes) || !is_quoted_pair_char(bytes[i]) {
				return i, false
			}
		} else if !is_quoted_text_char(bytes[i]) {
			return i, false
		}
		i += 1
	}
	return i, false
}

is_quoted_text_char :: proc(c: u8) -> bool {
	return(
		c == '\t' ||
		c == ' ' ||
		c == '!' ||
		(c >= '#' && c <= '[') ||
		(c >= ']' && c <= '~') ||
		c >= 0x80 \
	)
}

is_quoted_pair_char :: proc(c: u8) -> bool {
	return c == '\t' || c == ' ' || (c >= '!' && c <= '~') || c >= 0x80
}
