package http1

MAX_CONTENT_LENGTH :: u64(0xffffffffffffffff)

parse_content_length :: proc(value: []u8) -> (content_length: u64, valid: bool) {
	i := 0
	has_value := false

	for {
		for i < len(value) && is_ows(value[i]) {
			i += 1
		}

		if i == len(value) {
			return 0, false
		}

		current := u64(0)
		digit_count := 0
		for i < len(value) && value[i] >= '0' && value[i] <= '9' {
			digit := u64(value[i] - '0')

			if current > (MAX_CONTENT_LENGTH - digit) / 10 {
				return 0, false
			}
			current = current * 10 + digit
			digit_count += 1
			i += 1
		}

		if digit_count == 0 {
			return 0, false
		}

		if has_value && current != content_length {
			return 0, false
		}

		if !has_value {
			content_length = current
			has_value = true
		}

		for i < len(value) && is_ows(value[i]) {
			i += 1
		}

		if i == len(value) {
			return content_length, true
		}

		if value[i] != ',' {
			return 0, false
		}
		i += 1
	}
}
