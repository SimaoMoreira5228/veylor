package http1

MAX_EMPTY_EXPECTATION_ELEMENTS :: 16

parse_expectations :: proc(value: []u8) -> (expect_continue, supported, valid: bool) {
	i := 0
	empty_element_count := 0
	supported = true

	for i < len(value) {
		i = skip_expectation_ows(value, i)

		if i == len(value) {
			break
		}

		if value[i] == ',' {
			empty_element_count += 1

			if empty_element_count > MAX_EMPTY_EXPECTATION_ELEMENTS {
				return false, false, false
			}
			i += 1
			continue
		}

		name_start := i
		for i < len(value) && is_token(value[i]) {
			i += 1
		}

		if i == name_start {
			return false, false, false
		}

		expectation_name := value[name_start:i]
		is_continue := ascii_case_insensitive_equal(expectation_name, "100-continue")
		has_value := false
		has_parameters := false

		i = skip_expectation_ows(value, i)

		if i < len(value) && value[i] == '=' {
			has_value = true
			i += 1
			i = skip_expectation_ows(value, i)
			value_end, value_valid := parse_expectation_value(value, i)

			if !value_valid {
				return false, false, false
			}
			i = value_end
		}

		for {
			i = skip_expectation_ows(value, i)

			if i >= len(value) || value[i] != ';' {
				break
			}
			has_parameters = true
			i += 1
			i = skip_expectation_ows(value, i)

			if i >= len(value) || value[i] == ',' || value[i] == ';' {
				continue
			}

			parameter_start := i
			for i < len(value) && is_token(value[i]) {
				i += 1
			}

			if i == parameter_start {
				return false, false, false
			}
			i = skip_expectation_ows(value, i)

			if i >= len(value) || value[i] != '=' {
				return false, false, false
			}
			i += 1
			i = skip_expectation_ows(value, i)
			value_end, value_valid := parse_expectation_value(value, i)

			if !value_valid {
				return false, false, false
			}
			i = value_end
		}

		empty_element_count = 0

		if is_continue && !has_value && !has_parameters {
			expect_continue = true
		} else {
			supported = false
		}

		i = skip_expectation_ows(value, i)

		if i < len(value) {
			if value[i] != ',' {
				return false, false, false
			}
			i += 1

			if i == len(value) {
				empty_element_count += 1

				if empty_element_count > MAX_EMPTY_EXPECTATION_ELEMENTS {
					return false, false, false
				}
			}
		}
	}

	return expect_continue, supported, true
}

parse_expectation_value :: proc(value: []u8, start: int) -> (end: int, valid: bool) {
	if start >= len(value) {
		return start, false
	}

	if value[start] == '"' {
		return parse_quoted_string(value, start)
	}

	i := start
	for i < len(value) && is_token(value[i]) {
		i += 1
	}
	return i, i > start
}

skip_expectation_ows :: proc(value: []u8, start: int) -> int {
	i := start
	for i < len(value) && is_ows(value[i]) {
		i += 1
	}
	return i
}
