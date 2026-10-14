package http1

Connection_Options :: struct {
	close:      bool,
	keep_alive: bool,
	upgrade:    bool,
}

parse_connection_options :: proc(value: []u8) -> (options: Connection_Options, valid: bool) {
	options, _, valid = scan_connection_options(value, nil)
	return
}

connection_option_present :: proc(value, target: []u8) -> (present: bool, valid: bool) {
	_, present, valid = scan_connection_options(value, target)
	return
}

scan_connection_options :: proc(
	value, target: []u8,
) -> (
	options: Connection_Options,
	present: bool,
	valid: bool,
) {
	i := 0
	has_option := false
	for {
		for i < len(value) && is_ows(value[i]) {
			i += 1
		}

		if i == len(value) {
			return options, present, has_option
		}

		start := i
		for i < len(value) && is_token(value[i]) {
			i += 1
		}

		if i == start {
			return {}, false, false
		}

		option := value[start:i]
		options.close = options.close || ascii_case_insensitive_equal(option, "close")
		options.keep_alive =
			options.keep_alive || ascii_case_insensitive_equal(option, "keep-alive")
		options.upgrade = options.upgrade || ascii_case_insensitive_equal(option, "upgrade")
		present = present || ascii_case_insensitive_bytes_equal(option, target)
		has_option = true

		for i < len(value) && is_ows(value[i]) {
			i += 1
		}

		if i == len(value) {
			return options, present, true
		}

		if value[i] != ',' {
			return {}, false, false
		}
		i += 1
		for i < len(value) && is_ows(value[i]) {
			i += 1
		}

		if i == len(value) {
			return {}, false, false
		}
	}
}
