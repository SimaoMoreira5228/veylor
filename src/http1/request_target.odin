package http1

Request_Target_Form :: enum {
	Origin_Form,
	Absolute_Form,
	Authority_Form,
	Asterisk_Form,
}

classify_request_target :: proc(
	method, target: []u8,
) -> (
	form: Request_Target_Form,
	authority: Span,
	valid: bool,
) {
	if len(target) == 0 {
		return {}, {}, false
	}

	if bytes_equal(target, "*") {
		return .Asterisk_Form, {}, bytes_equal(method, "OPTIONS")
	}

	if bytes_equal(method, "CONNECT") {
		return .Authority_Form, Span {
			offset = 0,
			length = len(target),
		}, valid_target_authority(target, true)
	}

	if target[0] == '/' {
		return .Origin_Form, {}, valid_origin_form(target)
	}
	absolute_authority, authority_valid := absolute_form_authority(target)
	return .Absolute_Form, absolute_authority, authority_valid
}

valid_origin_form :: proc(target: []u8) -> bool {
	for c, i in target {
		if c == '?' {
			continue
		}

		if c == '%' {
			if len(target) - i < 3 {
				return false
			}

			if !is_hex(target[i + 1]) {
				return false
			}

			if !is_hex(target[i + 2]) {
				return false
			}
			continue
		}

		if !is_uri_unreserved(c) && !is_uri_sub_delim(c) && c != ':' && c != '@' && c != '/' {
			return false
		}
	}
	return true
}

absolute_form_authority :: proc(target: []u8) -> (authority: Span, valid: bool) {
	colon := -1
	for c, i in target {
		if c == ':' {
			colon = i
			break
		}

		if !is_scheme_char(c, i == 0) {
			return {}, false
		}
	}

	if colon <= 0 {
		return {}, false
	}

	if len(target) - colon <= 3 {
		return {}, false
	}

	if target[colon + 1] != '/' {
		return {}, false
	}

	if target[colon + 2] != '/' {
		return {}, false
	}
	authority_start := colon + 3
	authority_end := authority_start
	for authority_end < len(target) &&
	    target[authority_end] != '/' &&
	    target[authority_end] != '?' {
		authority_end += 1
	}

	if authority_start == authority_end {
		return {}, false
	}

	if !valid_target_authority(target[authority_start:authority_end], false) {
		return {}, false
	}

	if !valid_absolute_suffix(target[authority_end:]) {
		return {}, false
	}
	host_start := authority_start
	for i := authority_start; i < authority_end; i += 1 {
		if target[i] == '@' {
			host_start = i + 1
			break
		}
	}
	return Span{offset = host_start, length = authority_end - host_start}, true
}

valid_target_authority :: proc(authority: []u8, require_port: bool) -> bool {
	if len(authority) == 0 {
		return false
	}
	host_start := 0

	if !require_port {
		at := -1
		for c, i in authority {
			if c == '@' {
				if at >= 0 {
					return false
				}
				at = i
			}
		}

		if at >= 0 {
			if at == 0 {
				return false
			}

			if !valid_userinfo(authority[:at]) {
				return false
			}
			host_start = at + 1
		}
	}

	if host_start >= len(authority) {
		return false
	}
	return valid_host_port(authority[host_start:], require_port)
}

valid_host_field :: proc(value: []u8) -> bool {
	if len(value) == 0 {
		return true
	}
	return valid_host_port(value, false)
}

valid_host_port :: proc(host_port: []u8, require_port: bool) -> bool {
	if len(host_port) == 0 {
		return false
	}
	port_start := -1
	host_end := len(host_port)

	if host_port[0] == '[' {
		close := -1
		for c, i in host_port {
			if c == ']' {
				close = i
				break
			}
		}

		if close <= 1 {
			return false
		}

		if !valid_ip_literal(host_port[1:close]) {
			return false
		}
		host_end = close + 1

		if host_end < len(host_port) {
			if host_port[host_end] != ':' {
				return false
			}
			port_start = host_end + 1
		}
	} else {
		for c, i in host_port {
			if c == ':' {
				if port_start >= 0 {
					return false
				}
				host_end = i
				port_start = i + 1
			} else if c == '%' {
				if len(host_port) - i < 3 {
					return false
				}

				if !is_hex(host_port[i + 1]) {
					return false
				}

				if !is_hex(host_port[i + 2]) {
					return false
				}
			} else if !is_uri_unreserved(c) && !is_uri_sub_delim(c) {
				return false
			}
		}

		if host_end == 0 {
			return false
		}
	}

	if require_port && port_start < 0 {
		return false
	}

	if port_start >= 0 {
		if port_start >= len(host_port) {
			return false
		}
		for c in host_port[port_start:] {
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}

valid_ip_literal :: proc(literal: []u8) -> bool {
	if len(literal) >= 4 && (literal[0] == 'v' || literal[0] == 'V') {
		dot := -1
		for c, i in literal {
			if c == '.' {
				dot = i
				break
			}
		}

		if dot <= 1 || dot == len(literal) - 1 {
			return false
		}
		for c in literal[1:dot] {
			if !is_hex(c) {
				return false
			}
		}
		for c in literal[dot + 1:] {
			if !is_uri_unreserved(c) && !is_uri_sub_delim(c) && c != ':' {
				return false
			}
		}
		return true
	}
	double_colon := -1
	for i := 0; i < len(literal) - 1; i += 1 {
		if literal[i] == ':' && literal[i + 1] == ':' {
			if double_colon >= 0 {
				return false
			}
			double_colon = i
		}
	}

	if double_colon >= 0 {
		left_groups, left_valid := ipv6_group_count(literal[:double_colon], false)
		right_groups, right_valid := ipv6_group_count(literal[double_colon + 2:], true)

		if !left_valid || !right_valid {
			return false
		}
		return left_groups + right_groups < 8
	}
	groups, valid := ipv6_group_count(literal, true)

	if !valid {
		return false
	}
	return groups == 8
}

ipv6_group_count :: proc(part: []u8, allow_ipv4_tail: bool) -> (count: int, valid: bool) {
	if len(part) == 0 {
		return 0, true
	}
	start := 0
	for i := 0; i < len(part); i += 1 {
		if part[i] != ':' {
			continue
		}

		if i == start {
			return 0, false
		}
		segment_count, segment_valid := ipv6_segment_count(part[start:i], false)

		if !segment_valid {
			return 0, false
		}
		count += segment_count
		start = i + 1
	}

	if start == len(part) {
		return 0, false
	}
	segment_count, segment_valid := ipv6_segment_count(part[start:], allow_ipv4_tail)

	if !segment_valid {
		return 0, false
	}
	return count + segment_count, true
}

ipv6_segment_count :: proc(segment: []u8, allow_ipv4: bool) -> (count: int, valid: bool) {
	is_ipv4 := false
	for c in segment {
		if c == '.' {
			is_ipv4 = true
			break
		}
	}

	if is_ipv4 {
		if !allow_ipv4 {
			return 0, false
		}

		if !valid_ipv4_address(segment) {
			return 0, false
		}
		return 2, true
	}

	if len(segment) > 4 {
		return 0, false
	}
	for c in segment {
		if !is_hex(c) {
			return 0, false
		}
	}
	return 1, true
}

valid_ipv4_address :: proc(address: []u8) -> bool {
	octet := 0
	digit_count := 0
	octet_count := 0
	leading_zero := false

	for c in address {
		if c == '.' {
			if digit_count == 0 {
				return false
			}

			if octet > 255 {
				return false
			}
			octet_count += 1
			octet = 0
			digit_count = 0
			leading_zero = false
			continue
		}

		if c < '0' || c > '9' {
			return false
		}

		if leading_zero && digit_count > 0 {
			return false
		}

		if digit_count == 0 {
			leading_zero = c == '0'
		}
		digit_count += 1

		if digit_count > 3 {
			return false
		}
		octet = octet * 10 + int(c - '0')
	}

	if octet_count != 3 || digit_count == 0 {
		return false
	}
	return octet <= 255
}

valid_userinfo :: proc(bytes: []u8) -> bool {
	for c, i in bytes {
		if c == '%' {
			if len(bytes) - i < 3 {
				return false
			}

			if !is_hex(bytes[i + 1]) {
				return false
			}

			if !is_hex(bytes[i + 2]) {
				return false
			}
		} else if !is_uri_unreserved(c) && !is_uri_sub_delim(c) && c != ':' {
			return false
		}
	}
	return true
}

valid_absolute_suffix :: proc(bytes: []u8) -> bool {
	for c, i in bytes {
		if c == '?' {
			continue
		}

		if c == '%' {
			if len(bytes) - i < 3 {
				return false
			}

			if !is_hex(bytes[i + 1]) {
				return false
			}

			if !is_hex(bytes[i + 2]) {
				return false
			}
			continue
		}

		if is_uri_unreserved(c) || is_uri_sub_delim(c) {
			continue
		}

		if c == ':' || c == '@' || c == '/' {
			continue
		}
		return false
	}
	return true
}

is_scheme_char :: proc(c: u8, first: bool) -> bool {
	return(
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(!first && ((c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.')) \
	)
}

is_uri_unreserved :: proc(c: u8) -> bool {
	return(
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '-' ||
		c == '.' ||
		c == '_' ||
		c == '~' \
	)
}

is_uri_sub_delim :: proc(c: u8) -> bool {
	return(
		c == '!' ||
		c == '$' ||
		c == '&' ||
		c == '\'' ||
		c == '(' ||
		c == ')' ||
		c == '*' ||
		c == '+' ||
		c == ',' ||
		c == ';' ||
		c == '=' \
	)
}

is_hex :: proc(c: u8) -> bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
