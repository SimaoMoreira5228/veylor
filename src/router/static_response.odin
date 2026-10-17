package router

import http1 "veylor:http1"
import response "veylor:http1/response"

Static_Response_Result :: enum {
	Written,
	Route_Not_Found,
	Buffer_Small,
	Managed,
	Invalid,
}

Static_Response_Event :: struct {
	written:    int,
	keep_alive: bool,
	result:     Static_Response_Result,
}

encode_static_route_response :: proc(
	table: ^Route_Table,
	head: http1.Request_Head,
	input: []u8,
	output: []u8,
	body_limit: u64,
) -> Static_Response_Event {
	route, route_result := find_static_request_route(table, head, input)

	if route_result == .Not_Found {
		return Static_Response_Event{result = .Route_Not_Found}
	}

	if route_result != .Found {
		return Static_Response_Event{result = .Invalid}
	}

	if route.kind == .Managed {
		return Static_Response_Event{result = .Managed}
	}

	if head.length < 0 {
		return Static_Response_Event{result = .Invalid}
	}

	if head.length > len(input) {
		return Static_Response_Event{result = .Invalid}
	}

	if head.version.offset < 0 {
		return Static_Response_Event{result = .Invalid}
	}

	if head.version.length < 0 {
		return Static_Response_Event{result = .Invalid}
	}

	if head.version.offset > len(input) {
		return Static_Response_Event{result = .Invalid}
	}

	if head.version.length > len(input) - head.version.offset {
		return Static_Response_Event{result = .Invalid}
	}

	method := input[head.method.offset:head.method.offset + head.method.length]
	version := input[head.version.offset:head.version.offset + head.version.length]
	request_is_http11 := http1.bytes_equal(version, "HTTP/1.1")

	if !request_is_http11 {
		if !http1.bytes_equal(version, "HTTP/1.0") {
			return Static_Response_Event{result = .Invalid}
		}
	}

	if head.has_content_length && head.content_length > body_limit {
		error_reason: string = "Content Too Large"
		error_head := response.encode_response_head(
			method,
			413,
			transmute([]u8)error_reason,
			[]response.Response_Header{},
			.Fixed_Length,
			0,
			output,
			.Close,
		)

		if error_head.result == .Buffer_Small {
			return Static_Response_Event{result = .Buffer_Small}
		}

		if error_head.result != .Written {
			return Static_Response_Event{result = .Invalid}
		}

		return Static_Response_Event{written = error_head.written, result = .Written}
	}

	connect_tunnel := false

	if http1.bytes_equal(method, "CONNECT") {
		connect_tunnel = route.response.status >= 200 && route.response.status < 300
	}

	if connect_tunnel {
		return Static_Response_Event{result = .Invalid}
	}

	body_complete := !head.is_chunked

	if head.has_content_length {
		if head.content_length != 0 {
			body_complete = false
		}
	}

	if len(input) != head.length {
		body_complete = false
	}

	policy := response.request_connection_policy(head, version, true, body_complete, false)
	connection_header := response.response_connection_header(
		method,
		route.response.status,
		policy,
		request_is_http11,
	)
	content_length := u64(len(route.response.body))

	if route.response.status == 204 {
		content_length = 0
	}

	if route.response.status == 205 {
		content_length = 0
	}

	head_event := response.encode_response_head(
		method,
		route.response.status,
		route.response.reason,
		route.response.headers,
		.Fixed_Length,
		content_length,
		output,
		connection_header,
	)

	if head_event.result == .Buffer_Small {
		return Static_Response_Event{result = .Buffer_Small}
	}

	if head_event.result != .Written {
		return Static_Response_Event{result = .Invalid}
	}

	written := head_event.written

	if head_event.body_allowed {
		if len(route.response.body) > len(output) - written {
			return Static_Response_Event{result = .Buffer_Small}
		}
		copy(output[written:], route.response.body)
		written += len(route.response.body)
	}
	return Static_Response_Event {
		written = written,
		keep_alive = policy == .Keep_Alive,
		result = .Written,
	}
}
