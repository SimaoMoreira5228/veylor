#+build linux
package linux_runtime

import http1 "veylor:http1"
import response "veylor:http1/response"
import router "veylor:router"

Native_Dispatch_Result :: enum {
	Responded,
	Not_Found,
	Buffer_Small,
	Invalid,
}

Native_Dispatch_Event :: struct {
	written:    int,
	keep_alive: bool,
	result:     Native_Dispatch_Result,
}

dispatch_native_route_response :: proc(
	native_table: ^router.Native_Table,
	head: http1.Request_Head,
	input: []u8,
	output: []u8,
	body_limit: u64,
) -> Native_Dispatch_Event {
	if native_table == nil {
		return Native_Dispatch_Event{result = .Not_Found}
	}
	if head.method.offset < 0 ||
	   head.method.length < 0 ||
	   head.method.offset > len(input) ||
	   head.method.length > len(input) - head.method.offset {
		return Native_Dispatch_Event{result = .Invalid}
	}
	if head.target.offset < 0 ||
	   head.target.length < 0 ||
	   head.target.offset > len(input) ||
	   head.target.length > len(input) - head.target.offset {
		return Native_Dispatch_Event{result = .Invalid}
	}
	if head.length < 0 || head.length > len(input) {
		return Native_Dispatch_Event{result = .Invalid}
	}
	if head.version.offset < 0 ||
	   head.version.length < 0 ||
	   head.version.offset > len(input) ||
	   head.version.length > len(input) - head.version.offset {
		return Native_Dispatch_Event{result = .Invalid}
	}

	method := input[head.method.offset:head.method.offset + head.method.length]
	target := input[head.target.offset:head.target.offset + head.target.length]
	path, path_offset, path_result := router.request_path(head.target_form, method, target)
	if path_result != .Found {
		return Native_Dispatch_Event{result = .Invalid}
	}
	path_input_offset := -1
	if path_offset >= 0 {
		path_input_offset = head.target.offset + path_offset
	}
	route, route_result := router.find_native_request_route(
		native_table,
		method,
		path,
		path_input_offset,
	)
	if route_result == .Not_Found {
		return Native_Dispatch_Event{result = .Not_Found}
	}
	if route_result != .Found || route.handler == nil {
		return Native_Dispatch_Event{result = .Invalid}
	}

	version := input[head.version.offset:head.version.offset + head.version.length]
	request_is_http11 := http1.bytes_equal(version, "HTTP/1.1")
	if !request_is_http11 && !http1.bytes_equal(version, "HTTP/1.0") {
		return Native_Dispatch_Event{result = .Invalid}
	}

	if head.has_content_length && head.content_length > body_limit {
		return encode_native_error_response(method, output, 413, "Content Too Large", .Close)
	}

	request_view := router.Native_Request_View {
		method_data = raw_data(method),
		method_len  = u32(len(method)),
		path_data   = raw_data(path),
		path_len    = u32(len(path)),
	}
	response_view := router.Native_Response_View{}
	handler_result := route.handler(route.user_data, &request_view, &response_view)
	if handler_result != i32(router.Native_Handler_Result.Ok) {
		return Native_Dispatch_Event{result = .Invalid}
	}
	if response_view.status < 200 || response_view.status > 599 {
		return Native_Dispatch_Event{result = .Invalid}
	}
	if response_view.reason_len > u32(len(output)) {
		return Native_Dispatch_Event{result = .Invalid}
	}
	if response_view.body_len > u32(len(output)) {
		return Native_Dispatch_Event{result = .Buffer_Small}
	}
	reason := slice_from_native_bytes(response_view.reason_data, response_view.reason_len)
	body := slice_from_native_bytes(response_view.body_data, response_view.body_len)
	if reason == nil && response_view.reason_len > 0 {
		return Native_Dispatch_Event{result = .Invalid}
	}
	if body == nil && response_view.body_len > 0 {
		return Native_Dispatch_Event{result = .Invalid}
	}
	if !response.valid_response_value(reason) {
		return Native_Dispatch_Event{result = .Invalid}
	}

	body_complete := !head.is_chunked
	if head.has_content_length && head.content_length != 0 {
		body_complete = false
	}
	if len(input) != head.length {
		body_complete = false
	}
	status := u16(response_view.status)
	policy := response.request_connection_policy(head, version, true, body_complete, false)
	connection_header := response.response_connection_header(
		method,
		status,
		policy,
		request_is_http11,
	)
	content_length := u64(len(body))
	if status == 204 || status == 205 {
		content_length = 0
	}
	head_event := response.encode_response_head(
		method,
		status,
		reason,
		[]response.Response_Header{},
		.Fixed_Length,
		content_length,
		output,
		connection_header,
	)
	if head_event.result == .Buffer_Small {
		return Native_Dispatch_Event{result = .Buffer_Small}
	}
	if head_event.result != .Written {
		return Native_Dispatch_Event{result = .Invalid}
	}
	written := head_event.written
	if head_event.body_allowed {
		if len(body) > len(output) - written {
			return Native_Dispatch_Event{result = .Buffer_Small}
		}
		copy(output[written:], body)
		written += len(body)
	}
	return Native_Dispatch_Event {
		written = written,
		keep_alive = policy == .Keep_Alive,
		result = .Responded,
	}
}

encode_native_error_response :: proc(
	method: []u8,
	output: []u8,
	status: u16,
	reason_text: string,
	connection_header: response.Response_Connection_Header,
) -> Native_Dispatch_Event {
	reason := transmute([]u8)reason_text
	head_event := response.encode_response_head(
		method,
		status,
		reason,
		[]response.Response_Header{},
		.Fixed_Length,
		0,
		output,
		connection_header,
	)
	if head_event.result == .Buffer_Small {
		return Native_Dispatch_Event{result = .Buffer_Small}
	}
	if head_event.result != .Written {
		return Native_Dispatch_Event{result = .Invalid}
	}
	return Native_Dispatch_Event {
		written = head_event.written,
		keep_alive = false,
		result = .Responded,
	}
}

slice_from_native_bytes :: proc(data: [^]u8, length: u32) -> []u8 {
	if length == 0 {
		return []u8{}
	}
	if data == nil {
		return nil
	}
	return data[:int(length)]
}
