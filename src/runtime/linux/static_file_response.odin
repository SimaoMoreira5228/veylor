#+build linux
package linux_runtime

import "core:sys/linux"
import http1 "veylor:http1"
import response "veylor:http1/response"
import router "veylor:router"

STATIC_FILE_INLINE_BODY_LIMIT :: 8192

Static_File_Response_Result :: enum {
	Encoded,
	Not_Mounted,
	Not_Found,
	Body_Too_Large,
	Buffer_Small,
	System_Error,
	Invalid,
}

Static_File_Response_Event :: struct {
	file:            Static_File,
	response_length: int,
	keep_alive:      bool,
	stream_body:     bool,
	result:          Static_File_Response_Result,
}

encode_static_file_response :: proc(
	root: ^Static_File_Root,
	head: http1.Request_Head,
	input: []u8,
	output: []u8,
	body_limit: u64,
) -> Static_File_Response_Event {
	if !valid_static_file_root(root) {
		return static_file_response_failure(.Not_Mounted)
	}

	method, method_valid := static_file_span(input, head.method)
	target, target_valid := static_file_span(input, head.target)
	version, version_valid := static_file_span(input, head.version)
	if !method_valid || !target_valid || !version_valid {
		return static_file_response_failure(.Invalid)
	}
	if !http1.bytes_equal(method, "GET") && !http1.bytes_equal(method, "HEAD") {
		return static_file_response_failure(.Not_Mounted)
	}

	path, _, path_result := router.request_path(head.target_form, method, target)
	if path_result != .Found {
		return static_file_response_failure(.Not_Mounted)
	}
	if len(path) <= len(root.mount_prefix) ||
	   !static_file_prefix_matches(path, root.mount_prefix) {
		return static_file_response_failure(.Not_Mounted)
	}

	if head.has_content_length && head.content_length > body_limit {
		return static_file_response_failure(.Body_Too_Large)
	}

	relative_path := path[len(root.mount_prefix):]
	file, open_result := open_static_file_beneath(root.fd, relative_path)
	if open_result == .Invalid_Path ||
	   open_result == .Not_Found ||
	   open_result == .Not_Regular ||
	   open_result == .Rejected {
		return static_file_response_failure(.Not_Found)
	}
	if open_result != .Opened {
		return static_file_response_failure(.System_Error)
	}

	body_complete := !head.is_chunked
	if head.has_content_length && head.content_length != 0 {
		body_complete = false
	}
	if len(input) != head.length {
		body_complete = false
	}
	policy := response.request_connection_policy(head, version, true, body_complete, false)

	request_is_http11 := http1.bytes_equal(version, "HTTP/1.1")
	connection_header := response.response_connection_header(
		method,
		200,
		policy,
		request_is_http11,
	)
	content_type_name := "Content-Type"
	content_type_value := "application/octet-stream"
	reason := "OK"

	headers := [?]response.Response_Header {
		{name = transmute([]u8)content_type_name, value = transmute([]u8)content_type_value},
	}
	head_event := response.encode_response_head(
		method,
		200,
		transmute([]u8)reason,
		headers[:],
		.Fixed_Length,
		file.size,
		output,
		connection_header,
	)
	if head_event.result == .Buffer_Small {
		close_static_file(&file)
		return static_file_response_failure(.Buffer_Small)
	}
	if head_event.result != .Written {
		close_static_file(&file)
		return static_file_response_failure(.Invalid)
	}

	response_length := head_event.written
	stream_body := http1.bytes_equal(method, "GET") && head_event.body_allowed && file.size > 0
	if stream_body {
		inline_body_capacity := min(len(output) - response_length, STATIC_FILE_INLINE_BODY_LIMIT)
		if file.size <= u64(inline_body_capacity) {
			body_length := int(file.size)
			body_read := 0
			for body_read < body_length {
				read_length, read_error := linux.pread(
					file.fd,
					output[response_length + body_read:response_length + body_length],
					i64(body_read),
				)
				if read_error == .EINTR {
					continue
				}
				if read_error != .NONE {
					close_static_file(&file)
					return static_file_response_failure(.System_Error)
				}
				if read_length == 0 {
					break
				}

				body_read += read_length
			}
			if body_read == body_length {
				response_length += body_length
				stream_body = false
			}
		}
	}

	if !stream_body && !close_static_file(&file) {
		return static_file_response_failure(.System_Error)
	}

	return Static_File_Response_Event {
		file = file,
		response_length = response_length,
		keep_alive = policy == .Keep_Alive,
		stream_body = stream_body,
		result = .Encoded,
	}
}

static_file_response_failure :: proc(
	result: Static_File_Response_Result,
) -> Static_File_Response_Event {
	return Static_File_Response_Event{file = Static_File{fd = -1}, result = result}
}

static_file_prefix_matches :: proc(path, prefix: []u8) -> bool {
	for i := 0; i < len(prefix); i += 1 {
		if path[i] != prefix[i] {
			return false
		}
	}
	return true
}

static_file_span :: proc(input: []u8, span: http1.Span) -> (value: []u8, valid: bool) {
	if span.offset < 0 || span.length < 0 || span.offset > len(input) {
		return nil, false
	}
	if span.length > len(input) - span.offset {
		return nil, false
	}
	return input[span.offset:span.offset + span.length], true
}
