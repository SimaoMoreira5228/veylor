#+build linux
package abi

import odin_runtime "base:runtime"
import response "veylor:http1/response"
import http2 "veylor:http2"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import server_runtime "veylor:runtime/linux"

h2_request_pull_body :: proc "c" (
	server: ^Vyl_Server,
	worker: ^server_runtime.Static_Worker,
	token: Vyl_Request_Token,
	max_bytes: u32,
) -> i32 {
	context = odin_runtime.default_context()

	if worker.command_queue == nil {
		return i32(Vyl_Result.Bad_State)
	}
	_, _, h2ok := server_runtime.h2_command_target(worker, managed_connection_token(token))

	if !h2ok {
		return i32(Vyl_Result.Stale_Token)
	}
	cmd := host.Host_Command {
		kind      = .Read_Body,
		token     = managed_connection_token(token),
		max_bytes = max_bytes,
	}

	switch enqueue_managed_command(worker, cmd) {
	case .Enqueued:
		return i32(Vyl_Result.Ok)
	case .Full:
		return i32(Vyl_Result.Would_Block)
	case .Invalid:
		return i32(Vyl_Result.Bad_State)
	}

	return i32(Vyl_Result.Bad_State)
}

h2_request_read :: proc "c" (
	server: ^Vyl_Server,
	worker: ^server_runtime.Static_Worker,
	token: Vyl_Request_Token,
	dst: [^]u8,
	capacity: u32,
	read_bytes: ^u32,
) -> i32 {
	context = odin_runtime.default_context()

	_, record, h2ok := server_runtime.h2_command_target(worker, managed_connection_token(token))

	if !h2ok {
		return i32(Vyl_Result.Stale_Token)
	}
	staged := record.staged_len

	if staged < 0 {
		staged = 0
	}

	if staged > len(record.staged) {
		staged = len(record.staged)
	}
	take := int(capacity)

	if take > staged {
		take = staged
	}

	if take > 0 {
		copy(dst[:take], record.staged[:take])
		remaining := staged - take

		if remaining > 0 {
			copy(record.staged[:remaining], record.staged[take:staged])
		}
		record.staged_len = remaining
		server_runtime.signal_static_worker(worker)
	}
	read_bytes^ = u32(take)

	if record.end_received && record.staged_len == 0 {
		return i32(Vyl_Result.End_Of_Body)
	}

	if take > 0 {
		return i32(Vyl_Result.Ok)
	}

	return i32(Vyl_Result.Would_Block)
}

h2_response_start :: proc "c" (
	server: ^Vyl_Server,
	worker: ^server_runtime.Static_Worker,
	token: Vyl_Request_Token,
	status: i32,
	reason: Vyl_Bytes,
	body: Vyl_Bytes,
) -> i32 {
	context = odin_runtime.default_context()

	if worker.command_queue == nil {
		return i32(Vyl_Result.Bad_State)
	}
	_, record, h2ok := server_runtime.h2_command_target(worker, managed_connection_token(token))

	if !h2ok || record.resp_started || record.resp_ended {
		return i32(Vyl_Result.Stale_Token)
	}
	cmd := host.Host_Command {
		kind          = .Response_Start,
		token         = managed_connection_token(token),
		status        = status,
		reason_length = reason.length,
		body_length   = body.length,
	}
	copy(cmd.reason[:], reason.data[:int(reason.length)])

	if body.length > 0 {
		copy(cmd.body[:], body.data[:int(body.length)])
	}

	switch enqueue_managed_command(worker, cmd) {
	case .Enqueued:
		return i32(Vyl_Result.Ok)
	case .Full:
		return i32(Vyl_Result.Would_Block)
	case .Invalid:
		return i32(Vyl_Result.Bad_State)
	}

	return i32(Vyl_Result.Bad_State)
}

h2_response_begin :: proc "c" (
	server: ^Vyl_Server,
	worker: ^server_runtime.Static_Worker,
	token: Vyl_Request_Token,
	status: i32,
	reason: Vyl_Bytes,
) -> i32 {
	context = odin_runtime.default_context()

	if worker.command_queue == nil {
		return i32(Vyl_Result.Bad_State)
	}
	_, record, h2ok := server_runtime.h2_command_target(worker, managed_connection_token(token))

	if !h2ok || record.resp_started || record.resp_ended {
		return i32(Vyl_Result.Stale_Token)
	}

	if record.method_len == 4 {
		head_method := record.method[:record.method_len]

		if head_method[0] == 'H' &&
		   head_method[1] == 'E' &&
		   head_method[2] == 'A' &&
		   head_method[3] == 'D' {
			return i32(Vyl_Result.Invalid_Argument)
		}
	}
	reason_bytes := reason.data[:int(reason.length)]

	if !response.valid_response_value(reason_bytes) {
		return i32(Vyl_Result.Invalid_Argument)
	}
	cmd := host.Host_Command {
		kind          = .Response_Begin,
		token         = managed_connection_token(token),
		status        = status,
		reason_length = reason.length,
	}
	copy(cmd.reason[:], reason.data[:int(reason.length)])

	switch enqueue_managed_command(worker, cmd) {
	case .Enqueued:
		return i32(Vyl_Result.Ok)
	case .Full:
		return i32(Vyl_Result.Would_Block)
	case .Invalid:
		return i32(Vyl_Result.Bad_State)
	}

	return i32(Vyl_Result.Bad_State)
}

h2_response_write :: proc "c" (
	server: ^Vyl_Server,
	worker: ^server_runtime.Static_Worker,
	token: Vyl_Request_Token,
	data: Vyl_Bytes,
) -> i32 {
	context = odin_runtime.default_context()

	if worker.command_queue == nil {
		return i32(Vyl_Result.Bad_State)
	}
	_, record, h2ok := server_runtime.h2_command_target(worker, managed_connection_token(token))

	if !h2ok || !record.resp_started || record.resp_ended {
		return i32(Vyl_Result.Stale_Token)
	}
	cmd := host.Host_Command {
		kind        = .Response_Write,
		token       = managed_connection_token(token),
		body_length = data.length,
	}
	copy(cmd.body[:], data.data[:int(data.length)])

	switch enqueue_managed_command(worker, cmd) {
	case .Enqueued:
		return i32(Vyl_Result.Ok)
	case .Full:
		return i32(Vyl_Result.Would_Block)
	case .Invalid:
		return i32(Vyl_Result.Bad_State)
	}

	return i32(Vyl_Result.Bad_State)
}

h2_response_end :: proc "c" (
	server: ^Vyl_Server,
	worker: ^server_runtime.Static_Worker,
	token: Vyl_Request_Token,
) -> i32 {
	context = odin_runtime.default_context()

	if worker.command_queue == nil {
		return i32(Vyl_Result.Bad_State)
	}
	_, record, h2ok := server_runtime.h2_command_target(worker, managed_connection_token(token))

	if !h2ok || !record.resp_started || record.resp_ended {
		return i32(Vyl_Result.Stale_Token)
	}
	cmd := host.Host_Command {
		kind  = .Response_End,
		token = managed_connection_token(token),
	}

	switch enqueue_managed_command(worker, cmd) {
	case .Enqueued:
		return i32(Vyl_Result.Ok)
	case .Full:
		return i32(Vyl_Result.Would_Block)
	case .Invalid:
		return i32(Vyl_Result.Bad_State)
	}

	return i32(Vyl_Result.Bad_State)
}

h2_request_cancel :: proc "c" (
	server: ^Vyl_Server,
	worker: ^server_runtime.Static_Worker,
	token: Vyl_Request_Token,
) -> i32 {
	context = odin_runtime.default_context()

	if worker.command_queue == nil {
		return i32(Vyl_Result.Bad_State)
	}

	if token.stream_id == 0 {
		slot, found := server_runtime.find_h2_worker_slot(worker, int(token.slot))

		if !found || slot.token.request_seq != token.request_seq {
			return i32(Vyl_Result.Stale_Token)
		}
	} else {
		_, _, h2ok := server_runtime.h2_command_target(worker, managed_connection_token(token))

		if !h2ok {
			return i32(Vyl_Result.Stale_Token)
		}
	}
	cmd := host.Host_Command {
		kind  = .Cancel,
		token = managed_connection_token(token),
	}

	switch enqueue_managed_command(worker, cmd) {
	case .Enqueued:
		return i32(Vyl_Result.Ok)
	case .Full:
		return i32(Vyl_Result.Would_Block)
	case .Invalid:
		return i32(Vyl_Result.Bad_State)
	}

	return i32(Vyl_Result.Bad_State)
}

h2_response_trailers :: proc "c" (
	server: ^Vyl_Server,
	worker: ^server_runtime.Static_Worker,
	token: Vyl_Request_Token,
	trailers: [^]Vyl_Header_Ref,
	trailer_count: u32,
	data: [^]u8,
	data_length: u32,
) -> i32 {
	context = odin_runtime.default_context()

	if worker.command_queue == nil {
		return i32(Vyl_Result.Bad_State)
	}
	_, record, h2ok := server_runtime.h2_command_target(worker, managed_connection_token(token))

	if !h2ok || !record.resp_started || record.resp_ended {
		return i32(Vyl_Result.Stale_Token)
	}
	refs := trailers[:int(trailer_count)]
	host_bytes := data[:int(data_length)]
	wire: [host.HOST_MAX_RESPONSE_BODY_BYTES]u8
	used := 0

	for i := 0; i < len(refs); i += 1 {
		name_end := u64(refs[i].name_offset) + u64(refs[i].name_length)
		value_end := u64(refs[i].value_offset) + u64(refs[i].value_length)

		if name_end > u64(len(host_bytes)) || value_end > u64(len(host_bytes)) {
			return i32(Vyl_Result.Invalid_Argument)
		}
		name := host_bytes[refs[i].name_offset:refs[i].name_offset + refs[i].name_length]
		value := host_bytes[refs[i].value_offset:refs[i].value_offset + refs[i].value_length]

		if !router.valid_trailer_name(name) {
			return i32(Vyl_Result.Invalid_Argument)
		}

		if !response.valid_response_value(value) {
			return i32(Vyl_Result.Invalid_Argument)
		}
		written, enc_ok := http2.encode_literal_header(
			.Without_Indexing,
			0,
			name,
			value,
			false,
			false,
			wire[used:],
		)

		if !enc_ok {
			return i32(Vyl_Result.Invalid_Argument)
		}
		used += written
	}

	if used == 0 {
		return i32(Vyl_Result.Invalid_Argument)
	}
	cmd := host.Host_Command {
		kind        = .Response_Trailers,
		token       = managed_connection_token(token),
		body_length = u32(used),
	}
	copy(cmd.body[:used], wire[:used])

	switch enqueue_managed_command(worker, cmd) {
	case .Enqueued:
		return i32(Vyl_Result.Ok)
	case .Full:
		return i32(Vyl_Result.Would_Block)
	case .Invalid:
		return i32(Vyl_Result.Bad_State)
	}

	return i32(Vyl_Result.Bad_State)
}

h2_request_copy_trailers :: proc "c" (
	server: ^Vyl_Server,
	worker: ^server_runtime.Static_Worker,
	token: Vyl_Request_Token,
	headers: [^]Vyl_Header_Ref,
	header_cap: u32,
	data: [^]u8,
	data_cap: u32,
	out_info: ^Vyl_Trailer_Info,
) -> i32 {
	context = odin_runtime.default_context()
	out_info^ = Vyl_Trailer_Info{}

	_, record, h2ok := server_runtime.h2_command_target(worker, managed_connection_token(token))

	if !h2ok {
		return i32(Vyl_Result.Stale_Token)
	}

	if !record.end_received {
		return i32(Vyl_Result.Would_Block)
	}
	record_refs := record.trailers[:record.trailer_count]
	header_slice := headers[:int(header_cap)]
	data_slice := data[:int(data_cap)]
	data_needed := u32(record.trailer_used)

	if u32(record.trailer_count) > header_cap || data_needed > data_cap {
		out_info^ = Vyl_Trailer_Info {
			headers_required = u32(record.trailer_count),
			data_required    = data_needed,
		}

		return i32(Vyl_Result.Buffer_Too_Small)
	}
	used := u32(0)

	for i in 0 ..< len(record_refs) {
		field := &record_refs[i]
		name := record.trailer_data[field.name_offset:field.name_offset + field.name_length]
		value := record.trailer_data[field.value_offset:field.value_offset + field.value_length]
		header_slice[i] = Vyl_Header_Ref {
			name_offset  = used,
			name_length  = u32(field.name_length),
			value_offset = used + u32(field.name_length),
			value_length = u32(field.value_length),
		}
		copy(data_slice[used:], name)
		used += u32(field.name_length)
		copy(data_slice[used:], value)
		used += u32(field.value_length)
	}
	out_info^ = Vyl_Trailer_Info {
		header_count = u32(record.trailer_count),
		data_used    = used,
	}

	return i32(Vyl_Result.Ok)
}
