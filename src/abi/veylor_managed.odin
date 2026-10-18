#+build linux
package abi

import odin_runtime "base:runtime"
import response "veylor:http1/response"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"
import server_runtime "veylor:runtime/linux"

@(export)
vyl_request_pull_body :: proc "c" (
	server: ^Vyl_Server,
	token: Vyl_Request_Token,
	max_bytes: u32,
) -> i32 {
	context = odin_runtime.default_context()

	if server == nil || max_bytes == 0 {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if token.worker_id == 0 || token.worker_generation == 0 || token.connection_generation == 0 {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if server.state != .Running && server.state != .Stop_Requested {
		return i32(Vyl_Result.Bad_State)
	}
	worker, found := managed_worker_for_token(server, token)
	if !found {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if worker.command_queue == nil {
		return i32(Vyl_Result.Bad_State)
	}
	if worker.table.worker_id != token.worker_id ||
	   worker.table.worker_generation != token.worker_generation {
		return i32(Vyl_Result.Stale_Token)
	}
	if int(token.slot) >= len(worker.connection_states) {
		return i32(Vyl_Result.Invalid_Argument)
	}
	state := &worker.connection_states[int(token.slot)]
	if !state.active || state.token != managed_connection_token(token) {
		return i32(Vyl_Result.Stale_Token)
	}
	if state.workspace_slot < 0 {
		return i32(Vyl_Result.Bad_State)
	}
	if state.workspace_pool == nil || state.workspace_slot >= len(state.workspace_pool.storage) {
		return i32(Vyl_Result.Stale_Token)
	}
	cmd := host.Host_Command {
		kind      = .Read_Body,
		token     = managed_connection_token(token),
		max_bytes = max_bytes,
	}
	switch host.enqueue_host_command(worker.command_queue, cmd) {
	case .Enqueued:
		return i32(Vyl_Result.Ok)
	case .Full:
		return i32(Vyl_Result.Would_Block)
	case .Invalid:
		return i32(Vyl_Result.Bad_State)
	}
	return i32(Vyl_Result.Bad_State)
}

@(export)
vyl_request_read :: proc "c" (
	server: ^Vyl_Server,
	token: Vyl_Request_Token,
	dst: [^]u8,
	capacity: u32,
	read_bytes: ^u32,
) -> i32 {
	context = odin_runtime.default_context()

	if server == nil || read_bytes == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if capacity == 0 || dst == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if token.worker_id == 0 || token.worker_generation == 0 || token.connection_generation == 0 {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if server.state != .Running && server.state != .Stop_Requested {
		return i32(Vyl_Result.Bad_State)
	}
	worker, found := managed_worker_for_token(server, token)
	if !found {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if worker.table.worker_id != token.worker_id ||
	   worker.table.worker_generation != token.worker_generation {
		return i32(Vyl_Result.Stale_Token)
	}
	if int(token.slot) >= len(worker.connection_states) {
		return i32(Vyl_Result.Invalid_Argument)
	}
	state := &worker.connection_states[int(token.slot)]
	if !state.active || state.token != managed_connection_token(token) {
		return i32(Vyl_Result.Stale_Token)
	}
	if state.workspace_slot < 0 {
		read_bytes^ = 0
		return i32(Vyl_Result.End_Of_Body)
	}
	if state.workspace_pool == nil || state.workspace_slot >= len(state.workspace_pool.storage) {
		return i32(Vyl_Result.Stale_Token)
	}
	workspace := &state.workspace_pool.storage[state.workspace_slot]
	staged := workspace.staged_length
	if staged < 0 {
		staged = 0
	}
	if staged > len(workspace.staged_body) {
		staged = len(workspace.staged_body)
	}
	take := int(capacity)
	if take > staged {
		take = staged
	}
	if take > 0 {
		copy(dst[:take], workspace.staged_body[:take])
		remaining := staged - take
		if remaining > 0 {
			copy(workspace.staged_body[:remaining], workspace.staged_body[take:staged])
		}
		workspace.staged_length = remaining
	}
	read_bytes^ = u32(take)
	if workspace.body_complete && workspace.staged_length == 0 {
		return i32(Vyl_Result.End_Of_Body)
	}
	if take > 0 {
		return i32(Vyl_Result.Ok)
	}
	return i32(Vyl_Result.Would_Block)
}

managed_worker_for_token :: proc(
	server: ^Vyl_Server,
	token: Vyl_Request_Token,
) -> (
	^server_runtime.Static_Worker,
	bool,
) {
	count := int(server.config.worker_count)
	if count <= 0 || int(token.worker_id) > count {
		return nil, false
	}
	if int(token.worker_id) > len(server.server_workers) {
		return nil, false
	}
	worker := server.server_workers[int(token.worker_id) - 1]
	if worker == nil {
		return nil, false
	}
	return worker, true
}

managed_connection_token :: proc(token: Vyl_Request_Token) -> connections.Connection_Token {
	return connections.Connection_Token {
		worker_id = token.worker_id,
		worker_generation = token.worker_generation,
		slot = token.slot,
		generation = token.connection_generation,
	}
}

@(export)
vyl_response_start :: proc "c" (
	server: ^Vyl_Server,
	token: Vyl_Request_Token,
	status: i32,
	reason: Vyl_Bytes,
	body: Vyl_Bytes,
) -> i32 {
	context = odin_runtime.default_context()

	if server == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if status < 200 || status > 599 {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if !valid_input_bytes(reason, VYL_MAX_REASON_BYTES, false) ||
	   !valid_input_bytes(body, VYL_MAX_RESPONSE_BODY_BYTES, true) {
		return i32(Vyl_Result.Invalid_Argument)
	}
	reason_bytes := reason.data[:int(reason.length)]
	if !response.valid_response_value(reason_bytes) {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if token.worker_id == 0 || token.worker_generation == 0 || token.connection_generation == 0 {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if server.state != .Running && server.state != .Stop_Requested {
		return i32(Vyl_Result.Bad_State)
	}
	worker, found := managed_worker_for_token(server, token)
	if !found {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if worker.command_queue == nil {
		return i32(Vyl_Result.Bad_State)
	}
	if worker.table.worker_id != token.worker_id ||
	   worker.table.worker_generation != token.worker_generation {
		return i32(Vyl_Result.Stale_Token)
	}
	if int(token.slot) >= len(worker.connection_states) {
		return i32(Vyl_Result.Invalid_Argument)
	}
	state := &worker.connection_states[int(token.slot)]
	if !state.active || state.token != managed_connection_token(token) {
		return i32(Vyl_Result.Stale_Token)
	}
	cmd := host.Host_Command {
		kind          = .Response_Start,
		token         = managed_connection_token(token),
		status        = status,
		reason_length = reason.length,
		body_length   = body.length,
	}
	copy(cmd.reason[:], reason_bytes)
	if body.length > 0 {
		copy(cmd.body[:], body.data[:int(body.length)])
	}
	switch host.enqueue_host_command(worker.command_queue, cmd) {
	case .Enqueued:
		return i32(Vyl_Result.Ok)
	case .Full:
		return i32(Vyl_Result.Would_Block)
	case .Invalid:
		return i32(Vyl_Result.Bad_State)
	}
	return i32(Vyl_Result.Bad_State)
}
