#+build linux
package abi

import odin_runtime "base:runtime"
import "core:mem"
import "core:sync"
import response "veylor:http1/response"
import router "veylor:router"
import server_runtime "veylor:runtime/linux"
import workers "veylor:runtime/workers"
import socket "veylor:transport/linux/socket"

VYL_LISTENER_BACKLOG :: 128

Vyl_Server_State :: enum {
	Configuring,
	Listening,
	Running,
	Stop_Requested,
	Joined,
	Failed,
}

Vyl_Server :: struct {
	config:               Vyl_Config,
	state:                Vyl_Server_State,
	route_storage:        [VYL_MAX_ROUTES]router.Static_Route,
	route_index_storage:  [VYL_ROUTE_INDEX_CAPACITY]router.Route_Index_Slot,
	routes:               router.Route_Table,
	route_bytes:          [VYL_ROUTE_BYTES_CAPACITY]u8,
	route_bytes_used:     int,
	native_route_storage: [VYL_MAX_ROUTES]router.Native_Route,
	native_routes:        router.Native_Table,
	worker_storage:       []server_runtime.Static_Worker_Storage,
	server_workers:       [VYL_MAX_WORKERS]^server_runtime.Static_Worker,
	worker_threads:       [VYL_MAX_WORKERS]workers.Worker_Thread,
	managed_server:       server_runtime.Managed_Server,
}

@(export)
vyl_server_create :: proc "c" (config: ^Vyl_Config, out_server: ^^Vyl_Server) -> i32 {
	context = odin_runtime.default_context()

	if config == nil || out_server == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}
	out_server^ = nil

	if config.abi_version != VYL_ABI_VERSION || config.struct_size != size_of(Vyl_Config) {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if config.reserved != 0 || config.worker_count == 0 || config.worker_count > VYL_MAX_WORKERS {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if config.shutdown_grace_ns == 0 {
		return i32(Vyl_Result.Invalid_Argument)
	}

	server_memory, allocation_error := mem.alloc(size_of(Vyl_Server), align_of(Vyl_Server))
	if allocation_error != .None {
		return i32(Vyl_Result.Out_Of_Memory)
	}
	mem.zero(server_memory, size_of(Vyl_Server))

	server := cast(^Vyl_Server)server_memory
	worker_storage_bytes, worker_allocation_error := mem.alloc_bytes(
		int(config.worker_count) * size_of(server_runtime.Static_Worker_Storage),
		align_of(server_runtime.Static_Worker_Storage),
	)
	if worker_allocation_error != .None {
		mem.free(server_memory)
		return i32(Vyl_Result.Out_Of_Memory)
	}
	mem.zero(raw_data(worker_storage_bytes), len(worker_storage_bytes))
	worker_storage_ptr := cast([^]server_runtime.Static_Worker_Storage)raw_data(
		worker_storage_bytes,
	)
	server.worker_storage = worker_storage_ptr[:int(config.worker_count)]
	server.config = config^
	server.routes = router.init_route_table(server.route_storage[:], server.route_index_storage[:])
	server.native_routes = router.init_native_table(server.native_route_storage[:])
	server.state = .Configuring

	out_server^ = server
	return i32(Vyl_Result.Ok)
}

@(export)
vyl_server_route_static :: proc "c" (
	server: ^Vyl_Server,
	method: Vyl_Bytes,
	path: Vyl_Bytes,
	status: i32,
	reason: Vyl_Bytes,
	body: Vyl_Bytes,
) -> i32 {
	context = odin_runtime.default_context()

	if server == nil || !valid_configuring_server(server) {
		return i32(Vyl_Result.Bad_State)
	}
	if !valid_input_bytes(method, VYL_MAX_METHOD_BYTES, false) ||
	   !valid_input_bytes(path, VYL_MAX_PATH_BYTES, false) ||
	   !valid_input_bytes(reason, VYL_MAX_REASON_BYTES, false) ||
	   !valid_input_bytes(body, VYL_MAX_RESPONSE_BODY_BYTES, true) {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if status < 200 || status > 599 {
		return i32(Vyl_Result.Invalid_Argument)
	}

	method_bytes := method.data[:int(method.length)]
	path_bytes := path.data[:int(path.length)]
	reason_bytes := reason.data[:int(reason.length)]
	body_bytes: []u8
	if body.length > 0 {
		body_bytes = body.data[:int(body.length)]
	}
	if !response.valid_response_value(reason_bytes) {
		return i32(Vyl_Result.Invalid_Argument)
	}
	_, native_conflict := router.find_native_route(&server.native_routes, method_bytes, path_bytes)
	if native_conflict == .Found {
		return i32(Vyl_Result.Duplicate_Route)
	}

	bytes_needed := int(method.length + path.length + reason.length + body.length)
	if server.route_bytes_used > len(server.route_bytes) - bytes_needed {
		return i32(Vyl_Result.Capacity)
	}
	previous_bytes_used := server.route_bytes_used
	owned_method := retain_route_bytes(server, method_bytes)
	owned_path := retain_route_bytes(server, path_bytes)
	owned_reason := retain_route_bytes(server, reason_bytes)
	owned_body := retain_route_bytes(server, body_bytes)

	register_result := router.register_static_route(
		&server.routes,
		owned_method,
		owned_path,
		router.Static_Response{status = u16(status), reason = owned_reason, body = owned_body},
	)
	if register_result != .Added {
		server.route_bytes_used = previous_bytes_used
		if register_result == .Duplicate {
			return i32(Vyl_Result.Duplicate_Route)
		}
		if register_result == .Full || register_result == .Index_Capacity {
			return i32(Vyl_Result.Capacity)
		}
		return i32(Vyl_Result.Invalid_Argument)
	}

	return i32(Vyl_Result.Ok)
}

@(export)
vyl_server_route_native :: proc "c" (
	server: ^Vyl_Server,
	method: Vyl_Bytes,
	path: Vyl_Bytes,
	handler: Vyl_Native_Handler,
	user_data: rawptr,
) -> i32 {
	context = odin_runtime.default_context()

	if server == nil || !valid_configuring_server(server) {
		return i32(Vyl_Result.Bad_State)
	}
	if !valid_input_bytes(method, VYL_MAX_METHOD_BYTES, false) ||
	   !valid_input_bytes(path, VYL_MAX_PATH_BYTES, false) {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if handler == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}

	method_bytes := method.data[:int(method.length)]
	path_bytes := path.data[:int(path.length)]

	bytes_needed := int(method.length + path.length)
	if server.route_bytes_used > len(server.route_bytes) - bytes_needed {
		return i32(Vyl_Result.Capacity)
	}
	for i := 0; i < server.routes.count; i += 1 {
		static_route := server.routes.routes[i]
		if router.compare_bytes(static_route.method, method_bytes) == 0 &&
		   router.compare_bytes(static_route.path, path_bytes) == 0 {
			return i32(Vyl_Result.Duplicate_Route)
		}
	}

	previous_bytes_used := server.route_bytes_used
	owned_method := retain_route_bytes(server, method_bytes)
	owned_path := retain_route_bytes(server, path_bytes)

	register_result := router.register_native_route(
		&server.native_routes,
		owned_method,
		owned_path,
		router.Native_Handler(handler),
		user_data,
	)
	if register_result != .Added {
		server.route_bytes_used = previous_bytes_used
		if register_result == .Duplicate {
			return i32(Vyl_Result.Duplicate_Route)
		}
		if register_result == .Full {
			return i32(Vyl_Result.Capacity)
		}
		return i32(Vyl_Result.Invalid_Argument)
	}

	return i32(Vyl_Result.Ok)
}

@(export)
vyl_server_listen :: proc "c" (
	server: ^Vyl_Server,
	address: [^]u8,
	port: u16,
	bound_port: ^u16,
	system_error: ^i32,
) -> i32 {
	context = odin_runtime.default_context()

	if server == nil || address == nil || bound_port == nil || system_error == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if !valid_configuring_server(server) {
		return i32(Vyl_Result.Bad_State)
	}
	bound_port^ = 0
	system_error^ = 0

	freeze_result := router.freeze_route_table(&server.routes)
	if freeze_result != .Frozen {
		server.state = .Failed
		return route_table_result(freeze_result)
	}

	listen_address := [4]u8{address[0], address[1], address[2], address[3]}
	worker_count := int(server.config.worker_count)
	for worker_index := 0; worker_index < worker_count; worker_index += 1 {
		listen_port := port
		if worker_index > 0 {
			listen_port = server.worker_storage[0].worker.listener.local_address.port
		}

		listener, listen_result, listen_error := socket.listen_ipv4(
			listen_address,
			listen_port,
			VYL_LISTENER_BACKLOG,
			worker_count > 1,
		)
		if listen_result != .Ready {
			system_error^ = listen_error
			close_unmanaged_workers(server)
			server.state = .Failed
			return socket_result(listen_result)
		}

		worker_storage := &server.worker_storage[worker_index]
		worker_result := server_runtime.init_static_worker_storage(
			worker_storage,
			listener,
			u32(worker_index + 1),
			1,
			&server.routes,
			nil,
			&server.native_routes,
		)
		if worker_result.error != .None {
			system_error^ = worker_result.system_error
			close_unmanaged_workers(server)
			server.state = .Failed
			if worker_result.error == .Invalid {
				return i32(Vyl_Result.Invalid_Argument)
			}
			if worker_result.error == .Connection_Table_Failed ||
			   worker_result.error == .Timer_Queue_Failed ||
			   worker_result.error == .Workspace_Pool_Failed {
				return i32(Vyl_Result.Capacity)
			}
			return i32(Vyl_Result.System_Error)
		}
		server.server_workers[worker_index] = &worker_storage.worker
	}

	bound_port^ = server.worker_storage[0].worker.listener.local_address.port
	managed_result := server_runtime.init_managed_server(
		&server.managed_server,
		server.server_workers[:worker_count],
		server.worker_threads[:worker_count],
		server.config.shutdown_grace_ns,
	)
	if managed_result != .Initialized {
		close_unmanaged_workers(server)
		server.state = .Failed
		return managed_server_result(managed_result)
	}

	server.state = .Listening
	return i32(Vyl_Result.Ok)
}

@(export)
vyl_server_start :: proc "c" (server: ^Vyl_Server) -> i32 {
	context = odin_runtime.default_context()

	if server == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if server.state != .Listening {
		return i32(Vyl_Result.Bad_State)
	}

	start_result := server_runtime.start_managed_server(&server.managed_server)
	if start_result != .Started {
		server.state = .Failed
		return managed_server_result(start_result)
	}

	server.state = .Running
	return i32(Vyl_Result.Ok)
}

@(export)
vyl_server_stop_accepting :: proc "c" (server: ^Vyl_Server) -> i32 {
	context = odin_runtime.default_context()

	if server == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if server.state != .Running {
		return i32(Vyl_Result.Bad_State)
	}

	stop_result := server_runtime.request_managed_server_stop(&server.managed_server)
	if server.managed_server.phase == .Stop_Requested {
		server.state = .Stop_Requested
	}
	if stop_result != .Stop_Requested && stop_result != .Already_Stopping {
		return managed_server_result(stop_result)
	}

	return i32(Vyl_Result.Ok)
}

@(export)
vyl_server_join :: proc "c" (server: ^Vyl_Server) -> i32 {
	context = odin_runtime.default_context()

	if server == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if server.state != .Stop_Requested {
		return i32(Vyl_Result.Bad_State)
	}

	caller := sync.current_thread_id()
	for i := 0; i < int(server.config.worker_count); i += 1 {
		worker_thread := server.worker_threads[i].thread
		if worker_thread != nil && worker_thread.id == caller {
			return i32(Vyl_Result.Busy)
		}
	}

	join_result := server_runtime.join_managed_server(&server.managed_server)
	if join_result != .Joined {
		if server.managed_server.phase == .Joined {
			server.state = .Joined
		}
		return managed_server_result(join_result)
	}

	server.state = .Joined
	return i32(Vyl_Result.Ok)
}

@(export)
vyl_server_destroy :: proc "c" (server: ^Vyl_Server) -> i32 {
	context = odin_runtime.default_context()

	if server == nil {
		return i32(Vyl_Result.Invalid_Argument)
	}
	if server.state == .Running || server.state == .Stop_Requested {
		return i32(Vyl_Result.Busy)
	}

	if len(server.managed_server.server_workers) > 0 {
		destroy_result := server_runtime.destroy_managed_server(&server.managed_server)
		if destroy_result != .Destroyed {
			return managed_server_result(destroy_result)
		}
	} else {
		close_unmanaged_workers(server)
	}

	if len(server.worker_storage) > 0 {
		worker_storage_free_error := mem.free(raw_data(server.worker_storage))
		if worker_storage_free_error != .None {
			return i32(Vyl_Result.System_Error)
		}
		server.worker_storage = nil
	}

	free_error := mem.free(rawptr(server))
	if free_error != .None {
		return i32(Vyl_Result.System_Error)
	}

	return i32(Vyl_Result.Ok)
}

valid_configuring_server :: proc(server: ^Vyl_Server) -> bool {
	return server.state == .Configuring
}

valid_input_bytes :: proc(bytes: Vyl_Bytes, maximum: u32, allow_empty: bool) -> bool {
	if bytes.length > maximum {
		return false
	}
	if bytes.length == 0 {
		return allow_empty
	}
	return bytes.data != nil
}

retain_route_bytes :: proc(server: ^Vyl_Server, source: []u8) -> []u8 {
	first := server.route_bytes_used
	last := first + len(source)
	copy(server.route_bytes[first:last], source)
	server.route_bytes_used = last
	return server.route_bytes[first:last]
}

close_unmanaged_workers :: proc(server: ^Vyl_Server) {
	for worker_index := 0; worker_index < len(server.worker_storage); worker_index += 1 {
		server_runtime.close_unmanaged_static_worker_storage(&server.worker_storage[worker_index])
	}
}

route_table_result :: proc(result: router.Route_Table_Result) -> i32 {
	if result == .Duplicate {
		return i32(Vyl_Result.Duplicate_Route)
	}
	if result == .Full || result == .Index_Capacity {
		return i32(Vyl_Result.Capacity)
	}
	return i32(Vyl_Result.Invalid_Argument)
}

socket_result :: proc(result: socket.Result) -> i32 {
	if result == .Error {
		return i32(Vyl_Result.System_Error)
	}
	if result == .Descriptor_Limit {
		return i32(Vyl_Result.Capacity)
	}
	return i32(Vyl_Result.Invalid_Argument)
}

managed_server_result :: proc(result: server_runtime.Managed_Server_Result) -> i32 {
	if result == .System_Error {
		return i32(Vyl_Result.System_Error)
	}
	if result == .Capacity {
		return i32(Vyl_Result.Capacity)
	}
	if result == .Busy {
		return i32(Vyl_Result.Busy)
	}
	if result == .Worker_Error {
		return i32(Vyl_Result.Worker_Error)
	}
	return i32(Vyl_Result.Bad_State)
}
