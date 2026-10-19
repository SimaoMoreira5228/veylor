#+build linux
package linux_runtime

import router "veylor:router"
import host "veylor:runtime/host"
import epoll "veylor:transport/linux"

init_static_connection :: proc(
	worker: Worker_Connection,
	workspace_pool: ^Static_Workspace_Pool,
	file_root: ^Static_File_Root = nil,
	native_routes: ^router.Native_Table = nil,
	host_queue: ^host.Host_Queue = nil,
) -> (
	state: Static_Connection,
	result: Static_Connection_Result,
) {
	if worker.fd < 0 ||
	   !valid_static_workspace_pool(workspace_pool) ||
	   (file_root != nil && !valid_static_file_root(file_root)) {
		return {}, .Invalid
	}
	state = Static_Connection {
		token = worker.token,
		fd = worker.fd,
		workspace_pool = workspace_pool,
		file_root = file_root,
		native_routes = native_routes,
		host_queue = host_queue,
		workspace_slot = -1,
		file = Static_File{fd = -1},
		phase = .Reading_Request_Head,
		active = true,
	}

	return state, .Need_More
}

register_static_connection :: proc(
	poller: epoll.Epoll,
	state: ^Static_Connection,
) -> (
	result: epoll.Result,
	error_code: i32,
) {
	if state == nil || state.fd < 0 || state.phase != .Reading_Request_Head {
		return .Invalid, 0
	}
	result, error_code = epoll.register(
		poller,
		state.fd,
		static_connection_event_id(state.token),
		epoll.Interest{readable = true},
	)

	if result == .Ready {
		state.registered = true
	}

	return
}
