#+build linux
package linux_runtime

import "core:sys/linux"
import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
import shutdown "veylor:runtime/shutdown"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

STATIC_WORKER_CONNECTION_CAPACITY :: 128
STATIC_WORKER_TIMER_CAPACITY :: STATIC_WORKER_CONNECTION_CAPACITY + 2
STATIC_WORKER_TIMER_INDEX_CAPACITY ::
	STATIC_WORKER_TIMER_CAPACITY * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR
STATIC_WORKER_WORKSPACE_CAPACITY :: 8
STATIC_WORKER_REQUEST_INPUT_CAPACITY :: 16 * 1024
STATIC_WORKER_HEADER_CAPACITY :: 100
STATIC_WORKER_RESPONSE_CAPACITY :: 1024
STATIC_WORKER_EVENT_CAPACITY :: 128
STATIC_WORKER_ACCEPT_CAPACITY :: 16

Static_Worker_Storage_Error :: enum {
	None,
	Invalid,
	Poller_Failed,
	Connection_Table_Failed,
	Timer_Queue_Failed,
	Workspace_Pool_Failed,
}

Static_Worker_Storage_Result :: struct {
	error:        Static_Worker_Storage_Error,
	system_error: i32,
}

Static_Worker_Storage :: struct {
	worker:                Static_Worker,
	connection_slots:      [STATIC_WORKER_CONNECTION_CAPACITY]connections.Connection_Slot,
	connection_free_stack: [STATIC_WORKER_CONNECTION_CAPACITY]u32,
	connection_table:      connections.Connection_Table,
	shutdown_state:        shutdown.State,
	timer_storage:         [STATIC_WORKER_TIMER_CAPACITY]transport.Timer_Expiration,
	timer_index_storage:   [STATIC_WORKER_TIMER_INDEX_CAPACITY]u32,
	timers:                transport.Timer_Queue,
	connection_states:     [STATIC_WORKER_CONNECTION_CAPACITY]Static_Connection,
	workspace_storage:     [STATIC_WORKER_WORKSPACE_CAPACITY]Static_Connection_Workspace,
	workspace_free_stack:  [STATIC_WORKER_WORKSPACE_CAPACITY]u32,
	workspace_pool:        Static_Workspace_Pool,
	request_input_storage: [STATIC_WORKER_WORKSPACE_CAPACITY][STATIC_WORKER_REQUEST_INPUT_CAPACITY]u8,
	header_storage:        [STATIC_WORKER_WORKSPACE_CAPACITY][STATIC_WORKER_HEADER_CAPACITY]http1.Header,
	response_storage:      [STATIC_WORKER_WORKSPACE_CAPACITY][STATIC_WORKER_RESPONSE_CAPACITY]u8,
	native_events:         [STATIC_WORKER_EVENT_CAPACITY]linux.EPoll_Event,
	readiness_events:      [STATIC_WORKER_EVENT_CAPACITY]transport.Readiness_Event,
	accepted_storage:      [STATIC_WORKER_ACCEPT_CAPACITY]socket.Accepted_Connection,
	accepted_connections:  [STATIC_WORKER_ACCEPT_CAPACITY]Worker_Connection,
	expired_timer_storage: [STATIC_WORKER_TIMER_CAPACITY]transport.Timer_Expiration,
	initialized:           bool,
}

init_static_worker_storage :: proc(
	storage: ^Static_Worker_Storage,
	listener: socket.Listener,
	worker_id: u32,
	worker_generation: u64,
	routes: ^router.Route_Table,
	file_root: ^Static_File_Root,
) -> Static_Worker_Storage_Result {
	if storage == nil {
		return {error = .Invalid}
	}

	storage.worker.poller.fd = -1
	storage.worker.listener = listener
	storage.initialized = true

	if worker_id == 0 || worker_generation == 0 || routes == nil {
		return {error = .Invalid}
	}

	if listener.fd < 0 || routes.state != .Frozen {
		return {error = .Invalid}
	}

	if file_root != nil && !valid_static_file_root(file_root) {
		return {error = .Invalid}
	}

	poller, poll_result, poll_error := epoll.create()
	if poll_result != .Ready {
		return {error = .Poller_Failed, system_error = poll_error}
	}

	table, table_result := connections.init_connection_table(
		worker_id,
		worker_generation,
		storage.connection_slots[:],
		storage.connection_free_stack[:],
	)
	if table_result != .Ready {
		epoll.close(&poller)
		return {error = .Connection_Table_Failed}
	}
	storage.connection_table = table

	storage.timers = transport.init_timer_queue(
		storage.timer_storage[:],
		storage.timer_index_storage[:],
	)
	if !transport.valid_timer_queue(&storage.timers) {
		epoll.close(&poller)
		return {error = .Timer_Queue_Failed}
	}

	for workspace_index := 0; workspace_index < STATIC_WORKER_WORKSPACE_CAPACITY; workspace_index += 1 {
		storage.workspace_storage[workspace_index] = Static_Connection_Workspace {
			request_input = storage.request_input_storage[workspace_index][:],
			headers       = storage.header_storage[workspace_index][:],
			response      = storage.response_storage[workspace_index][:],
		}
	}

	workspace_pool, pool_ready := init_static_workspace_pool(
		storage.workspace_storage[:],
		storage.workspace_free_stack[:],
	)
	if !pool_ready {
		epoll.close(&poller)
		return {error = .Workspace_Pool_Failed}
	}
	storage.workspace_pool = workspace_pool

	storage.worker = Static_Worker {
		poller = poller,
		listener = listener,
		shutdown_state = &storage.shutdown_state,
		table = &storage.connection_table,
		routes = routes,
		file_root = file_root,
		connection_states = storage.connection_states[:],
		workspace_pool = &storage.workspace_pool,
		native_events = storage.native_events[:],
		readiness_events = storage.readiness_events[:],
		accepted_storage = storage.accepted_storage[:],
		accepted_connections = storage.accepted_connections[:],
		timers = &storage.timers,
		expired_timer_storage = storage.expired_timer_storage[:],
		event_budget = STATIC_WORKER_EVENT_CAPACITY,
		byte_budget = 64 * 1024,
		accept_attempt_budget = STATIC_WORKER_ACCEPT_CAPACITY,
		timeout_ms = 100,
		body_limit = 16 * 1024 * 1024,
		timeouts = connections.Connection_Timeouts {
			request_head_ns = 10_000_000_000,
			response_write_ns = 10_000_000_000,
			keep_alive_idle_ns = 5_000_000_000,
		},
	}

	return {error = .None}
}

close_unmanaged_static_worker_storage :: proc(storage: ^Static_Worker_Storage) {
	if storage == nil || !storage.initialized {
		return
	}

	if storage.worker.poller.fd >= 0 {
		epoll.close(&storage.worker.poller)
	}

	if storage.worker.listener.fd >= 0 {
		socket.close_listener(&storage.worker.listener)
	}

	storage.initialized = false
}
