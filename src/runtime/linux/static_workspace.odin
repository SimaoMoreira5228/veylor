#+build linux
package linux_runtime

import connection "veylor:http1/connection"

init_static_workspace_pool :: proc(
	storage: []Static_Connection_Workspace,
	free_stack: []u32,
) -> (
	pool: Static_Workspace_Pool,
	result: bool,
) {
	if len(storage) == 0 || len(free_stack) < len(storage) {
		return {}, false
	}

	for i := 0; i < len(storage); i += 1 {
		if len(storage[i].request_input) == 0 ||
		   len(storage[i].headers) == 0 ||
		   len(storage[i].response) == 0 {
			return {}, false
		}
		free_stack[i] = u32(len(storage) - i - 1)
	}

	return Static_Workspace_Pool {
			storage = storage,
			free_stack = free_stack,
			free_count = len(storage),
			initialized = true,
		},
		true
}

acquire_static_workspace :: proc(
	state: ^Static_Connection,
) -> (
	workspace: ^Static_Connection_Workspace,
	acquired: bool,
) {
	if state == nil || !valid_static_workspace_pool(state.workspace_pool) {
		return nil, false
	}
	pool := state.workspace_pool

	if state.workspace_slot >= 0 {
		if state.workspace_slot >= len(pool.storage) {
			return nil, false
		}

		return &pool.storage[state.workspace_slot], true
	}

	if state.workspace_slot != -1 {
		return nil, false
	}

	if pool.free_count == 0 {
		pool.exhaustion_count += 1

		return nil, false
	}
	stack_index := pool.free_count - 1
	slot := pool.free_stack[stack_index]

	if u64(slot) >= u64(len(pool.storage)) {
		return nil, false
	}
	pool.free_count = stack_index
	pool.active_count += 1
	pool.acquisition_count += 1
	pool.high_water_mark = max(pool.high_water_mark, pool.active_count)
	state.workspace_slot = int(slot)
	workspace = &pool.storage[int(slot)]
	workspace.head_driver = connection.begin_connection_head(
		static_connection_event_id(state.token),
	)
	workspace.body_driver = {}
	workspace.managed_route_id = 0
	workspace.body_start = 0
	workspace.body_valid = 0
	workspace.staged_length = 0
	workspace.body_complete = false
	workspace.trailer_head = nil
	workspace.trailer_allow_count = 0
	workspace.trailer_count = 0
	workspace.trailer_data_used = 0
	workspace.streaming = false
	workspace.stream_ended = false
	workspace.stream_terminal = false
	workspace.stream_writable = false

	return workspace, true
}

release_static_workspace :: proc(state: ^Static_Connection) -> bool {
	if state == nil {
		return false
	}

	if state.workspace_slot == -1 {
		return true
	}

	if !valid_static_workspace_pool(state.workspace_pool) {
		return false
	}
	pool := state.workspace_pool

	if state.workspace_slot < 0 || state.workspace_slot >= len(pool.storage) {
		return false
	}

	if pool.free_count >= len(pool.storage) {
		return false
	}

	if pool.active_count <= 0 {
		return false
	}
	slot := state.workspace_slot
	reset_static_workspace(&pool.storage[slot])
	pool.free_stack[pool.free_count] = u32(slot)
	pool.free_count += 1
	pool.active_count -= 1
	pool.release_count += 1
	state.workspace_slot = -1
	state.write_driver = {}

	return true
}

reset_static_workspace :: proc(workspace: ^Static_Connection_Workspace) {
	workspace.head_driver.parser.input_base = nil
	workspace.head_driver.parser.header_base = nil
	workspace.body_driver = {}
	workspace.managed_route_id = 0
	workspace.body_start = 0
	workspace.body_valid = 0
	workspace.staged_length = 0
	workspace.body_complete = false
	workspace.trailer_head = nil
	workspace.trailer_allow_count = 0
	workspace.trailer_count = 0
	workspace.trailer_data_used = 0
	workspace.streaming = false
	workspace.stream_ended = false
	workspace.stream_terminal = false
	workspace.stream_writable = false
}

valid_static_workspace_pool :: proc(pool: ^Static_Workspace_Pool) -> bool {
	if pool == nil || !pool.initialized || len(pool.storage) == 0 {
		return false
	}

	if len(pool.free_stack) < len(pool.storage) ||
	   pool.free_count < 0 ||
	   pool.free_count > len(pool.storage) ||
	   pool.active_count < 0 ||
	   pool.active_count > len(pool.storage) ||
	   pool.free_count + pool.active_count != len(pool.storage) ||
	   pool.high_water_mark < pool.active_count ||
	   pool.high_water_mark > len(pool.storage) {
		return false
	}

	return true
}
