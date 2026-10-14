package connections

Connection_Token :: struct {
	worker_id:         u32,
	worker_generation: u64,
	slot:              u32,
	generation:        u64,
}

Connection_Slot_State :: enum {
	Available,
	Occupied,
	Retired,
}

Connection_Phase :: enum {
	Reading_Request_Head,
	Reading_Request_Body,
	Awaiting_Handler,
	Writing_Response,
	Keep_Alive_Idle,
	Closing,
}

Connection_Slot :: struct {
	generation:  u64,
	resource_id: u64,
	state:       Connection_Slot_State,
	phase:       Connection_Phase,
}

Connection_Table :: struct {
	worker_id:         u32,
	worker_generation: u64,
	slots:             []Connection_Slot,
	free_stack:        []u32,
	free_count:        int,
}

Connection_Table_Result :: enum {
	Ready,
	Full,
	Found,
	Released,
	Retired,
	Stale,
	Transitioned,
	Invalid_Transition,
	Invalid,
}

MAX_CONNECTION_SLOT_COUNT :: u64(0x1_0000_0000)
MAX_CONNECTION_GENERATION :: u64(0xffff_ffff_ffff_ffff)

init_connection_table :: proc(
	worker_id: u32,
	worker_generation: u64,
	slots: []Connection_Slot,
	free_stack: []u32,
) -> (
	table: Connection_Table,
	result: Connection_Table_Result,
) {
	if worker_generation == 0 ||
	   len(free_stack) < len(slots) ||
	   u64(len(slots)) > MAX_CONNECTION_SLOT_COUNT {
		return {}, .Invalid
	}
	for i := 0; i < len(slots); i += 1 {
		slots[i] = Connection_Slot {
			generation = 1,
			state      = .Available,
		}
		free_stack[i] = u32(len(slots) - i - 1)
	}
	return Connection_Table {
			worker_id = worker_id,
			worker_generation = worker_generation,
			slots = slots,
			free_stack = free_stack,
			free_count = len(slots),
		},
		.Ready
}

allocate_connection :: proc(
	table: ^Connection_Table,
	resource_id: u64,
) -> (
	token: Connection_Token,
	result: Connection_Table_Result,
) {
	if table.free_count < 0 ||
	   table.free_count > len(table.slots) ||
	   table.free_count > len(table.free_stack) {
		return {}, .Invalid
	}
	if table.free_count == 0 {
		return {}, .Full
	}

	stack_index := table.free_count - 1
	slot_index := table.free_stack[stack_index]
	if u64(slot_index) >= u64(len(table.slots)) {
		return {}, .Invalid
	}
	slot := &table.slots[int(slot_index)]
	if slot.state != .Available || slot.generation == 0 {
		return {}, .Invalid
	}

	table.free_count = stack_index
	slot.resource_id = resource_id
	slot.state = .Occupied
	slot.phase = .Reading_Request_Head
	return Connection_Token {
			worker_id = table.worker_id,
			worker_generation = table.worker_generation,
			slot = slot_index,
			generation = slot.generation,
		},
		.Ready
}

lookup_connection :: proc(
	table: ^Connection_Table,
	token: Connection_Token,
) -> (
	resource_id: u64,
	result: Connection_Table_Result,
) {
	if token.worker_id != table.worker_id || token.worker_generation != table.worker_generation {
		return 0, .Stale
	}
	if u64(token.slot) >= u64(len(table.slots)) {
		return 0, .Invalid
	}
	slot := table.slots[int(token.slot)]
	if slot.state != .Occupied || slot.generation != token.generation {
		return 0, .Stale
	}
	return slot.resource_id, .Found
}

connection_phase :: proc(
	table: ^Connection_Table,
	token: Connection_Token,
) -> (
	phase: Connection_Phase,
	result: Connection_Table_Result,
) {
	if token.worker_id != table.worker_id || token.worker_generation != table.worker_generation {
		return {}, .Stale
	}
	if u64(token.slot) >= u64(len(table.slots)) {
		return {}, .Invalid
	}
	slot := table.slots[int(token.slot)]
	if slot.state != .Occupied || slot.generation != token.generation {
		return {}, .Stale
	}
	return slot.phase, .Found
}

transition_connection :: proc(
	table: ^Connection_Table,
	token: Connection_Token,
	next: Connection_Phase,
) -> Connection_Table_Result {
	if token.worker_id != table.worker_id || token.worker_generation != table.worker_generation {
		return .Stale
	}
	if u64(token.slot) >= u64(len(table.slots)) {
		return .Invalid
	}
	slot := &table.slots[int(token.slot)]
	if slot.state != .Occupied || slot.generation != token.generation {
		return .Stale
	}
	if !connection_phase_transition_allowed(slot.phase, next) {
		return .Invalid_Transition
	}
	slot.phase = next
	return .Transitioned
}

release_connection :: proc(
	table: ^Connection_Table,
	token: Connection_Token,
) -> Connection_Table_Result {
	if token.worker_id != table.worker_id || token.worker_generation != table.worker_generation {
		return .Stale
	}
	if u64(token.slot) >= u64(len(table.slots)) ||
	   table.free_count < 0 ||
	   table.free_count >= len(table.free_stack) {
		return .Invalid
	}
	slot := &table.slots[int(token.slot)]
	if slot.state != .Occupied || slot.generation != token.generation {
		return .Stale
	}
	slot.resource_id = 0
	slot.phase = .Closing
	if slot.generation == MAX_CONNECTION_GENERATION {
		slot.state = .Retired
		return .Retired
	}
	slot.generation += 1
	slot.state = .Available
	table.free_stack[table.free_count] = token.slot
	table.free_count += 1
	return .Released
}

connection_phase_transition_allowed :: proc(
	current: Connection_Phase,
	next: Connection_Phase,
) -> bool {
	if next == .Closing {
		return current != .Closing
	}
	switch current {
	case .Reading_Request_Head:
		return(
			next == .Reading_Request_Body ||
			next == .Awaiting_Handler ||
			next == .Writing_Response \
		)
	case .Reading_Request_Body:
		return next == .Awaiting_Handler || next == .Writing_Response
	case .Awaiting_Handler:
		return next == .Writing_Response
	case .Writing_Response:
		return next == .Keep_Alive_Idle
	case .Keep_Alive_Idle:
		return next == .Reading_Request_Head
	case .Closing:
		return false
	}
	return false
}
