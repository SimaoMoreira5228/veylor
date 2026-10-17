package connections

Connection_Token :: struct {
	worker_id:         u32,
	worker_generation: u64,
	slot:              u32,
	generation:        u64,
}

Connection_Slot_State :: enum u8 {
	Available,
	Occupied,
	Retired,
}

Connection_Phase :: enum u8 {
	Reading_Request_Head,
	Reading_Request_Body,
	Awaiting_Handler,
	Writing_Response,
	Keep_Alive_Idle,
	Closing,
}

Connection_Deadline_Kind :: enum u8 {
	Request_Head,
	Body_Progress,
	Response_Write,
	Keep_Alive_Idle,
}

Connection_Deadline :: struct {
	token:    Connection_Token,
	timer_id: u64,
	kind:     Connection_Deadline_Kind,
}

Connection_Slot :: struct {
	generation:        u64,
	resource_id:       u64,
	deadline_timer_id: u64,
	deadline_kind:     Connection_Deadline_Kind,
	state:             Connection_Slot_State,
	phase:             Connection_Phase,
	has_deadline:      bool,
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
	Deadline_Set,
	Deadline_Cleared,
	Deadline_Expired,
	Duplicate_Deadline,
	Not_Found,
	Invalid,
}

MAX_CONNECTION_SLOT_COUNT :: u64(0x1_0000_0000)
MAX_CONNECTION_GENERATION :: u64(0x7fff_ffff)

connection_deadline_timer_id :: proc(token: Connection_Token) -> u64 {
	return token.generation << 32 | u64(token.slot)
}

connection_token_is_for_worker :: proc(table: ^Connection_Table, token: Connection_Token) -> bool {
	if token.worker_id != table.worker_id {
		return false
	}
	return token.worker_generation == table.worker_generation
}

connection_slot_matches_token :: proc(slot: ^Connection_Slot, token: Connection_Token) -> bool {
	if slot.state != .Occupied {
		return false
	}
	return slot.generation == token.generation
}

init_connection_table :: proc(
	worker_id: u32,
	worker_generation: u64,
	slots: []Connection_Slot,
	free_stack: []u32,
) -> (
	table: Connection_Table,
	result: Connection_Table_Result,
) {
	if worker_generation == 0 {
		return {}, .Invalid
	}

	if len(free_stack) < len(slots) {
		return {}, .Invalid
	}

	if u64(len(slots)) > MAX_CONNECTION_SLOT_COUNT {
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
	if table.free_count < 0 {
		return {}, .Invalid
	}

	if table.free_count > len(table.slots) {
		return {}, .Invalid
	}

	if table.free_count > len(table.free_stack) {
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
	if !connection_token_is_for_worker(table, token) {
		return 0, .Stale
	}

	if u64(token.slot) >= u64(len(table.slots)) {
		return 0, .Invalid
	}

	slot := &table.slots[int(token.slot)]

	if !connection_slot_matches_token(slot, token) {
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
	if !connection_token_is_for_worker(table, token) {
		return {}, .Stale
	}

	if u64(token.slot) >= u64(len(table.slots)) {
		return {}, .Invalid
	}

	slot := &table.slots[int(token.slot)]

	if !connection_slot_matches_token(slot, token) {
		return {}, .Stale
	}
	return slot.phase, .Found
}

bind_connection_deadline :: proc(
	table: ^Connection_Table,
	token: Connection_Token,
	kind: Connection_Deadline_Kind,
) -> Connection_Table_Result {
	if !connection_token_is_for_worker(table, token) {
		return .Stale
	}

	if u64(token.slot) >= u64(len(table.slots)) {
		return .Invalid
	}
	slot := &table.slots[int(token.slot)]

	if !connection_slot_matches_token(slot, token) {
		return .Stale
	}
	expected_kind, has_deadline := connection_deadline_kind(slot.phase)

	if !has_deadline {
		return .Invalid_Transition
	}

	if expected_kind != kind {
		return .Invalid_Transition
	}

	if slot.has_deadline {
		return .Duplicate_Deadline
	}

	slot.deadline_timer_id = connection_deadline_timer_id(token)
	slot.deadline_kind = kind
	slot.has_deadline = true
	return .Deadline_Set
}

clear_connection_deadline :: proc(
	table: ^Connection_Table,
	token: Connection_Token,
) -> Connection_Table_Result {
	if !connection_token_is_for_worker(table, token) {
		return .Stale
	}

	if u64(token.slot) >= u64(len(table.slots)) {
		return .Invalid
	}
	slot := &table.slots[int(token.slot)]

	if !connection_slot_matches_token(slot, token) {
		return .Stale
	}

	if !slot.has_deadline {
		return .Not_Found
	}

	slot.deadline_timer_id = 0
	slot.deadline_kind = {}
	slot.has_deadline = false
	return .Deadline_Cleared
}

expire_connection_deadline :: proc(
	table: ^Connection_Table,
	timer_id: u64,
) -> (
	deadline: Connection_Deadline,
	result: Connection_Table_Result,
) {
	generation := timer_id >> 32
	slot_index := u32(timer_id)

	if generation == 0 || generation > MAX_CONNECTION_GENERATION {
		return {}, .Not_Found
	}

	if u64(slot_index) >= u64(len(table.slots)) {
		return {}, .Not_Found
	}

	slot := &table.slots[int(slot_index)]
	if slot.state != .Occupied || slot.generation != generation || !slot.has_deadline {
		return {}, .Not_Found
	}

	if slot.deadline_timer_id != timer_id {
		return {}, .Not_Found
	}

	deadline = Connection_Deadline {
		token = Connection_Token {
			worker_id = table.worker_id,
			worker_generation = table.worker_generation,
			slot = slot_index,
			generation = generation,
		},
		timer_id = timer_id,
		kind = slot.deadline_kind,
	}
	slot.deadline_timer_id = 0
	slot.deadline_kind = {}
	slot.has_deadline = false
	return deadline, .Deadline_Expired
}

transition_connection :: proc(
	table: ^Connection_Table,
	token: Connection_Token,
	next: Connection_Phase,
) -> Connection_Table_Result {
	if !connection_token_is_for_worker(table, token) {
		return .Stale
	}

	if u64(token.slot) >= u64(len(table.slots)) {
		return .Invalid
	}

	slot := &table.slots[int(token.slot)]

	if !connection_slot_matches_token(slot, token) {
		return .Stale
	}

	if slot.has_deadline {
		expected_kind, has_deadline := connection_deadline_kind(next)

		if !has_deadline {
			return .Invalid_Transition
		}

		if expected_kind != slot.deadline_kind {
			return .Invalid_Transition
		}
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
	if !connection_token_is_for_worker(table, token) {
		return .Stale
	}

	if u64(token.slot) >= u64(len(table.slots)) {
		return .Invalid
	}

	if table.free_count < 0 {
		return .Invalid
	}

	if table.free_count >= len(table.free_stack) {
		return .Invalid
	}
	slot := &table.slots[int(token.slot)]

	if !connection_slot_matches_token(slot, token) {
		return .Stale
	}

	if slot.has_deadline {
		return .Invalid_Transition
	}

	slot.resource_id = 0
	slot.deadline_timer_id = 0
	slot.deadline_kind = {}
	slot.has_deadline = false
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

connection_deadline_kind :: proc(
	phase: Connection_Phase,
) -> (
	kind: Connection_Deadline_Kind,
	valid: bool,
) {
	switch phase {
	case .Reading_Request_Head:
		return .Request_Head, true
	case .Reading_Request_Body:
		return .Body_Progress, true
	case .Writing_Response:
		return .Response_Write, true
	case .Keep_Alive_Idle:
		return .Keep_Alive_Idle, true
	case .Awaiting_Handler, .Closing:
		return {}, false
	}
	return {}, false
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
		if next == .Reading_Request_Body {
			return true
		}

		if next == .Awaiting_Handler {
			return true
		}
		return next == .Writing_Response
	case .Reading_Request_Body:
		if next == .Awaiting_Handler {
			return true
		}
		return next == .Writing_Response
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
