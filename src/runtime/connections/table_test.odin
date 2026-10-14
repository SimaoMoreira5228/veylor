#+test
package connections

import "core:testing"

@(test)
test_connection_table_rejects_stale_tokens_after_slot_reuse :: proc(t: ^testing.T) {
	slots: [2]Connection_Slot
	free_stack: [2]u32
	table, result := init_connection_table(7, 1, slots[:], free_stack[:])
	testing.expect_value(t, result, Connection_Table_Result.Ready)

	first: Connection_Token
	first, result = allocate_connection(&table, 10)
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	second: Connection_Token
	second, result = allocate_connection(&table, 20)
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	_, result = allocate_connection(&table, 30)
	testing.expect_value(t, result, Connection_Table_Result.Full)

	resource_id: u64
	resource_id, result = lookup_connection(&table, first)
	testing.expect_value(t, result, Connection_Table_Result.Found)
	testing.expect_value(t, resource_id, u64(10))
	testing.expect_value(t, release_connection(&table, first), Connection_Table_Result.Released)
	_, result = lookup_connection(&table, first)
	testing.expect_value(t, result, Connection_Table_Result.Stale)
	testing.expect_value(t, release_connection(&table, first), Connection_Table_Result.Stale)

	reused: Connection_Token
	reused, result = allocate_connection(&table, 30)
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	testing.expect_value(t, reused.slot, first.slot)
	testing.expect_value(t, reused.generation, first.generation + 1)
	_, result = lookup_connection(&table, first)
	testing.expect_value(t, result, Connection_Table_Result.Stale)
	resource_id, result = lookup_connection(&table, reused)
	testing.expect_value(t, result, Connection_Table_Result.Found)
	testing.expect_value(t, resource_id, u64(30))
	testing.expect_value(t, release_connection(&table, second), Connection_Table_Result.Released)
	testing.expect_value(t, release_connection(&table, reused), Connection_Table_Result.Released)
}

@(test)
test_connection_table_retires_slots_before_generation_wrap :: proc(t: ^testing.T) {
	slots: [1]Connection_Slot
	free_stack: [1]u32
	table, result := init_connection_table(3, 1, slots[:], free_stack[:])
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	first: Connection_Token
	first, result = allocate_connection(&table, 42)
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	table.slots[int(first.slot)].generation = MAX_CONNECTION_GENERATION
	first.generation = MAX_CONNECTION_GENERATION
	testing.expect_value(t, release_connection(&table, first), Connection_Table_Result.Retired)
	testing.expect_value(t, table.slots[0].state, Connection_Slot_State.Retired)
	_, result = allocate_connection(&table, 43)
	testing.expect_value(t, result, Connection_Table_Result.Full)
}

@(test)
test_connection_table_rejects_insufficient_caller_storage :: proc(t: ^testing.T) {
	slots: [2]Connection_Slot
	free_stack: [1]u32
	slots[0].resource_id = 99
	_, result := init_connection_table(1, 1, slots[:], free_stack[:])
	testing.expect_value(t, result, Connection_Table_Result.Invalid)
	testing.expect_value(t, slots[0].resource_id, u64(99))
}

@(test)
test_connection_table_rejects_tokens_from_other_worker_generations :: proc(t: ^testing.T) {
	first_slots: [1]Connection_Slot
	first_free: [1]u32
	first_table, result := init_connection_table(4, 10, first_slots[:], first_free[:])
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	first_token: Connection_Token
	first_token, result = allocate_connection(&first_table, 71)
	testing.expect_value(t, result, Connection_Table_Result.Ready)

	other_slots: [1]Connection_Slot
	other_free: [1]u32
	other_table: Connection_Table
	other_table, result = init_connection_table(5, 10, other_slots[:], other_free[:])
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	_, result = lookup_connection(&other_table, first_token)
	testing.expect_value(t, result, Connection_Table_Result.Stale)
	testing.expect_value(
		t,
		release_connection(&other_table, first_token),
		Connection_Table_Result.Stale,
	)

	restarted_slots: [1]Connection_Slot
	restarted_free: [1]u32
	restarted_table: Connection_Table
	restarted_table, result = init_connection_table(4, 11, restarted_slots[:], restarted_free[:])
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	_, result = lookup_connection(&restarted_table, first_token)
	testing.expect_value(t, result, Connection_Table_Result.Stale)
}

@(test)
test_connection_table_tracks_valid_http_connection_phase_changes :: proc(t: ^testing.T) {
	slots: [1]Connection_Slot
	free_stack: [1]u32
	table, result := init_connection_table(9, 2, slots[:], free_stack[:])
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	token: Connection_Token
	token, result = allocate_connection(&table, 81)
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	phase: Connection_Phase
	phase, result = connection_phase(&table, token)
	testing.expect_value(t, result, Connection_Table_Result.Found)
	testing.expect_value(t, phase, Connection_Phase.Reading_Request_Head)

	result = transition_connection(&table, token, .Keep_Alive_Idle)
	testing.expect_value(t, result, Connection_Table_Result.Invalid_Transition)
	phase, result = connection_phase(&table, token)
	testing.expect_value(t, result, Connection_Table_Result.Found)
	testing.expect_value(t, phase, Connection_Phase.Reading_Request_Head)

	result = transition_connection(&table, token, .Reading_Request_Body)
	testing.expect_value(t, result, Connection_Table_Result.Transitioned)
	result = transition_connection(&table, token, .Awaiting_Handler)
	testing.expect_value(t, result, Connection_Table_Result.Transitioned)
	result = transition_connection(&table, token, .Writing_Response)
	testing.expect_value(t, result, Connection_Table_Result.Transitioned)
	result = transition_connection(&table, token, .Keep_Alive_Idle)
	testing.expect_value(t, result, Connection_Table_Result.Transitioned)
	result = transition_connection(&table, token, .Reading_Request_Head)
	testing.expect_value(t, result, Connection_Table_Result.Transitioned)
	result = transition_connection(&table, token, .Closing)
	testing.expect_value(t, result, Connection_Table_Result.Transitioned)
	result = transition_connection(&table, token, .Reading_Request_Head)
	testing.expect_value(t, result, Connection_Table_Result.Invalid_Transition)
	testing.expect_value(t, release_connection(&table, token), Connection_Table_Result.Released)
}

@(test)
test_connection_deadlines_bind_clear_and_expire_only_for_live_tokens :: proc(t: ^testing.T) {
	slots: [2]Connection_Slot
	free_stack: [2]u32
	table, result := init_connection_table(11, 3, slots[:], free_stack[:])
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	first: Connection_Token
	first, result = allocate_connection(&table, 101)
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	second: Connection_Token
	second, result = allocate_connection(&table, 102)
	testing.expect_value(t, result, Connection_Table_Result.Ready)

	testing.expect_value(
		t,
		bind_connection_deadline(&table, first, 701),
		Connection_Table_Result.Deadline_Set,
	)
	testing.expect_value(
		t,
		bind_connection_deadline(&table, second, 701),
		Connection_Table_Result.Duplicate_Deadline,
	)
	testing.expect_value(
		t,
		clear_connection_deadline(&table, first, 702),
		Connection_Table_Result.Not_Found,
	)
	testing.expect_value(
		t,
		clear_connection_deadline(&table, first, 701),
		Connection_Table_Result.Deadline_Cleared,
	)
	_, result = expire_connection_deadline(&table, 701)
	testing.expect_value(t, result, Connection_Table_Result.Not_Found)

	testing.expect_value(
		t,
		bind_connection_deadline(&table, first, 703),
		Connection_Table_Result.Deadline_Set,
	)
	testing.expect_value(t, release_connection(&table, first), Connection_Table_Result.Released)
	_, result = expire_connection_deadline(&table, 703)
	testing.expect_value(t, result, Connection_Table_Result.Not_Found)

	reused: Connection_Token
	reused, result = allocate_connection(&table, 103)
	testing.expect_value(t, result, Connection_Table_Result.Ready)
	testing.expect_value(t, reused.slot, first.slot)
	testing.expect_value(t, reused.generation, first.generation + 1)
	testing.expect_value(
		t,
		bind_connection_deadline(&table, reused, 704),
		Connection_Table_Result.Deadline_Set,
	)
	completed: Connection_Token
	completed, result = expire_connection_deadline(&table, 704)
	testing.expect_value(t, result, Connection_Table_Result.Deadline_Expired)
	testing.expect_value(t, completed, reused)
	testing.expect_value(t, release_connection(&table, second), Connection_Table_Result.Released)
	testing.expect_value(t, release_connection(&table, reused), Connection_Table_Result.Released)
}
