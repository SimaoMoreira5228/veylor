#+test
package connections

import "core:testing"

@(test)
test_connection_table_rejects_stale_tokens_after_slot_reuse :: proc(t: ^testing.T) {
	slots: [2]Connection_Slot
	free_stack: [2]u32
	table, result := init_connection_table(slots[:], free_stack[:])
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
	table, result := init_connection_table(slots[:], free_stack[:])
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
	_, result := init_connection_table(slots[:], free_stack[:])
	testing.expect_value(t, result, Connection_Table_Result.Invalid)
	testing.expect_value(t, slots[0].resource_id, u64(99))
}
