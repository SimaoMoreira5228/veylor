#+test
package workers

import "core:testing"

Worker_Probe :: struct {
	visited: [4]u32,
}

record_worker :: proc(worker_id: u32, user_data: rawptr) {
	probe := cast(^Worker_Probe)user_data
	probe.visited[int(worker_id)] = 1
}

@(test)
test_worker_pool_starts_joins_and_destroys_caller_owned_workers :: proc(t: ^testing.T) {
	worker_storage: [4]Worker_Thread
	pool := init_worker_pool(worker_storage[:])
	probe: Worker_Probe

	testing.expect_value(
		t,
		start_worker_pool(&pool, 3, record_worker, rawptr(&probe)),
		Worker_Pool_Result.Started,
	)
	testing.expect_value(t, pool.phase, Worker_Pool_Phase.Running)
	testing.expect_value(t, pool.count, 3)
	testing.expect_value(t, destroy_worker_pool(&pool), Worker_Pool_Result.Busy)

	testing.expect_value(t, join_worker_pool(&pool), Worker_Pool_Result.Joined)
	testing.expect_value(t, pool.phase, Worker_Pool_Phase.Joined)
	for worker_id in 0 ..< 3 {
		testing.expect_value(t, probe.visited[worker_id], u32(1))
	}

	testing.expect_value(t, probe.visited[3], u32(0))
	testing.expect_value(
		t,
		start_worker_pool(&pool, 1, record_worker, rawptr(&probe)),
		Worker_Pool_Result.Invalid,
	)
	testing.expect_value(t, destroy_worker_pool(&pool), Worker_Pool_Result.Destroyed)
	testing.expect_value(t, pool.count, 0)
}

@(test)
test_worker_pool_rejects_invalid_capacity_before_starting_threads :: proc(t: ^testing.T) {
	worker_storage: [2]Worker_Thread
	pool := init_worker_pool(worker_storage[:])
	probe: Worker_Probe

	testing.expect_value(
		t,
		start_worker_pool(&pool, 0, record_worker, rawptr(&probe)),
		Worker_Pool_Result.Invalid,
	)
	testing.expect_value(
		t,
		start_worker_pool(&pool, 3, record_worker, rawptr(&probe)),
		Worker_Pool_Result.Capacity,
	)
	testing.expect_value(t, pool.phase, Worker_Pool_Phase.Unstarted)
	testing.expect_value(t, pool.count, 0)
	testing.expect_value(t, probe.visited, [4]u32{})
	testing.expect_value(t, destroy_worker_pool(&pool), Worker_Pool_Result.Destroyed)
}
