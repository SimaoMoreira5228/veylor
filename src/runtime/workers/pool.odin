package workers

import "core:thread"

Worker_Run :: #type proc(worker_id: u32, user_data: rawptr)

Worker_Pool_Phase :: enum {
	Unstarted,
	Starting,
	Running,
	Joined,
	Destroyed,
}

Worker_Pool_Result :: enum {
	Started,
	Joined,
	Destroyed,
	Capacity,
	Unsupported,
	Thread_Create_Failed,
	Busy,
	Invalid,
}

Worker_Thread :: struct {
	worker_id: u32,
	thread:    ^thread.Thread,
	run:       Worker_Run,
	user_data: rawptr,
}

Worker_Pool :: struct {
	workers: []Worker_Thread,
	count:   int,
	phase:   Worker_Pool_Phase,
}

init_worker_pool :: proc(storage: []Worker_Thread) -> Worker_Pool {
	for i := 0; i < len(storage); i += 1 {
		storage[i] = Worker_Thread{}
	}

	return Worker_Pool{workers = storage}
}

start_worker_pool :: proc(
	pool: ^Worker_Pool,
	worker_count: int,
	run: Worker_Run,
	user_data: rawptr,
) -> Worker_Pool_Result {
	if pool == nil || pool.phase != .Unstarted || run == nil || worker_count <= 0 {
		return .Invalid
	}

	if worker_count > len(pool.workers) {
		return .Capacity
	}

	when !thread.IS_SUPPORTED {
		return .Unsupported
	} else {
		pool.phase = .Starting
		created_count := 0
		for worker_id := 0; worker_id < worker_count; worker_id += 1 {
			worker := &pool.workers[worker_id]
			worker.worker_id = u32(worker_id)
			worker.run = run
			worker.user_data = user_data
			worker.thread = thread.create(worker_thread_entry)

			if worker.thread == nil {
				abort_created_workers(pool, created_count)
				pool.phase = .Unstarted
				return .Thread_Create_Failed
			}

			worker.thread.data = rawptr(worker)
			created_count += 1
			pool.count = created_count
		}

		pool.phase = .Running

		for i := 0; i < pool.count; i += 1 {
			thread.start(pool.workers[i].thread)
		}

		return .Started
	}
}

join_worker_pool :: proc(pool: ^Worker_Pool) -> Worker_Pool_Result {
	if pool == nil || pool.phase != .Running {
		return .Invalid
	}

	for i := 0; i < pool.count; i += 1 {
		thread.join(pool.workers[i].thread)
	}

	pool.phase = .Joined
	return .Joined
}

destroy_worker_pool :: proc(pool: ^Worker_Pool) -> Worker_Pool_Result {
	if pool == nil {
		return .Invalid
	}

	if pool.phase == .Destroyed {
		return .Destroyed
	}

	if pool.phase == .Starting || pool.phase == .Running {
		return .Busy
	}

	for i := 0; i < pool.count; i += 1 {
		thread.destroy(pool.workers[i].thread)
		pool.workers[i] = Worker_Thread{}
	}

	pool.count = 0
	pool.phase = .Destroyed
	return .Destroyed
}

worker_thread_entry :: proc(t: ^thread.Thread) {
	worker := cast(^Worker_Thread)t.data

	if worker.run != nil {
		worker.run(worker.worker_id, worker.user_data)
	}
}

abort_created_workers :: proc(pool: ^Worker_Pool, count: int) {
	for i := 0; i < count; i += 1 {
		pool.workers[i].run = nil
	}

	for i := 0; i < count; i += 1 {
		worker := &pool.workers[i]
		thread.start(worker.thread)
		thread.join(worker.thread)
		thread.destroy(worker.thread)
		worker^ = Worker_Thread{}
	}

	pool.count = 0
}
