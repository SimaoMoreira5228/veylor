#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import connections "veylor:runtime/connections"
import http2 "veylor:http2"
import "veylor:transport"

@(test)
test_h2_worker_pool_acquire_release_roundtrip :: proc(t: ^testing.T) {
	states: [H2_WORKER_CONNECTION_CAPACITY]H2_Worker_Connection
	free_stack: [H2_WORKER_CONNECTION_CAPACITY]u32
	testing.expect(t, init_h2_worker_pool(states[:], free_stack[:]))

	bad_states: [H2_WORKER_CONNECTION_CAPACITY - 1]H2_Worker_Connection
	testing.expect(t, !init_h2_worker_pool(bad_states[:], free_stack[:]))
	testing.expect(t, !init_h2_worker_pool(states[:], free_stack[:1]))

	free_count := H2_WORKER_CONNECTION_CAPACITY
	acquired: [H2_WORKER_CONNECTION_CAPACITY]^H2_Worker_Connection

	for i := 0; i < H2_WORKER_CONNECTION_CAPACITY; i += 1 {
		slot, ok := acquire_h2_worker_slot(states[:], free_stack[:], &free_count)
		testing.expect(t, ok)

		if !ok {
			return
		}
		acquired[i] = slot
	}
	testing.expect_value(t, free_count, 0)
	_, ok := acquire_h2_worker_slot(states[:], free_stack[:], &free_count)
	testing.expect(t, !ok)

	testing.expect(t, !release_h2_worker_slot(states[:], free_stack[:], &free_count, nil))
	other: H2_Worker_Connection
	testing.expect(t, !release_h2_worker_slot(states[:], free_stack[:], &free_count, &other))

	testing.expect(
		t,
		release_h2_worker_slot(states[:], free_stack[:], &free_count, acquired[0]),
	)
	testing.expect_value(t, free_count, 1)
	testing.expect_value(t, acquired[0].fd, -1)
	testing.expect_value(t, acquired[0].active, false)

	slot, slot_ok := acquire_h2_worker_slot(states[:], free_stack[:], &free_count)
	testing.expect(t, slot_ok)
	testing.expect(t, slot == acquired[0])
	testing.expect_value(t, free_count, 0)
}

@(test)
test_h2_worker_supervise_handshake_to_body_transition :: proc(t: ^testing.T) {
	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(11, 3, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)

	if table_result != .Ready {
		return
	}
	token, allocation_result := connections.allocate_connection(&table, 701)
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)

	if allocation_result != .Ready {
		return
	}
	timer_storage: [4]transport.Timer_Expiration
	timer_index_storage: [4 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	worker := Static_Worker {
		table    = &table,
		timers   = &timers,
		timeouts = connections.Connection_Timeouts {
			request_head_ns = 10_000_000_000,
			body_progress_ns = 10_000_000_000,
			response_write_ns = 10_000_000_000,
			keep_alive_idle_ns = 5_000_000_000,
		},
	}

	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return
	}
	defer linux.close(sockets[0])
	defer linux.close(sockets[1])

	slot: H2_Worker_Connection
	testing.expect(t, h2_worker_connection_init(&slot, sockets[0], token, {}))

	if !slot.active {
		return
	}
	now_ns := u64(1_000_000_000)
	schedule := connections.schedule_connection_phase_deadline(
		&table,
		&timers,
		token,
		now_ns,
		worker.timeouts,
	)
	testing.expect_value(t, schedule.result, connections.Connection_Deadline_Schedule_Result.Scheduled)

	testing.expect(t, supervise_h2_worker_connection(&worker, &slot, now_ns, false))
	phase, phase_result := connections.connection_phase(&table, token)
	testing.expect_value(t, phase_result, connections.Connection_Table_Result.Found)
	testing.expect_value(t, phase, connections.Connection_Phase.Reading_Request_Head)

	magic_string := http2.CLIENT_CONNECTION_PREFACE
	magic := transmute([]u8)magic_string
	write_length, write_error := linux.write(sockets[1], magic)
	testing.expect_value(t, write_error, linux.Errno.NONE)
	testing.expect_value(t, write_length, len(magic))

	if write_error != .NONE {
		return
	}
	settings_frame: [9]u8
	settings_header := http2.Frame_Header {
		length    = 0,
		kind      = .Settings,
		stream_id = 0,
	}
	testing.expect(t, http2.encode_frame_header(settings_header, settings_frame[:]))
	write_length, write_error = linux.write(sockets[1], settings_frame[:])
	testing.expect_value(t, write_error, linux.Errno.NONE)

	if write_error != .NONE {
		return
	}
	handshake_turn := h2_worker_connection_turn(&slot, 64 * 1024)
	testing.expect_value(t, handshake_turn.result, H2_Worker_Turn_Result.Open)

	testing.expect(t, supervise_h2_worker_connection(&worker, &slot, now_ns, false))
	phase, phase_result = connections.connection_phase(&table, token)
	testing.expect_value(t, phase_result, connections.Connection_Table_Result.Found)
	testing.expect_value(t, phase, connections.Connection_Phase.Reading_Request_Body)

	testing.expect(t, supervise_h2_worker_connection(&worker, &slot, now_ns, true))
	phase, phase_result = connections.connection_phase(&table, token)
	testing.expect_value(t, phase_result, connections.Connection_Table_Result.Found)
	testing.expect_value(t, phase, connections.Connection_Phase.Reading_Request_Body)

	deadline, deadline_result := connections.expire_connection_deadline(
		&table,
		connections.connection_deadline_timer_id(token),
	)
	testing.expect_value(t, deadline_result, connections.Connection_Table_Result.Deadline_Expired)
	testing.expect_value(t, deadline.kind, connections.Connection_Deadline_Kind.Body_Progress)
}

@(test)
test_h2_worker_supervise_rejects_bad_state :: proc(t: ^testing.T) {
	slots: [1]connections.Connection_Slot
	free_stack: [1]u32
	table, table_result := connections.init_connection_table(11, 3, slots[:], free_stack[:])
	testing.expect_value(t, table_result, connections.Connection_Table_Result.Ready)

	if table_result != .Ready {
		return
	}
	token, allocation_result := connections.allocate_connection(&table, 701)
	testing.expect_value(t, allocation_result, connections.Connection_Table_Result.Ready)

	if allocation_result != .Ready {
		return
	}
	timer_storage: [4]transport.Timer_Expiration
	timer_index_storage: [4 * transport.TIMER_QUEUE_INDEX_STORAGE_FACTOR]u32
	timers := transport.init_timer_queue(timer_storage[:], timer_index_storage[:])
	worker := Static_Worker {
		table    = &table,
		timers   = &timers,
		timeouts = connections.Connection_Timeouts {
			request_head_ns = 10_000_000_000,
			body_progress_ns = 10_000_000_000,
			response_write_ns = 10_000_000_000,
			keep_alive_idle_ns = 5_000_000_000,
		},
	}
	slot := H2_Worker_Connection {
		fd     = 701,
		token  = token,
		active = true,
	}
	now_ns := u64(1_000_000_000)

	testing.expect(t, !supervise_h2_worker_connection(nil, &slot, now_ns, false))
	testing.expect(t, !supervise_h2_worker_connection(&worker, nil, now_ns, false))
	slot.active = false
	testing.expect(t, !supervise_h2_worker_connection(&worker, &slot, now_ns, false))
	slot.active = true

	testing.expect(t, supervise_h2_worker_connection(&worker, &slot, now_ns, false))

	schedule := connections.schedule_connection_phase_deadline(
		&table,
		&timers,
		token,
		now_ns,
		worker.timeouts,
	)
	testing.expect_value(t, schedule.result, connections.Connection_Deadline_Schedule_Result.Scheduled)
	testing.expect(t, supervise_h2_worker_connection(&worker, &slot, now_ns, false))
}
