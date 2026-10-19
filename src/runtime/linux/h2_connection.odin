#+build linux
package linux_runtime

import "core:sys/linux"
import http2 "veylor:http2"
import connections "veylor:runtime/connections"
import "veylor:transport"
import socket "veylor:transport/linux/socket"

H2_WORKER_CONNECTION_CAPACITY :: 8

H2_WORKER_STREAM_CAPACITY :: 32
H2_WORKER_REASM_CAPACITY :: 16 * 1024
H2_WORKER_SCRATCH_CAPACITY :: 16 * 1024
// NOTE: sized for a full burst of concurrently opened streams in one read: 32 streams with four
// pseudo-headers each, plus margin. Larger bursts fail closed at decode time.
H2_WORKER_HEADER_CAPACITY :: 256
H2_WORKER_PENDING_CAPACITY :: 32
H2_WORKER_DYN_ENTRY_CAPACITY :: 64
H2_WORKER_READ_CAPACITY :: 16 * 1024 + 64
H2_WORKER_WRITE_CAPACITY :: 16 * 1024 + 64
// NOTE: sized alongside the header capacity so one read's worth of HEADERS plus their close
// events fits a single feed; the host queue still bounds what dispatch can emit per turn.
H2_WORKER_EVENT_CAPACITY :: 96
// NOTE: upper bound for a queued response head plus an empty frame; the peek gate holds response
// commands until at least this much write space is free.
H2_RESPONSE_HEAD_RESERVE :: 128
H2_WORKER_STREAM_STAGED_CAPACITY :: 2048
H2_WORKER_STREAM_METHOD_CAPACITY :: 16
// NOTE: mirrors the router allowlist bound so any permitted set fits the record.
H2_WORKER_TRAILER_CAPACITY :: 32
// NOTE: caps per-stream trailer bytes; the copy API reports short host buffers instead of growing
// the record. Keep this small because each concurrent stream owns one fixed trailer buffer.
H2_WORKER_TRAILER_DATA_CAPACITY :: 1024

H2_WORKER_MAX_FRAME_SIZE :: u32(16384)
H2_WORKER_MAX_HEADER_LIST_SIZE :: u32(16384)
H2_WORKER_HPACK_TABLE_SIZE :: u64(4096)
// NOTE: the advertised stream window matches the per-stream staged reservoir above. A larger
// window would let a compliant burst overflow staging and force a cancel; grants issued as the
// host drains pace sustained flow instead.
H2_WORKER_INITIAL_WINDOW_SIZE :: u32(2048)

H2_Worker_Close_Reason :: enum {
	Unknown,
	Peer_Closed,
	Connection_Error,
	Shutdown,
	Shutdown_Forced,
	Timed_Out,
	Internal_Error,
}

H2_Worker_Turn_Result :: enum {
	Open,
	Closed,
	Error,
	Invalid,
}

H2_Worker_Turn :: struct {
	bytes_read:    int,
	bytes_written: int,
	events:        int,
	result:        H2_Worker_Turn_Result,
}

H2_Stream_Record :: struct {
	occupied:      bool,
	stream_id:     u32,
	route_id:      u64,
	method:        [H2_WORKER_STREAM_METHOD_CAPACITY]u8,
	method_len:    int,
	end_received:  bool,
	staged:        [H2_WORKER_STREAM_STAGED_CAPACITY]u8,
	staged_len:    int,
	ungranted:     i64,
	resp_started:  bool,
	resp_ended:    bool,
	write_stalled: bool,
	trailers:      [H2_WORKER_TRAILER_CAPACITY]H2_Trailer_Field,
	trailer_count: int,
	trailer_data:  [H2_WORKER_TRAILER_DATA_CAPACITY]u8,
	trailer_used:  int,
}

H2_Trailer_Field :: struct {
	name_offset:  int,
	name_length:  int,
	value_offset: int,
	value_length: int,
}

H2_Worker_Connection :: struct {
	fd:             linux.Fd,
	token:          connections.Connection_Token,
	peer:           transport.Peer_Address,
	active:         bool,
	registered:     bool,
	closing:        bool,
	close_reason:   H2_Worker_Close_Reason,
	conn:           http2.H2_Connection,
	streams:        [H2_WORKER_STREAM_CAPACITY]http2.Stream,
	reasm:          [H2_WORKER_REASM_CAPACITY]u8,
	dyn_entries:    [H2_WORKER_DYN_ENTRY_CAPACITY]http2.Hpack_Entry,
	scratch:        [H2_WORKER_SCRATCH_CAPACITY]u8,
	headers:        [H2_WORKER_HEADER_CAPACITY]http2.Hpack_Header,
	pending:        [H2_WORKER_PENDING_CAPACITY]http2.H2_Pending,
	events:         [H2_WORKER_EVENT_CAPACITY]http2.H2_Event,
	read_buf:       [H2_WORKER_READ_CAPACITY]u8,
	read_used:      int,
	write_buf:      [H2_WORKER_WRITE_CAPACITY]u8,
	write_used:     int,
	write_sent:     int,
	// NOTE: per-feed DATA delivery ring; sized to the assembler so any single feed's staged
	// drain fits without touching the monotonic header scratch.
	delivery_buf:   [H2_WORKER_REASM_CAPACITY]u8,
	records:        [H2_WORKER_STREAM_CAPACITY]H2_Stream_Record,
	writable_check: bool,
	ungranted_conn: i64,
}

h2_has_live_records :: proc(state: ^H2_Worker_Connection) -> bool {
	if state == nil {
		return false
	}

	for i := 0; i < len(state.records); i += 1 {
		if state.records[i].occupied {
			return true
		}
	}

	return false
}

h2_find_record :: proc(
	state: ^H2_Worker_Connection,
	stream_id: u32,
) -> (
	record: ^H2_Stream_Record,
	found: bool,
) {
	if state == nil || stream_id == 0 {
		return nil, false
	}

	for i := 0; i < len(state.records); i += 1 {
		if state.records[i].occupied && state.records[i].stream_id == stream_id {
			return &state.records[i], true
		}
	}

	return nil, false
}

h2_acquire_record :: proc(
	state: ^H2_Worker_Connection,
	stream_id: u32,
) -> (
	record: ^H2_Stream_Record,
	ok: bool,
) {
	if state == nil || stream_id == 0 {
		return nil, false
	}

	for i := 0; i < len(state.records); i += 1 {
		if !state.records[i].occupied {
			state.records[i] = H2_Stream_Record {
				occupied  = true,
				stream_id = stream_id,
			}

			return &state.records[i], true
		}
	}

	return nil, false
}

h2_release_record :: proc(state: ^H2_Worker_Connection, record: ^H2_Stream_Record) {
	if state == nil || record == nil {
		return
	}

	if record.ungranted > 0 {
		state.ungranted_conn -= record.ungranted

		if state.ungranted_conn < 0 {
			state.ungranted_conn = 0
		}
	}
	record^ = H2_Stream_Record{}
}

h2_worker_connection_init :: proc(
	state: ^H2_Worker_Connection,
	fd: linux.Fd,
	token: connections.Connection_Token,
	peer: transport.Peer_Address,
) -> bool {
	if state == nil {
		return false
	}

	if fd < 0 {
		return false
	}
	state.fd = fd
	state.token = token
	state.peer = peer
	state.active = true
	state.registered = false
	state.closing = false
	state.close_reason = .Unknown
	state.read_used = 0
	state.write_used = 0
	state.write_sent = 0

	if !http2.h2conn_init(
		&state.conn,
		state.streams[:],
		state.reasm[:],
		state.dyn_entries[:],
		state.scratch[:],
		state.headers[:],
		state.pending[:],
		state.delivery_buf[:],
		H2_WORKER_MAX_FRAME_SIZE,
		H2_WORKER_STREAM_CAPACITY,
		H2_WORKER_MAX_HEADER_LIST_SIZE,
		H2_WORKER_HPACK_TABLE_SIZE,
	) {
		state.active = false

		return false
	}
	params := [6]http2.Settings_Parameter {
		{id = http2.SETTINGS_HEADER_TABLE_SIZE, value = u32(H2_WORKER_HPACK_TABLE_SIZE)},
		{id = http2.SETTINGS_ENABLE_PUSH, value = 0},
		{id = http2.SETTINGS_MAX_CONCURRENT_STREAMS, value = H2_WORKER_STREAM_CAPACITY},
		{id = http2.SETTINGS_INITIAL_WINDOW_SIZE, value = H2_WORKER_INITIAL_WINDOW_SIZE},
		{id = http2.SETTINGS_MAX_FRAME_SIZE, value = H2_WORKER_MAX_FRAME_SIZE},
		{id = http2.SETTINGS_MAX_HEADER_LIST_SIZE, value = H2_WORKER_MAX_HEADER_LIST_SIZE},
	}
	used, settings_ok := http2.h2conn_queue_settings(&state.conn, params[:], state.write_buf[:])

	if !settings_ok || used <= 0 {
		state.active = false

		return false
	}
	state.write_used = used

	return true
}

h2_worker_connection_begin_shutdown :: proc(state: ^H2_Worker_Connection) -> bool {
	if state == nil || !state.active {
		return false
	}

	if state.closing {
		return true
	}
	last := state.conn.table.highest_remote
	out := state.write_buf[state.write_used:]
	used, goaway_ok := http2.h2conn_queue_goaway(&state.conn, last, .No_Error, out)

	if !goaway_ok {
		state.close_reason = .Internal_Error

		return false
	}
	state.write_used += used
	state.closing = true

	if state.close_reason == .Unknown {
		state.close_reason = .Shutdown
	}

	return true
}

h2_worker_connection_turn :: proc(
	state: ^H2_Worker_Connection,
	byte_budget: int,
) -> H2_Worker_Turn {
	if state == nil || !state.active || state.fd < 0 {
		return H2_Worker_Turn{result = .Invalid}
	}

	if byte_budget <= 0 {
		return H2_Worker_Turn{result = .Invalid}
	}
	turn := H2_Worker_Turn {
		result = .Open,
	}
	budget := byte_budget

	flush := h2_worker_flush_writes(state, &budget)
	turn.bytes_written += flush.transferred

	if flush.fatal {
		state.close_reason = .Internal_Error

		return H2_Worker_Turn {
			bytes_read = turn.bytes_read,
			bytes_written = turn.bytes_written,
			result = .Error,
		}
	}

	if flush.peer_closed {
		state.close_reason = .Peer_Closed

		return H2_Worker_Turn {
			bytes_read = turn.bytes_read,
			bytes_written = turn.bytes_written,
			result = .Closed,
		}
	}

	if !state.closing {
		read := h2_worker_read_available(state, &budget)
		turn.bytes_read += read.transferred

		if read.fatal {
			state.close_reason = .Internal_Error

			return H2_Worker_Turn {
				bytes_read = turn.bytes_read,
				bytes_written = turn.bytes_written,
				result = .Error,
			}
		}

		if read.peer_closed {
			state.close_reason = .Peer_Closed

			return H2_Worker_Turn {
				bytes_read = turn.bytes_read,
				bytes_written = turn.bytes_written,
				result = .Closed,
			}
		}

		if read.transferred > 0 || state.conn.staged_active {
			fed_events, feed_ok := h2_worker_feed_buffered(state)

			if !feed_ok {
				h2_worker_fail_connection(state)
				flush_after_fail := h2_worker_flush_writes(state, &budget)
				turn.bytes_written += flush_after_fail.transferred

				return H2_Worker_Turn {
					bytes_read = turn.bytes_read,
					bytes_written = turn.bytes_written,
					result = .Closed,
				}
			}
			turn.events += fed_events
		}
	}
	tail_flush := h2_worker_flush_writes(state, &budget)
	turn.bytes_written += tail_flush.transferred

	if tail_flush.fatal {
		state.close_reason = .Internal_Error

		return H2_Worker_Turn {
			bytes_read = turn.bytes_read,
			bytes_written = turn.bytes_written,
			result = .Error,
		}
	}

	if tail_flush.peer_closed {
		state.close_reason = .Peer_Closed

		return H2_Worker_Turn {
			bytes_read = turn.bytes_read,
			bytes_written = turn.bytes_written,
			result = .Closed,
		}
	}

	if state.closing && state.write_sent >= state.write_used && !h2_has_live_records(state) {
		return H2_Worker_Turn {
			bytes_read = turn.bytes_read,
			bytes_written = turn.bytes_written,
			result = .Closed,
		}
	}

	return turn
}

H2_Worker_IO_Progress :: struct {
	transferred: int,
	peer_closed: bool,
	fatal:       bool,
}

h2_worker_flush_writes :: proc(
	state: ^H2_Worker_Connection,
	budget: ^int,
) -> (
	progress: H2_Worker_IO_Progress,
) {
	for state.write_sent < state.write_used {
		if budget^ <= 0 {
			return progress
		}
		pending := state.write_buf[state.write_sent:state.write_used]
		chunk := pending

		if len(chunk) > budget^ {
			chunk = chunk[:budget^]
		}
		result := socket.write_stream(state.fd, chunk)

		if result.kind == .Progress {
			if result.transferred_bytes <= 0 || result.transferred_bytes > len(chunk) {
				progress.fatal = true

				return progress
			}
			state.write_sent += result.transferred_bytes
			budget^ -= result.transferred_bytes
			progress.transferred += result.transferred_bytes

			continue
		}

		if result.kind == .Would_Block || result.kind == .Interrupted {
			return progress
		}

		if result.kind == .Closed || result.kind == .End_Of_Stream {
			progress.peer_closed = true

			return progress
		}
		progress.fatal = true

		return progress
	}

	if state.write_sent >= state.write_used {
		state.write_used = 0
		state.write_sent = 0
	}

	return progress
}

h2_worker_read_available :: proc(
	state: ^H2_Worker_Connection,
	budget: ^int,
) -> (
	progress: H2_Worker_IO_Progress,
) {
	if budget^ <= 0 {
		return progress
	}

	if state.read_used >= len(state.read_buf) {
		progress.fatal = true

		return progress
	}
	tail := state.read_buf[state.read_used:]

	if len(tail) > budget^ {
		tail = tail[:budget^]
	}
	result := socket.read_stream(state.fd, tail)

	if result.kind == .Progress {
		if result.transferred_bytes <= 0 || result.transferred_bytes > len(tail) {
			progress.fatal = true

			return progress
		}
		state.read_used += result.transferred_bytes
		budget^ -= result.transferred_bytes
		progress.transferred = result.transferred_bytes

		return progress
	}

	if result.kind == .Would_Block || result.kind == .Interrupted {
		return progress
	}

	if result.kind == .End_Of_Stream || result.kind == .Closed {
		progress.peer_closed = true

		return progress
	}
	progress.fatal = true

	return progress
}

h2_worker_feed_buffered :: proc(state: ^H2_Worker_Connection) -> (events: int, ok: bool) {
	for state.read_used > 0 || state.conn.staged_active {
		input := state.read_buf[:state.read_used]
		out_tail := state.write_buf[state.write_used:]
		consumed, event_count, out_used, feed_ok := http2.h2conn_feed(
			&state.conn,
			input,
			state.events[:],
			out_tail,
		)
		state.write_used += out_used

		if state.write_used > len(state.write_buf) {
			return events, false
		}

		if consumed > 0 {
			if consumed > state.read_used {
				return events, false
			}
			remaining := state.read_used - consumed

			for i in 0 ..< remaining {
				state.read_buf[i] = state.read_buf[consumed + i]
			}
			state.read_used = remaining
		}

		if !feed_ok {
			return events, false
		}
		events += event_count

		if consumed == 0 {
			break
		}
	}

	return events, true
}

h2_worker_fail_connection :: proc(state: ^H2_Worker_Connection) {
	code, _ := http2.h2conn_error(&state.conn)
	out := state.write_buf[state.write_used:]
	used, goaway_ok := http2.h2conn_queue_goaway(
		&state.conn,
		state.conn.table.highest_remote,
		code,
		out,
	)

	if goaway_ok {
		state.write_used += used
	}
	state.closing = true

	if state.close_reason == .Unknown {
		state.close_reason = .Connection_Error
	}
}
