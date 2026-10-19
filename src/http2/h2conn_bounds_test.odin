#+test
package http2

import "core:testing"

@(test)
test_h2_init_rejects_bad_bounds :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f, 100, max(u32), DEFAULT_MAX_HEADER_LIST_SIZE, 4096), false)
	testing.expect_value(
		t,
		h2t_init(&f, 1 << 24, max(u32), DEFAULT_MAX_HEADER_LIST_SIZE, 4096),
		false,
	)
	testing.expect_value(
		t,
		h2conn_init(
			&f.conn,
			nil,
			f.reasm[:],
			f.dyn[:],
			f.scratch[:],
			f.headers[:],
			f.pending[:],
			f.delivery[:],
		),
		false,
	)
	testing.expect_value(
		t,
		h2t_init(&f, DEFAULT_MAX_FRAME_SIZE, 0, DEFAULT_MAX_HEADER_LIST_SIZE, 4096),
		true,
	)
}

@(test)
test_h2_budget_reports_usage_and_caps :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect(t, h2t_init(&f))
	su, sc, hu, hc := h2conn_budget(&f.conn)
	testing.expect_value(t, sc, H2T_Scratch_Cap)
	testing.expect_value(t, hc, H2T_Headers_Cap)
	testing.expect_value(t, su, 0)
	testing.expect_value(t, hu, 0)
	h2t_handshake(&f, t)
	blob: [42]u8
	testing.expect_value(t, h2t_split_get(blob[:]), 42)
	_, _, _, feed_ok := h2conn_feed(&f.conn, blob[:], f.events[:], f.out[:])
	testing.expect_value(t, feed_ok, true)
	su, _, hu, _ = h2conn_budget(&f.conn)
	testing.expect(t, su > 0)
	testing.expect(t, hu > 0)
}

@(test)
test_h2_exhausted_scratch_fails_connection_closed :: proc(t: ^testing.T) {
	streams: [4]Stream
	reasm: [256]u8
	dyn: [4]Hpack_Entry
	scratch: [32]u8
	headers: [8]Hpack_Header
	pending: [4]H2_Pending
	events: [4]H2_Event
	out: [256]u8
	delivery: [256]u8
	conn: H2_Connection
	testing.expect(
		t,
		h2conn_init(
			&conn,
			streams[:],
			reasm[:],
			dyn[:],
			scratch[:],
			headers[:],
			pending[:],
			delivery[:],
			DEFAULT_MAX_FRAME_SIZE,
			max(u32),
			DEFAULT_MAX_HEADER_LIST_SIZE,
			64,
		),
	)
	input: [128]u8
	off := h2t_handshake_input(input[:])
	_, _, _, ok := h2conn_feed(&conn, input[:off], events[:], out[:])
	testing.expect(t, ok)
	su, sc, _, _ := h2conn_budget(&conn)
	testing.expect(t, sc == 32)
	testing.expect(t, su == 0)
	frame: [64]u8
	foff := h2t_put_header(frame[:], 0, 1, FLAG_END_HEADERS, 1, 0)
	_ = foff
	payload := frame[FRAME_HEADER_LEN:]
	poff := 0
	payload[poff] = 0x00
	poff += 1
	payload[poff] = 0x08
	poff += 1
	for i in 0 ..< 8 {
		payload[poff + i] = 'x'
	}
	poff += 8
	payload[poff] = 0x1f
	poff += 1
	for i in 0 ..< 31 {
		payload[poff + i] = 'v'
	}
	poff += 31
	payload_len := poff
	total := FRAME_HEADER_LEN + payload_len
	frame[0] = u8(payload_len >> 16)
	frame[1] = u8(payload_len >> 8)
	frame[2] = u8(payload_len)
	_, _, _, ok = h2conn_feed(&conn, frame[:total], events[:], out[:])
	testing.expect(t, !ok)
	code, failed := h2conn_error(&conn)
	testing.expect_value(t, failed, true)
	testing.expect_value(t, code, Error_Code.Compression_Error)
}

@(test)
test_h2_conn_continuation_flood_fails_closed :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)

	open: [13]u8
	off := h2t_put_header(open[:], 0, 1, 0, 1, 4)
	open[off + 0] = 0x04
	open[off + 1] = 0x7f
	open[off + 2] = 0xa1
	open[off + 3] = 0x1e
	testing.expect_value(t, off + 4, 13)
	_, _, _, open_ok := h2conn_feed(&f.conn, open[:], f.events[:], f.out[:])
	testing.expect_value(t, open_ok, true)
	testing.expect_value(t, f.conn.table.assembler.active, true)

	fragment: [25]u8
	frag_off := h2t_put_header(fragment[:], 0, 9, 0, 1, 16)
	for i in 0 ..< 16 {
		fragment[frag_off + i] = 'a'
	}
	failed := false
	fed := 0

	for i in 0 ..< 300 {
		_, _, _, feed_ok := h2conn_feed(&f.conn, fragment[:], f.events[:], f.out[:])
		fed += 1

		if !feed_ok {
			failed = true

			break
		}
	}
	testing.expect_value(t, failed, true)
	testing.expect_value(t, fed < 300, true)
	code, conn_failed := h2conn_error(&f.conn)
	testing.expect_value(t, conn_failed, true)
	testing.expect_value(t, code, Error_Code.Compression_Error)
	testing.expect_value(t, f.conn.table.assembler.active, false)
}

@(test)
test_h2_conn_split_continuations_under_cap_accepted :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)

	open: [11]u8
	off := h2t_headers_open(open[:], 1, 0, H2T_Open2[:])
	testing.expect_value(t, off, 11)
	_, ea, _, open_ok := h2conn_feed(&f.conn, open[:], f.events[:], f.out[:])
	testing.expect_value(t, open_ok, true)
	testing.expect_value(t, ea, 0)
	testing.expect_value(t, f.conn.table.assembler.active, true)

	one: [10]u8
	one_off := h2t_put_header(one[:], 0, 9, 0, 1, 1)
	one[one_off] = 0x82
	last: [10]u8
	last_off := h2t_put_header(last[:], 0, 9, FLAG_END_HEADERS, 1, 1)
	last[last_off] = 0x82
	ready := 0

	for i in 0 ..< 19 {
		_, events, _, feed_ok := h2conn_feed(&f.conn, one[:one_off + 1], f.events[:], f.out[:])
		testing.expect_value(t, feed_ok, true)
		ready += events
	}
	_, events, _, feed_ok := h2conn_feed(&f.conn, last[:last_off + 1], f.events[:], f.out[:])
	testing.expect_value(t, feed_ok, true)
	ready += events
	testing.expect_value(t, ready, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Headers_Ready)
	testing.expect_value(t, f.events[0].stream_id, u32(1))
	testing.expect_value(t, len(f.events[0].headers), 22)
	code, conn_failed := h2conn_error(&f.conn)
	testing.expect_value(t, conn_failed, false)
	testing.expect_value(t, code, Error_Code.No_Error)
}

@(test)
test_h2_conn_settings_flood_stays_bounded :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)

	empty: [9]u8
	testing.expect_value(t, h2t_put_header(empty[:], 0, 4, 0, 0, 0), 9)

	for i in 0 ..< 64 {
		_, events, out_used, feed_ok := h2conn_feed(&f.conn, empty[:], f.events[:], f.out[:])
		testing.expect_value(t, feed_ok, true)
		testing.expect_value(t, events, 0)
		testing.expect_value(t, out_used, 9)
		testing.expect_value(t, f.conn.pending_count, 0)
	}
	code, failed := h2conn_error(&f.conn)
	testing.expect_value(t, failed, false)
	testing.expect_value(t, code, Error_Code.No_Error)
	testing.expect_value(t, f.conn.table.unacked_settings, u32(0))
	testing.expect_value(t, f.conn.table.remote.header_table_size, u32(4096))
	testing.expect_value(t, f.conn.table.remote.max_concurrent, max(u32))
}

@(test)
test_h2_conn_remote_table_size_clamped_to_cap :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)

	frame: [15]u8
	off := h2t_put_header(frame[:], 0, 4, 0, 0, 6)
	frame[off + 0] = 0x00
	frame[off + 1] = 0x01
	frame[off + 2] = 0x00
	frame[off + 3] = 0x01
	frame[off + 4] = 0x00
	frame[off + 5] = 0x00
	testing.expect_value(t, off + 6, 15)
	_, _, out_used, feed_ok := h2conn_feed(&f.conn, frame[:], f.events[:], f.out[:])
	testing.expect_value(t, feed_ok, true)
	testing.expect_value(t, out_used, 9)
	testing.expect_value(t, f.conn.hpack.max_size, u64(4096))
	testing.expect_value(t, f.conn.table.remote.header_table_size, u32(65536))
	code, failed := h2conn_error(&f.conn)
	testing.expect_value(t, failed, false)
	testing.expect_value(t, code, Error_Code.No_Error)

	open: [11]u8
	open_off := h2t_headers_open(open[:], 1, FLAG_END_HEADERS, H2T_Open2[:])
	testing.expect_value(t, open_off, 11)
	_, ea, _, open_ok := h2conn_feed(&f.conn, open[:], f.events[:], f.out[:])
	testing.expect_value(t, open_ok, true)
	testing.expect_value(t, ea, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Headers_Ready)
}
