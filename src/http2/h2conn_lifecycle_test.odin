#+test
package http2

import "core:testing"

@(test)
test_h2_rst_mid_stream_then_closed_errors :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)

	rst: [13]u8
	off := h2t_rst(rst[:], 1, 8)
	cb, eb, ob, ok_rst := h2conn_feed(&f.conn, rst[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_rst, true)
	testing.expect_value(t, cb, 13)
	testing.expect_value(t, eb, 1)
	testing.expect_value(t, ob, 0)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Stream_Reset)
	testing.expect_value(t, f.events[0].code, Error_Code.Cancel)

	raw4 := [?]u8{1, 2, 3, 4}
	data: [13]u8
	off = h2t_data(data[:], 1, 0, raw4[:])
	cc, ec, oc, ok_data := h2conn_feed(&f.conn, data[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_data, true)
	testing.expect_value(t, cc, 13)
	testing.expect_value(t, ec, 0)
	testing.expect_value(t, oc, 13)
	h2t_expect_bytes(t, f.out[:oc], []u8{0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 5})
}

@(test)
test_h2_goaway_mid_stream_rejects_new :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)

	gw: [17]u8
	off := h2t_goaway(gw[:], 1, 0)
	cb, eb, _, ok_gw := h2conn_feed(&f.conn, gw[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_gw, true)
	testing.expect_value(t, cb, 17)
	testing.expect_value(t, eb, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Goaway_Received)
	testing.expect_value(t, f.events[0].last_stream_id, u32(1))
	testing.expect_value(t, f.events[0].code, Error_Code.No_Error)

	fresh: [10]u8
	off = h2t_headers_open(fresh[:], 5, FLAG_END_HEADERS, H2T_Open1[:])
	cc, ec, oc, ok_fresh := h2conn_feed(&f.conn, fresh[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_fresh, true)
	testing.expect_value(t, cc, 10)
	testing.expect_value(t, ec, 0)
	testing.expect_value(t, oc, 13)
	h2t_expect_bytes(t, f.out[:oc], []u8{0, 0, 4, 3, 0, 0, 0, 0, 5, 0, 0, 0, 7})

	raw4 := [?]u8{9, 8, 7, 6}
	data: [13]u8
	off = h2t_data(data[:], 1, 0, raw4[:])
	cd, ed, _, ok_data := h2conn_feed(&f.conn, data[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_data, true)
	testing.expect_value(t, cd, 13)
	testing.expect_value(t, ed, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Data_Ready)
	testing.expect_value(t, len(f.events[0].data), 4)
}

@(test)
test_h2_unknown_frame_skipped :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)

	blob: [21]u8
	off := h2t_put_header(blob[:], 0, 0x0b, 0, 1, 3)
	blob[off] = 1
	blob[off + 1] = 2
	blob[off + 2] = 3
	off += 3
	off = h2t_put_header(blob[:], off, 0x0c, 0, 0, 0)
	cb, eb, ob, ok_unk := h2conn_feed(&f.conn, blob[:off], f.events[:], f.out[:])
	testing.expect_value(t, ok_unk, true)
	testing.expect_value(t, cb, off)
	testing.expect_value(t, eb, 0)
	testing.expect_value(t, ob, 0)

	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(t, f.conn.table.slots[index].state, Stream_State.Open)
}

@(test)
test_h2_bad_preface_rejected :: proc(t: ^testing.T) {
	bad: [33]u8
	h2t_put_magic(bad[:])
	bad[0] = 'X'

	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	_, _, _, ok := h2conn_feed(&f.conn, bad[:], f.events[:], f.out[:])
	testing.expect_value(t, ok, false)
	code, failed := h2conn_error(&f.conn)
	testing.expect_value(t, failed, true)
	testing.expect_value(t, code, Error_Code.Protocol_Error)

	ping_first: [33]u8
	off := h2t_put_magic(ping_first[:])
	off = h2t_put_header(ping_first[:], off, 6, 0, 0, 8)

	g: H2T_Fixture
	testing.expect_value(t, h2t_init(&g), true)
	_, _, _, ping_ok := h2conn_feed(&g.conn, ping_first[:off], g.events[:], g.out[:])
	testing.expect_value(t, ping_ok, false)
	code, failed = h2conn_error(&g.conn)
	testing.expect_value(t, failed, true)
	testing.expect_value(t, code, Error_Code.Protocol_Error)

	h: H2T_Fixture
	testing.expect_value(t, h2t_init(&h), true)
	c, e, o, short_ok := h2conn_feed(&h.conn, ping_first[:10], h.events[:], h.out[:])
	testing.expect_value(t, short_ok, true)
	testing.expect_value(t, c, 0)
	testing.expect_value(t, e, 0)
	testing.expect_value(t, o, 0)
}

@(test)
test_h2_settings_bad_value_connection_error :: proc(t: ^testing.T) {
	input: [39]u8
	off := h2t_settings_value(input[:], 4, 0x80000000)

	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	consumed, _, _, ok := h2conn_feed(&f.conn, input[:off], f.events[:], f.out[:])
	testing.expect_value(t, ok, false)
	testing.expect_value(t, consumed, 24)
	code, failed := h2conn_error(&f.conn)
	testing.expect_value(t, failed, true)
	testing.expect_value(t, code, Error_Code.Flow_Control_Error)

	push: [39]u8
	off = h2t_settings_value(push[:], 2, 2)

	g: H2T_Fixture
	testing.expect_value(t, h2t_init(&g), true)
	_, _, _, push_ok := h2conn_feed(&g.conn, push[:off], g.events[:], g.out[:])
	testing.expect_value(t, push_ok, false)
	code, failed = h2conn_error(&g.conn)
	testing.expect_value(t, failed, true)
	testing.expect_value(t, code, Error_Code.Protocol_Error)
}

@(test)
test_h2_oversize_header_block_rejected :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f, DEFAULT_MAX_FRAME_SIZE, max(u32), 32, 4096), true)
	h2t_handshake(&f, t)

	block: [74]u8
	block[0] = 0x82
	block[1] = 0x84
	block[2] = 0x00
	block[3] = 0x05
	block[4] = 'x'
	block[5] = '-'
	block[6] = 'b'
	block[7] = 'i'
	block[8] = 'g'
	block[9] = 0x40

	for i in 0 ..< 64 {
		block[10 + i] = 'A'
	}

	frame: [83]u8
	off := h2t_put_header(frame[:], 0, 1, FLAG_END_STREAM | FLAG_END_HEADERS, 1, 74)

	for i in 0 ..< 74 {
		frame[off + i] = block[i]
	}
	_, _, _, big_ok := h2conn_feed(&f.conn, frame[:], f.events[:], f.out[:])
	testing.expect_value(t, big_ok, false)
	code, failed := h2conn_error(&f.conn)
	testing.expect_value(t, failed, true)
	testing.expect_value(t, code, Error_Code.Compression_Error)
}
