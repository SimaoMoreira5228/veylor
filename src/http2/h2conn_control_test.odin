#+test
package http2

import "core:testing"

@(test)
test_h2_outbound_goldens :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)

	used, ok := h2conn_queue_settings_ack(&f.conn, f.out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9)
	h2t_expect_bytes(t, f.out[:used], []u8{0, 0, 0, 4, 1, 0, 0, 0, 0})
	h2t_handshake(&f, t)

	ping: [17]u8
	off := h2t_put_header(ping[:], 0, 6, 0, 0, 8)

	for i in 0 ..< 8 {
		ping[off + i] = u8(i + 1)
	}
	ca, ea, oa, ping_ok := h2conn_feed(&f.conn, ping[:], f.events[:], f.out[:])
	testing.expect_value(t, ping_ok, true)
	testing.expect_value(t, ca, 17)
	testing.expect_value(t, ea, 0)
	testing.expect_value(t, oa, 17)
	h2t_expect_bytes(t, f.out[:oa], []u8{0, 0, 8, 6, 1, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8})

	ping_ack: [17]u8
	off = h2t_put_header(ping_ack[:], 0, 6, PING_ACK, 0, 8)

	for i in 0 ..< 8 {
		ping_ack[off + i] = u8(i + 1)
	}
	cb, eb, ob, ack_ok := h2conn_feed(&f.conn, ping_ack[:], f.events[:], f.out[:])
	testing.expect_value(t, ack_ok, true)
	testing.expect_value(t, cb, 17)
	testing.expect_value(t, eb, 0)
	testing.expect_value(t, ob, 0)

	open: [11]u8
	off = h2t_headers_open(open[:], 1, FLAG_END_HEADERS, H2T_Open2[:])
	_, ec, _, open_ok := h2conn_feed(&f.conn, open[:], f.events[:], f.out[:])
	testing.expect_value(t, open_ok, true)
	testing.expect_value(t, ec, 1)

	used, ok = h2conn_queue_rst_stream(&f.conn, 1, .Cancel, f.out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 13)
	h2t_expect_bytes(t, f.out[:used], []u8{0, 0, 4, 3, 0, 0, 0, 0, 1, 0, 0, 0, 8})

	used, ok = h2conn_queue_goaway(&f.conn, 1, .No_Error, f.out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 17)
	h2t_expect_bytes(t, f.out[:used], []u8{0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0})

	small: [4]u8 = {0xaa, 0xaa, 0xaa, 0xaa}
	used, ok = h2conn_queue_settings_ack(&f.conn, small[:])
	testing.expect_value(t, ok, false)
	testing.expect_value(t, used, 0)
	testing.expect_value(t, small, [4]u8{0xaa, 0xaa, 0xaa, 0xaa})
}

@(test)
test_h2_settings_ack_silent_no_event :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	used, ok := h2conn_queue_settings(&f.conn, nil, f.out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9)
	h2t_handshake(&f, t)
	testing.expect_value(t, f.conn.table.unacked_settings, u32(1))

	ack := [?]u8{0, 0, 0, 4, 1, 0, 0, 0, 0}
	c, e, o, ack_ok := h2conn_feed(&f.conn, ack[:], f.events[:], f.out[:])
	testing.expect_value(t, ack_ok, true)
	testing.expect_value(t, c, 9)
	testing.expect_value(t, e, 0)
	testing.expect_value(t, o, 0)
	testing.expect_value(t, f.conn.table.unacked_settings, u32(0))
}

@(test)
test_h2_max_concurrent_refused :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(
		t,
		h2t_init(&f, DEFAULT_MAX_FRAME_SIZE, 1, DEFAULT_MAX_HEADER_LIST_SIZE, 4096),
		true,
	)
	h2t_handshake(&f, t)

	first: [10]u8
	off := h2t_headers_open(first[:], 1, FLAG_END_HEADERS, H2T_Open1[:])
	ca, ea, _, ok_first := h2conn_feed(&f.conn, first[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_first, true)
	testing.expect_value(t, ca, 10)
	testing.expect_value(t, ea, 1)

	second: [10]u8
	off = h2t_headers_open(second[:], 3, FLAG_END_HEADERS, H2T_Open1[:])
	cb, eb, ob, ok_second := h2conn_feed(&f.conn, second[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_second, true)
	testing.expect_value(t, cb, 10)
	testing.expect_value(t, eb, 0)
	testing.expect_value(t, ob, 13)
	h2t_expect_bytes(t, f.out[:ob], []u8{0, 0, 4, 3, 0, 0, 0, 0, 3, 0, 0, 0, 7})
}

@(test)
test_h2_frame_size_error_on_receive :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)

	blob: [42]u8
	off := h2t_handshake_input(blob[:])
	off = h2t_put_header(blob[:], off, 0, 0, 1, 16385)
	consumed, _, _, ok := h2conn_feed(&f.conn, blob[:off], f.events[:], f.out[:])
	testing.expect_value(t, ok, false)
	testing.expect_value(t, consumed, 33)
	code, failed := h2conn_error(&f.conn)
	testing.expect_value(t, failed, true)
	testing.expect_value(t, code, Error_Code.Frame_Size_Error)
}

@(test)
test_h2_short_out_buffer_defers_output :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)

	input: [33]u8
	h2t_handshake_input(input[:])
	tiny: [4]u8
	consumed, events, out_used, ok := h2conn_feed(&f.conn, input[:], f.events[:], tiny[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, consumed, 33)
	testing.expect_value(t, events, 0)
	testing.expect_value(t, out_used, 0)
	testing.expect_value(t, f.conn.pending_count, 1)

	empty: [0]u8
	c2, e2, o2, ok2 := h2conn_feed(&f.conn, empty[:], f.events[:], f.out[:])
	testing.expect_value(t, ok2, true)
	testing.expect_value(t, c2, 0)
	testing.expect_value(t, e2, 0)
	testing.expect_value(t, o2, 9)
	h2t_expect_bytes(t, f.out[:o2], []u8{0, 0, 0, 4, 1, 0, 0, 0, 0})
}

@(test)
test_h2_window_update_rejects :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)

	zero: [13]u8
	off := h2t_put_header(zero[:], 0, 8, 0, 0, 4)
	zero[off] = 0
	zero[off + 1] = 0
	zero[off + 2] = 0
	zero[off + 3] = 0
	_, _, _, zero_ok := h2conn_feed(&f.conn, zero[:], f.events[:], f.out[:])
	testing.expect_value(t, zero_ok, false)
	code, failed := h2conn_error(&f.conn)
	testing.expect_value(t, failed, true)
	testing.expect_value(t, code, Error_Code.Protocol_Error)

	g: H2T_Fixture
	testing.expect_value(t, h2t_init(&g), true)
	h2t_handshake(&g, t)
	g.conn.conn_send.value = FLOW_MAX_WINDOW_SIZE - 10

	big: [13]u8
	off = h2t_window_update(big[:], 0, 100)
	_, _, _, big_ok := h2conn_feed(&g.conn, big[:], g.events[:], g.out[:])
	testing.expect_value(t, big_ok, false)
	code, failed = h2conn_error(&g.conn)
	testing.expect_value(t, failed, true)
	testing.expect_value(t, code, Error_Code.Flow_Control_Error)
}

@(test)
test_h2_push_promise_rejected :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)

	push: [13]u8
	off := h2t_put_header(push[:], 0, 5, FLAG_END_HEADERS, 1, 4)
	push[off] = 0
	push[off + 1] = 0
	push[off + 2] = 0
	push[off + 3] = 2
	_, _, _, push_ok := h2conn_feed(&f.conn, push[:], f.events[:], f.out[:])
	testing.expect_value(t, push_ok, false)
	code, failed := h2conn_error(&f.conn)
	testing.expect_value(t, failed, true)
	testing.expect_value(t, code, Error_Code.Protocol_Error)
}
