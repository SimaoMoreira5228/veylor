#+test
package http2

import "core:testing"

@(test)
test_h2_handshake_server_roundtrip :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)

	used, ok := h2conn_queue_settings(&f.conn, nil, f.out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9)
	h2t_expect_bytes(t, f.out[:used], []u8{0, 0, 0, 4, 0, 0, 0, 0, 0})
	testing.expect_value(t, f.conn.table.unacked_settings, u32(1))

	input: [33]u8
	testing.expect_value(t, h2t_handshake_input(input[:]), 33)
	consumed, events, out_used, feed_ok := h2conn_feed(&f.conn, input[:], f.events[:], f.out[:])
	testing.expect_value(t, feed_ok, true)
	testing.expect_value(t, consumed, 33)
	testing.expect_value(t, events, 0)
	testing.expect_value(t, out_used, 9)
	h2t_expect_bytes(t, f.out[:out_used], []u8{0, 0, 0, 4, 1, 0, 0, 0, 0})
	testing.expect_value(t, f.conn.preface_done, true)

	ack := [?]u8{0, 0, 0, 4, 1, 0, 0, 0, 0}
	c2, e2, o2, ok2 := h2conn_feed(&f.conn, ack[:], f.events[:], f.out[:])
	testing.expect_value(t, ok2, true)
	testing.expect_value(t, c2, 9)
	testing.expect_value(t, e2, 0)
	testing.expect_value(t, o2, 0)
	testing.expect_value(t, f.conn.table.unacked_settings, u32(0))
}

@(test)
test_h2_handshake_truncation_every_split :: proc(t: ^testing.T) {
	input: [33]u8
	testing.expect_value(t, h2t_handshake_input(input[:]), 33)

	for cut in 0 ..< 33 {
		f: H2T_Fixture
		testing.expect_value(t, h2t_init(&f), true)
		consumed, events, out_used, ok := h2conn_feed(&f.conn, input[:cut], f.events[:], f.out[:])
		testing.expect_value(t, ok, true)
		testing.expect_value(t, consumed, 0)
		testing.expect_value(t, events, 0)
		testing.expect_value(t, out_used, 0)
		testing.expect_value(t, f.conn.preface_done, false)
	}
}

@(test)
test_h2_get_split_headers_continuations :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)

	blob: [42]u8
	testing.expect_value(t, h2t_split_get(blob[:]), 42)
	consumed, events, out_used, feed_ok := h2conn_feed(&f.conn, blob[:], f.events[:], f.out[:])
	testing.expect_value(t, feed_ok, true)
	testing.expect_value(t, consumed, 42)
	testing.expect_value(t, events, 2)
	testing.expect_value(t, out_used, 0)

	hev := f.events[0]
	testing.expect_value(t, hev.kind, H2_Event_Kind.Headers_Ready)
	testing.expect_value(t, hev.stream_id, u32(1))
	testing.expect_value(t, hev.end_stream, true)
	testing.expect_value(t, len(hev.headers), 3)
	testing.expect_value(t, hev.headers[0].name, ":method")
	testing.expect_value(t, hev.headers[0].value, "GET")
	testing.expect_value(t, hev.headers[1].name, ":path")
	testing.expect_value(t, hev.headers[1].value, "/")
	testing.expect_value(t, hev.headers[2].name, "x-custom")
	testing.expect_value(t, hev.headers[2].value, "v1")

	cev := f.events[1]
	testing.expect_value(t, cev.kind, H2_Event_Kind.Stream_Closed)
	testing.expect_value(t, cev.stream_id, u32(1))
	testing.expect_value(t, f.conn.hpack.count, 1)
	testing.expect_value(t, f.conn.hpack.entries[0].name, "x-custom")
	testing.expect_value(t, f.conn.hpack.entries[0].value, "v1")
}

@(test)
test_h2_headers_flow_truncation_resume :: proc(t: ^testing.T) {
	blob: [42]u8
	testing.expect_value(t, h2t_split_get(blob[:]), 42)

	for cut in 0 ..< 42 {
		f: H2T_Fixture
		testing.expect_value(t, h2t_init(&f), true)
		h2t_handshake(&f, t)
		c1, e1, _, ok1 := h2conn_feed(&f.conn, blob[:cut], f.events[:], f.out[:])
		testing.expect_value(t, ok1, true)
		testing.expect_value(t, e1, 0)
		testing.expect(t, c1 == 0 || c1 == 14 || c1 == 28)
		c2, e2, _, ok2 := h2conn_feed(&f.conn, blob[c1:], f.events[:], f.out[:])
		testing.expect_value(t, ok2, true)
		testing.expect_value(t, c1 + c2, 42)
		testing.expect_value(t, e2, 2)
		testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Headers_Ready)
		testing.expect_value(t, f.events[1].kind, H2_Event_Kind.Stream_Closed)
	}
}

@(test)
test_h2_data_partial_local_grant_resume :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Headers_Ready)
	testing.expect_value(t, f.events[0].end_stream, false)

	f.conn.conn_recv.value = 100
	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)
	f.conn.table.slots[index].recv_window.value = 100

	payload: [512]u8

	for i in 0 ..< 512 {
		payload[i] = u8(i % 251)
	}
	frame: [521]u8
	doff := h2t_put_header(frame[:], 0, 0, 0, 1, 512)

	for i in 0 ..< 512 {
		frame[doff + i] = payload[i]
	}
	cb, eb, _, ok_data := h2conn_feed(&f.conn, frame[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_data, true)
	testing.expect_value(t, cb, 521)
	testing.expect_value(t, eb, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Data_Ready)
	testing.expect_value(t, len(f.events[0].data), 100)
	testing.expect_value(t, f.events[0].end_stream, false)
	h2t_expect_bytes(t, f.events[0].data, payload[:100])

	grant_out: [64]u8
	_, stream_grant_ok := h2conn_queue_window_update(&f.conn, 1, 500, grant_out[:])
	testing.expect_value(t, stream_grant_ok, true)
	_, conn_grant_ok := h2conn_queue_window_update(&f.conn, 0, 500, grant_out[:])
	testing.expect_value(t, conn_grant_ok, true)
	empty: [0]u8
	cc, ec, _, ok_resume := h2conn_feed(&f.conn, empty[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_resume, true)
	testing.expect_value(t, cc, 0)
	testing.expect_value(t, ec, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Data_Ready)
	testing.expect_value(t, len(f.events[0].data), 412)
	h2t_expect_bytes(t, f.events[0].data, payload[100:])

	big: [209]u8
	off := h2t_put_header(big[:], 0, 0, FLAG_END_STREAM, 1, 200)

	for i in 0 ..< 200 {
		big[off + i] = u8((i * 7) % 251)
	}
	ce, ee, _, ok_big := h2conn_feed(&f.conn, big[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_big, true)
	testing.expect_value(t, ce, 209)
	testing.expect_value(t, ee, 1)
	testing.expect_value(t, len(f.events[0].data), 88)
	testing.expect_value(t, f.events[0].end_stream, false)

	_, stream_top_ok := h2conn_queue_window_update(&f.conn, 1, 200, grant_out[:])
	testing.expect_value(t, stream_top_ok, true)
	_, conn_top_ok := h2conn_queue_window_update(&f.conn, 0, 200, grant_out[:])
	testing.expect_value(t, conn_top_ok, true)
	cg, eg, _, ok_top := h2conn_feed(&f.conn, empty[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_top, true)
	testing.expect_value(t, cg, 0)
	testing.expect_value(t, eg, 2)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Data_Ready)
	testing.expect_value(t, len(f.events[0].data), 112)
	testing.expect_value(t, f.events[0].end_stream, true)
	testing.expect_value(t, f.events[1].kind, H2_Event_Kind.Stream_Closed)

	for i in 0 ..< 112 {
		testing.expect_value(t, f.events[0].data[i], u8(((88 + i) * 7) % 251))
	}
}

@(test)
test_h2_zero_window_defers_then_resumes :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)

	open: [11]u8
	off := h2t_headers_open(open[:], 3, FLAG_END_HEADERS, H2T_Open2[:])
	_, ea, _, ok_open := h2conn_feed(&f.conn, open[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_open, true)
	testing.expect_value(t, ea, 1)

	f.conn.conn_recv.value = 0
	index, found := streams_find(&f.conn.table, 3)
	testing.expect_value(t, found, true)
	f.conn.table.slots[index].recv_window.value = 0

	data: [19]u8
	off = h2t_put_header(data[:], 0, 0, 0, 3, 10)

	for i in 0 ..< 10 {
		data[off + i] = u8(65 + i)
	}
	cb, eb, _, ok_data := h2conn_feed(&f.conn, data[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_data, true)
	testing.expect_value(t, cb, 19)
	testing.expect_value(t, eb, 0)

	grant_out: [64]u8
	_, stream_grant_ok := h2conn_queue_window_update(&f.conn, 3, 10, grant_out[:])
	testing.expect_value(t, stream_grant_ok, true)
	_, conn_grant_ok := h2conn_queue_window_update(&f.conn, 0, 10, grant_out[:])
	testing.expect_value(t, conn_grant_ok, true)
	empty: [0]u8
	cd, ed, _, ok_resume := h2conn_feed(&f.conn, empty[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_resume, true)
	testing.expect_value(t, cd, 0)
	testing.expect_value(t, ed, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Data_Ready)
	testing.expect_value(t, len(f.events[0].data), 10)

	for i in 0 ..< 10 {
		testing.expect_value(t, f.events[0].data[i], u8(65 + i))
	}

	recv_before := f.conn.conn_recv.value
	stream_recv_before := f.conn.table.slots[index].recv_window.value
	wu: [13]u8
	off = h2t_window_update(wu[:], 3, 10)
	cc, ec, _, ok_wu := h2conn_feed(&f.conn, wu[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_wu, true)
	testing.expect_value(t, cc, 13)
	testing.expect_value(t, ec, 0)
	testing.expect_value(t, f.conn.conn_recv.value, recv_before)
	testing.expect_value(t, f.conn.table.slots[index].recv_window.value, stream_recv_before)
}

@(test)
test_h2_inbound_window_update_opens_send :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)
	conn_send_before := f.conn.conn_send.value
	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)

	if !found {
		return
	}
	stream_send_before := f.conn.table.slots[index].send_window.value
	recv_before := f.conn.conn_recv.value
	stream_recv_before := f.conn.table.slots[index].recv_window.value

	wu: [13]u8
	off := h2t_window_update(wu[:], 0, 500)
	_, ec, _, ok_wu := h2conn_feed(&f.conn, wu[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_wu, true)
	testing.expect_value(t, ec, 0)
	testing.expect_value(t, f.conn.conn_send.value, conn_send_before + 500)
	testing.expect_value(t, f.conn.conn_recv.value, recv_before)

	second: [13]u8
	off = h2t_window_update(second[:], 1, 500)
	_, ed, _, ok_wu2 := h2conn_feed(&f.conn, second[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_wu2, true)
	testing.expect_value(t, ed, 0)
	testing.expect_value(t, f.conn.table.slots[index].send_window.value, stream_send_before + 500)
	testing.expect_value(t, f.conn.table.slots[index].recv_window.value, stream_recv_before)
}

@(test)
test_h2_sustained_data_leaves_scratch_stable :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)
	scratch_before := f.conn.scratch_used
	payload: [1024]u8

	for i in 0 ..< len(payload) {
		payload[i] = u8(i % 251)
	}

	for i in 0 ..< 20 {
		frame: [1033]u8
		doff := h2t_put_header(frame[:], 0, 0, 0, 1, 1024)

		for j in 0 ..< len(payload) {
			frame[doff + j] = payload[j]
		}
		consumed, event_count, _, feed_ok := h2conn_feed(
			&f.conn,
			frame[:doff + len(payload)],
			f.events[:],
			f.out[:],
		)
		testing.expect_value(t, feed_ok, true)
		testing.expect_value(t, consumed, doff + len(payload))
		testing.expect_value(t, event_count, 1)
		testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Data_Ready)
		testing.expect_value(t, len(f.events[0].data), len(payload))
		testing.expect_value(t, f.events[0].stream_id, u32(1))

		if i == 0 {
			h2t_expect_bytes(t, f.events[0].data, payload[:])
		}
	}
	testing.expect_value(t, f.conn.scratch_used, scratch_before)
	testing.expect_value(t, f.conn.delivery_used, 0)
}

@(test)
test_h2_closed_stream_slots_reused :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)

	for i in 0 ..< H2T_Streams_Cap {
		sid := u32(i * 2 + 1)
		open: [11]u8
		off := h2t_headers_open(open[:], sid, FLAG_END_HEADERS, H2T_Open2[:])
		_, events_open, _, open_ok := h2conn_feed(&f.conn, open[:off], f.events[:], f.out[:])
		testing.expect_value(t, open_ok, true)
		testing.expect_value(t, events_open, 1)
		rst: [13]u8
		h2t_rst(rst[:], sid, 8)
		_, events_rst, _, rst_ok := h2conn_feed(&f.conn, rst[:], f.events[:], f.out[:])
		testing.expect_value(t, rst_ok, true)
		testing.expect_value(t, events_rst, 1)
		testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Stream_Reset)
	}
	testing.expect_value(t, f.conn.table.used, H2T_Streams_Cap)
	last: [11]u8
	off := h2t_headers_open(last[:], 33, FLAG_END_HEADERS, H2T_Open2[:])
	_, events_last, _, last_ok := h2conn_feed(&f.conn, last[:off], f.events[:], f.out[:])
	testing.expect_value(t, last_ok, true)
	testing.expect_value(t, events_last, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Headers_Ready)
	testing.expect_value(t, f.events[0].stream_id, u32(33))
	testing.expect_value(t, f.conn.table.used, H2T_Streams_Cap)
}

@(test)
test_h2_sequential_request_reuses_dynamic_table :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)

	block := [?]u8 {
		0x82,
		0x84,
		0x40,
		0x08,
		0x78,
		0x2d,
		0x63,
		0x75,
		0x73,
		0x74,
		0x6f,
		0x6d,
		0x02,
		0x76,
		0x31,
	}
	req: [24]u8
	off := h2t_put_header(req[:], 0, 1, FLAG_END_STREAM | FLAG_END_HEADERS, 1, 15)

	for i in 0 ..< 15 {
		req[off + i] = block[i]
	}
	ca, ea, _, ok_req := h2conn_feed(&f.conn, req[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_req, true)
	testing.expect_value(t, ca, 24)
	testing.expect_value(t, ea, 2)
	testing.expect_value(t, f.conn.hpack.count, 1)

	rst: [13]u8
	off = h2t_rst(rst[:], 1, 8)
	cb, eb, _, ok_rst := h2conn_feed(&f.conn, rst[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_rst, true)
	testing.expect_value(t, cb, 13)
	testing.expect_value(t, eb, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Stream_Reset)
	testing.expect_value(t, f.events[0].stream_id, u32(1))
	testing.expect_value(t, f.events[0].code, Error_Code.Cancel)

	req2: [11]u8
	off = h2t_put_header(req2[:], 0, 1, FLAG_END_STREAM | FLAG_END_HEADERS, 3, 2)
	req2[off] = 0xbe
	req2[off + 1] = 0x84
	cc, ec, _, ok_req2 := h2conn_feed(&f.conn, req2[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_req2, true)
	testing.expect_value(t, cc, 11)
	testing.expect_value(t, ec, 2)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Headers_Ready)
	testing.expect_value(t, f.events[0].stream_id, u32(3))
	testing.expect_value(t, len(f.events[0].headers), 2)
	testing.expect_value(t, f.events[0].headers[0].name, "x-custom")
	testing.expect_value(t, f.events[0].headers[0].value, "v1")
	testing.expect_value(t, f.events[0].headers[1].name, ":path")
	testing.expect_value(t, f.events[0].headers[1].value, "/")
	testing.expect_value(t, f.events[1].kind, H2_Event_Kind.Stream_Closed)
	testing.expect_value(t, f.conn.hpack.count, 1)
}
