#+test
package http2

import "core:testing"

H2T_Streams_Cap :: 16
H2T_Reasm_Cap :: 4096
H2T_Dyn_Cap :: 32
H2T_Scratch_Cap :: 8192
H2T_Headers_Cap :: 32
H2T_Pending_Cap :: 16
H2T_Events_Cap :: 8
H2T_Out_Cap :: 256

H2T_Fixture :: struct {
	conn:    H2_Connection,
	streams: [H2T_Streams_Cap]Stream,
	reasm:   [H2T_Reasm_Cap]u8,
	dyn:     [H2T_Dyn_Cap]Hpack_Entry,
	scratch: [H2T_Scratch_Cap]u8,
	headers: [H2T_Headers_Cap]Hpack_Header,
	pending: [H2T_Pending_Cap]H2_Pending,
	events:  [H2T_Events_Cap]H2_Event,
	out:     [H2T_Out_Cap]u8,
}

H2T_Frag0 := [5]u8{0x82, 0x84, 0x40, 0x08, 0x78}
H2T_Frag1 := [5]u8{0x2d, 0x63, 0x75, 0x73, 0x74}
H2T_Frag2 := [5]u8{0x6f, 0x6d, 0x02, 0x76, 0x31}
H2T_Open2 := [2]u8{0x82, 0x84}
H2T_Open1 := [1]u8{0x88}

h2t_init :: proc(
	f: ^H2T_Fixture,
	max_frame: u32 = DEFAULT_MAX_FRAME_SIZE,
	max_concurrent: u32 = max(u32),
	max_header_list: u32 = DEFAULT_MAX_HEADER_LIST_SIZE,
	table_cap: u64 = 4096,
) -> bool {
	return h2conn_init(
		&f.conn,
		f.streams[:],
		f.reasm[:],
		f.dyn[:],
		f.scratch[:],
		f.headers[:],
		f.pending[:],
		max_frame,
		max_concurrent,
		max_header_list,
		table_cap,
	)
}

h2t_put_header :: proc(
	buf: []u8,
	off: int,
	type_byte: u8,
	flags: u8,
	sid: u32,
	length: u32,
) -> int {
	buf[off + 0] = u8(length >> 16)
	buf[off + 1] = u8(length >> 8)
	buf[off + 2] = u8(length)
	buf[off + 3] = type_byte
	buf[off + 4] = flags
	buf[off + 5] = u8(sid >> 24)
	buf[off + 6] = u8(sid >> 16)
	buf[off + 7] = u8(sid >> 8)
	buf[off + 8] = u8(sid)

	return off + FRAME_HEADER_LEN
}

h2t_put_magic :: proc(buf: []u8) -> int {
	magic := CLIENT_CONNECTION_PREFACE

	for i in 0 ..< len(magic) {
		buf[i] = magic[i]
	}

	return len(magic)
}

h2t_handshake_input :: proc(buf: []u8) -> int {
	off := h2t_put_magic(buf)

	return h2t_put_header(buf, off, 4, 0, 0, 0)
}

h2t_handshake :: proc(f: ^H2T_Fixture, t: ^testing.T) {
	input: [33]u8
	h2t_handshake_input(input[:])
	_, _, _, ok := h2conn_feed(&f.conn, input[:], f.events[:], f.out[:])
	testing.expect_value(t, ok, true)
}

h2t_open_stream1 :: proc(f: ^H2T_Fixture, t: ^testing.T) {
	h2t_handshake(f, t)
	open: [11]u8
	off := h2t_headers_open(open[:], 1, FLAG_END_HEADERS, H2T_Open2[:])
	testing.expect_value(t, off, 11)
	_, ea, _, ok_open := h2conn_feed(&f.conn, open[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_open, true)
	testing.expect_value(t, ea, 1)
}

h2t_split_get :: proc(blob: []u8) -> int {
	off := h2t_put_header(blob, 0, 1, FLAG_END_STREAM, 1, 5)

	for i in 0 ..< 5 {
		blob[off + i] = H2T_Frag0[i]
	}
	off += 5
	off = h2t_put_header(blob, off, 9, 0, 1, 5)

	for i in 0 ..< 5 {
		blob[off + i] = H2T_Frag1[i]
	}
	off += 5
	off = h2t_put_header(blob, off, 9, FLAG_END_HEADERS, 1, 5)

	for i in 0 ..< 5 {
		blob[off + i] = H2T_Frag2[i]
	}

	return off + 5
}

h2t_window_update :: proc(buf: []u8, sid: u32, inc: u32) -> int {
	off := h2t_put_header(buf, 0, 8, 0, sid, 4)
	buf[off] = u8(inc >> 24)
	buf[off + 1] = u8(inc >> 16)
	buf[off + 2] = u8(inc >> 8)
	buf[off + 3] = u8(inc)

	return off + 4
}

h2t_rst :: proc(buf: []u8, sid: u32, code: u32) -> int {
	off := h2t_put_header(buf, 0, 3, 0, sid, 4)
	buf[off] = u8(code >> 24)
	buf[off + 1] = u8(code >> 16)
	buf[off + 2] = u8(code >> 8)
	buf[off + 3] = u8(code)

	return off + 4
}

h2t_headers_open :: proc(buf: []u8, sid: u32, flags: u8, frag: []u8) -> int {
	off := h2t_put_header(buf, 0, 1, flags, sid, u32(len(frag)))

	for i in 0 ..< len(frag) {
		buf[off + i] = frag[i]
	}

	return off + len(frag)
}

h2t_data :: proc(buf: []u8, sid: u32, flags: u8, payload: []u8) -> int {
	off := h2t_put_header(buf, 0, 0, flags, sid, u32(len(payload)))

	for i in 0 ..< len(payload) {
		buf[off + i] = payload[i]
	}

	return off + len(payload)
}

h2t_goaway :: proc(buf: []u8, last: u32, code: u32) -> int {
	off := h2t_put_header(buf, 0, 7, 0, 0, 8)
	buf[off] = u8(last >> 24)
	buf[off + 1] = u8(last >> 16)
	buf[off + 2] = u8(last >> 8)
	buf[off + 3] = u8(last)
	buf[off + 4] = u8(code >> 24)
	buf[off + 5] = u8(code >> 16)
	buf[off + 6] = u8(code >> 8)
	buf[off + 7] = u8(code)

	return off + 8
}

h2t_settings_value :: proc(buf: []u8, id: u16, value: u32) -> int {
	off := h2t_put_magic(buf)
	off = h2t_put_header(buf, off, 4, 0, 0, 6)
	buf[off] = u8(id >> 8)
	buf[off + 1] = u8(id)
	buf[off + 2] = u8(value >> 24)
	buf[off + 3] = u8(value >> 16)
	buf[off + 4] = u8(value >> 8)
	buf[off + 5] = u8(value)

	return off + 6
}

h2t_expect_bytes :: proc(t: ^testing.T, got: []u8, want: []u8, loc := #caller_location) {
	testing.expect_value(t, len(got), len(want), loc)
	n := len(got)

	if len(want) < n {
		n = len(want)
	}

	for i in 0 ..< n {
		testing.expect_value(t, got[i], want[i], loc)
	}
}

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
test_h2_data_partial_window_update_resume :: proc(t: ^testing.T) {
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

	wu: [13]u8
	off := h2t_window_update(wu[:], 0, 500)
	cc, ec, _, ok_wu := h2conn_feed(&f.conn, wu[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_wu, true)
	testing.expect_value(t, cc, 13)
	testing.expect_value(t, ec, 0)

	second: [13]u8
	off = h2t_window_update(second[:], 1, 500)
	cd, ed, _, ok_wu2 := h2conn_feed(&f.conn, second[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_wu2, true)
	testing.expect_value(t, cd, 13)
	testing.expect_value(t, ed, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Data_Ready)
	testing.expect_value(t, len(f.events[0].data), 412)
	h2t_expect_bytes(t, f.events[0].data, payload[100:])

	big: [209]u8
	off = h2t_put_header(big[:], 0, 0, FLAG_END_STREAM, 1, 200)

	for i in 0 ..< 200 {
		big[off + i] = u8((i * 7) % 251)
	}
	ce, ee, _, ok_big := h2conn_feed(&f.conn, big[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_big, true)
	testing.expect_value(t, ce, 209)
	testing.expect_value(t, ee, 1)
	testing.expect_value(t, len(f.events[0].data), 88)
	testing.expect_value(t, f.events[0].end_stream, false)

	stream_top: [13]u8
	off = h2t_window_update(stream_top[:], 1, 200)
	cf, ef, _, ok_stop := h2conn_feed(&f.conn, stream_top[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_stop, true)
	testing.expect_value(t, cf, 13)
	testing.expect_value(t, ef, 0)

	top: [13]u8
	off = h2t_window_update(top[:], 0, 200)
	cg, eg, _, ok_top := h2conn_feed(&f.conn, top[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_top, true)
	testing.expect_value(t, cg, 13)
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

	wu: [13]u8
	off = h2t_window_update(wu[:], 3, 10)
	cc, ec, _, ok_wu := h2conn_feed(&f.conn, wu[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_wu, true)
	testing.expect_value(t, cc, 13)
	testing.expect_value(t, ec, 0)

	conn_wu: [13]u8
	off = h2t_window_update(conn_wu[:], 0, 10)
	cd, ed, _, ok_cwu := h2conn_feed(&f.conn, conn_wu[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_cwu, true)
	testing.expect_value(t, cd, 13)
	testing.expect_value(t, ed, 1)
	testing.expect_value(t, f.events[0].kind, H2_Event_Kind.Data_Ready)
	testing.expect_value(t, len(f.events[0].data), 10)

	for i in 0 ..< 10 {
		testing.expect_value(t, f.events[0].data[i], u8(65 + i))
	}
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
	g.conn.conn_recv.value = FLOW_MAX_WINDOW_SIZE - 10

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
		h2conn_init(&f.conn, nil, f.reasm[:], f.dyn[:], f.scratch[:], f.headers[:], f.pending[:]),
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
