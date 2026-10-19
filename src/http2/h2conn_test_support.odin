#+test
package http2

import "core:testing"

H2T_Streams_Cap :: 16
H2T_Reasm_Cap :: 4096
H2T_Dyn_Cap :: 32
H2T_Scratch_Cap :: 8192
H2T_Delivery_Cap :: 4096
H2T_Headers_Cap :: 32
H2T_Pending_Cap :: 16
H2T_Events_Cap :: 8
H2T_Out_Cap :: 256

H2T_Fixture :: struct {
	conn:     H2_Connection,
	streams:  [H2T_Streams_Cap]Stream,
	reasm:    [H2T_Reasm_Cap]u8,
	dyn:      [H2T_Dyn_Cap]Hpack_Entry,
	scratch:  [H2T_Scratch_Cap]u8,
	headers:  [H2T_Headers_Cap]Hpack_Header,
	pending:  [H2T_Pending_Cap]H2_Pending,
	events:   [H2T_Events_Cap]H2_Event,
	out:      [H2T_Out_Cap]u8,
	delivery: [H2T_Delivery_Cap]u8,
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
		f.delivery[:],
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
