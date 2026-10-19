#+test
package http2

import "core:testing"

@(test)
test_h2_status_text_vectors :: proc(t: ^testing.T) {
	out: [3]u8
	n, ok := h2_status_text(200, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, n, 3)
	testing.expect_value(t, out, [3]u8{'2', '0', '0'})

	n, ok = h2_status_text(404, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, out, [3]u8{'4', '0', '4'})

	n, ok = h2_status_text(599, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, out, [3]u8{'5', '9', '9'})

	_, ok = h2_status_text(99, out[:])
	testing.expect_value(t, ok, false)
	_, ok = h2_status_text(1000, out[:])
	testing.expect_value(t, ok, false)
	short: [2]u8
	_, ok = h2_status_text(200, short[:])
	testing.expect_value(t, ok, false)
}

@(test)
test_h2_response_headers_signals_table_size_shrink :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)
	testing.expect(t, hpack_table_set_max_size(&f.conn.hpack, 0))
	out: [128]u8
	used, ok := h2conn_queue_response_headers(&f.conn, 1, 200, false, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9 + 1 + 13)
	testing.expect_value(t, out[9], u8(0x20))
	second: [128]u8
	used_next, ok_next := h2conn_queue_response_headers(&f.conn, 1, 200, true, second[:])
	testing.expect_value(t, ok_next, true)
	testing.expect_value(t, used_next, 9 + 13)
	testing.expect_value(t, second[9] != u8(0x20), true)
}

@(test)
test_h2_queue_response_headers_golden :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)

	out: [128]u8
	used, ok := h2conn_queue_response_headers(&f.conn, 1, 200, false, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9 + 13)
	testing.expect_value(t, out[0], u8(0))
	testing.expect_value(t, out[1], u8(0))
	testing.expect_value(t, out[2], u8(13))
	testing.expect_value(t, out[3], u8(1))
	testing.expect_value(t, out[4], FLAG_END_HEADERS)
	testing.expect_value(t, out[5], u8(0))
	testing.expect_value(t, out[6], u8(0))
	testing.expect_value(t, out[7], u8(0))
	testing.expect_value(t, out[8], u8(1))
	testing.expect_value(t, out[9], u8(0x00))
	testing.expect_value(t, out[10], u8(0x07))
	testing.expect_value(t, string(out[11:18]), ":status")
	testing.expect_value(t, out[18], u8(0x03))
	testing.expect_value(t, string(out[19:22]), "200")

	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(t, f.conn.table.slots[index].state, Stream_State.Open)
}

@(test)
test_h2_queue_response_headers_end_stream_closes_send :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)

	out: [128]u8
	used, ok := h2conn_queue_response_headers(&f.conn, 1, 404, true, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9 + 13)
	testing.expect_value(t, out[4] & FLAG_END_STREAM, FLAG_END_STREAM)

	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(t, f.conn.table.slots[index].state, Stream_State.Half_Closed_Local)
}

@(test)
test_h2_queue_response_headers_rejects_bad_state :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)

	out: [128]u8
	_, ok := h2conn_queue_response_headers(&f.conn, 9, 200, false, out[:])
	testing.expect_value(t, ok, false)
	_, ok = h2conn_queue_response_headers(&f.conn, 0, 200, false, out[:])
	testing.expect_value(t, ok, false)
	_, ok = h2conn_queue_response_headers(&f.conn, 1, 99, false, out[:])
	testing.expect_value(t, ok, false)
	tiny: [4]u8
	_, ok = h2conn_queue_response_headers(&f.conn, 1, 200, false, tiny[:])
	testing.expect_value(t, ok, false)

	used, closed_ok := h2conn_queue_response_headers(&f.conn, 1, 200, true, out[:])
	testing.expect_value(t, closed_ok, true)
	testing.expect_value(t, used, 9 + 13)
	_, ok = h2conn_queue_response_data(&f.conn, 1, transmute([]u8)string("x"), false, out[:])
	testing.expect_value(t, ok, false)
}

@(test)
test_h2_queue_response_data_chunks_against_windows :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)

	out: [256]u8
	body := transmute([]u8)string("hello")
	used, ok := h2conn_queue_response_data(&f.conn, 1, body, false, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9 + 5)
	testing.expect_value(t, out[0], u8(0))
	testing.expect_value(t, out[1], u8(0))
	testing.expect_value(t, out[2], u8(5))
	testing.expect_value(t, out[3], u8(0))
	testing.expect_value(t, out[4], u8(0))
	testing.expect_value(t, string(out[9:14]), "hello")
	testing.expect_value(t, f.conn.conn_send.value, FLOW_INITIAL_WINDOW_SIZE - 5)

	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(
		t,
		f.conn.table.slots[index].send_window.value,
		FLOW_INITIAL_WINDOW_SIZE - 5,
	)
	testing.expect_value(t, f.conn.table.slots[index].state, Stream_State.Open)
}

@(test)
test_h2_queue_response_data_chunks_against_remote_max_frame :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)
	f.conn.table.remote.max_frame_size = 100
	out: [256]u8
	body: [200]u8

	for i in 0 ..< len(body) {
		body[i] = u8(i % 251)
	}
	used, ok := h2conn_queue_response_data(&f.conn, 1, body[:], false, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9 + 100)
	testing.expect_value(t, out[2], u8(100))
	testing.expect_value(t, f.conn.conn_send.value, FLOW_INITIAL_WINDOW_SIZE - 100)

	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(
		t,
		f.conn.table.slots[index].send_window.value,
		FLOW_INITIAL_WINDOW_SIZE - 100,
	)
	testing.expect_value(t, f.conn.table.slots[index].state, Stream_State.Open)
}

@(test)
test_h2_queue_response_data_end_stream_terminal :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)

	out: [256]u8
	body := transmute([]u8)string("bye")
	used, ok := h2conn_queue_response_data(&f.conn, 1, body, true, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9 + 3)
	testing.expect_value(t, out[4] & FLAG_END_STREAM, FLAG_END_STREAM)

	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(t, f.conn.table.slots[index].state, Stream_State.Half_Closed_Local)
}

@(test)
test_h2_queue_response_data_empty_end_sends_header_only :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)

	out: [256]u8
	empty: [0]u8
	used, ok := h2conn_queue_response_data(&f.conn, 1, empty[:], true, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9)
	testing.expect_value(t, out[2], u8(0))
	testing.expect_value(t, out[4] & FLAG_END_STREAM, FLAG_END_STREAM)

	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(t, f.conn.table.slots[index].state, Stream_State.Half_Closed_Local)
}

@(test)
test_h2_queue_response_trailers_ends_stream :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)
	open: [11]u8
	off := h2t_headers_open(open[:], 1, FLAG_END_HEADERS | FLAG_END_STREAM, H2T_Open2[:])
	_, _, _, open_ok := h2conn_feed(&f.conn, open[:off], f.events[:], f.out[:])
	testing.expect_value(t, open_ok, true)
	name := transmute([]u8)string("x-checksum")
	value := transmute([]u8)string("abc")
	block: [64]u8
	n, enc_ok := encode_literal_header(.Without_Indexing, 0, name, value, false, false, block[:])
	testing.expect_value(t, enc_ok, true)
	out: [128]u8
	used, ok := h2conn_queue_response_trailers(&f.conn, 1, block[:n], out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9 + n)
	testing.expect_value(t, out[3], u8(1))
	testing.expect_value(t, out[4] & FLAG_END_STREAM, FLAG_END_STREAM)
	testing.expect_value(t, out[4] & FLAG_END_HEADERS, FLAG_END_HEADERS)
	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(t, f.conn.table.slots[index].state, Stream_State.Closed)
	_, closed_ok := h2conn_queue_response_data(
		&f.conn,
		1,
		transmute([]u8)string("x"),
		false,
		out[:],
	)
	testing.expect_value(t, closed_ok, false)
	_, second_ok := h2conn_queue_response_trailers(&f.conn, 1, block[:n], out[:])
	testing.expect_value(t, second_ok, false)
}

@(test)
test_h2_queue_response_data_stalls_on_closed_window :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_open_stream1(&f, t)

	out: [256]u8
	f.conn.conn_send.value = 0
	body := transmute([]u8)string("hello")
	used, ok := h2conn_queue_response_data(&f.conn, 1, body, false, out[:])
	testing.expect_value(t, ok, false)
	testing.expect_value(t, used, 0)
	testing.expect_value(t, f.conn.failed, false)
}

@(test)
test_h2_queue_response_headers_after_half_close_remote :: proc(t: ^testing.T) {
	f: H2T_Fixture
	testing.expect_value(t, h2t_init(&f), true)
	h2t_handshake(&f, t)
	open: [11]u8
	off := h2t_headers_open(open[:], 1, FLAG_END_HEADERS | FLAG_END_STREAM, H2T_Open2[:])
	testing.expect_value(t, off, 11)
	_, ea, _, ok_open := h2conn_feed(&f.conn, open[:], f.events[:], f.out[:])
	testing.expect_value(t, ok_open, true)
	testing.expect_value(t, ea, 2)

	out: [128]u8
	used, ok := h2conn_queue_response_headers(&f.conn, 1, 200, true, out[:])
	testing.expect_value(t, ok, true)
	testing.expect_value(t, used, 9 + 13)

	index, found := streams_find(&f.conn.table, 1)
	testing.expect_value(t, found, true)
	testing.expect_value(t, f.conn.table.slots[index].state, Stream_State.Closed)
}
