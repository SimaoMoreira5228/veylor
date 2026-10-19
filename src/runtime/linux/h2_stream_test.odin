#+build linux
#+test
package linux_runtime

import "core:testing"
import host "veylor:runtime/host"
import http2 "veylor:http2"
import connections "veylor:runtime/connections"

h2m_response_cmd :: proc(kind: host.Host_Command_Kind, stream_id: u32) -> host.Host_Command {
	token := connections.Connection_Token{stream_id = stream_id}

	return host.Host_Command{kind = kind, token = token}
}

@(test)
test_h2_response_start_sends_head_and_body :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}

	if !h2m_park(t, &f, 1, true, 2_000_000_000) {
		return
	}
	body_text := "hello"
	cmd := h2m_response_cmd(.Response_Start, 1)
	cmd.status = 200
	cmd.reason_length = 2
	cmd.reason[0] = 'O'
	cmd.reason[1] = 'K'
	cmd.body_length = u32(len(body_text))
	copy(cmd.body[:len(body_text)], transmute([]u8)body_text)
	execute_h2_host_command(&f.worker, cmd, 2_000_000_000)

	_, gone := h2_find_record(h2m_slot(&f), 1)
	testing.expect_value(t, gone, false)

	slot := h2m_slot(&f)
	base := 0
	kind, stream, flags, length, frame_ok := h2m_parse_first_frame(t, slot.write_buf[base:])
	testing.expect_value(t, frame_ok, true)
	testing.expect_value(t, kind, http2.Frame_Type.Headers)
	testing.expect_value(t, stream, u32(1))
	testing.expect_value(t, flags & http2.FLAG_END_HEADERS, http2.FLAG_END_HEADERS)
	head_len := 9 + int(length)
	kind, stream, flags, length, frame_ok = h2m_parse_first_frame(t, slot.write_buf[base + head_len:])
	testing.expect_value(t, frame_ok, true)
	testing.expect_value(t, kind, http2.Frame_Type.Data)
	testing.expect_value(t, stream, u32(1))
	testing.expect_value(t, flags & http2.FLAG_END_STREAM, http2.FLAG_END_STREAM)
	testing.expect_value(t, length, u32(len(body_text)))
	testing.expect_value(
		t,
		string(slot.write_buf[base + head_len + 9:base + head_len + 9 + len(body_text)]),
		body_text,
	)
}

@(test)
test_h2_response_streaming_begin_write_end :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}

	if !h2m_park(t, &f, 1, true, 2_000_000_000) {
		return
	}
	slot := h2m_slot(&f)
	base := slot.write_used
	begin := h2m_response_cmd(.Response_Begin, 1)
	begin.status = 200
	begin.reason_length = 2
	begin.reason[0] = 'O'
	begin.reason[1] = 'K'
	execute_h2_host_command(&f.worker, begin, 2_000_000_000)

	record, found := h2_find_record(slot, 1)
	testing.expect_value(t, found, true)

	if !found {
		return
	}
	testing.expect_value(t, record.resp_started, true)

	writable, writable_ok := h2m_poll_one(t, &f.host_queue)
	testing.expect_value(t, writable_ok, true)
	testing.expect_value(t, writable.kind, host.Host_Event_Kind.Writable)
	testing.expect_value(t, writable.token.stream_id, u32(1))

	chunk_text := "chunk-one"
	write := h2m_response_cmd(.Response_Write, 1)
	write.body_length = u32(len(chunk_text))
	copy(write.body[:len(chunk_text)], transmute([]u8)chunk_text)
	execute_h2_host_command(&f.worker, write, 2_000_000_000)

	end := h2m_response_cmd(.Response_End, 1)
	execute_h2_host_command(&f.worker, end, 2_000_000_000)
	_, freed := h2_find_record(slot, 1)
	testing.expect_value(t, freed, false)

	frames := slot.write_buf[base:]
	kind, stream, flags, length, frame_ok := h2m_parse_first_frame(t, frames)
	testing.expect_value(t, frame_ok, true)
	testing.expect_value(t, kind, http2.Frame_Type.Headers)
	testing.expect_value(t, stream, u32(1))
	off := 9 + int(length)
	kind, stream, flags, length, frame_ok = h2m_parse_first_frame(t, frames[off:])
	testing.expect_value(t, frame_ok, true)
	testing.expect_value(t, kind, http2.Frame_Type.Data)
	testing.expect_value(t, length, u32(len(chunk_text)))
	testing.expect_value(t, flags & http2.FLAG_END_STREAM, u8(0))
	testing.expect_value(
		t,
		string(frames[off + 9:off + 9 + len(chunk_text)]),
		chunk_text,
	)
	off += 9 + int(length)
	kind, stream, flags, length, frame_ok = h2m_parse_first_frame(t, frames[off:])
	testing.expect_value(t, frame_ok, true)
	testing.expect_value(t, kind, http2.Frame_Type.Data)
	testing.expect_value(t, length, u32(0))
	testing.expect_value(t, flags & http2.FLAG_END_STREAM, http2.FLAG_END_STREAM)
}

@(test)
test_h2_response_write_stalls_on_closed_window :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}

	if !h2m_park(t, &f, 1, true, 2_000_000_000) {
		return
	}
	begin := h2m_response_cmd(.Response_Begin, 1)
	begin.status = 200
	begin.reason_length = 2
	begin.reason[0] = 'O'
	begin.reason[1] = 'K'
	execute_h2_host_command(&f.worker, begin, 2_000_000_000)
	_, _ = h2m_poll_one(t, &f.host_queue)

	slot := h2m_slot(&f)
	slot.conn.conn_send.value = 0
	chunk_text := "stalled"
	write := h2m_response_cmd(.Response_Write, 1)
	write.body_length = u32(len(chunk_text))
	copy(write.body[:len(chunk_text)], transmute([]u8)chunk_text)
	execute_h2_host_command(&f.worker, write, 2_000_000_000)

	record, found := h2_find_record(slot, 1)
	testing.expect_value(t, found, true)

	if !found {
		return
	}
	testing.expect_value(t, record.write_stalled, true)
	testing.expect_value(t, host.pending_host_events(&f.host_queue), 0)

	slot.conn.conn_send.value = 65535
	index, index_found := http2.streams_find(&slot.conn.table, 1)
	testing.expect_value(t, index_found, true)
	slot.conn.table.slots[index].send_window.value = 65535
	testing.expect_value(t, h2m_dispatch(t, &f, 0, 2_000_000_000), true)
	testing.expect_value(t, record.write_stalled, false)

	writable, writable_ok := h2m_poll_one(t, &f.host_queue)
	testing.expect_value(t, writable_ok, true)
	testing.expect_value(t, writable.kind, host.Host_Event_Kind.Writable)
}

@(test)
test_h2_cancel_command_resets_stream :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}

	if !h2m_park(t, &f, 1, true, 2_000_000_000) {
		return
	}
	slot := h2m_slot(&f)
	base := slot.write_used
	execute_h2_host_command(&f.worker, h2m_response_cmd(.Cancel, 1), 2_000_000_000)
	_, gone := h2_find_record(slot, 1)
	testing.expect_value(t, gone, false)

	kind, stream, _, length, frame_ok := h2m_parse_first_frame(t, slot.write_buf[base:])
	testing.expect_value(t, frame_ok, true)
	testing.expect_value(t, kind, http2.Frame_Type.Rst_Stream)
	testing.expect_value(t, stream, u32(1))
	testing.expect_value(t, length, u32(http2.RST_STREAM_PAYLOAD_LEN))
}

@(test)
test_h2_response_begin_rejects_head :: proc(t: ^testing.T) {
	f: H2M_Fixture

	if !h2m_init(&f, t) {
		return
	}
	defer h2m_close(&f)

	if !h2m_handshake(t, &f) {
		return
	}
	wire: [512]u8
	head_path := transmute([]u8)string("/managed")
	head_method := transmute([]u8)string("HEAD")
	n, frame_ok := h2m_client_headers(wire[:], 1, http2.FLAG_END_HEADERS | http2.FLAG_END_STREAM, 0, head_method, 0, head_path, 6, nil)
	testing.expect_value(t, frame_ok, true)

	if !frame_ok {
		return
	}
	turn, turn_ok := h2m_feed_turn(t, &f, wire[:n])

	if !turn_ok {
		return
	}

	if !h2m_dispatch(t, &f, turn.events, 2_000_000_000) {
		return
	}
	_, _ = h2m_poll_one(t, &f.host_queue)

	begin := h2m_response_cmd(.Response_Begin, 1)
	begin.status = 200
	begin.reason_length = 2
	begin.reason[0] = 'O'
	begin.reason[1] = 'K'
	execute_h2_host_command(&f.worker, begin, 2_000_000_000)
	_, gone := h2_find_record(h2m_slot(&f), 1)
	testing.expect_value(t, gone, false)
}
