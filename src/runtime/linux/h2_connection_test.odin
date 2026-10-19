#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http2 "veylor:http2"

h2t_magic_bytes :: proc() -> []u8 {
	magic_string := http2.CLIENT_CONNECTION_PREFACE

	return transmute([]u8)magic_string
}

h2t_socketpair :: proc(t: ^testing.T) -> (server_fd, peer_fd: linux.Fd, ok: bool) {
	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)

	if socket_error != .NONE {
		return -1, -1, false
	}

	return sockets[0], sockets[1], true
}

h2t_write_all :: proc(t: ^testing.T, fd: linux.Fd, data: []u8) -> bool {
	written := 0

	for written < len(data) {
		n, write_error := linux.write(fd, data[written:])

		if write_error != .NONE {
			testing.expect_value(t, write_error, linux.Errno.NONE)

			return false
		}

		if n <= 0 {
			testing.expect(t, false)

			return false
		}
		written += n
	}

	return true
}

h2t_read_exact :: proc(t: ^testing.T, fd: linux.Fd, data: []u8) -> bool {
	read_total := 0

	for read_total < len(data) {
		n, read_error := linux.read(fd, data[read_total:])

		if read_error != .NONE {
			testing.expect_value(t, read_error, linux.Errno.NONE)

			return false
		}

		if n <= 0 {
			testing.expect(t, false)

			return false
		}
		read_total += n
	}

	return true
}

h2t_run_until_quiet :: proc(state: ^H2_Worker_Connection, budget: int) -> H2_Worker_Turn {
	turn := H2_Worker_Turn{result = .Open}

	for i in 0 ..< 32 {
		turn = h2_worker_connection_turn(state, budget)

		if turn.result != .Open {
			return turn
		}

		if turn.bytes_read == 0 && turn.bytes_written == 0 {
			return turn
		}
	}

	return turn
}

h2t_client_settings_frame :: proc() -> (frame: [9]u8, ok: bool) {
	header := http2.Frame_Header {
		length    = 0,
		kind      = .Settings,
		stream_id = 0,
	}

	if !http2.encode_frame_header(header, frame[:]) {
		return {}, false
	}

	return frame, true
}

h2t_drain_server_greeting :: proc(t: ^testing.T, peer_fd: linux.Fd) -> bool {
	header_buf: [9]u8

	if !h2t_read_exact(t, peer_fd, header_buf[:]) {
		return false
	}
	header, hdr_ok, too_large := http2.h2_parse_header(header_buf[:], 1 << 24)
	testing.expect(t, hdr_ok && !too_large)
	testing.expect_value(t, header.kind, http2.Frame_Type.Settings)
	testing.expect(t, header.flags & http2.SETTINGS_ACK == 0)

	if !hdr_ok || header.kind != .Settings {
		return false
	}
	payload := make([]u8, header.length)
	defer delete(payload)

	if !h2t_read_exact(t, peer_fd, payload) {
		return false
	}

	if !h2t_read_exact(t, peer_fd, header_buf[:]) {
		return false
	}
	ack, ack_ok, ack_large := http2.h2_parse_header(header_buf[:], 1 << 24)
	testing.expect(t, ack_ok && !ack_large)
	testing.expect_value(t, ack.kind, http2.Frame_Type.Settings)
	testing.expect_value(t, ack.flags & http2.SETTINGS_ACK != 0, true)

	return ack_ok && ack.kind == .Settings && ack.flags & http2.SETTINGS_ACK != 0
}

h2t_handshake :: proc(t: ^testing.T, peer_fd: linux.Fd) -> bool {
	magic := h2t_magic_bytes()

	if !h2t_write_all(t, peer_fd, magic) {
		return false
	}
	settings, settings_ok := h2t_client_settings_frame()
	testing.expect(t, settings_ok)

	if !settings_ok {
		return false
	}

	return h2t_write_all(t, peer_fd, settings[:])
}

@(test)
test_h2_worker_init_rejects_bad_arguments :: proc(t: ^testing.T) {
	testing.expect(t, !h2_worker_connection_init(nil, 1, {}, {}))
	state: H2_Worker_Connection
	testing.expect(t, !h2_worker_connection_init(&state, -1, {}, {}))
	testing.expect_value(
		t,
		h2_worker_connection_turn(nil, 64 * 1024).result,
		H2_Worker_Turn_Result.Invalid,
	)
	testing.expect_value(
		t,
		h2_worker_connection_turn(&state, 0).result,
		H2_Worker_Turn_Result.Invalid,
	)
}

@(test)
test_h2_worker_handshake_exchanges_settings :: proc(t: ^testing.T) {
	server_fd, peer_fd, pair_ok := h2t_socketpair(t)

	if !pair_ok {
		return
	}
	defer linux.close(server_fd)
	defer linux.close(peer_fd)

	state: H2_Worker_Connection
	testing.expect(t, h2_worker_connection_init(&state, server_fd, {}, {}))

	if !state.active {
		return
	}

	if !h2t_handshake(t, peer_fd) {
		return
	}
	turn := h2t_run_until_quiet(&state, 64 * 1024)
	testing.expect_value(t, turn.result, H2_Worker_Turn_Result.Open)
	testing.expect_value(t, state.conn.preface_done, true)
	testing.expect(t, h2t_drain_server_greeting(t, peer_fd))
}

@(test)
test_h2_worker_split_preface_resumes_across_turns :: proc(t: ^testing.T) {
	server_fd, peer_fd, pair_ok := h2t_socketpair(t)

	if !pair_ok {
		return
	}
	defer linux.close(server_fd)
	defer linux.close(peer_fd)

	state: H2_Worker_Connection
	testing.expect(t, h2_worker_connection_init(&state, server_fd, {}, {}))

	if !state.active {
		return
	}
	magic := h2t_magic_bytes()
	half := len(magic) / 2

	if !h2t_write_all(t, peer_fd, magic[:half]) {
		return
	}
	first := h2_worker_connection_turn(&state, 64 * 1024)
	testing.expect_value(t, first.result, H2_Worker_Turn_Result.Open)
	testing.expect_value(t, state.conn.preface_done, false)

	if !h2t_write_all(t, peer_fd, magic[half:]) {
		return
	}
	settings, settings_ok := h2t_client_settings_frame()
	testing.expect(t, settings_ok)

	if !settings_ok {
		return
	}

	if !h2t_write_all(t, peer_fd, settings[:]) {
		return
	}
	second := h2t_run_until_quiet(&state, 64 * 1024)
	testing.expect_value(t, second.result, H2_Worker_Turn_Result.Open)
	testing.expect_value(t, state.conn.preface_done, true)
	testing.expect(t, h2t_drain_server_greeting(t, peer_fd))
}

@(test)
test_h2_worker_ping_ack_loopback :: proc(t: ^testing.T) {
	server_fd, peer_fd, pair_ok := h2t_socketpair(t)

	if !pair_ok {
		return
	}
	defer linux.close(server_fd)
	defer linux.close(peer_fd)

	state: H2_Worker_Connection
	testing.expect(t, h2_worker_connection_init(&state, server_fd, {}, {}))

	if !state.active {
		return
	}

	if !h2t_handshake(t, peer_fd) {
		return
	}
	_ = h2t_run_until_quiet(&state, 64 * 1024)
	testing.expect(t, h2t_drain_server_greeting(t, peer_fd))

	opaque := [8]u8{1, 2, 3, 4, 5, 6, 7, 8}
	frame: [9 + 8]u8
	header := http2.Frame_Header {
		length    = 8,
		kind      = .Ping,
		stream_id = 0,
	}
	testing.expect(t, http2.encode_frame_header(header, frame[:9]))
	testing.expect(t, http2.encode_ping_payload(opaque, frame[9:]))

	if !h2t_write_all(t, peer_fd, frame[:]) {
		return
	}
	turn := h2t_run_until_quiet(&state, 64 * 1024)
	testing.expect_value(t, turn.result, H2_Worker_Turn_Result.Open)

	header_buf: [9]u8

	if !h2t_read_exact(t, peer_fd, header_buf[:]) {
		return
	}
	ack_header, hdr_ok, too_large := http2.h2_parse_header(header_buf[:], 1 << 24)
	testing.expect(t, hdr_ok && !too_large)
	testing.expect_value(t, ack_header.kind, http2.Frame_Type.Ping)
	testing.expect_value(t, ack_header.flags & http2.PING_ACK != 0, true)

	payload: [8]u8

	if !h2t_read_exact(t, peer_fd, payload[:]) {
		return
	}
	got, dec_ok := http2.decode_ping_payload(ack_header, payload[:])
	testing.expect(t, dec_ok)
	testing.expect_value(t, got, opaque)
}

h2t_read_frame :: proc(
	t: ^testing.T,
	peer_fd: linux.Fd,
) -> (
	header: http2.Frame_Header,
	payload: [4096]u8,
	length: int,
	ok: bool,
) {
	header_buf: [9]u8

	if !h2t_read_exact(t, peer_fd, header_buf[:]) {
		return {}, {}, 0, false
	}
	hdr, hdr_ok, too_large := http2.h2_parse_header(header_buf[:], 1 << 24)

	if !hdr_ok || too_large {
		testing.expect(t, hdr_ok && !too_large)

		return {}, {}, 0, false
	}

	if int(hdr.length) > len(payload) {
		testing.expect(t, false)

		return {}, {}, 0, false
	}

	if !h2t_read_exact(t, peer_fd, payload[:hdr.length]) {
		return {}, {}, 0, false
	}

	return hdr, payload, int(hdr.length), true
}

@(test)
test_h2_worker_bad_magic_sends_goaway_and_closes :: proc(t: ^testing.T) {
	server_fd, peer_fd, pair_ok := h2t_socketpair(t)

	if !pair_ok {
		return
	}
	defer linux.close(server_fd)
	defer linux.close(peer_fd)

	state: H2_Worker_Connection
	testing.expect(t, h2_worker_connection_init(&state, server_fd, {}, {}))

	if !state.active {
		return
	}
	junk_string := string("GET / HTTP/1.1\r\nHost: x\r\n\r\n0123456789")
	junk := transmute([]u8)junk_string

	if !h2t_write_all(t, peer_fd, junk) {
		return
	}
	turn := h2t_run_until_quiet(&state, 64 * 1024)
	testing.expect_value(t, turn.result, H2_Worker_Turn_Result.Closed)
	testing.expect_value(t, state.close_reason, H2_Worker_Close_Reason.Connection_Error)

	greeting, _, _, greeting_ok := h2t_read_frame(t, peer_fd)
	testing.expect(t, greeting_ok)
	testing.expect_value(t, greeting.kind, http2.Frame_Type.Settings)

	if !greeting_ok {
		return
	}
	header, payload, _, read_ok := h2t_read_frame(t, peer_fd)
	testing.expect(t, read_ok)

	if !read_ok {
		return
	}
	testing.expect_value(t, header.kind, http2.Frame_Type.GoAway)
	last, code, _, dec_ok := http2.decode_goaway_payload(header, payload[:http2.GOAWAY_FIXED_LEN])
	testing.expect(t, dec_ok)
	testing.expect_value(t, code, http2.Error_Code.Protocol_Error)
	testing.expect_value(t, last, u32(0))
}

@(test)
test_h2_worker_shutdown_sends_goaway_and_closes :: proc(t: ^testing.T) {
	server_fd, peer_fd, pair_ok := h2t_socketpair(t)

	if !pair_ok {
		return
	}
	defer linux.close(server_fd)
	defer linux.close(peer_fd)

	state: H2_Worker_Connection
	testing.expect(t, h2_worker_connection_init(&state, server_fd, {}, {}))

	if !state.active {
		return
	}
	testing.expect(t, h2_worker_connection_begin_shutdown(&state))

	turn := h2t_run_until_quiet(&state, 64 * 1024)
	testing.expect_value(t, turn.result, H2_Worker_Turn_Result.Closed)
	testing.expect_value(t, state.close_reason, H2_Worker_Close_Reason.Shutdown)

	greeting, _, _, greeting_ok := h2t_read_frame(t, peer_fd)
	testing.expect(t, greeting_ok)
	testing.expect_value(t, greeting.kind, http2.Frame_Type.Settings)

	if !greeting_ok {
		return
	}
	header, _, _, read_ok := h2t_read_frame(t, peer_fd)
	testing.expect(t, read_ok)

	if !read_ok {
		return
	}
	testing.expect_value(t, header.kind, http2.Frame_Type.GoAway)
}
