#+build linux
#+test
package linux_runtime

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"

@(test)
test_static_file_open_is_beneath_root_and_rejects_symlinks :: proc(t: ^testing.T) {
	root_fd, root_error := linux.open("/", {.CLOEXEC, .DIRECTORY})
	testing.expect_value(t, root_error, linux.Errno.NONE)
	if root_error != .NONE {
		return
	}
	defer linux.close(root_fd)

	regular_path := "etc/passwd"
	file, open_result := open_static_file_beneath(root_fd, transmute([]u8)regular_path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Opened)
	if open_result == .Opened {
		testing.expect_value(t, file.size > 0, true)
		testing.expect_value(t, close_static_file(&file), true)
		testing.expect_value(t, file.fd, linux.Fd(-1))
		testing.expect_value(t, close_static_file(&file), false)
	}

	encoded_path := "etc/pass%77d"
	file, open_result = open_static_file_beneath(root_fd, transmute([]u8)encoded_path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Opened)
	if open_result == .Opened {
		testing.expect_value(t, close_static_file(&file), true)
	}

	traversal_path := "../etc/passwd"
	_, open_result = open_static_file_beneath(root_fd, transmute([]u8)traversal_path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Invalid_Path)
	encoded_traversal_path := "%2e%2e/etc/passwd"
	_, open_result = open_static_file_beneath(root_fd, transmute([]u8)encoded_traversal_path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Invalid_Path)
	encoded_separator_path := "etc%2fpasswd"
	_, open_result = open_static_file_beneath(root_fd, transmute([]u8)encoded_separator_path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Invalid_Path)
	absolute_path := "/etc/passwd"
	_, open_result = open_static_file_beneath(root_fd, transmute([]u8)absolute_path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Invalid_Path)
	empty_component_path := "etc//passwd"
	_, open_result = open_static_file_beneath(root_fd, transmute([]u8)empty_component_path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Invalid_Path)

	directory_path := "etc"
	_, open_result = open_static_file_beneath(root_fd, transmute([]u8)directory_path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Not_Regular)

	proc_fd, proc_error := linux.open("/proc", {.CLOEXEC, .DIRECTORY})
	testing.expect_value(t, proc_error, linux.Errno.NONE)
	if proc_error != .NONE {
		return
	}
	defer linux.close(proc_fd)

	symlink_path := "self"
	_, open_result = open_static_file_beneath(proc_fd, transmute([]u8)symlink_path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Rejected)
	nested_symlink_path := "self/status"
	_, open_result = open_static_file_beneath(proc_fd, transmute([]u8)nested_symlink_path)
	testing.expect_value(t, open_result != Static_File_Open_Result.Opened, true)
}

@(test)
test_static_file_sendfile_honors_byte_budget_and_offset :: proc(t: ^testing.T) {
	root_fd, root_error := linux.open("/", {.CLOEXEC, .DIRECTORY})
	testing.expect_value(t, root_error, linux.Errno.NONE)
	if root_error != .NONE {
		return
	}
	defer linux.close(root_fd)

	path := "etc/passwd"
	expected_file, open_result := open_static_file_beneath(root_fd, transmute([]u8)path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Opened)
	if open_result != .Opened {
		return
	}

	expected_bytes: [4096]u8
	expected_length, read_error := linux.read(expected_file.fd, expected_bytes[:])
	testing.expect_value(t, read_error, linux.Errno.NONE)
	testing.expect_value(t, expected_length, int(expected_file.size))
	testing.expect_value(t, close_static_file(&expected_file), true)
	if expected_length < 0 || expected_length > len(expected_bytes) {
		return
	}
	file, write_open_result := open_static_file_beneath(root_fd, transmute([]u8)path)
	testing.expect_value(t, write_open_result, Static_File_Open_Result.Opened)
	if write_open_result != .Opened {
		return
	}

	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		close_static_file(&file)
		return
	}
	defer linux.close(sockets[0])
	defer linux.close(sockets[1])

	actual_bytes: [4096]u8
	offset := i64(0)
	remaining := file.size
	written_total := 0
	for remaining > 0 {
		turn := write_static_file_turn(&file, sockets[0], &offset, &remaining, 64)
		testing.expect_value(t, turn.bytes_written <= 64, true)
		if turn.result != .Progress && turn.result != .Complete {
			testing.expect_value(t, turn.result, Static_File_Write_Result.Progress)
			break
		}
		read_total := 0
		for read_total < turn.bytes_written {
			read_length, peer_error := linux.read(
				sockets[1],
				actual_bytes[written_total + read_total:written_total + turn.bytes_written],
			)
			testing.expect_value(t, peer_error, linux.Errno.NONE)
			if peer_error != .NONE || read_length <= 0 {
				break
			}
			read_total += read_length
		}
		testing.expect_value(t, read_total, turn.bytes_written)
		written_total += turn.bytes_written
	}

	testing.expect_value(t, written_total, expected_length)
	testing.expect_value(t, offset, i64(expected_length))
	testing.expect_value(
		t,
		string(actual_bytes[:written_total]),
		string(expected_bytes[:expected_length]),
	)
	testing.expect_value(t, close_static_file(&file), true)
}

STATIC_FILE_TEST_LARGE_SIZE :: 128 * 1024

@(test)
test_static_file_sendfile_resumes_after_slow_peer_and_honors_byte_budget :: proc(t: ^testing.T) {
	contents: [STATIC_FILE_TEST_LARGE_SIZE]u8
	for i := 0; i < len(contents); i += 1 {
		contents[i] = u8(i % 251)
	}
	file, file_ready := create_static_file_test_file(contents[:])
	testing.expect_value(t, file_ready, true)
	if !file_ready {
		return
	}
	defer close_static_file(&file)

	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		return
	}
	defer linux.close(sockets[0])
	defer linux.close(sockets[1])

	send_buffer_size := i32(4096)
	socket_error = linux.setsockopt(sockets[0], .SOCKET, .SNDBUF, &send_buffer_size)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		return
	}
	for socket_fd in sockets {
		flags, flags_error := linux.fcntl_getfl(socket_fd, .GETFL)
		testing.expect_value(t, flags_error, linux.Errno.NONE)
		if flags_error != .NONE {
			return
		}
		flags_error = linux.fcntl_setfl(socket_fd, .SETFL, flags + {.NONBLOCK})
		testing.expect_value(t, flags_error, linux.Errno.NONE)
		if flags_error != .NONE {
			return
		}
	}

	received: [STATIC_FILE_TEST_LARGE_SIZE]u8
	received_total := 0
	offset := i64(0)
	remaining := file.size
	blocked := false
	transfer_failed := false
	for remaining > 0 && !transfer_failed {
		turn := write_static_file_turn(&file, sockets[0], &offset, &remaining, 64 * 1024)
		testing.expect_value(t, turn.bytes_written <= 64 * 1024, true)
		switch turn.result {
		case .Progress, .Complete, .Interrupted:
			continue
		case .Would_Block:
			blocked = true
			for received_total < len(received) {
				read_length, read_error := linux.read(sockets[1], received[received_total:])
				if read_error == .EINTR {
					continue
				}
				if read_error == .EAGAIN {
					break
				}
				testing.expect_value(t, read_error, linux.Errno.NONE)
				if read_error != .NONE || read_length <= 0 {
					transfer_failed = true
					break
				}
				received_total += read_length
			}
		case .Truncated, .Invalid, .Error:
			testing.expect_value(t, turn.result, Static_File_Write_Result.Progress)
			transfer_failed = true
		}
	}

	for received_total < len(received) {
		read_length, read_error := linux.read(sockets[1], received[received_total:])
		if read_error == .EINTR {
			continue
		}
		if read_error != .NONE || read_length <= 0 {
			break
		}
		received_total += read_length
	}

	testing.expect_value(t, transfer_failed, false)
	testing.expect_value(t, blocked, true)
	testing.expect_value(t, offset, i64(len(contents)))
	testing.expect_value(t, remaining, u64(0))
	testing.expect_value(t, received_total, len(contents))
	testing.expect_value(t, string(received[:]), string(contents[:]))
}

@(test)
test_static_file_sendfile_detects_truncation_after_open :: proc(t: ^testing.T) {
	contents: [64]u8
	for i := 0; i < len(contents); i += 1 {
		contents[i] = 'x'
	}
	file, file_ready := create_static_file_test_file(contents[:])
	testing.expect_value(t, file_ready, true)
	if !file_ready {
		return
	}
	defer close_static_file(&file)

	truncate_error := linux.ftruncate(file.fd, 0)
	testing.expect_value(t, truncate_error, linux.Errno.NONE)
	if truncate_error != .NONE {
		return
	}

	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		return
	}
	defer linux.close(sockets[0])
	defer linux.close(sockets[1])

	offset := i64(0)
	remaining := file.size
	turn := write_static_file_turn(&file, sockets[0], &offset, &remaining, 64)
	testing.expect_value(t, turn.result, Static_File_Write_Result.Truncated)
	testing.expect_value(t, offset, i64(0))
	testing.expect_value(t, remaining, file.size)
}

@(test)
test_static_file_response_streams_files_above_inline_limit :: proc(t: ^testing.T) {
	root_fd, root_error := linux.open("/etc", {.CLOEXEC, .DIRECTORY})
	testing.expect_value(t, root_error, linux.Errno.NONE)
	if root_error != .NONE {
		return
	}
	defer linux.close(root_fd)

	mount_prefix := "/assets/"
	file_root: Static_File_Root
	root_ready := init_static_file_root(&file_root, root_fd, transmute([]u8)mount_prefix)
	testing.expect_value(t, root_ready, true)
	if !root_ready {
		return
	}

	request := "GET /assets/services HTTP/1.1\r\nHost: example.test\r\n\r\n"
	input := transmute([]u8)request
	headers: [4]http1.Header
	parser: http1.Parser
	head, parse_result := http1.parse_request_head(&parser, input, headers[:])
	testing.expect_value(t, parse_result, http1.Parse_Result.Ready)
	if parse_result != .Ready {
		return
	}

	response: [1024]u8
	file_event := encode_static_file_response(&file_root, head, input, response[:], 0)
	testing.expect_value(t, file_event.result, Static_File_Response_Result.Encoded)
	testing.expect_value(t, file_event.file.size > STATIC_FILE_INLINE_BODY_LIMIT, true)
	testing.expect_value(t, file_event.stream_body, true)
	testing.expect_value(t, file_event.response_length > 0, true)
	if file_event.file.fd >= 0 {
		testing.expect_value(t, close_static_file(&file_event.file), true)
	}
}

create_static_file_test_file :: proc(contents: []u8) -> (file: Static_File, ready: bool) {
	if len(contents) == 0 {
		return Static_File{fd = -1}, false
	}

	file_fd, open_error := linux.memfd_create("veylor-static-file-test", {.CLOEXEC})
	if open_error != .NONE {
		return Static_File{fd = -1}, false
	}

	written_total := 0
	for written_total < len(contents) {
		written, write_error := linux.write(file_fd, contents[written_total:])
		if write_error != .NONE || written <= 0 {
			linux.close(file_fd)
			return Static_File{fd = -1}, false
		}
		written_total += written
	}

	return Static_File{fd = file_fd, size = u64(len(contents))}, true
}

@(test)
test_static_file_sendfile_suppresses_sigpipe_for_closed_peer :: proc(t: ^testing.T) {
	root_fd, root_error := linux.open("/", {.CLOEXEC, .DIRECTORY})
	testing.expect_value(t, root_error, linux.Errno.NONE)

	if root_error != .NONE {
		return
	}
	defer linux.close(root_fd)

	path := "etc/passwd"
	file, open_result := open_static_file_beneath(root_fd, transmute([]u8)path)
	testing.expect_value(t, open_result, Static_File_Open_Result.Opened)

	if open_result != .Opened {
		return
	}
	defer close_static_file(&file)

	sockets: [2]linux.Fd
	socket_error := linux.socketpair(.UNIX, .STREAM, .HOPOPT, &sockets)
	testing.expect_value(t, socket_error, linux.Errno.NONE)
	if socket_error != .NONE {
		return
	}

	defer {
		if sockets[0] >= 0 {
			linux.close(sockets[0])
		}
		if sockets[1] >= 0 {
			linux.close(sockets[1])
		}
	}
	linux.close(sockets[1])
	sockets[1] = -1

	sigpipe_set: linux.Sig_Set
	sigpipe_bit := uint(1) << uint(int(linux.Signal.SIGPIPE) - 1)
	sigpipe_set[0] = sigpipe_bit
	previous_mask: linux.Sig_Set
	mask_error := linux.rt_sigprocmask(.SIG_BLOCK, &sigpipe_set, &previous_mask)
	testing.expect_value(t, mask_error, linux.Errno.NONE)
	if mask_error != .NONE {
		return
	}

	pending_before: linux.Sig_Set
	pending_error := linux.rt_sigpending(&pending_before)
	testing.expect_value(t, pending_error, linux.Errno.NONE)
	if pending_error != .NONE {
		linux.rt_sigprocmask(.SIG_SETMASK, &previous_mask, nil)
		return
	}

	if (pending_before[0] & sigpipe_bit) != 0 {
		testing.expect_value(t, pending_before[0] & sigpipe_bit, uint(0))
		linux.rt_sigprocmask(.SIG_SETMASK, &previous_mask, nil)
		return
	}

	defer {
		pending_after: linux.Sig_Set
		if linux.rt_sigpending(&pending_after) == .NONE && (pending_after[0] & sigpipe_bit) != 0 {
			zero_timeout: linux.Time_Spec
			linux.rt_sigtimedwait(&sigpipe_set, nil, &zero_timeout)
		}
		linux.rt_sigprocmask(.SIG_SETMASK, &previous_mask, nil)
	}

	blocked_mask: linux.Sig_Set
	mask_error = linux.rt_sigprocmask(.SIG_BLOCK, nil, &blocked_mask)
	testing.expect_value(t, mask_error, linux.Errno.NONE)

	offset := i64(0)
	remaining := file.size
	turn := write_static_file_turn(&file, sockets[0], &offset, &remaining, 64)
	testing.expect_value(t, turn.result, Static_File_Write_Result.Error)
	testing.expect_value(t, turn.error_code, i32(linux.Errno.EPIPE))
	testing.expect_value(t, offset, i64(0))
	testing.expect_value(t, remaining, file.size)

	pending_after: linux.Sig_Set
	pending_error = linux.rt_sigpending(&pending_after)
	testing.expect_value(t, pending_error, linux.Errno.NONE)
	if pending_error == .NONE {
		testing.expect_value(t, pending_after[0] & sigpipe_bit, uint(0))
	}

	after_mask: linux.Sig_Set
	mask_error = linux.rt_sigprocmask(.SIG_BLOCK, nil, &after_mask)
	testing.expect_value(t, mask_error, linux.Errno.NONE)
	if mask_error == .NONE {
		for i := 0; i < len(blocked_mask); i += 1 {
			testing.expect_value(t, after_mask[i], blocked_mask[i])
		}
	}
}
