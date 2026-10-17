#+build linux
package linux_runtime

import "core:sys/linux"

Static_File :: struct {
	fd:   linux.Fd,
	size: u64,
}

STATIC_FILE_MAX_SIZE :: u64(0x7fff_ffff_ffff_ffff)

Static_File_Root :: struct {
	fd:           linux.Fd,
	mount_prefix: []u8,
}

Static_File_Open_Result :: enum {
	Opened,
	Invalid_Path,
	Not_Found,
	Not_Regular,
	Rejected,
	System_Error,
}

Static_File_Write_Result :: enum {
	Progress,
	Would_Block,
	Interrupted,
	Complete,
	Truncated,
	Invalid,
	Error,
}

Static_File_Write_Turn :: struct {
	bytes_written: int,
	error_code:    i32,
	result:        Static_File_Write_Result,
}

init_static_file_root :: proc(root: ^Static_File_Root, fd: linux.Fd, mount_prefix: []u8) -> bool {
	if root == nil || fd < 0 || !valid_static_file_mount_prefix(mount_prefix) {
		return false
	}

	stat: linux.Statx
	stat_error := linux.statx(fd, "", {.EMPTY_PATH}, {.TYPE}, &stat)
	if stat_error != .NONE || stat.mode & linux.S_IFMT != linux.S_IFDIR {
		return false
	}

	root^ = Static_File_Root {
		fd           = fd,
		mount_prefix = mount_prefix,
	}
	return true
}

valid_static_file_root :: proc(root: ^Static_File_Root) -> bool {
	return root != nil && root.fd >= 0 && valid_static_file_mount_prefix(root.mount_prefix)
}

open_static_file_beneath :: proc(
	root_fd: linux.Fd,
	path: []u8,
) -> (
	file: Static_File,
	result: Static_File_Open_Result,
) {
	if root_fd < 0 || len(path) == 0 || path[0] == '/' {
		return Static_File{fd = -1}, .Invalid_Path
	}

	current_fd := root_fd
	owns_current_fd := false
	component_storage: [256]u8
	path_offset := 0
	for path_offset < len(path) {
		component_start := path_offset
		for path_offset < len(path) && path[path_offset] != '/' {
			path_offset += 1
		}

		component_length, valid_component := decode_static_file_component(
			path[component_start:path_offset],
			component_storage[:],
		)
		if !valid_component {
			if owns_current_fd {
				linux.close(current_fd)
			}
			return Static_File{fd = -1}, .Invalid_Path
		}

		component_storage[component_length] = 0
		component_path := cast(cstring)raw_data(component_storage[:component_length + 1])
		is_final_component := path_offset == len(path)

		if !is_final_component {
			path_offset += 1
		}
		flags: linux.Open_Flags = {.CLOEXEC, .NOFOLLOW}
		if is_final_component {
			flags += {.NONBLOCK}
		} else {
			flags += {.DIRECTORY}
		}

		opened_fd, open_error := linux.openat(current_fd, component_path, flags)
		if owns_current_fd {
			linux.close(current_fd)
			owns_current_fd = false
		}

		if open_error != .NONE {
			return Static_File{fd = -1}, static_file_open_error(open_error)
		}
		if !is_final_component {
			current_fd = opened_fd
			owns_current_fd = true
			continue
		}

		stat: linux.Statx
		stat_error := linux.statx(opened_fd, "", {.EMPTY_PATH}, {.TYPE, .SIZE}, &stat)
		if stat_error != .NONE {
			linux.close(opened_fd)
			return Static_File{fd = -1}, .System_Error
		}

		if stat.mode & linux.S_IFMT != linux.S_IFREG {
			linux.close(opened_fd)
			return Static_File{fd = -1}, .Not_Regular
		}

		if stat.size > STATIC_FILE_MAX_SIZE {
			linux.close(opened_fd)
			return Static_File{fd = -1}, .System_Error
		}

		return Static_File{fd = opened_fd, size = stat.size}, .Opened
	}
	if owns_current_fd {
		linux.close(current_fd)
	}

	return Static_File{fd = -1}, .Invalid_Path
}

close_static_file :: proc(file: ^Static_File) -> bool {
	if file == nil || file.fd < 0 {
		return false
	}
	close_error := linux.close(file.fd)
	file.fd = -1
	return close_error == .NONE
}

write_static_file_turn :: proc(
	file: ^Static_File,
	socket_fd: linux.Fd,
	offset: ^i64,
	remaining: ^u64,
	byte_budget: int,
) -> Static_File_Write_Turn {
	if file == nil ||
	   file.fd < 0 ||
	   socket_fd < 0 ||
	   offset == nil ||
	   remaining == nil ||
	   offset^ < 0 ||
	   u64(offset^) > file.size ||
	   remaining^ > file.size - u64(offset^) ||
	   byte_budget <= 0 {
		return Static_File_Write_Turn{result = .Invalid}
	}

	if remaining^ == 0 {
		return Static_File_Write_Turn{result = .Complete}
	}

	requested_bytes := min(u64(byte_budget), remaining^)
	updated_offset := offset^

	written, write_error := send_static_file_without_sigpipe(
		socket_fd,
		file.fd,
		&updated_offset,
		uint(requested_bytes),
	)
	if write_error == .EAGAIN {
		return Static_File_Write_Turn{result = .Would_Block}
	}
	if write_error == .EINTR {
		return Static_File_Write_Turn{result = .Interrupted}
	}
	if write_error != .NONE {
		return Static_File_Write_Turn{error_code = i32(write_error), result = .Error}
	}

	if written == 0 {
		return Static_File_Write_Turn{result = .Truncated}
	}

	if written < 0 || u64(written) > requested_bytes || updated_offset < offset^ {
		return Static_File_Write_Turn{result = .Invalid}
	}
	expected_offset := offset^ + i64(written)
	if updated_offset != expected_offset {
		return Static_File_Write_Turn{result = .Invalid}
	}
	offset^ = updated_offset
	remaining^ -= u64(written)
	result := Static_File_Write_Result.Progress
	if remaining^ == 0 {
		result = .Complete
	}
	return Static_File_Write_Turn{bytes_written = written, result = result}
}

send_static_file_without_sigpipe :: proc(
	socket_fd: linux.Fd,
	file_fd: linux.Fd,
	offset: ^i64,
	count: uint,
) -> (
	written: int,
	result: linux.Errno,
) {
	// NOTE: sendfile has no MSG_NOSIGNAL flag.
	sigpipe_set: linux.Sig_Set
	sigpipe_bit := uint(1) << uint(int(linux.Signal.SIGPIPE) - 1)
	sigpipe_set[0] = sigpipe_bit
	previous_mask: linux.Sig_Set
	mask_error := linux.rt_sigprocmask(.SIG_BLOCK, &sigpipe_set, &previous_mask)
	if mask_error != .NONE {
		return -1, mask_error
	}

	pending_before: linux.Sig_Set
	pending_error := linux.rt_sigpending(&pending_before)
	if pending_error != .NONE {
		restore_error := linux.rt_sigprocmask(.SIG_SETMASK, &previous_mask, nil)
		if restore_error != .NONE {
			return -1, restore_error
		}
		return -1, pending_error
	}

	sent_bytes, write_error := linux.sendfile(socket_fd, file_fd, offset, count)
	if write_error == .EPIPE && (pending_before[0] & sigpipe_bit) == 0 {
		zero_timeout: linux.Time_Spec
		for {
			_, wait_error := linux.rt_sigtimedwait(&sigpipe_set, nil, &zero_timeout)
			if wait_error == .EINTR {
				continue
			}
			if wait_error != .NONE && wait_error != .EAGAIN {
				write_error = wait_error
			}
			break
		}
	}

	restore_error := linux.rt_sigprocmask(.SIG_SETMASK, &previous_mask, nil)
	if restore_error != .NONE {
		return -1, restore_error
	}
	return sent_bytes, write_error
}

valid_static_file_component :: proc(component: []u8) -> bool {
	if len(component) == 0 {
		return false
	}
	if len(component) == 1 && component[0] == '.' {
		return false
	}
	if len(component) == 2 && component[0] == '.' && component[1] == '.' {
		return false
	}
	for byte in component {
		if byte == 0 || byte == '\\' {
			return false
		}
	}
	return true
}

decode_static_file_component :: proc(encoded: []u8, output: []u8) -> (length: int, valid: bool) {
	if len(encoded) == 0 || len(output) < 255 {
		return 0, false
	}
	for i := 0; i < len(encoded); {
		byte := encoded[i]
		if byte == '%' {
			if len(encoded) - i < 3 {
				return 0, false
			}
			high, high_valid := static_file_hex_value(encoded[i + 1])
			low, low_valid := static_file_hex_value(encoded[i + 2])
			if !high_valid || !low_valid {
				return 0, false
			}
			byte = high << 4 | low
			i += 3
		} else {
			i += 1
		}

		if length >= 255 || byte == '/' || byte == '\\' || byte == 0 {
			return 0, false
		}
		output[length] = byte
		length += 1
	}
	return length, valid_static_file_component(output[:length])
}

static_file_hex_value :: proc(byte: u8) -> (value: u8, valid: bool) {
	if byte >= '0' && byte <= '9' {
		return byte - '0', true
	}
	if byte >= 'A' && byte <= 'F' {
		return byte - 'A' + 10, true
	}
	if byte >= 'a' && byte <= 'f' {
		return byte - 'a' + 10, true
	}
	return 0, false
}

valid_static_file_mount_prefix :: proc(prefix: []u8) -> bool {
	if len(prefix) == 0 || prefix[0] != '/' || prefix[len(prefix) - 1] != '/' {
		return false
	}
	if len(prefix) == 1 {
		return true
	}

	component_start := 1
	for i := 1; i < len(prefix); i += 1 {
		if prefix[i] != '/' {
			if prefix[i] == '?' || prefix[i] == '#' || prefix[i] == '%' {
				return false
			}
			continue
		}
		if !valid_static_file_component(prefix[component_start:i]) {
			return false
		}
		component_start = i + 1
	}
	return component_start == len(prefix)
}

static_file_open_error :: proc(err: linux.Errno) -> Static_File_Open_Result {
	if err == .ENOENT || err == .ENOTDIR {
		return .Not_Found
	}
	if err == .ELOOP || err == .EACCES || err == .EPERM {
		return .Rejected
	}
	return .System_Error
}
