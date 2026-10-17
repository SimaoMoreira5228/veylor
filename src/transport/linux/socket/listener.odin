#+build linux
package socket

import "core:sys/linux"
import "veylor:transport"

Result :: enum {
	Ready,
	Would_Block,
	Interrupted,
	Descriptor_Limit,
	Invalid,
	Error,
}

Accept_Batch_Result :: enum {
	Drained,
	Budget_Exhausted,
	Storage_Full,
	Descriptor_Limit,
	Invalid,
	Error,
}

Listener :: struct {
	fd:            linux.Fd,
	local_address: transport.Peer_Address,
}

Accepted_Connection :: struct {
	fd:   linux.Fd,
	peer: transport.Peer_Address,
}

Accept_Batch :: struct {
	connections: []Accepted_Connection,
	attempts:    int,
	result:      Accept_Batch_Result,
	error_code:  i32,
}

listen_ipv4 :: proc(
	address: [4]u8,
	port: u16,
	backlog: i32,
	reuse_port := false,
) -> (
	listener: Listener,
	result: Result,
	error_code: i32,
) {
	if backlog <= 0 {
		return {}, .Invalid, 0
	}

	socket_flags: linux.Socket_FD_Flags = {.NONBLOCK, .CLOEXEC}
	fd, socket_error := linux.socket(.INET, .STREAM, socket_flags, .TCP)

	if socket_error != .NONE {
		return {}, .Error, i32(socket_error)
	}

	reuse_address := i32(1)
	address_option_error := linux.setsockopt(fd, .SOCKET, .REUSEADDR, &reuse_address)
	if address_option_error != .NONE {
		linux.close(fd)
		return {}, .Error, i32(address_option_error)
	}

	if reuse_port {
		reuse_port_value := i32(1)
		port_option_error := linux.setsockopt(fd, .SOCKET, .REUSEPORT, &reuse_port_value)
		if port_option_error != .NONE {
			linux.close(fd)
			return {}, .Error, i32(port_option_error)
		}
	}

	local := linux.Sock_Addr_In {
		sin_family = .INET,
		sin_port   = u16be(port),
		sin_addr   = address,
	}

	bind_error := linux.bind(fd, &local)

	if bind_error != .NONE {
		linux.close(fd)
		return {}, .Error, i32(bind_error)
	}

	listen_error := linux.listen(fd, backlog)

	if listen_error != .NONE {
		linux.close(fd)
		return {}, .Error, i32(listen_error)
	}

	bound: linux.Sock_Addr_Any
	name_error := linux.getsockname(fd, &bound)

	if name_error != .NONE {
		linux.close(fd)
		return {}, .Error, i32(name_error)
	}

	return Listener{fd = fd, local_address = Peer_Address(bound.ipv4)}, .Ready, 0
}

accept_one :: proc(
	listener: Listener,
) -> (
	connection: Accepted_Connection,
	result: Result,
	error_code: i32,
) {
	if listener.fd < 0 {
		return {}, .Invalid, 0
	}

	peer: linux.Sock_Addr_In
	socket_flags: linux.Socket_FD_Flags = {.NONBLOCK, .CLOEXEC}
	fd, accept_error := linux.accept(listener.fd, &peer, socket_flags)
	result = accept_error_result(accept_error)

	if result == .Ready {
		return Accepted_Connection{fd = fd, peer = Peer_Address(peer)}, .Ready, 0
	}

	if result == .Would_Block || result == .Interrupted {
		return {}, result, 0
	}

	return {}, result, i32(accept_error)
}

accept_error_result :: proc(err: linux.Errno) -> Result {
	if err == .NONE {
		return .Ready
	}

	if err == .EAGAIN {
		return .Would_Block
	}

	if err == .EINTR {
		return .Interrupted
	}

	if err == .EMFILE || err == .ENFILE {
		return .Descriptor_Limit
	}

	return .Error
}

accept_ready :: proc(
	listener: Listener,
	storage: []Accepted_Connection,
	max_attempts: int,
) -> Accept_Batch {
	if listener.fd < 0 || max_attempts <= 0 {
		return Accept_Batch{result = .Invalid}
	}

	if len(storage) == 0 {
		return Accept_Batch{result = .Storage_Full}
	}

	accepted_count := 0
	attempts := 0
	for attempts < max_attempts && accepted_count < len(storage) {
		connection, result, error_code := accept_one(listener)
		attempts += 1

		switch result {
		case .Ready:
			storage[accepted_count] = connection
			accepted_count += 1

		case .Would_Block:
			return Accept_Batch {
				connections = storage[:accepted_count],
				attempts = attempts,
				result = .Drained,
			}

		case .Interrupted:
			continue

		case .Descriptor_Limit:
			return Accept_Batch {
				connections = storage[:accepted_count],
				attempts = attempts,
				result = .Descriptor_Limit,
				error_code = error_code,
			}

		case .Invalid:
			return Accept_Batch {
				connections = storage[:accepted_count],
				attempts = attempts,
				result = .Invalid,
			}

		case .Error:
			return Accept_Batch {
				connections = storage[:accepted_count],
				attempts = attempts,
				result = .Error,
				error_code = error_code,
			}
		}
	}

	result := Accept_Batch_Result.Budget_Exhausted

	if accepted_count == len(storage) {
		result = .Storage_Full
	}
	return Accept_Batch {
		connections = storage[:accepted_count],
		attempts = attempts,
		result = result,
	}
}

close_listener :: proc(listener: ^Listener) -> (result: Result, error_code: i32) {
	if listener.fd < 0 {
		return .Invalid, 0
	}

	err := linux.close(listener.fd)

	if err != .NONE {
		return .Error, i32(err)
	}

	listener.fd = -1
	return .Ready, 0
}

close_connection :: proc(connection: ^Accepted_Connection) -> (result: Result, error_code: i32) {
	if connection.fd < 0 {
		return .Invalid, 0
	}

	err := linux.close(connection.fd)

	if err != .NONE {
		return .Error, i32(err)
	}

	connection.fd = -1
	return .Ready, 0
}

Peer_Address :: proc(addr: linux.Sock_Addr_In) -> transport.Peer_Address {
	address: [16]u8
	address[0] = addr.sin_addr[0]
	address[1] = addr.sin_addr[1]
	address[2] = addr.sin_addr[2]
	address[3] = addr.sin_addr[3]

	return transport.Peer_Address{family = .IPv4, address = address, port = u16(addr.sin_port)}
}
