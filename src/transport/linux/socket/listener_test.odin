#+build linux
#+test
package socket

import "core:sys/linux"
import "core:testing"
import http1 "veylor:http1"
import connection "veylor:http1/connection"
import response "veylor:http1/response"
import "veylor:transport"
import epoll "veylor:transport/linux"

@(test)
test_accept_error_classifies_empty_queue_and_descriptor_exhaustion :: proc(t: ^testing.T) {
	testing.expect_value(t, accept_error_result(linux.Errno.EAGAIN), Result.Would_Block)
	testing.expect_value(t, accept_error_result(linux.Errno.EINTR), Result.Interrupted)
	testing.expect_value(t, accept_error_result(linux.Errno.EMFILE), Result.Descriptor_Limit)
	testing.expect_value(t, accept_error_result(linux.Errno.ENFILE), Result.Descriptor_Limit)
	testing.expect_value(t, accept_error_result(linux.Errno.ECONNRESET), Result.Error)
}

@(test)
test_nonblocking_ipv4_listener_accepts_and_registers_stream :: proc(t: ^testing.T) {
	loopback := [4]u8{127, 0, 0, 1}
	listener, result, error_code := listen_ipv4(loopback, 0, 8)
	testing.expect_value(t, result, Result.Ready)
	testing.expect_value(t, error_code, i32(0))
	if result != .Ready {
		return
	}
	testing.expect_value(t, listener.local_address.port > 0, true)

	poller, poller_result, poller_error := epoll.create()
	testing.expect_value(t, poller_result, epoll.Result.Ready)
	testing.expect_value(t, poller_error, i32(0))
	if poller_result != .Ready {
		close_listener(&listener)
		return
	}

	poller_result, poller_error = epoll.register(
		poller,
		listener.fd,
		100,
		epoll.Interest{readable = true},
	)
	testing.expect_value(t, poller_result, epoll.Result.Ready)
	testing.expect_value(t, poller_error, i32(0))
	if poller_result != .Ready {
		epoll.close(&poller)
		close_listener(&listener)
		return
	}

	client_flags: linux.Socket_FD_Flags = {.CLOEXEC}
	client, client_error := linux.socket(.INET, .STREAM, client_flags, .TCP)
	testing.expect_value(t, client_error, linux.Errno.NONE)
	if client_error != .NONE {
		epoll.unregister(poller, listener.fd)
		epoll.close(&poller)
		close_listener(&listener)
		return
	}

	destination := linux.Sock_Addr_In {
		sin_family = .INET,
		sin_port   = u16be(listener.local_address.port),
		sin_addr   = loopback,
	}
	connect_error := linux.connect(client, &destination)
	testing.expect_value(t, connect_error, linux.Errno.NONE)
	if connect_error != .NONE {
		epoll.unregister(poller, listener.fd)
		epoll.close(&poller)
		linux.close(client)
		close_listener(&listener)
		return
	}

	native_events: [4]linux.EPoll_Event
	ready_events: [4]transport.Readiness_Event
	batch := epoll.wait(poller, native_events[:], ready_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	testing.expect_value(t, len(batch.events), 1)
	if len(batch.events) == 1 {
		testing.expect_value(t, batch.events[0].connection_id, u64(100))
		testing.expect_value(t, batch.events[0].readable, true)
	}

	accepted: [4]Accepted_Connection
	accept_batch := accept_ready(listener, accepted[:], 1)
	testing.expect_value(t, accept_batch.result, Accept_Batch_Result.Budget_Exhausted)
	testing.expect_value(t, accept_batch.attempts, 1)
	testing.expect_value(t, len(accept_batch.connections), 1)
	if len(accept_batch.connections) != 1 {
		epoll.unregister(poller, listener.fd)
		epoll.close(&poller)
		linux.close(client)
		close_listener(&listener)
		return
	}
	accepted_stream := accepted[0]
	testing.expect_value(t, accepted_stream.peer.family, transport.Address_Family.IPv4)
	testing.expect_value(t, string(accepted_stream.peer.address[:4]), string(loopback[:]))
	testing.expect_value(t, accepted_stream.peer.port > 0, true)

	accept_batch = accept_ready(listener, accepted[1:], 2)
	testing.expect_value(t, accept_batch.result, Accept_Batch_Result.Drained)
	testing.expect_value(t, accept_batch.attempts, 1)
	testing.expect_value(t, len(accept_batch.connections), 0)

	epoll.unregister(poller, listener.fd)

	poller_result, poller_error = epoll.register(
		poller,
		accepted_stream.fd,
		101,
		epoll.Interest{readable = true},
	)
	testing.expect_value(t, poller_result, epoll.Result.Ready)
	testing.expect_value(t, poller_error, i32(0))
	if poller_result != .Ready {
		epoll.close(&poller)
		close_connection(&accepted_stream)
		linux.close(client)
		close_listener(&listener)
		return
	}

	message := "GET /health HTTP/1.1\r\nHost: example.com\r\n\r\n"
	write_result := write_stream(client, transmute([]u8)message)
	testing.expect_value(t, write_result.kind, transport.IO_Result_Kind.Progress)
	testing.expect_value(t, write_result.transferred_bytes, len(message))
	if write_result.kind != .Progress || write_result.transferred_bytes != len(message) {
		epoll.unregister(poller, accepted_stream.fd)
		epoll.close(&poller)
		close_connection(&accepted_stream)
		linux.close(client)
		close_listener(&listener)
		return
	}

	batch = epoll.wait(poller, native_events[:], ready_events[:], 100)
	testing.expect_value(t, batch.result, epoll.Result.Ready)
	if len(batch.events) == 1 {
		testing.expect_value(t, batch.events[0].connection_id, u64(101))
		testing.expect_value(t, batch.events[0].readable, true)
	}

	request_input: [256]u8
	request_headers: [8]http1.Header
	head_driver := connection.begin_connection_head(101)
	input_length := 0
	read_operation_id := u64(0)
	head_event: connection.Connection_Head_Event
	for attempt in 0 ..< 8 {
		read_result := read_stream(accepted_stream.fd, request_input[input_length:])
		read_operation_id += 1
		if read_result.kind == .Progress {
			input_length += read_result.transferred_bytes
		}
		completion := transport.IO_Completion {
			connection_id = 101,
			operation_id  = read_operation_id,
			operation     = .Read,
			result        = read_result,
		}
		head_event = connection.consume_read_completion(
			&head_driver,
			completion,
			request_input[:input_length],
			request_headers[:],
		)
		if head_event.result == .Ready ||
		   head_event.result == .Invalid ||
		   head_event.result == .Too_Large ||
		   head_event.result == .Header_Capacity ||
		   head_event.result == .Expectation_Failed ||
		   head_event.result == .Buffer_Changed ||
		   head_event.result == .Peer_Closed ||
		   head_event.result == .Transport_Error {
			break
		}
		if head_event.result == .Would_Block || head_event.result == .Interrupted {
			batch = epoll.wait(poller, native_events[:], ready_events[:], 100)
		}
	}
	testing.expect_value(t, head_event.result, connection.Connection_Head_Result.Ready)
	if head_event.result != .Ready {
		epoll.unregister(poller, accepted_stream.fd)
		epoll.close(&poller)
		close_connection(&accepted_stream)
		linux.close(client)
		close_listener(&listener)
		return
	}
	testing.expect_value(
		t,
		string(
			request_input[head_event.head.target.offset:head_event.head.target.offset +
			head_event.head.target.length],
		),
		"/health",
	)

	method := request_input[head_event.head.method.offset:head_event.head.method.offset +
	head_event.head.method.length]
	reason := "OK"
	encoded_head: [128]u8
	encoded := response.encode_response_head(
		method,
		200,
		transmute([]u8)reason,
		nil,
		.Fixed_Length,
		2,
		encoded_head[:],
	)
	testing.expect_value(t, encoded.result, response.Response_Head_Result.Written)
	body := "ok"
	response_bytes: [160]u8
	for value, i in encoded_head[:encoded.written] {
		response_bytes[i] = value
	}
	for value, i in transmute([]u8)body {
		response_bytes[encoded.written + i] = value
	}
	response_length := encoded.written + len(body)
	write_driver := connection.begin_connection_write(101, response_bytes[:response_length])
	epoll_result, epoll_error := epoll.modify(
		poller,
		accepted_stream.fd,
		101,
		epoll.Interest{writable = true},
	)
	testing.expect_value(t, epoll_result, epoll.Result.Ready)
	testing.expect_value(t, epoll_error, i32(0))
	epoll_result, epoll_error = epoll.register(
		poller,
		client,
		102,
		epoll.Interest{readable = true},
	)
	testing.expect_value(t, epoll_result, epoll.Result.Ready)
	testing.expect_value(t, epoll_error, i32(0))
	write_operation_id := u64(0)
	write_event: connection.Connection_Write_Event
	for attempt in 0 ..< 8 {
		pending := connection.connection_write_pending(&write_driver)
		write_result := write_stream(accepted_stream.fd, pending)
		write_operation_id += 1
		completion := transport.IO_Completion {
			connection_id = 101,
			operation_id  = write_operation_id,
			operation     = .Write,
			result        = write_result,
		}
		write_event = connection.consume_write_completion(&write_driver, completion)
		if write_event.result == .Complete ||
		   write_event.result == .Invalid ||
		   write_event.result == .Peer_Closed ||
		   write_event.result == .Cancelled ||
		   write_event.result == .Transport_Error {
			break
		}
		if write_event.result == .Would_Block || write_event.result == .Interrupted {
			batch = epoll.wait(poller, native_events[:], ready_events[:], 100)
		}
	}
	testing.expect_value(t, write_event.result, connection.Connection_Write_Result.Complete)
	epoll.unregister(poller, accepted_stream.fd)

	received: [160]u8
	received_length := 0
	for attempt in 0 ..< 8 {
		batch = epoll.wait(poller, native_events[:], ready_events[:], 100)
		if batch.result != .Ready {
			continue
		}
		client_readable := false
		for event in batch.events {
			if event.connection_id == 102 && event.readable {
				client_readable = true
				break
			}
		}
		if !client_readable {
			continue
		}
		response_read := read_stream(client, received[received_length:])
		if response_read.kind == .Progress {
			received_length += response_read.transferred_bytes
			if received_length == response_length {
				break
			}
			continue
		}
		if response_read.kind != .Would_Block && response_read.kind != .Interrupted {
			break
		}
	}
	testing.expect_value(t, received_length, response_length)
	testing.expect_value(
		t,
		string(received[:received_length]),
		string(response_bytes[:response_length]),
	)

	epoll.unregister(poller, client)
	epoll.close(&poller)
	close_connection(&accepted_stream)
	linux.close(client)
	close_listener(&listener)
}
