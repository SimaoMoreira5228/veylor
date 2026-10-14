#+test
package transport

import "core:testing"

Fake_Stream :: struct {
	input:           []u8,
	input_offset:    int,
	output:          []u8,
	output_length:   int,
	max_read_bytes:  int,
	max_write_bytes: int,
	read_blocked:    bool,
	write_blocked:   bool,
	read_cancelled:  bool,
	write_cancelled: bool,
	write_closed:    bool,
	read_error:      i32,
	write_error:     i32,
}

fake_stream_read :: proc(stream: ^Fake_Stream, destination: []u8) -> IO_Result {
	if stream.read_cancelled {
		return IO_Result{kind = .Cancelled}
	}

	if stream.read_error != 0 {
		return IO_Result{kind = .Error, error_code = stream.read_error}
	}

	if stream.read_blocked {
		return IO_Result{kind = .Would_Block}
	}

	if stream.input_offset < 0 || stream.input_offset > len(stream.input) {
		return IO_Result{kind = .Would_Block}
	}

	if stream.input_offset >= len(stream.input) {
		return IO_Result{kind = .End_Of_Stream}
	}

	if len(destination) == 0 || stream.max_read_bytes <= 0 {
		return IO_Result{kind = .Would_Block}
	}

	available := len(stream.input) - stream.input_offset
	read_length := min(available, len(destination), stream.max_read_bytes)
	for i := 0; i < read_length; i += 1 {
		destination[i] = stream.input[stream.input_offset + i]
	}
	stream.input_offset += read_length
	return IO_Result{kind = .Progress, transferred_bytes = read_length}
}

fake_stream_write :: proc(stream: ^Fake_Stream, source: []u8) -> IO_Result {
	if stream.write_cancelled {
		return IO_Result{kind = .Cancelled}
	}

	if stream.write_error != 0 {
		return IO_Result{kind = .Error, error_code = stream.write_error}
	}

	if stream.write_closed {
		return IO_Result{kind = .Closed}
	}

	if stream.write_blocked {
		return IO_Result{kind = .Would_Block}
	}

	if stream.max_write_bytes <= 0 {
		return IO_Result{kind = .Would_Block}
	}

	if len(source) == 0 {
		return IO_Result{kind = .Progress}
	}

	if stream.output_length < 0 || stream.output_length > len(stream.output) {
		return IO_Result{kind = .Error, error_code = -1}
	}

	if stream.output_length == len(stream.output) {
		return IO_Result{kind = .Would_Block}
	}

	available := len(stream.output) - stream.output_length
	write_length := min(available, len(source), stream.max_write_bytes)

	if write_length == 0 {
		return IO_Result{kind = .Would_Block}
	}
	for i := 0; i < write_length; i += 1 {
		stream.output[stream.output_length + i] = source[i]
	}
	stream.output_length += write_length
	return IO_Result{kind = .Progress, transferred_bytes = write_length}
}

@(test)
test_fake_stream_reads_partial_data_then_eof :: proc(t: ^testing.T) {
	input := "request"
	destination: [8]u8
	stream := Fake_Stream {
		input          = transmute([]u8)input,
		max_read_bytes = 3,
	}
	result := fake_stream_read(&stream, destination[:])
	testing.expect_value(t, result.kind, IO_Result_Kind.Progress)
	testing.expect_value(t, result.transferred_bytes, 3)
	testing.expect_value(t, string(destination[:3]), "req")

	result = fake_stream_read(&stream, destination[:])
	testing.expect_value(t, result.kind, IO_Result_Kind.Progress)
	testing.expect_value(t, result.transferred_bytes, 3)
	testing.expect_value(t, string(destination[:3]), "ues")

	result = fake_stream_read(&stream, destination[:])
	testing.expect_value(t, result.kind, IO_Result_Kind.Progress)
	testing.expect_value(t, result.transferred_bytes, 1)
	testing.expect_value(t, destination[0], u8('t'))

	result = fake_stream_read(&stream, destination[:])
	testing.expect_value(t, result.kind, IO_Result_Kind.End_Of_Stream)
}

@(test)
test_fake_stream_write_preserves_partial_progress_and_backpressure :: proc(t: ^testing.T) {
	response := "response"
	output: [5]u8
	stream := Fake_Stream {
		output          = output[:],
		max_write_bytes = 2,
	}
	result := fake_stream_write(&stream, transmute([]u8)response)
	testing.expect_value(t, result.kind, IO_Result_Kind.Progress)
	testing.expect_value(t, result.transferred_bytes, 2)

	result = fake_stream_write(&stream, transmute([]u8)response[2:])
	testing.expect_value(t, result.transferred_bytes, 2)

	result = fake_stream_write(&stream, transmute([]u8)response[4:])
	testing.expect_value(t, result.transferred_bytes, 1)
	testing.expect_value(t, string(stream.output[:stream.output_length]), "respo")
	result = fake_stream_write(&stream, transmute([]u8)response[5:])
	testing.expect_value(t, result.kind, IO_Result_Kind.Would_Block)
	result = fake_stream_write(&stream, nil)
	testing.expect_value(t, result.kind, IO_Result_Kind.Progress)
	testing.expect_value(t, result.transferred_bytes, 0)
}

@(test)
test_fake_stream_reports_would_block_cancellation_and_errors :: proc(t: ^testing.T) {
	destination: [4]u8
	stream := Fake_Stream {
		read_blocked = true,
	}
	result := fake_stream_read(&stream, destination[:])
	testing.expect_value(t, result.kind, IO_Result_Kind.Would_Block)

	stream.read_blocked = false
	stream.read_cancelled = true

	result = fake_stream_read(&stream, destination[:])
	testing.expect_value(t, result.kind, IO_Result_Kind.Cancelled)

	stream.read_cancelled = false
	stream.read_error = 5

	result = fake_stream_read(&stream, destination[:])
	testing.expect_value(t, result.kind, IO_Result_Kind.Error)
	testing.expect_value(t, result.error_code, i32(5))

	input := "x"
	stream = Fake_Stream {
		input = transmute([]u8)input,
	}

	result = fake_stream_read(&stream, destination[:])
	testing.expect_value(t, result.kind, IO_Result_Kind.Would_Block)

	output: [4]u8
	response := "data"
	stream = Fake_Stream {
		output          = output[:],
		max_write_bytes = len(output),
		write_cancelled = true,
	}

	write_result := fake_stream_write(&stream, transmute([]u8)response)
	testing.expect_value(t, write_result.kind, IO_Result_Kind.Cancelled)
	testing.expect_value(t, stream.output_length, 0)

	stream.write_cancelled = false
	stream.write_error = 5

	write_result = fake_stream_write(&stream, transmute([]u8)response)
	testing.expect_value(t, write_result.kind, IO_Result_Kind.Error)
	testing.expect_value(t, write_result.error_code, i32(5))
	testing.expect_value(t, stream.output_length, 0)
}

@(test)
test_fake_stream_read_half_close_keeps_write_side_open :: proc(t: ^testing.T) {
	input := "request"
	output: [4]u8
	stream := Fake_Stream {
		input           = transmute([]u8)input,
		output          = output[:],
		max_read_bytes  = len(input),
		max_write_bytes = len(output),
	}
	destination: [8]u8
	read_result := fake_stream_read(&stream, destination[:])
	testing.expect_value(t, read_result.kind, IO_Result_Kind.Progress)
	read_result = fake_stream_read(&stream, destination[:])
	testing.expect_value(t, read_result.kind, IO_Result_Kind.End_Of_Stream)

	response := "ok"
	write_result := fake_stream_write(&stream, transmute([]u8)response)
	testing.expect_value(t, write_result.kind, IO_Result_Kind.Progress)
	testing.expect_value(t, string(stream.output[:stream.output_length]), response)

	stream.write_closed = true
	write_result = fake_stream_write(&stream, transmute([]u8)response)
	testing.expect_value(t, write_result.kind, IO_Result_Kind.Closed)
}

@(test)
test_readiness_notification_is_not_an_io_completion :: proc(t: ^testing.T) {
	input := "data"
	destination: [4]u8
	stream := Fake_Stream {
		input          = transmute([]u8)input,
		max_read_bytes = len(destination),
	}

	ready := Readiness_Event {
		connection_id = 12,
		readable      = true,
	}
	testing.expect_value(t, ready.readable, true)
	testing.expect_value(t, stream.input_offset, 0)

	completion := IO_Completion {
		connection_id = ready.connection_id,
		operation_id  = 3,
		operation     = .Read,
		result        = fake_stream_read(&stream, destination[:]),
	}

	testing.expect_value(t, completion.operation, Transport_Operation.Read)
	testing.expect_value(t, completion.result.transferred_bytes, 4)
	testing.expect_value(t, stream.input_offset, 4)
}
