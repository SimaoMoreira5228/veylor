#+test
package connection

import "core:testing"
import "veylor:transport"

@(test)
test_connection_write_driver_resumes_partial_writes_after_backpressure :: proc(t: ^testing.T) {
	connection_id := u64(61)
	response := "response"
	driver := begin_connection_write(connection_id, transmute([]u8)response)
	output: [16]u8
	fake := transport.Fake_Stream {
		output          = output[:],
		max_write_bytes = 2,
		write_blocked   = true,
	}

	blocked := transport.IO_Completion {
		connection_id = connection_id,
		operation_id  = 1,
		operation     = .Write,
		result        = transport.fake_stream_write(&fake, connection_write_pending(&driver)),
	}
	event := consume_write_completion(&driver, blocked)
	testing.expect_value(t, event.result, Connection_Write_Result.Would_Block)
	testing.expect_value(t, driver.cursor.offset, 0)

	fake.write_blocked = false
	for operation_id in 2 ..< 6 {
		pending := connection_write_pending(&driver)
		completion := transport.IO_Completion {
			connection_id = connection_id,
			operation_id  = u64(operation_id),
			operation     = .Write,
			result        = transport.fake_stream_write(&fake, pending),
		}
		event = consume_write_completion(&driver, completion)

		if operation_id < 5 {
			testing.expect_value(t, event.result, Connection_Write_Result.Progress)
		} else {
			testing.expect_value(t, event.result, Connection_Write_Result.Complete)
		}
	}
	testing.expect_value(t, string(fake.output[:fake.output_length]), response)
	testing.expect_value(t, len(connection_write_pending(&driver)), 0)
}

@(test)
test_connection_write_driver_rejects_invalid_counts_and_terminal_errors :: proc(t: ^testing.T) {
	connection_id := u64(62)
	response := "data"
	driver := begin_connection_write(connection_id, transmute([]u8)response)
	wrong_count := transport.IO_Completion {
		connection_id = connection_id,
		operation = .Write,
		result = transport.IO_Result{kind = .Progress, transferred_bytes = len(response) + 1},
	}
	event := consume_write_completion(&driver, wrong_count)
	testing.expect_value(t, event.result, Connection_Write_Result.Invalid)
	testing.expect_value(t, driver.state, Connection_Write_State.Failed)
	testing.expect_value(t, len(connection_write_pending(&driver)), 0)

	driver = begin_connection_write(connection_id, transmute([]u8)response)
	failed := transport.IO_Completion {
		connection_id = connection_id,
		operation = .Write,
		result = transport.IO_Result{kind = .Error, error_code = 5},
	}
	event = consume_write_completion(&driver, failed)
	testing.expect_value(t, event.result, Connection_Write_Result.Transport_Error)
	testing.expect_value(t, event.error_code, i32(5))
	testing.expect_value(t, len(connection_write_pending(&driver)), 0)

	driver = begin_connection_write(connection_id, transmute([]u8)response)
	partial := transport.IO_Completion {
		connection_id = connection_id,
		operation_id = 8,
		operation = .Write,
		result = transport.IO_Result{kind = .Progress, transferred_bytes = 2},
	}
	event = consume_write_completion(&driver, partial)
	testing.expect_value(t, event.result, Connection_Write_Result.Progress)
	event = consume_write_completion(&driver, partial)
	testing.expect_value(t, event.result, Connection_Write_Result.Invalid)
	testing.expect_value(t, len(connection_write_pending(&driver)), 0)
}
