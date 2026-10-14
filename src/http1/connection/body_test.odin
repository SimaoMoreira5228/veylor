#+test
package connection

import "core:testing"
import http1 "veylor:http1"
import body "veylor:http1/body"
import "veylor:transport"

@(test)
test_connection_body_driver_streams_fixed_length_chunks_and_preserves_extra_bytes :: proc(
	t: ^testing.T,
) {
	connection_id := u64(51)
	head := http1.Request_Head {
		content_length     = 5,
		has_content_length = true,
	}
	driver := begin_connection_body(connection_id, head)
	body := "helloNEXT"
	fake := transport.Fake_Stream {
		input          = transmute([]u8)body,
		max_read_bytes = len(body),
	}
	scratch: [16]u8
	spans: [1]http1.Span
	completion := transport.IO_Completion {
		connection_id = connection_id,
		operation_id  = 1,
		operation     = .Read,
		result        = transport.fake_stream_read(&fake, scratch[:]),
	}
	event := consume_body_completion(
		&driver,
		completion,
		scratch[:completion.result.transferred_bytes],
		spans[:],
		{},
	)
	testing.expect_value(t, event.result, Connection_Body_Result.Data)
	testing.expect_value(t, event.body_complete, true)
	testing.expect_value(t, event.consumed, 5)
	testing.expect_value(t, event.spans[0].length, 5)
	testing.expect_value(
		t,
		string(scratch[event.spans[0].offset:event.spans[0].offset + event.spans[0].length]),
		"hello",
	)
	testing.expect_value(
		t,
		string(scratch[event.consumed:completion.result.transferred_bytes]),
		"NEXT",
	)
}

@(test)
test_connection_body_driver_decodes_chunked_trailer_then_finishes_buffered_bytes :: proc(
	t: ^testing.T,
) {
	request_head :=
		"POST / HTTP/1.1\r\nHost: example.com\r\n" + "Transfer-Encoding: chunked\r\n\r\n"
	head_input := transmute([]u8)request_head
	headers: [4]http1.Header
	parser: http1.Parser
	head, parse_result := http1.parse_request_head(&parser, head_input, headers[:])
	testing.expect_value(t, parse_result, http1.Parse_Result.Ready)

	allowed_names := [?]string{"Digest"}
	trailer_context := body.Chunked_Trailer_Context {
		headers                 = head.headers,
		head_input              = head_input[:head.length],
		permitted_trailer_names = allowed_names[:],
	}
	connection_id := u64(52)
	driver := begin_connection_body(connection_id, head)
	chunked_body := "4\r\nWiki\r\n0\r\nDigest: sha-256=x\r\n\r\n"
	fake := transport.Fake_Stream {
		input          = transmute([]u8)chunked_body,
		max_read_bytes = len(chunked_body),
	}
	scratch: [64]u8
	spans: [4]http1.Span
	completion := transport.IO_Completion {
		connection_id = connection_id,
		operation_id  = 1,
		operation     = .Read,
		result        = transport.fake_stream_read(&fake, scratch[:]),
	}
	event := consume_body_completion(
		&driver,
		completion,
		scratch[:completion.result.transferred_bytes],
		spans[:],
		trailer_context,
	)
	testing.expect_value(t, event.result, Connection_Body_Result.Trailer)
	testing.expect_value(t, len(event.spans), 1)
	testing.expect_value(t, event.spans[0].length, 4)
	testing.expect_value(
		t,
		string(scratch[event.spans[0].offset:event.spans[0].offset + event.spans[0].length]),
		"Wiki",
	)
	testing.expect_value(t, string(event.trailer_name), "Digest")
	testing.expect_value(t, string(event.trailer_value), "sha-256=x")

	remaining := scratch[event.consumed:completion.result.transferred_bytes]
	event = consume_buffered_body(&driver, remaining, spans[:], trailer_context)
	testing.expect_value(t, event.result, Connection_Body_Result.Complete)
	testing.expect_value(t, event.body_complete, true)
}

@(test)
test_connection_body_driver_rejects_replayed_read_completion :: proc(t: ^testing.T) {
	connection_id := u64(53)
	head := http1.Request_Head {
		content_length     = 4,
		has_content_length = true,
	}
	driver := begin_connection_body(connection_id, head)
	body := "data"
	fake := transport.Fake_Stream {
		input          = transmute([]u8)body,
		max_read_bytes = 2,
	}
	scratch: [4]u8
	spans: [1]http1.Span
	completion := transport.IO_Completion {
		connection_id = connection_id,
		operation_id  = 1,
		operation     = .Read,
		result        = transport.fake_stream_read(&fake, scratch[:]),
	}
	event := consume_body_completion(&driver, completion, scratch[:2], spans[:], {})
	testing.expect_value(t, event.result, Connection_Body_Result.Data)

	event = consume_body_completion(&driver, completion, scratch[:2], spans[:], {})
	testing.expect_value(t, event.result, Connection_Body_Result.Invalid)
	testing.expect_value(t, driver.state, Connection_Body_State.Failed)
}
