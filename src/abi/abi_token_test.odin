#+build linux
#+test
package abi

import "core:testing"

@(test)
test_managed_connection_token_maps_stream_id :: proc(t: ^testing.T) {
	token := Vyl_Request_Token {
		worker_id             = 2,
		slot                  = 5,
		request_seq           = 9,
		stream_id             = 0,
		worker_generation     = 3,
		connection_generation = 7,
	}
	mapped := managed_connection_token(token)
	testing.expect_value(t, mapped.worker_id, u32(2))
	testing.expect_value(t, mapped.slot, u32(5))
	testing.expect_value(t, mapped.request_seq, u32(9))
	testing.expect_value(t, mapped.stream_id, u32(0))
	testing.expect_value(t, mapped.worker_generation, u64(3))
	testing.expect_value(t, mapped.generation, u64(7))

	token.stream_id = 13
	mapped = managed_connection_token(token)
	testing.expect_value(t, mapped.stream_id, u32(13))
	testing.expect_value(t, mapped.slot, u32(5))
	testing.expect_value(t, mapped.generation, u64(7))
}
