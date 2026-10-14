package body

import "veylor:http1"

Fixed_Body_Result :: enum {
	Need_More,
	Data,
	Complete,
	Invalid,
	Too_Large,
}

Fixed_Body_Event :: struct {
	data:     http1.Span,
	consumed: int,
	result:   Fixed_Body_Result,
}

Fixed_Body_Decoder :: struct {
	remaining:      u64,
	content_length: u64,
	started:        bool,
	state:          Fixed_Body_Result,
}

DEFAULT_FIXED_BODY_LIMIT :: u64(16 * 1024 * 1024)

read_fixed_body :: proc(
	decoder: ^Fixed_Body_Decoder,
	input: []u8,
	content_length: u64,
	body_limit := DEFAULT_FIXED_BODY_LIMIT,
) -> Fixed_Body_Event {
	if decoder.started {
		if decoder.content_length != content_length {
			decoder.state = .Invalid
		}
	} else {
		decoder.started = true
		decoder.content_length = content_length
		decoder.remaining = content_length

		if content_length > body_limit {
			decoder.state = .Too_Large
		} else if content_length == 0 {
			decoder.state = .Complete
		} else {
			decoder.state = .Need_More
		}
	}

	if decoder.state == .Invalid || decoder.state == .Too_Large {
		return Fixed_Body_Event{result = decoder.state}
	}

	if decoder.state == .Complete {
		return Fixed_Body_Event{result = .Complete}
	}

	if len(input) == 0 {
		return Fixed_Body_Event{result = .Need_More}
	}

	consumed := len(input)

	if u64(consumed) > decoder.remaining {
		consumed = int(decoder.remaining)
	}
	decoder.remaining -= u64(consumed)

	if decoder.remaining == 0 {
		decoder.state = .Complete
	}
	return Fixed_Body_Event {
		data = http1.Span{offset = 0, length = consumed},
		consumed = consumed,
		result = .Data,
	}
}
