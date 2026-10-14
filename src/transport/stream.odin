package transport

IO_Result_Kind :: enum {
	Progress,
	Would_Block,
	End_Of_Stream,
	Closed,
	Cancelled,
	Error,
}

IO_Result :: struct {
	kind:              IO_Result_Kind,
	transferred_bytes: int,
	error_code:        i32,
}

Readiness_Event :: struct {
	connection_id: u64,
	readable:      bool,
	writable:      bool,
	peer_closed:   bool,
	error:         bool,
}

Transport_Operation :: enum {
	Accept,
	Read,
	Write,
}

IO_Completion :: struct {
	connection_id: u64,
	operation_id:  u64,
	operation:     Transport_Operation,
	result:        IO_Result,
}

Address_Family :: enum {
	IPv4,
	IPv6,
}

Peer_Address :: struct {
	family:   Address_Family,
	address:  [16]u8,
	port:     u16,
	scope_id: u32,
}

Timer_Expiration :: struct {
	timer_id:    u64,
	deadline_ns: u64,
}
