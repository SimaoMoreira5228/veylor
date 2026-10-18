package abi

VYL_ABI_VERSION :: u32(1)
VYL_MAX_WORKERS :: 256
VYL_MAX_ROUTES :: 64
VYL_ROUTE_INDEX_CAPACITY :: VYL_MAX_ROUTES * 2
VYL_ROUTE_BYTES_CAPACITY :: 64 * 1024
VYL_MAX_METHOD_BYTES :: 16
VYL_MAX_PATH_BYTES :: 2048
VYL_MAX_REASON_BYTES :: 64
VYL_MAX_RESPONSE_BODY_BYTES :: 768

Vyl_Result :: enum i32 {
	Ok,
	Invalid_Argument,
	Bad_State,
	Capacity,
	System_Error,
	Out_Of_Memory,
	Duplicate_Route,
	Worker_Error,
	Busy,
	Buffer_Too_Small,
}

Vyl_Config :: struct {
	abi_version:       u32,
	struct_size:       u32,
	worker_count:      u32,
	reserved:          u32,
	shutdown_grace_ns: u64,
}

Vyl_Bytes :: struct {
	data:   [^]u8,
	length: u32,
}

Vyl_Native_Request :: struct {
	method: Vyl_Bytes,
	path:   Vyl_Bytes,
}

Vyl_Native_Response :: struct {
	status: i32,
	reason: Vyl_Bytes,
	body:   Vyl_Bytes,
}

Vyl_Native_Handler :: #type proc "c" (
	user_data: rawptr,
	request: ^Vyl_Native_Request,
	response: ^Vyl_Native_Response,
) -> i32

Vyl_Request_Token :: struct {
	worker_id:              u32,
	slot:                   u32,
	worker_generation:      u64,
	connection_generation:  u64,
}

Vyl_Host_Event_Kind :: enum u32 {
	Request,
	Shutdown,
}

Vyl_Host_Event :: struct {
	kind:          Vyl_Host_Event_Kind,
	reserved:      u32,
	token:         Vyl_Request_Token,
	route_id:      u64,
	method_offset: u32,
	method_length: u32,
	path_offset:   u32,
	path_length:   u32,
}

Vyl_Batch_Info :: struct {
	event_count:     u32,
	data_used:       u32,
	events_required: u32,
	data_required:   u32,
}
