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
