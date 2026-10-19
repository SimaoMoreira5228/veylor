#+build linux
package linux_runtime

import "core:sys/linux"
import http1 "veylor:http1"
import connection "veylor:http1/connection"
import router "veylor:router"
import connections "veylor:runtime/connections"
import host "veylor:runtime/host"

Static_Connection_Phase :: enum {
	Reading_Request_Head,
	Writing_Response,
	Writing_File_Body,
	Awaiting_Host,
	Keep_Alive_Idle,
	Closed,
	Failed,
}

Static_Connection_Result :: enum {
	Need_More,
	Response_Progress,
	Keep_Alive,
	Closed,
	Response_Buffer_Small,
	Capacity_Limit,
	Transport_Error,
	Invalid,
}

Static_Connection_Close_Reason :: enum u8 {
	Unknown,
	Normal,
	Peer_Closed,
	Request_Rejected,
	Route_Not_Found,
	Workspace_Exhausted,
	Deadline_Capacity,
	Response_Buffer_Limit,
	Transport_Error,
	Timed_Out,
	Host_Queue_Full,
	Shutdown_Forced,
	Initialization_Failed,
	Internal_Error,
	Cancelled,
	Count,
}

STAGED_BODY_CAPACITY :: 2048
MANAGED_TRAILER_BYTES_LIMIT :: 8 * 1024

Managed_Trailer_Field :: struct {
	name_offset:  int,
	name_length:  int,
	value_offset: int,
	value_length: int,
}

Static_Connection_Workspace :: struct {
	head_driver:         connection.Connection_Head_Driver,
	body_driver:         connection.Connection_Body_Driver,
	managed_route_id:    u64,
	request_input:       []u8,
	headers:             []http1.Header,
	response:            []u8,
	staged_body:         [STAGED_BODY_CAPACITY]u8,
	body_start:          int,
	body_valid:          int,
	staged_length:       int,
	body_complete:       bool,
	trailer_head:        []http1.Header,
	trailer_allow:       [router.MAX_MANAGED_TRAILER_NAMES]string,
	trailer_allow_count: int,
	trailer_count:       int,
	trailer_fields:      [router.MAX_MANAGED_TRAILER_NAMES]Managed_Trailer_Field,
	trailer_data:        [MANAGED_TRAILER_BYTES_LIMIT]u8,
	trailer_data_used:   int,
	streaming:           bool,
	stream_ended:        bool,
	stream_terminal:     bool,
	stream_writable:     bool,
}

Static_Workspace_Pool :: struct {
	storage:           []Static_Connection_Workspace,
	free_stack:        []u32,
	free_count:        int,
	active_count:      int,
	high_water_mark:   int,
	acquisition_count: u64,
	release_count:     u64,
	exhaustion_count:  u64,
	initialized:       bool,
}

Static_Connection :: struct {
	token:                  connections.Connection_Token,
	fd:                     linux.Fd,
	workspace_pool:         ^Static_Workspace_Pool,
	file_root:              ^Static_File_Root,
	host_queue:             ^host.Host_Queue,
	native_routes:          ^router.Native_Table,
	workspace_slot:         int,
	write_driver:           connection.Connection_Write_Driver,
	file:                   Static_File,
	file_offset:            i64,
	file_remaining:         u64,
	has_file:               bool,
	phase:                  Static_Connection_Phase,
	response_length:        int,
	keep_alive:             bool,
	active:                 bool,
	registered:             bool,
	idle_counted:           bool,
	close_reason:           Static_Connection_Close_Reason,
	managed_method:         [16]u8,
	managed_method_length:  int,
	managed_http11:         bool,
	managed_req_close:      bool,
	managed_req_keep_alive: bool,
}

Static_Connection_Turn :: struct {
	bytes_read:    int,
	bytes_written: int,
	result:        Static_Connection_Result,
	error_code:    i32,
}
