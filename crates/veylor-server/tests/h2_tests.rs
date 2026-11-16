#[path = "h2_tests/h2_peer.rs"]
mod h2_peer;

#[path = "h2_tests/frame_io.rs"]
mod frame_io;

#[path = "h2_tests/h2_response.rs"]
mod h2_response;

#[path = "h2_tests/batch_io.rs"]
mod batch_io;

#[path = "h2_tests/closed_data.rs"]
mod closed_data;

#[path = "h2_tests/file_mount.rs"]
mod file_mount;

#[path = "h2_tests/flow_control.rs"]
mod flow_control;

#[path = "h2_tests/gzip_responses.rs"]
mod gzip_responses;

#[path = "h2_tests/host_events.rs"]
mod host_events;

#[path = "h2_tests/idle_slabs.rs"]
mod idle_slabs;

#[path = "h2_tests/managed_parking.rs"]
mod managed_parking;

#[path = "h2_tests/managed_request_body.rs"]
mod managed_request_body;

#[path = "h2_tests/managed_requests.rs"]
mod managed_requests;

#[path = "h2_tests/managed_trailers.rs"]
mod managed_trailers;

#[path = "h2_tests/method_not_allowed.rs"]
mod method_not_allowed;

#[path = "h2_tests/native_routes.rs"]
mod native_routes;

#[path = "h2_tests/padding.rs"]
mod padding;

#[path = "h2_tests/protocol_abuse.rs"]
mod protocol_abuse;

#[path = "h2_tests/request_routing.rs"]
mod request_routing;

#[path = "h2_tests/response_headers.rs"]
mod response_headers;

#[path = "h2_tests/server_lifecycle.rs"]
mod server_lifecycle;

#[path = "h2_tests/tunnel.rs"]
mod tunnel;
