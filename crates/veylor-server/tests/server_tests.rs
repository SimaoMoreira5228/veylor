#[path = "server_tests/h1_peer.rs"]
mod h1_peer;

#[path = "server_tests/file_conditional.rs"]
mod file_conditional;

#[path = "server_tests/file_ranges.rs"]
mod file_ranges;

#[path = "server_tests/file_serving.rs"]
mod file_serving;

#[path = "server_tests/gzip_responses.rs"]
mod gzip_responses;

#[path = "server_tests/native_routes.rs"]
mod native_routes;

#[path = "server_tests/request_bodies.rs"]
mod request_bodies;

#[path = "server_tests/request_routing.rs"]
mod request_routing;

#[path = "server_tests/server_lifecycle.rs"]
mod server_lifecycle;
