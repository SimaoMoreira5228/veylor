use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::net::SocketAddr;
use veylor_http2::{ErrorCode, HpackEntrySlot, HpackTable};
use veylor_server::{MAX_STATIC_BODY_BYTES, RespondOutcome, Server, ServerConfig};

// NOTE: H2C 405 pins mirroring H1 MethodNotAllowed (respond.rs): a path match with no method
// match answers 405 with the exact registered Allow (never synthesized), Date first after
// :status, content-length 18 with the fixed body, and the connection survives for siblings.
// Unknown paths still 404 on the same connection; every live test below runs through a real
// Server::bind plus H2C bytes (never driver-only) so projection wiring is covered.

fn assert_405(response: &Response, allow: &[u8]) {
    assert_eq!(response.status, 405);
    assert_eq!(response.content_length, 18);
    assert_eq!(response.allow, allow);
    date_shape(response);
    assert_date_first(&response.order, true);
    assert_eq!(
        response.order,
        vec![
            b":status".to_vec(),
            b"date".to_vec(),
            b"content-length".to_vec(),
            b"allow".to_vec()
        ]
    );
}

fn h2_stateful_decoder<'a>(
    entries: &'a mut [HpackEntrySlot; 64],
    scratch: &'a mut [u8; 4096],
) -> HpackTable<'a> {
    HpackTable::new(entries, scratch, 4_096)
}

#[test]
fn h2_static_post_returns_405_with_allow_conn_survives() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = h2_stateful_decoder(&mut dec_entries, &mut dec_scratch);
    send_headers(&mut client, 1, b"POST", b"/hello", true);
    let denied = read_response_stateful(&mut client, 1, &mut dec);
    assert_405(&denied, b"GET");
    assert_eq!(denied.body, b"Method Not Allowed");
    send_headers(&mut client, 3, b"GET", b"/hello", true);
    let sibling = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    send_headers(&mut client, 5, b"GET", b"/missing", true);
    let missing = read_response_stateful(&mut client, 5, &mut dec);
    assert_eq!(missing.status, 404);
    assert_eq!(missing.body, b"Not Found");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 3, "counts: {counts:?}");
}

// NOTE: HEAD-fallback boundary: HEAD falls back to GET only, so HEAD over a POST-only path
// answers 405 with Allow listing exactly POST (no synthesized HEAD), headers-only with the
// 405 length like any static HEAD; the GET sibling proves the same Allow with a body.
#[test]
fn h2_head_to_post_only_returns_405_without_fallback() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_route(
            b"POST",
            b"/submit",
            200,
            b"OK",
            b"submitted",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register submit");
    let server = server.start().expect("start server");
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = h2_stateful_decoder(&mut dec_entries, &mut dec_scratch);
    send_headers(&mut client, 1, b"HEAD", b"/submit", true);
    let head = read_response_stateful(&mut client, 1, &mut dec);
    assert_405(&head, b"POST");
    assert!(head.body.is_empty());
    send_headers(&mut client, 3, b"GET", b"/submit", true);
    let get = read_response_stateful(&mut client, 3, &mut dec);
    assert_405(&get, b"POST");
    assert_eq!(get.body, b"Method Not Allowed");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

// NOTE: multi-method Allow order pin: PUT over a GET+POST path lists both in registration
// order, comma-joined into one literal field, with no other method synthesized.
#[test]
fn h2_multi_method_allow_lists_in_registration_order() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_route(b"GET", b"/multi", 200, b"OK", b"got", MAX_STATIC_BODY_BYTES)
        .expect("register get");
    server
        .register_route(
            b"POST",
            b"/multi",
            200,
            b"OK",
            b"posted",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register post");
    let server = server.start().expect("start server");
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = h2_stateful_decoder(&mut dec_entries, &mut dec_scratch);
    send_headers(&mut client, 1, b"PUT", b"/multi", true);
    let denied = read_response_stateful(&mut client, 1, &mut dec);
    assert_405(&denied, b"GET, POST");
    assert_eq!(denied.body, b"Method Not Allowed");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

// NOTE: native 405 through the live projection: POST over a GET native answers 405 with the
// native method listed, and the GET sibling still invokes the handler on the same connection.
#[test]
fn h2_native_post_returns_405_live() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_native(b"GET", b"/native", Some(live_native_hello), 0)
        .expect("register native");
    let server = server.start().expect("start server");
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = h2_stateful_decoder(&mut dec_entries, &mut dec_scratch);
    send_headers(&mut client, 1, b"POST", b"/native", true);
    let denied = read_response_stateful(&mut client, 1, &mut dec);
    assert_405(&denied, b"GET");
    assert_eq!(denied.body, b"Method Not Allowed");
    send_headers(&mut client, 3, b"GET", b"/native", true);
    let sibling = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"h2-native");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

// NOTE: managed 405 without parking: POST over a GET managed route answers 405 synchronously
// (parked_count stays 0, so no host event exists), and the GET sibling still parks and serves
// through the host on the same connection.
#[test]
fn h2_managed_post_returns_405_without_park_live() {
    let server = managed_h2_server();
    let host = server.host();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = h2_stateful_decoder(&mut dec_entries, &mut dec_scratch);
    send_headers(&mut client, 1, b"POST", b"/work", true);
    let denied = read_response_stateful(&mut client, 1, &mut dec);
    assert_405(&denied, b"GET");
    assert_eq!(denied.body, b"Method Not Allowed");
    assert_eq!(host.parked_count(), 0);
    send_headers(&mut client, 3, b"GET", b"/work", true);
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"GET");
    assert_eq!(path, b"/work");
    assert_eq!(event.token.stream_id, 3);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"managed-work"),
        RespondOutcome::Ok
    );
    let sibling = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"managed-work");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

// NOTE: file 405 oracle boundary beside the updated basic test: traversal and misses stay 404
// (no existence oracle) while POST over a real file answers 405 with Allow: GET, HEAD.
#[test]
fn h2_file_post_traversal_and_missing_stay_404() {
    let root = file_mount_root("405-oracle");
    std::fs::write(root.join("hello.txt"), b"file-content").expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = h2_stateful_decoder(&mut dec_entries, &mut dec_scratch);
    send_headers(&mut client, 1, b"POST", b"/assets/../hello.txt", true);
    let traversal = read_response_stateful(&mut client, 1, &mut dec);
    assert_eq!(traversal.status, 404);
    assert_eq!(traversal.body, b"Not Found");
    send_headers(&mut client, 3, b"POST", b"/assets/missing.txt", true);
    let missing = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(missing.status, 404);
    assert_eq!(missing.body, b"Not Found");
    send_headers(&mut client, 5, b"POST", b"/assets/hello.txt", true);
    let denied = read_response_stateful(&mut client, 5, &mut dec);
    assert_405(&denied, b"GET, HEAD");
    assert_eq!(denied.body, b"Method Not Allowed");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 3, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn options_asterisk_answers_server_capabilities_over_h2c() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers_no_authority(&mut client, 1, b"OPTIONS", b"*");
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    let allow = std::str::from_utf8(&response.allow).expect("allow utf8");
    assert!(allow.contains("GET"), "allow: {allow}");
    date_shape(&response);
    assert_date_first(&response.order, true);
    send_headers(&mut client, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut client, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    server.stop_accepting();
    let (_, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 2);
    assert_eq!(counts.shutdown, 1);
}
