use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::time::Duration;
use veylor_http2::{
    DATA_FLAG_END_STREAM, FrameType, HpackEntrySlot, HpackTable, decode_hpack_integer,
};
use veylor_server::RespondOutcome;

// NOTE: longer than SLAB_HOLD_NS plus the two worker sweeps the release gate also needs.
const HOLD_WINDOW: Duration = Duration::from_millis(140);

#[test]
fn keep_alive_answers_after_the_slab_hold_window() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    for stream in [1u32, 3, 5] {
        std::thread::sleep(HOLD_WINDOW);
        send_headers(&mut client, stream, b"GET", b"/hello", true);
        let response = read_response(&mut client, stream);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
    }
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 3);
}

#[test]
fn a_pipelined_pair_answers_after_a_released_window() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    std::thread::sleep(HOLD_WINDOW);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    send_headers(&mut client, 3, b"GET", b"/hello", true);
    let first = read_response(&mut client, 1);
    let second = read_response(&mut client, 3);
    assert_eq!(first.body, b"hello");
    assert_eq!(second.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2);
}

#[test]
fn a_body_that_arrives_after_the_hold_window_is_not_lost() {
    let server = managed_h2_body_server();
    let host = server.host();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    post_upload_open(&mut client, 1, b"x");
    std::thread::sleep(HOLD_WINDOW);
    write_frame(&mut client, FrameType::Data, DATA_FLAG_END_STREAM, 1, b"");
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    assert_eq!(pull_all(&host, event.token), b"x");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"taken"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"taken");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1);
}

// NOTE: the pooled encoder storage starts a new life with entries its peer never saw, so the
// first block after a release must open with a dynamic table size update to zero (RFC 7541
// 6.3); the peer decoder below is stateful, exactly like a real client.
#[test]
fn a_recycled_encoder_opens_its_next_block_with_a_zero_table_size_update() {
    let root = file_mount_root("recycled-encoder");
    std::fs::write(root.join("hello.txt"), b"file-content").expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);

    send_headers(&mut client, 1, b"GET", b"/assets/hello.txt", true);
    let first = read_response_stateful(&mut client, 1, &mut decoder);
    assert_eq!(first.status, 200);
    assert!(!decoder.is_empty(), "the first response refills the table");
    assert_eq!(first.content_type, b"text/plain");

    std::thread::sleep(HOLD_WINDOW);

    send_headers(&mut client, 3, b"GET", b"/assets/hello.txt", true);
    let (recycled, block) = read_response_stateful_block(&mut client, 3, &mut decoder);
    assert_eq!(recycled.status, 200);
    assert_eq!(recycled.body, b"file-content");
    assert_eq!(block[0] & 0xe0, 0x20, "block opens with a size update");
    let (size, _) = decode_hpack_integer(&block[..], 5).expect("size update");
    assert_eq!(size, 0, "the update names the empty table");
    assert_eq!(decoder.max_size(), 4_096, "and restores the table size");

    send_headers(&mut client, 5, b"GET", b"/assets/hello.txt", true);
    let refilled = read_response_stateful(&mut client, 5, &mut decoder);
    assert_eq!(refilled.status, 200);
    assert_eq!(refilled.body, b"file-content");
    assert_eq!(refilled.content_type, b"text/plain");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 3);
}

#[test]
fn separate_connections_release_and_retake_across_rounds() {
    let server = h2_server();
    let mut clients = Vec::new();
    for _ in 0..4 {
        let mut client = h2_connect(&server);
        handshake(&mut client);
        clients.push(client);
    }
    let mut stream = 1u32;
    for _ in 0..4 {
        std::thread::sleep(HOLD_WINDOW);
        for client in clients.iter_mut() {
            send_headers(client, stream, b"GET", b"/hello", true);
            let response = read_response(client, stream);
            assert_eq!(response.body, b"hello");
        }
        stream += 2;
    }
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 16);
}
