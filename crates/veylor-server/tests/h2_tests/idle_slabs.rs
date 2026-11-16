use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::time::Duration;
use veylor_http2::{DATA_FLAG_END_STREAM, FrameType};
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
