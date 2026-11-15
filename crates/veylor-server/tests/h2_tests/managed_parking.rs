use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::time::{Duration, Instant};
use veylor_http2::{
    ErrorCode, FrameType, SETTINGS_MAX_CONCURRENT_STREAMS, decode_rst_stream_payload,
};
use veylor_server::{CancelOutcome, EventSlot, RespondOutcome, h2::STREAM_SLOTS};

#[test]
fn h2_managed_park_respond_serves_host_body() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"GET");
    assert_eq!(path, b"/work");
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"managed-work"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"managed-work");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

// NOTE: HEAD-park mapping lock: HEAD /work parks through the GET fallback with its method
// intact, and an empty host respond serves HEADERS-only. Streaming Begin/EndWithTrailers on
// this park stays unenforced until the ParkedEntry method seam lands (see serve.rs Begin arm).
#[test]
fn h2_managed_head_park_responds_without_body() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"HEAD", b"/work", true);
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"HEAD");
    assert_eq!(path, b"/work");
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b""),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

#[test]
fn h2_managed_concurrent_parks_share_conn() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    send_headers(&mut stream, 3, b"GET", b"/work", true);
    let requests = poll_h2_requests(&host, 2);
    let (first, _, _) = &requests[0];
    let (second, _, _) = &requests[1];
    assert_eq!(first.token.stream_id, 1);
    assert_eq!(second.token.stream_id, 3);
    assert_eq!(host.parked_count(), 2);
    assert_eq!(
        host.respond(first.token, 200, b"OK", b"one"),
        RespondOutcome::Ok
    );
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"two"),
        RespondOutcome::Ok
    );
    let one = read_response(&mut stream, 1);
    assert_eq!(one.status, 200);
    assert_eq!(one.body, b"one");
    let three = read_response(&mut stream, 3);
    assert_eq!(three.status, 200);
    assert_eq!(three.body, b"two");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

// NOTE: stream-ceiling pin (RFC 9113 §5.1.2): the server advertises exactly STREAM_SLOTS, fills
// all 32 with parked managed streams, and the 33rd concurrent open fails per stream with RST
// RefusedStream (safe retry) while the connection survives; closing one recycles its slot for a
// follow-up stream, so table accounting never corrupts and nothing stalls silently.
#[test]
fn h2_stream_ceiling_refuses_33rd_and_recycles() {
    let server = managed_h2_server();
    let host = server.host();
    let mut client = h2_connect(&server);
    let advertised = handshake_advertised_setting(&mut client, SETTINGS_MAX_CONCURRENT_STREAMS);
    assert_eq!(advertised, STREAM_SLOTS as u32);
    for n in 0..STREAM_SLOTS {
        send_headers(&mut client, (2 * n + 1) as u32, b"GET", b"/work", true);
    }
    let last = (2 * STREAM_SLOTS + 1) as u32;
    send_headers(&mut client, last, b"GET", b"/work", true);
    let parked: Vec<EventSlot> = poll_h2_requests(&host, STREAM_SLOTS)
        .into_iter()
        .map(|event| event.0)
        .collect();
    assert_eq!(host.parked_count(), STREAM_SLOTS);
    let mut ids: Vec<u32> = parked.iter().map(|event| event.token.stream_id).collect();
    ids.sort_unstable();
    let expected: Vec<u32> = (0..STREAM_SLOTS).map(|n| (2 * n + 1) as u32).collect();
    assert_eq!(ids, expected);
    client
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("short timeout");
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut refused = false;
    while Instant::now() < deadline && !refused {
        match try_read_frame(&mut client) {
            Some((header, _)) if header.frame_type == FrameType::GoAway => {
                panic!("ceiling never goes away: {header:?}");
            }
            Some((header, payload))
                if header.frame_type == FrameType::RstStream && header.stream_id == last =>
            {
                let code = decode_rst_stream_payload(header, &payload).expect("decode rst");
                assert_eq!(code, ErrorCode::RefusedStream);
                refused = true;
            }
            Some(_) | None => {}
        }
    }
    assert!(refused, "33rd concurrent stream is refused");
    client
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    let first = parked
        .iter()
        .find(|event| event.token.stream_id == 1)
        .expect("park 1");
    assert_eq!(
        host.respond(first.token, 200, b"OK", b"one"),
        RespondOutcome::Ok
    );
    let one = read_response(&mut client, 1);
    assert_eq!(one.status, 200);
    assert_eq!(one.body, b"one");
    let follow = 129;
    // NOTE: follow-up reuses stream 1's host park sub-slot ((129 >> 1) % STREAM_SLOTS == 0), so
    // the recycle proof isolates H2 table accounting from host slot collisions on live siblings.
    send_headers(&mut client, follow, b"GET", b"/work", true);
    let next: Vec<EventSlot> = poll_h2_requests(&host, 1)
        .into_iter()
        .map(|event| event.0)
        .collect();
    assert_eq!(next[0].token.stream_id, follow);
    assert_eq!(
        host.respond(next[0].token, 200, b"OK", b"recycled"),
        RespondOutcome::Ok
    );
    let recycled = read_response(&mut client, follow);
    assert_eq!(recycled.status, 200);
    assert_eq!(recycled.body, b"recycled");
    for event in &parked {
        if event.token.stream_id == 1 {
            continue;
        }
        assert_eq!(
            host.respond(event.token, 200, b"OK", b"rest"),
            RespondOutcome::Ok
        );
    }
    for id in expected.iter().skip(1) {
        let response = read_response(&mut client, *id);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"rest");
    }
    let counts = stop_join(server);
    assert_eq!(
        counts.h2_served,
        (STREAM_SLOTS + 1) as u64,
        "counts: {counts:?}"
    );
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_cancel_rsts_only_its_stream() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(host.cancel(event.token), CancelOutcome::Ok);
    let (header, payload) = read_frame(&mut stream);
    assert_eq!(header.frame_type, FrameType::RstStream);
    assert_eq!(header.stream_id, 1);
    assert_eq!(
        decode_rst_stream_payload(header, &payload).expect("decode rst"),
        ErrorCode::Cancel
    );
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_teardown_unparks_parked_stream() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(event.token.stream_id, 1);
    assert_eq!(host.parked_count(), 1);
    // NOTE: shut_slot unparks before the fd dies, so the client-observed EOF below implies
    // the host mapping already released the stream slot: no strand window, no sleep. The
    // half-close (Write only) keeps the read side open so EOF genuinely proves server close.
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown client");
    read_eof(&mut stream);
    assert_eq!(host.parked_count(), 0);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone
    );
    stop_join(server);
}

#[test]
fn h2_managed_respond_with_headers_sends_them_after_pseudo() {
    let server = managed_h2_server();
    let host = server.host();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"content-type".as_slice(), b"text/plain".as_slice())],
            b"managed-h2",
        ),
        RespondOutcome::Ok
    );
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"managed-h2");
    assert_eq!(response.content_type, b"text/plain");
    assert_date_first(&response.order, true);
    let names: Vec<&[u8]> = response.order.iter().map(Vec::as_slice).collect();
    assert_eq!(names[..2], [b":status".as_slice(), b"date".as_slice()]);
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

#[test]
fn h2_managed_begin_with_headers_sends_them_after_date() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.begin_stream_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"content-type".as_slice(), b"text/plain".as_slice())],
        ),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(
        host.write_stream(event.token, b"managed-h2"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (response, trailers) = read_response_with_trailers(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"managed-h2");
    assert!(trailers.is_empty());
    assert_eq!(response.content_type, b"text/plain");
    date_shape(&response);
    let order: Vec<&[u8]> = response.order.iter().map(Vec::as_slice).collect();
    assert_eq!(order[0], b":status");
    assert_eq!(order[1], b"date");
    assert!(order.contains(&b"content-type".as_slice()));
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}
