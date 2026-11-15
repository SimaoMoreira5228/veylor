use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;
use veylor_http2::{
    CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM, FRAME_HEADER_LEN, FrameHeader, FrameType,
    HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackTable,
    encode_frame_header, encode_header_block,
};
use veylor_server::{DECODE_BYTES, DECODE_ENTRIES, RunningServer};

const LARGE_BODY: usize = 200_000;

// NOTE: the frame header and its payload land in different turns, so the batch reads a
// partial frame first; the connection must keep its claim and finish the head.
#[test]
fn h2_split_headers_frame_across_turns_is_served() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    let fields = [
        field(b":method", b"GET"),
        field(b":scheme", b"http"),
        field(b":path", b"/hello"),
        field(b":authority", b"localhost"),
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    let frame = FrameHeader {
        payload_length: len as u32,
        frame_type: FrameType::Headers,
        flags: HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        stream_id: 1,
    };
    let mut encoded = [0; FRAME_HEADER_LEN];
    encode_frame_header(frame, &mut encoded, 16_384).expect("encode header");

    let mut wire = Vec::with_capacity(FRAME_HEADER_LEN + len);
    wire.extend_from_slice(&encoded);
    wire.extend_from_slice(&block[..len]);

    stream
        .write_all(&wire[..FRAME_HEADER_LEN + 3])
        .expect("write head half");
    stream.flush().expect("flush head half");
    std::thread::sleep(Duration::from_millis(40));
    stream
        .write_all(&wire[FRAME_HEADER_LEN + 3..])
        .expect("write head rest");

    let response = read_response(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

// NOTE: a body larger than one staging read arrives over several turns; each turn must
// drain what it read and keep the stream's accounting intact.
#[test]
fn h2_body_split_across_turns_stays_ordered() {
    let server = managed_h2_body_server_with_cap(1 << 20);
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    let body = vec![0x5au8; 30_000];
    send_headers_len(
        &mut stream,
        1,
        b"POST",
        b"/upload",
        Some(body.len() as u64),
        false,
    );
    for chunk in body.chunks(7_000) {
        write_frame(&mut stream, FrameType::Data, 0, 1, chunk);
        stream.flush().expect("flush chunk");
        std::thread::sleep(Duration::from_millis(15));
    }
    write_frame(&mut stream, FrameType::Data, DATA_FLAG_END_STREAM, 1, &[]);

    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    let parsed = pull_all(&host, event.token);
    assert_eq!(parsed.len(), body.len(), "body reassembled short");
    assert!(parsed.iter().all(|byte| *byte == 0x5a));
    host.respond(event.token, 200, b"OK", b"ok");
    let response = read_response(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"ok");
    stop_join(server);
}

// NOTE: one turn can carry several streams of the same connection; ordering is per
// stream, so every response must arrive whole and only for its own stream id.
#[test]
fn h2_pipelined_streams_complete_in_batches() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    let ids: Vec<u32> = (0..16).map(|index| index * 2 + 1).collect();
    for id in &ids {
        send_headers(&mut stream, *id, b"GET", b"/hello", true);
    }

    let bodies = read_stream_bodies(&mut stream, &ids);
    for id in &ids {
        assert_eq!(
            bodies.get(id).map(Vec::as_slice),
            Some(&b"hello"[..]),
            "stream {id}"
        );
    }

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, ids.len() as u64, "counts: {counts:?}");
}

// NOTE: a window-stalled stream must not hold the sibling behind it, and the stalled
// one must resume on its WINDOW_UPDATE once the peer drains.
#[test]
fn h2_flow_control_stall_does_not_starve_sibling() {
    let (server, root, body) = large_file_server("batch-stall");
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    send_headers(&mut stream, 1, b"GET", b"/assets/large.bin", true);
    send_headers(&mut stream, 3, b"GET", b"/assets/exact", true);
    std::thread::sleep(Duration::from_millis(40));

    let bodies = read_stream_bodies(&mut stream, &[1, 3]);
    assert_eq!(bodies.get(&3).map(Vec::as_slice), Some(&b"static-wins"[..]));
    assert_eq!(bodies.get(&1).map(Vec::as_slice), Some(&body[..]));

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

// NOTE: the batch is one submission over several connections; a peer that vanishes
// mid-turn (FIN then close) must only cost its own slot.
#[test]
fn h2_closed_peer_does_not_disturb_batch() {
    let server = h2_server();
    let mut first = h2_connect(&server);
    let mut doomed = h2_connect(&server);
    let mut third = h2_connect(&server);
    handshake(&mut first);
    handshake(&mut doomed);
    handshake(&mut third);

    send_headers(&mut first, 1, b"GET", b"/hello", true);
    send_headers(&mut doomed, 1, b"GET", b"/hello", true);
    send_headers(&mut third, 1, b"GET", b"/hello", true);
    doomed.shutdown(std::net::Shutdown::Both).expect("shutdown");
    drop(doomed);

    let first_response = read_response(&mut first, 1);
    assert_eq!(first_response.status, 200);
    assert_eq!(first_response.body, b"hello");
    let third_response = read_response(&mut third, 1);
    assert_eq!(third_response.status, 200);
    assert_eq!(third_response.body, b"hello");

    let counts = stop_join(server);
    assert!(counts.h2_served >= 2, "counts: {counts:?}");
}

// NOTE: four large responses queued with no reader: the deferred write pass must park
// what the window allows, then finish every stream once the peer grants credit.
#[test]
fn h2_unread_responses_resume_without_loss() {
    let (server, root, body) = large_file_server("batch-unread");
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    let ids: Vec<u32> = vec![1, 3, 5, 7];
    for id in &ids {
        send_headers(&mut stream, *id, b"GET", b"/assets/large.bin", true);
    }
    std::thread::sleep(Duration::from_millis(60));

    let bodies = read_stream_bodies(&mut stream, &ids);
    for id in &ids {
        assert_eq!(
            bodies.get(id).map(Vec::as_slice),
            Some(&body[..]),
            "stream {id}"
        );
    }

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, ids.len() as u64, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

// NOTE: reads every frame until each requested stream ends, granting flow-control credit
// as bytes land so a body wider than the initial window completes; response HEADERS are
// decoded in arrival order against one shared HPACK table.
fn read_stream_bodies(stream: &mut TcpStream, ids: &[u32]) -> HashMap<u32, Vec<u8>> {
    let mut entries = [HpackEntrySlot::EMPTY; DECODE_ENTRIES];
    let mut scratch = [0; DECODE_BYTES];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut blocks: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut bodies: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut ended: Vec<u32> = Vec::new();
    while ended.len() < ids.len() {
        let (header, payload) = read_frame(stream);
        match header.frame_type {
            FrameType::Settings | FrameType::WindowUpdate | FrameType::Ping => continue,
            FrameType::Headers | FrameType::Continuation => {
                let block = blocks.entry(header.stream_id).or_default();
                block.extend_from_slice(&payload);
                let last = if header.frame_type == FrameType::Headers {
                    header.flags & HEADERS_FLAG_END_HEADERS != 0
                } else {
                    header.flags & CONTINUATION_FLAG_END_HEADERS != 0
                };
                if !last {
                    continue;
                }
                let block = blocks.remove(&header.stream_id).unwrap_or_default();
                let head = decode_with(&mut decoder, &block);
                assert_eq!(head.status, 200, "stream {}", header.stream_id);
                bodies.entry(header.stream_id).or_default();
                if header.flags & HEADERS_FLAG_END_STREAM != 0 {
                    ended.push(header.stream_id);
                }
            }
            FrameType::Data => {
                bodies
                    .entry(header.stream_id)
                    .or_default()
                    .extend_from_slice(&payload);
                if header.flags & DATA_FLAG_END_STREAM != 0 {
                    ended.push(header.stream_id);
                    continue;
                }
                let credit = payload.len() as u32;
                send_window_update(stream, 0, credit);
                send_window_update(stream, header.stream_id, credit);
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }
    bodies
}

fn large_file_server(name: &str) -> (RunningServer, std::path::PathBuf, Vec<u8>) {
    let root = file_mount_root(name);
    let mut body = vec![0u8; LARGE_BODY];
    for (index, byte) in body.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    std::fs::write(root.join("large.bin"), &body).expect("mount file");
    (file_mount_server(&root), root, body)
}
