use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::time::Duration;
use veylor_http2::{
    CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM, FrameType, HEADERS_FLAG_END_HEADERS,
    HpackEntrySlot, HpackTable, decode_goaway_payload,
};

// NOTE: H2C file-mount proof through a live Server::bind plus real H2C bytes (never
// driver-only, so projection wiring is covered): full GET/HEAD 200s with Last-Modified,
// 304 revalidation, single-range 206, multipart 206 byte-exact with H1, and fail-closed
// 404/413/416 mappings over one multiplexed connection.

fn expected_multipart(
    boundary: &[u8],
    content_type: &[u8],
    total: u64,
    windows: &[(u64, u64)],
    file: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    for (start, end) in windows.iter().copied() {
        body.extend_from_slice(b"--");
        body.extend_from_slice(boundary);
        body.extend_from_slice(b"\r\nContent-Type: ");
        body.extend_from_slice(content_type);
        body.extend_from_slice(
            format!(
                "\r\nContent-Range: bytes {start}-{}/{total}\r\n\r\n",
                end - 1
            )
            .as_bytes(),
        );
        body.extend_from_slice(&file[start as usize..end as usize]);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary);
    body.extend_from_slice(b"--\r\n");
    body
}

fn multipart_boundary_of(content_type: &[u8]) -> Vec<u8> {
    const PREFIX: &[u8] = b"multipart/byteranges; boundary=";
    assert!(content_type.starts_with(PREFIX), "multipart ctype");
    content_type[PREFIX.len()..].to_vec()
}

#[test]
fn h2_file_mount_serves_precompressed_twins() {
    let root = file_mount_root("encoding");
    std::fs::write(root.join("hello.txt"), b"file-content").expect("mount file");
    std::fs::write(root.join("hello.txt.br"), b"brotli").expect("br twin");
    std::fs::write(root.join("hello.txt.gz"), b"gzip!").expect("gz twin");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);

    send_headers_extra(
        &mut client,
        1,
        b"GET",
        b"/assets/hello.txt",
        &[(b"accept-encoding".as_slice(), b"gzip, br".as_slice())],
        true,
    );
    let got = read_response_stateful(&mut client, 1, &mut dec);
    assert_eq!(got.status, 200);
    assert_eq!(got.content_encoding, b"br");
    assert_eq!(got.vary, b"accept-encoding");
    assert_eq!(got.content_length, 6);
    assert_eq!(got.body, b"brotli");

    send_headers_extra(
        &mut client,
        3,
        b"GET",
        b"/assets/hello.txt",
        &[(b"accept-encoding".as_slice(), b"gzip".as_slice())],
        true,
    );
    let got = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(got.status, 200);
    assert_eq!(got.content_encoding, b"gzip");
    assert_eq!(got.content_length, 5);
    assert_eq!(got.body, b"gzip!");

    stop_join(server);
}

#[test]
fn h2_file_mount_get_head_last_modified_304() {
    let root = file_mount_root("basic");
    std::fs::write(root.join("hello.txt"), b"file-content").expect("mount file");
    std::fs::write(root.join("exact"), b"file-loses").expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);
    send_headers(&mut client, 1, b"GET", b"/assets/hello.txt", true);
    let got = read_response_stateful(&mut client, 1, &mut dec);
    assert_eq!(got.status, 200);
    assert_eq!(got.content_length, 12);
    assert_eq!(got.body, b"file-content");
    assert_eq!(got.content_type, b"text/plain");
    assert!(!got.last_modified.is_empty());
    send_headers(&mut client, 3, b"HEAD", b"/assets/hello.txt", true);
    let head = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(head.status, 200);
    assert_eq!(head.content_length, 12);
    assert!(head.body.is_empty());
    assert_eq!(head.last_modified, got.last_modified);
    let since: &[u8] = &got.last_modified;
    send_headers_extra(
        &mut client,
        5,
        b"GET",
        b"/assets/hello.txt",
        &[(b"if-modified-since".as_slice(), since)],
        true,
    );
    let fresh = read_response_stateful(&mut client, 5, &mut dec);
    assert_eq!(fresh.status, 304);
    assert_eq!(fresh.content_length, 0);
    assert!(fresh.body.is_empty());
    assert_eq!(fresh.last_modified, got.last_modified);
    send_headers_extra(
        &mut client,
        7,
        b"GET",
        b"/assets/hello.txt",
        &[(
            b"if-modified-since".as_slice(),
            b"Thu, 01 Jan 1970 00:00:00 GMT".as_slice(),
        )],
        true,
    );
    let stale = read_response_stateful(&mut client, 7, &mut dec);
    assert_eq!(stale.status, 200);
    assert_eq!(stale.body, b"file-content");
    send_headers(&mut client, 9, b"GET", b"/assets/missing.txt", true);
    let missing = read_response_stateful(&mut client, 9, &mut dec);
    assert_eq!(missing.status, 404);
    assert_eq!(missing.body, b"Not Found");
    send_headers(&mut client, 11, b"GET", b"/assets/exact", true);
    let exact = read_response_stateful(&mut client, 11, &mut dec);
    assert_eq!(exact.status, 200);
    assert_eq!(exact.body, b"static-wins");
    send_headers(&mut client, 13, b"GET", b"/assets/../hello.txt", true);
    let traversal = read_response_stateful(&mut client, 13, &mut dec);
    assert_eq!(traversal.status, 404);
    assert_eq!(traversal.body, b"Not Found");
    send_headers(&mut client, 15, b"POST", b"/assets/hello.txt", true);
    let wrong_method = read_response_stateful(&mut client, 15, &mut dec);
    assert_eq!(wrong_method.status, 405);
    assert_eq!(wrong_method.content_length, 18);
    assert_eq!(wrong_method.body, b"Method Not Allowed");
    assert_eq!(wrong_method.allow, b"GET, HEAD");
    date_shape(&wrong_method);
    assert_date_first(&wrong_method.order, true);
    assert_eq!(
        wrong_method.order,
        vec![
            b":status".to_vec(),
            b"date".to_vec(),
            b"content-length".to_vec(),
            b"allow".to_vec()
        ]
    );
    send_headers_extra(
        &mut client,
        17,
        b"HEAD",
        b"/assets/hello.txt",
        &[(b"range".as_slice(), b"bytes=0-3".as_slice())],
        true,
    );
    let head_range = read_response_stateful(&mut client, 17, &mut dec);
    assert_eq!(head_range.status, 200);
    assert_eq!(head_range.content_length, 12);
    assert!(head_range.body.is_empty());
    send_headers(&mut client, 19, b"GET", b"/assets/hello.txt?v=2", true);
    let query = read_response_stateful(&mut client, 19, &mut dec);
    assert_eq!(query.status, 200);
    assert_eq!(query.body, b"file-content");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 10, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn h2_file_mount_streams_large_file_across_turns() {
    let root = file_mount_root("large");
    let mut bytes = vec![0u8; 20_000];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    std::fs::write(root.join("big.bin"), &bytes).expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);
    send_headers(&mut client, 1, b"GET", b"/assets/big.bin", true);
    let got = read_response_stateful(&mut client, 1, &mut dec);
    assert_eq!(got.status, 200);
    assert_eq!(got.content_length, 20_000);
    assert_eq!(got.body, bytes);
    assert_eq!(got.content_type, b"application/octet-stream");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn h2_file_mount_large_file_survives_default_window_without_client_topups() {
    let root = file_mount_root("window");
    let mut bytes = vec![0u8; 100_000];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    std::fs::write(root.join("big.bin"), &bytes).expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);
    send_headers(&mut client, 1, b"GET", b"/assets/big.bin", true);
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    let mut block = Vec::new();
    let mut body = Vec::new();
    let mut headers_complete = false;
    let mut end_stream = false;
    while !headers_complete {
        let (header, payload) = read_frame(&mut client);
        match header.frame_type {
            FrameType::Headers => {
                assert_eq!(header.stream_id, 1);
                block.extend_from_slice(&payload);
                if header.flags & HEADERS_FLAG_END_HEADERS != 0 {
                    headers_complete = true;
                }
            }
            FrameType::Continuation => {
                assert_eq!(header.stream_id, 1);
                block.extend_from_slice(&payload);
                if header.flags & CONTINUATION_FLAG_END_HEADERS != 0 {
                    headers_complete = true;
                }
            }
            FrameType::Settings | FrameType::WindowUpdate | FrameType::Ping => {}
            FrameType::GoAway => {
                let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
                panic!(
                    "connection torn down before headers complete: error={:?}",
                    goaway.error_code,
                );
            }
            found => panic!("unexpected frame: {found:?}"),
        }
    }
    let response = decode_with(&mut dec, &block);
    assert_eq!(response.status, 200);
    // NOTE: the default 65,535-octet flow-control window bounds what a passive peer
    // receives unprompted; the stalled server must wait for WINDOW_UPDATE, not fail.
    while body.len() < 65_535 {
        let (header, payload) = read_frame(&mut client);
        match header.frame_type {
            FrameType::Data => {
                assert_eq!(header.stream_id, 1);
                assert!(
                    header.flags & DATA_FLAG_END_STREAM == 0,
                    "response ended early at {} bytes",
                    body.len(),
                );
                body.extend_from_slice(&payload);
            }
            FrameType::GoAway => {
                let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
                panic!(
                    "connection torn down mid-body: error={:?} body_bytes={}",
                    goaway.error_code,
                    body.len(),
                );
            }
            FrameType::Settings | FrameType::WindowUpdate | FrameType::Ping => {}
            found => panic!("unexpected frame: {found:?}"),
        }
    }
    assert_eq!(body.len(), 65_535);
    write_frame(
        &mut client,
        FrameType::WindowUpdate,
        0,
        1,
        &100_000u32.to_be_bytes(),
    );
    write_frame(
        &mut client,
        FrameType::WindowUpdate,
        0,
        0,
        &100_000u32.to_be_bytes(),
    );
    while !end_stream {
        let (header, payload) = read_frame(&mut client);
        match header.frame_type {
            FrameType::Data => {
                assert_eq!(header.stream_id, 1);
                body.extend_from_slice(&payload);
                if header.flags & DATA_FLAG_END_STREAM != 0 {
                    end_stream = true;
                }
            }
            FrameType::GoAway => {
                let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
                panic!(
                    "connection torn down mid-body: error={:?} body_bytes={}",
                    goaway.error_code,
                    body.len(),
                );
            }
            FrameType::Settings | FrameType::WindowUpdate | FrameType::Ping => {}
            found => panic!("unexpected frame: {found:?}"),
        }
    }
    assert_eq!(response.body, Vec::new());
    assert_eq!(body, bytes);
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn h2_file_mount_sequential_gets_stay_exact_across_1mib() {
    let root = file_mount_root("mib-seq");
    let mut bytes = vec![0u8; 32_768];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    std::fs::write(root.join("big-32k.bin"), &bytes).expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);
    let mut stream_id = 1;
    for _ in 0..32 {
        send_headers(&mut client, stream_id, b"GET", b"/assets/big-32k.bin", true);
        let got = read_response_stateful(&mut client, stream_id, &mut dec);
        assert_eq!(got.status, 200);
        assert_eq!(got.content_length, 32_768);
        assert_eq!(got.body, bytes);
        assert_eq!(got.content_type, b"application/octet-stream");
        send_window_update(&mut client, 0, 32_768);
        stream_id += 2;
    }
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 32, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn h2_file_mount_single_range_206_and_fail_closed() {
    let root = file_mount_root("range");
    let mut bytes = vec![0u8; 1000];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    std::fs::write(root.join("data.bin"), &bytes).expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);
    send_headers_extra(
        &mut client,
        1,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=0-99".as_slice())],
        true,
    );
    let ranged = read_response_stateful(&mut client, 1, &mut dec);
    assert_eq!(ranged.status, 206);
    assert_eq!(ranged.content_length, 100);
    assert_eq!(ranged.body, &bytes[..100]);
    assert_eq!(ranged.content_range, b"bytes 0-99/1000");
    send_headers_extra(
        &mut client,
        3,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=-10".as_slice())],
        true,
    );
    let suffix = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(suffix.status, 206);
    assert_eq!(suffix.body, &bytes[990..]);
    assert_eq!(suffix.content_range, b"bytes 990-999/1000");
    send_headers_extra(
        &mut client,
        5,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=5000-6000".as_slice())],
        true,
    );
    let bad = read_response_stateful(&mut client, 5, &mut dec);
    assert_eq!(bad.status, 416);
    assert!(bad.body.is_empty());
    assert_eq!(bad.content_range, b"bytes */1000");
    send_headers_extra(
        &mut client,
        7,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=0-1, 3-4".as_slice())],
        true,
    );
    let multi = read_response_stateful(&mut client, 7, &mut dec);
    assert_eq!(multi.status, 206);
    assert!(multi.content_range.is_empty());
    assert!(!multi.last_modified.is_empty());
    let boundary = multipart_boundary_of(&multi.content_type);
    let expected = expected_multipart(
        &boundary,
        b"application/octet-stream",
        1000,
        &[(0, 2), (3, 5)],
        &bytes,
    );
    assert_eq!(multi.body, expected);
    assert_eq!(multi.content_length, expected.len() as u64);
    send_headers(&mut client, 9, b"GET", b"/assets/data.bin", true);
    let sibling = read_response_stateful(&mut client, 9, &mut dec);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, bytes);
    send_headers_len(
        &mut client,
        11,
        b"GET",
        b"/assets/data.bin",
        Some(1_000_000),
        true,
    );
    let big = read_response_stateful(&mut client, 11, &mut dec);
    assert_eq!(big.status, 413);
    assert_eq!(big.body, b"Payload Too Large");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 6, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn h2_file_mount_multipart_subset_and_overcap_rules() {
    let root = file_mount_root("multi-rules");
    let mut bytes = vec![0u8; 1000];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    std::fs::write(root.join("data.bin"), &bytes).expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);
    send_headers_extra(
        &mut client,
        1,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=0-1,5000-6000".as_slice())],
        true,
    );
    let single = read_response_stateful(&mut client, 1, &mut dec);
    assert_eq!(single.status, 206);
    assert_eq!(single.body, &bytes[..2]);
    assert_eq!(single.content_range, b"bytes 0-1/1000");
    assert_eq!(single.content_type, b"application/octet-stream");
    send_headers_extra(
        &mut client,
        3,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=5000-,6000-7000".as_slice())],
        true,
    );
    let bad = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(bad.status, 416);
    assert!(bad.body.is_empty());
    assert_eq!(bad.content_range, b"bytes */1000");
    for (id, range) in [
        (5, b"bytes=a-b,c-d".as_slice()),
        (7, b"bytes=0-0,1-1,2-2,3-3,4-4,5-5,6-6,7-7,8-8".as_slice()),
    ] {
        send_headers_extra(
            &mut client,
            id,
            b"GET",
            b"/assets/data.bin",
            &[(b"range".as_slice(), range)],
            true,
        );
        let full = read_response_stateful(&mut client, id, &mut dec);
        assert_eq!(full.status, 200);
        assert_eq!(full.body, bytes);
        assert_eq!(full.content_type, b"application/octet-stream");
        assert!(full.content_range.is_empty());
    }
    send_headers_extra(
        &mut client,
        9,
        b"GET",
        b"/assets/data.bin",
        &[(b"range".as_slice(), b"bytes=0-1,4-5,5000-".as_slice())],
        true,
    );
    let multi = read_response_stateful(&mut client, 9, &mut dec);
    assert_eq!(multi.status, 206);
    assert!(multi.content_range.is_empty());
    let boundary = multipart_boundary_of(&multi.content_type);
    let expected = expected_multipart(
        &boundary,
        b"application/octet-stream",
        1000,
        &[(0, 2), (4, 6)],
        &bytes,
    );
    assert_eq!(multi.body, expected);
    assert_eq!(multi.content_length, expected.len() as u64);
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 5, "counts: {counts:?}");
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}
