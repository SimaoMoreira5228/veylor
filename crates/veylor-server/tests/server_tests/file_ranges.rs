use crate::h1_peer::*;
use std::io::Write;

#[test]
fn file_range_exact_slice_then_keepalive_full() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=0-3");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 206);
    assert_eq!(first.body, b"hell");
    assert_eq!(header(&first, "content-length"), "4");
    assert_eq!(header(&first, "content-range"), "bytes 0-3/10");
    assert_eq!(header(&first, "accept-ranges"), "bytes");
    assert_eq!(header(&first, "content-type"), "text/plain");
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    assert_eq!(header(&second, "content-length"), "10");
    assert_eq!(header(&second, "accept-ranges"), "bytes");
    assert!(
        second
            .headers
            .iter()
            .all(|(field, _)| !field.eq_ignore_ascii_case("content-range")),
        "200 carries no content-range"
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_open_suffix_and_clamp_serve_exact_windows() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=6-");
    let open = read_response(&mut stream);
    assert_eq!(open.status, 206);
    assert_eq!(open.body, b"file");
    assert_eq!(header(&open, "content-range"), "bytes 6-9/10");
    get_range(&mut stream, "/assets/hello.txt", "bytes=-4");
    let suffix = read_response(&mut stream);
    assert_eq!(suffix.status, 206);
    assert_eq!(suffix.body, b"file");
    assert_eq!(header(&suffix, "content-range"), "bytes 6-9/10");
    get_range(&mut stream, "/assets/hello.txt", "bytes=4-100");
    let clamped = read_response(&mut stream);
    assert_eq!(clamped.status, 206);
    assert_eq!(clamped.body, b"o file");
    assert_eq!(header(&clamped, "content-range"), "bytes 4-9/10");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_unsatisfiable_answers_416_then_closes() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=10-");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 416);
    assert!(response.body.is_empty());
    assert_eq!(header(&response, "content-range"), "bytes */10");
    assert_eq!(header(&response, "content-length"), "0");
    read_eof(&mut stream);
    let mut empty = connect(&server);
    get_range(&mut empty, "/assets/empty.txt", "bytes=0-");
    let starved = read_response(&mut empty);
    assert_eq!(starved.status, 416);
    assert_eq!(header(&starved, "content-range"), "bytes */0");
    read_eof(&mut empty);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 2, "counts: {counts:?}");
}

#[test]
fn file_range_ignored_headers_serve_200_full() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    for range in ["items=0-1", "bytes=nope", "bytes=5-3"] {
        get_range(&mut stream, "/assets/hello.txt", range);
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200, "range {range}");
        assert_eq!(response.body, b"hello file", "range {range}");
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_head_ignores_range_with_full_headers() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    let request = "HEAD /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nRange: bytes=0-3\r\n\r\n";
    stream
        .write_all(request.as_bytes())
        .expect("write HEAD range");
    let text = read_head(&mut stream);
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(text.contains("Content-Length: 10"), "length: {text}");
    assert!(!text.contains("Content-Range:"), "no range: {text}");
    assert!(text.contains("Accept-Ranges: bytes"), "accept: {text}");
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_overflow_start_is_unsatisfiable() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(
        &mut stream,
        "/assets/hello.txt",
        "bytes=18446744073709551615-",
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 416);
    assert_eq!(header(&response, "content-range"), "bytes */10");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn file_range_blob_slice_streams_exact_window() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/blob.bin", "bytes=1000-1999");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 206);
    assert_eq!(
        header(&response, "content-range"),
        format!("bytes 1000-1999/{}", FILE_BLOB_LEN)
    );
    assert_eq!(response.body, &fixture.blob[1000..2000]);
    get_range(&mut stream, "/assets/blob.bin", "bytes=-16");
    let tail = read_response(&mut stream);
    assert_eq!(tail.status, 206);
    assert_eq!(tail.body, &fixture.blob[FILE_BLOB_LEN - 16..]);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

fn multipart_boundary(response: &ClientResponse) -> &str {
    header(response, "content-type")
        .strip_prefix("multipart/byteranges; boundary=")
        .expect("multipart content type")
}

fn expected_multipart(
    boundary: &str,
    content_type: &str,
    total: usize,
    windows: &[(usize, usize)],
    file: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    for (start, end) in windows.iter().copied() {
        body.extend_from_slice(b"--");
        body.extend_from_slice(boundary.as_bytes());
        body.extend_from_slice(b"\r\nContent-Type: ");
        body.extend_from_slice(content_type.as_bytes());
        body.extend_from_slice(
            format!(
                "\r\nContent-Range: bytes {start}-{}/{total}\r\n\r\n",
                end - 1
            )
            .as_bytes(),
        );
        body.extend_from_slice(&file[start..end]);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"--\r\n");
    body
}

#[test]
fn file_range_two_part_serves_byte_exact_multipart() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=0-3,6-9");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 206);
    let boundary = multipart_boundary(&response).to_string();
    let expected = expected_multipart(
        &boundary,
        "text/plain",
        10,
        &[(0, 4), (6, 10)],
        b"hello file",
    );
    assert_eq!(response.body, expected);
    assert_eq!(
        header(&response, "content-length"),
        expected.len().to_string()
    );
    assert_eq!(header(&response, "accept-ranges"), "bytes");
    assert!(
        response
            .headers
            .iter()
            .all(|(field, _)| !field.eq_ignore_ascii_case("content-range")),
        "multipart carries no top-level content-range"
    );
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_mixed_set_serves_valid_subset() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=0-3,99-");
    let single = read_response(&mut stream);
    assert_eq!(single.status, 206);
    assert_eq!(single.body, b"hell");
    assert_eq!(header(&single, "content-range"), "bytes 0-3/10");
    assert_eq!(header(&single, "content-type"), "text/plain");
    get_range(&mut stream, "/assets/hello.txt", "bytes=0-1,4-5,99-");
    let multi = read_response(&mut stream);
    assert_eq!(multi.status, 206);
    let boundary = multipart_boundary(&multi).to_string();
    let expected = expected_multipart(
        &boundary,
        "text/plain",
        10,
        &[(0, 2), (4, 6)],
        b"hello file",
    );
    assert_eq!(multi.body, expected);
    assert_eq!(header(&multi, "content-length"), expected.len().to_string());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_all_unsatisfiable_set_answers_416() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_range(&mut stream, "/assets/hello.txt", "bytes=10-,20-30");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 416);
    assert!(response.body.is_empty());
    assert_eq!(header(&response, "content-range"), "bytes */10");
    assert_eq!(header(&response, "content-length"), "0");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn file_range_malformed_and_overcap_sets_serve_200_full() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    for range in [
        "bytes=a-b,c-d",
        "bytes=,",
        "bytes=0-0,1-1,2-2,3-3,4-4,5-5,6-6,7-7,8-8",
    ] {
        get_range(&mut stream, "/assets/hello.txt", range);
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200, "range {range}");
        assert_eq!(response.body, b"hello file", "range {range}");
        assert_eq!(
            header(&response, "content-type"),
            "text/plain",
            "range {range}"
        );
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_head_multi_ignores_range_with_full_headers() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    let request = "HEAD /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nRange: bytes=0-3,6-9\r\n\r\n";
    stream
        .write_all(request.as_bytes())
        .expect("write HEAD range");
    let text = read_head(&mut stream);
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(text.contains("Content-Length: 10"), "length: {text}");
    assert!(!text.contains("Content-Range:"), "no range: {text}");
    assert!(!text.contains("multipart"), "no multipart: {text}");
    assert!(text.contains("Accept-Ranges: bytes"), "accept: {text}");
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_range_large_multipart_streams_across_turns() {
    let (server, fixture) = file_server();
    let mut stream = connect(&server);
    get_range(
        &mut stream,
        "/assets/blob.bin",
        "bytes=0-99999,200000-299999,600000-699999",
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 206);
    let boundary = multipart_boundary(&response).to_string();
    let expected = expected_multipart(
        &boundary,
        "application/octet-stream",
        FILE_BLOB_LEN,
        &[(0, 100000), (200000, 300000), (600000, 700000)],
        &fixture.blob,
    );
    assert_eq!(response.body, expected);
    assert_eq!(
        header(&response, "content-length"),
        expected.len().to_string()
    );
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}
