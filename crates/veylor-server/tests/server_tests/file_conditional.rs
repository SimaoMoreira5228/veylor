use crate::h1_peer::*;
use std::io::Write;
use std::net::TcpStream;

fn get_conditional(stream: &mut TcpStream, path: &str, field: &str, value: &str) {
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n{field}: {value}\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .expect("write conditional request");
}

fn last_modified(response: &ClientResponse) -> String {
    header(response, "last-modified").to_string()
}

#[test]
fn file_last_modified_present_and_imf_shaped() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    let seen = last_modified(&response);
    assert_eq!(seen.len(), 29, "IMF-fixdate width: {seen}");
    assert!(seen.ends_with(" GMT"), "GMT only: {seen}");
    assert_eq!(seen.as_bytes()[3], b',', "weekday comma: {seen}");
    get_range(&mut stream, "/assets/hello.txt", "bytes=0-3");
    let partial = read_response(&mut stream);
    assert_eq!(partial.status, 206);
    assert_eq!(
        last_modified(&partial),
        seen,
        "206 carries the same validator"
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_conditional_round_trip_304_keeps_alive() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    let seen = last_modified(&first);
    get_conditional(&mut stream, "/assets/hello.txt", "If-Modified-Since", &seen);
    let not_modified = read_response(&mut stream);
    assert_eq!(not_modified.status, 304);
    assert!(not_modified.body.is_empty());
    assert!(
        not_modified
            .headers
            .iter()
            .all(|(field, _)| !field.eq_ignore_ascii_case("content-length")),
        "304 omits content-length: only the would-be-200 length is legal"
    );
    assert_eq!(
        last_modified(&not_modified),
        seen,
        "304 repeats the validator"
    );
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_conditional_older_and_garbage_dates_serve_200_full() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_conditional(
        &mut stream,
        "/assets/hello.txt",
        "If-Modified-Since",
        "Thu, 01 Jan 1970 00:00:00 GMT",
    );
    let older = read_response(&mut stream);
    assert_eq!(older.status, 200);
    assert_eq!(older.body, b"hello file");
    for garbage in ["not-a-date", "Sun, 06 Nov 1994 08:49:37 UTC", ""] {
        let request = format!(
            "GET /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nIf-Modified-Since: {garbage}\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .expect("write garbage date");
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200, "garbage {garbage:?}");
        assert_eq!(response.body, b"hello file", "garbage {garbage:?}");
    }
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_conditional_future_date_answers_304() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get_conditional(
        &mut stream,
        "/assets/hello.txt",
        "If-Modified-Since",
        "Tue, 06 Nov 2035 08:49:37 GMT",
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 304);
    assert!(response.body.is_empty());
    get(&mut stream, "/assets/hello.txt");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello file");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_conditional_head_matches_get_logic() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let first = read_response(&mut stream);
    let seen = last_modified(&first);
    let fresh = format!(
        "HEAD /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nIf-Modified-Since: {seen}\r\n\r\n"
    );
    stream
        .write_all(fresh.as_bytes())
        .expect("write HEAD fresh");
    let not_modified = read_head(&mut stream);
    assert!(
        not_modified.starts_with("HTTP/1.1 304 Not Modified\r\n"),
        "status: {not_modified}"
    );
    assert!(
        not_modified.contains("Last-Modified: "),
        "validator: {not_modified}"
    );
    let stale = "HEAD /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nIf-Modified-Since: Thu, 01 Jan 1970 00:00:00 GMT\r\n\r\n";
    stream
        .write_all(stale.as_bytes())
        .expect("write HEAD stale");
    let full = read_head(&mut stream);
    assert!(full.starts_with("HTTP/1.1 200 OK\r\n"), "status: {full}");
    assert!(full.contains("Content-Length: 10"), "length: {full}");
    assert!(full.contains("Last-Modified: "), "validator: {full}");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_if_none_match_ignored_serves_200() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    for etag in ["*", "\"abc\"", "W/\"abc\""] {
        get_conditional(&mut stream, "/assets/hello.txt", "If-None-Match", etag);
        let response = read_response(&mut stream);
        assert_eq!(response.status, 200, "etag {etag}");
        assert_eq!(response.body, b"hello file", "etag {etag}");
    }
    get(&mut stream, "/assets/hello.txt");
    let first = read_response(&mut stream);
    let seen = last_modified(&first);
    let both = format!(
        "GET /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nIf-None-Match: *\r\nIf-Modified-Since: {seen}\r\n\r\n"
    );
    stream
        .write_all(both.as_bytes())
        .expect("write both validators");
    let not_modified = read_response(&mut stream);
    assert_eq!(
        not_modified.status, 304,
        "If-Modified-Since still drives 304"
    );
    assert!(not_modified.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn file_conditional_with_range_still_304() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    get(&mut stream, "/assets/hello.txt");
    let first = read_response(&mut stream);
    let seen = last_modified(&first);
    let request = format!(
        "GET /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\nRange: bytes=0-3\r\nIf-Modified-Since: {seen}\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .expect("write ranged conditional");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 304, "precondition precedes Range");
    assert!(response.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}
