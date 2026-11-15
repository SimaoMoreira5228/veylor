use crate::h1_peer::*;
use std::io::{Read, Write};
use std::net::SocketAddr;
use veylor_server::{MAX_STATIC_BODY_BYTES, Server, ServerConfig};

#[test]
fn persistent_double_get_keep_alive() {
    let server = test_server();
    let mut stream = connect(&server);
    get(&mut stream, "/hello");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    get(&mut stream, "/empty");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert!(second.body.is_empty());
    let content_length = second
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .expect("content-length present");
    assert_eq!(content_length.1, "0");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn pipelined_double_get_served_from_tail() {
    let server = test_server();
    let mut stream = connect(&server);
    let pipelined = concat!(
        "GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n",
        "GET /empty HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n",
    );
    stream
        .write_all(pipelined.as_bytes())
        .expect("write pipelined");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert!(second.body.is_empty());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn unknown_path_returns_404_then_closes() {
    let server = test_server();
    let mut stream = connect(&server);
    get(&mut stream, "/missing");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 404);
    assert_eq!(response.body, b"Not Found");
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn method_mismatch_answers_405_with_allow_then_closes() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "POST /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 405);
    assert_eq!(response.body, b"Method Not Allowed");
    assert_eq!(header(&response, "allow"), "GET");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn head_on_post_only_answers_405_without_body() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "HEAD /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write HEAD");
    let text = read_head(&mut stream);
    assert!(
        text.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"),
        "status: {text}"
    );
    assert!(text.contains("Allow: POST"), "allow: {text}");
    assert!(text.contains("Content-Length: 18"), "length: {text}");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn native_method_mismatch_answers_405_with_allow() {
    let server = native_server();
    let mut stream = connect(&server);
    let request = "POST /fixed HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 405);
    assert_eq!(response.body, b"Method Not Allowed");
    assert_eq!(header(&response, "allow"), "GET");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn managed_method_mismatch_answers_405_without_parking() {
    let server = capped_server();
    let mut stream = connect(&server);
    let request = "GET /mupload HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write GET");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 405);
    assert_eq!(response.body, b"Method Not Allowed");
    assert_eq!(header(&response, "allow"), "POST");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    // NOTE: a parked managed request would surface a host event, so an empty nonblocking
    // poll proves the 405 rejected at dispatch instead of racing a late park.
    let report = server.host().poll(0, &mut [], &mut [], &mut []);
    assert_eq!(report.copied, 0);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

#[test]
fn file_method_mismatch_answers_405_with_allow() {
    let (server, _fixture) = file_server();
    let mut stream = connect(&server);
    let request = "POST /assets/hello.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write POST");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 405);
    assert_eq!(response.body, b"Method Not Allowed");
    assert_eq!(header(&response, "allow"), "GET, HEAD");
    date_shape(&response);
    assert_eq!(header(&response, "connection"), "close");
    read_eof(&mut stream);
    let mut other = connect(&server);
    let missing = "POST /assets/nope.txt HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n";
    other
        .write_all(missing.as_bytes())
        .expect("write missing POST");
    let not_found = read_response(&mut other);
    assert_eq!(not_found.status, 404);
    assert_eq!(not_found.body, b"Not Found");
    read_eof(&mut other);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 2, "counts: {counts:?}");
}

#[test]
fn duplicate_route_rejected() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_route(b"GET", b"/dup", 200, b"OK", b"a", MAX_STATIC_BODY_BYTES)
        .expect("first register");
    assert!(
        server
            .register_route(b"GET", b"/dup", 200, b"OK", b"b", u64::MAX)
            .is_err()
    );
    let running = server.start().expect("start server");
    running.stop_accepting();
    let _ = running.join().expect("join server");
}

#[test]
fn head_falls_back_to_get_without_body() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = b"HEAD /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    stream.write_all(request).expect("write HEAD");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
    }
    let text = String::from_utf8(head).expect("head utf8");
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(
        text.contains("Content-Length: 5"),
        "length preserved: {text}"
    );
    read_eof(&mut stream);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn h1_date_present_and_shaped_across_static_paths() {
    let server = test_server();
    let mut stream = connect(&server);
    get(&mut stream, "/hello");
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    date_shape(&first);
    get(&mut stream, "/hello");
    let reuse = read_response(&mut stream);
    assert_eq!(reuse.status, 200);
    assert_eq!(reuse.body, b"hello");
    date_shape(&reuse);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");

    let missing = test_server();
    let mut stream = connect(&missing);
    get(&mut stream, "/missing");
    let not_found = read_response(&mut stream);
    assert_eq!(not_found.status, 404);
    assert_eq!(not_found.body, b"Not Found");
    date_shape(&not_found);
    read_eof(&mut stream);
    missing.stop_accepting();
    let _ = missing.join().expect("join server");

    let rejected = test_server();
    let mut stream = connect(&rejected);
    let head = format!(
        "POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\n\r\n",
        MAX_STATIC_BODY_BYTES + 1024
    );
    stream.write_all(head.as_bytes()).expect("write head");
    let too_large = read_response(&mut stream);
    assert_eq!(too_large.status, 413);
    assert_eq!(too_large.body, b"Payload Too Large");
    date_shape(&too_large);
    read_closed(&mut stream);
    rejected.stop_accepting();
    let _ = rejected.join().expect("join server");

    let (files, _fixture) = file_server();
    let mut stream = connect(&files);
    get(&mut stream, "/assets/hello.txt");
    let file = read_response(&mut stream);
    assert_eq!(file.status, 200);
    assert_eq!(file.body, b"hello file");
    date_shape(&file);
    files.stop_accepting();
    let _ = files.join().expect("join server");

    let native = native_server();
    let mut stream = connect(&native);
    get(&mut stream, "/fixed");
    let served = read_response(&mut stream);
    assert_eq!(served.status, 200);
    assert_eq!(served.body, b"native-hello");
    date_shape(&served);
    native.stop_accepting();
    let _ = native.join().expect("join server");
}

#[test]
fn options_asterisk_answers_server_capabilities() {
    let server = test_server();
    let mut stream = connect(&server);
    let request = "OPTIONS * HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write OPTIONS");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    let allow = header(&response, "allow");
    assert!(allow.contains("GET"), "allow: {allow}");
    assert!(allow.contains("POST"), "allow: {allow}");
    date_shape(&response);
    get(&mut stream, "/hello");
    let second = read_response(&mut stream);
    assert_eq!(second.status, 200);
    assert_eq!(second.body, b"hello");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn options_asterisk_ignores_accept_encoding() {
    let server = gzip_server();
    let mut stream = connect(&server);
    let request = "OPTIONS * HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write OPTIONS");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case("content-encoding")),
        "no content-encoding"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}
