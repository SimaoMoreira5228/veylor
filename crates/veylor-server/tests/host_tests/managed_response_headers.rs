use crate::host_peer::*;
use veylor_server::RespondOutcome;

#[test]
fn managed_respond_with_headers_sends_them_after_date() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[
                (b"content-type".as_slice(), b"text/plain".as_slice()),
                (b"x-custom".as_slice(), b"yes".as_slice()),
            ],
            b"done",
        ),
        RespondOutcome::Ok
    );
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"done");
    assert_eq!(header(&first, "content-type"), "text/plain");
    assert_eq!(header(&first, "x-custom"), "yes");
    date_shape(&first);
    let names: Vec<&str> = first
        .headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(
        names,
        ["Date", "content-type", "x-custom", "Content-Length"]
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn managed_gzip_body_passes_through_with_declared_encoding() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    let plain = b"The quick brown fox jumps over the lazy dog. ".repeat(50);
    let gzipped = {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plain).expect("gzip body");
        encoder.finish().expect("finish gzip")
    };
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"content-encoding".as_slice(), b"gzip".as_slice())],
            &gzipped,
        ),
        RespondOutcome::Ok
    );
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, gzipped);
    assert_eq!(header(&first, "content-encoding"), "gzip");
    assert_eq!(
        first
            .headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("vary"))
            .count(),
        0
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_respond_with_headers_rejects_bad_fields() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"x bad".as_slice(), b"yes".as_slice())],
            b"done",
        ),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"x-ok".as_slice(), b"a\r\nb".as_slice())],
            b"done",
        ),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.respond_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"content-length".as_slice(), b"4".as_slice())],
            b"done",
        ),
        RespondOutcome::Invalid
    );
    let many: Vec<(&[u8], &[u8])> = (0..9)
        .map(|_| (b"x-a".as_slice(), b"b".as_slice()))
        .collect();
    assert_eq!(
        host.respond_with_headers(event.token, 200, b"OK", &many, b"done"),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let first = read_response(&mut stream);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"done");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_begin_with_headers_sends_them_after_date() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"content-type".as_slice(), b"text/plain".as_slice())],
        ),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"chunk"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (status, raw, body) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"chunk");
    let head_end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("head end")
        + 4;
    let head = std::str::from_utf8(&raw[..head_end]).expect("head utf8");
    assert!(
        head.starts_with("HTTP/1.1 200 OK\r\nDate: "),
        "head: {head:?}"
    );
    assert!(
        head.to_ascii_lowercase()
            .contains("\r\ncontent-type: text/plain\r\n")
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_begin_rejects_bad_headers() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream_with_headers(
            event.token,
            200,
            b"OK",
            &[(b"content-length".as_slice(), b"5".as_slice())],
        ),
        RespondOutcome::Invalid
    );
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (status, _, _) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_304_drops_staged_body_and_reuses() {
    // NOTE: managed twin of the native/H2 304-shed pins: a host-staged body on a 304
    // must be shed (no content-length, no bytes), not fail the exchange closed.
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(event.token, 304, b"Not Modified", b"stray"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 304);
    assert!(response.body.is_empty(), "body: {:?}", response.body);
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| name != "content-length"),
        "headers: {:?}",
        response.headers
    );
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"live"),
        RespondOutcome::Ok
    );
    let followed = read_response(&mut stream);
    assert_eq!(followed.status, 200);
    assert_eq!(followed.body, b"live");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}
