use super::*;

#[test]
fn h2_managed_request_body_uses_registered_cap_for_declared_and_unknown_lengths() {
    let server = managed_h2_body_server_with_cap(512);
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    send_headers_len(&mut stream, 1, b"POST", b"/upload", None, false);
    let allowed = vec![b'a'; 512];
    write_frame(
        &mut stream,
        FrameType::Data,
        DATA_FLAG_END_STREAM,
        1,
        &allowed,
    );
    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    assert_eq!(pull_all(&host, event.token), allowed);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"accepted"),
        RespondOutcome::Ok
    );
    assert_eq!(read_response(&mut stream, 1).body, b"accepted");

    send_headers_len(&mut stream, 3, b"POST", b"/upload", None, false);
    write_frame(&mut stream, FrameType::Data, 0, 3, &allowed);
    write_frame(&mut stream, FrameType::Data, DATA_FLAG_END_STREAM, 3, b"x");
    let reset = read_rst(&mut stream);
    assert_eq!(reset, (3, ErrorCode::Cancel));
    assert_eq!(host.parked_count(), 0);

    let (aborted, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    let mut scratch = [0; 8];
    assert_eq!(
        host.pull_body(aborted.token, &mut scratch).0,
        RespondOutcome::Gone
    );

    send_headers(&mut stream, 5, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 5);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

#[test]
fn h2_managed_over_cap_body_answers_413_and_survives() {
    let server = managed_h2_body_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers_len(
        &mut stream,
        1,
        b"POST",
        b"/upload",
        Some(H2_BODY_CAP + 1),
        false,
    );
    let too_large = read_response(&mut stream, 1);
    assert_eq!(too_large.status, 413);
    assert_eq!(too_large.body, b"Payload Too Large");
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    drop(stream);
    let counts = stop_join(server);
    assert_eq!(counts.h2_refused, 0, "counts: {counts:?}");
    assert_eq!(counts.h2_served, 2, "counts: {counts:?}");
}

#[test]
fn h2_managed_dynamic_route_copies_target_query_and_headers() {
    let server = managed_h2_body_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers_extra(
        &mut stream,
        1,
        b"GET",
        b"/accounts/42/statement?from=10",
        &[(b"authorization", b"Bearer test")],
        true,
    );

    let (event, method, path) = poll_h2_request(&host);
    assert_eq!(method, b"GET");
    assert_eq!(path, b"/accounts/42/statement?from=10");

    let mut request_headers = [veylor_server::RequestHeader::default(); 128];
    let mut bytes = [0; 16 * 1024];
    let report = host
        .request_head(event.token, &mut request_headers, &mut bytes)
        .unwrap();
    let target = &bytes[..report.target.length];
    let query = report.query.unwrap();
    assert_eq!(
        &bytes[query.offset..query.offset + query.length],
        b"from=10"
    );
    assert!(request_headers[..report.header_count].iter().any(|header| {
        let name = &bytes[header.name.offset..header.name.offset + header.name.length];
        let value = &bytes[header.value.offset..header.value.offset + header.value.length];
        name == b"authorization" && value == b"Bearer test"
    }));

    let mut captures = [veylor_router::RouteCapture::default(); veylor_router::MAX_ROUTE_CAPTURES];
    let count =
        veylor_router::match_route(b"/accounts/:id/statement", target, &mut captures).unwrap();
    assert_eq!(count, 1);
    assert_eq!(captures[0].value(target), Some(b"42".as_slice()));
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    assert_eq!(read_response(&mut stream, 1).body, b"done");

    drop(stream);
    stop_join(server);
}

#[test]
fn h2_managed_dynamic_route_declared_body_cap_answers_413() {
    let server = managed_h2_body_server_with_cap(512);
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers_len(&mut stream, 1, b"POST", b"/upload", Some(513), false);

    let response = read_response(&mut stream, 1);
    assert_eq!(response.status, 413);
    assert_eq!(response.body, b"Payload Too Large");

    drop(stream);
    stop_join(server);
}

#[test]
fn h2_managed_dynamic_route_unknown_body_cap_resets_only_that_stream() {
    let server = managed_h2_body_server_with_cap(512);
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers_len(&mut stream, 1, b"POST", b"/upload", None, false);
    let first = [b'x'; 512];
    write_frame(&mut stream, FrameType::Data, 0, 1, &first);
    write_frame(&mut stream, FrameType::Data, DATA_FLAG_END_STREAM, 1, b"x");

    assert_eq!(read_rst(&mut stream), (1, ErrorCode::Cancel));
    assert_eq!(host.parked_count(), 0);
    let (event, _, _) = poll_h2_request(&host);
    let mut scratch = [0; 1];
    assert_eq!(
        host.pull_body(event.token, &mut scratch).0,
        RespondOutcome::Gone
    );

    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");

    drop(stream);
    stop_join(server);
}
