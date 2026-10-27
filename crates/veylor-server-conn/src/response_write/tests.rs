use crate::{Header, ResponseFraming, ServerConnection, ServerError};

fn get() -> &'static [u8] {
    b"GET /health HTTP/1.1\r\nHost: example.test\r\n\r\n"
}

fn post() -> &'static [u8] {
    b"POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\n\r\n"
}

fn plain(status: u16) -> crate::ResponseHeadParams<'static> {
    crate::ResponseHeadParams {
        status,
        reason: b"OK",
        fields: &[],
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        request_body_complete: true,
        response_close: false,
    }
}

fn parse<'a>(
    connection: &mut ServerConnection<'a>,
    input: &'a [u8],
    headers: &mut [Header],
) -> crate::RequestHead {
    connection.parse_head(input, headers).unwrap()
}

#[test]
fn bodiless_response_completes_once_head_is_acknowledged() {
    let mut connection = ServerConnection::new();
    let input = get();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let mut output = [0u8; 256];
    let head = connection
        .encode_response_head(input, &headers, plain(200), &mut output)
        .unwrap();
    assert!(head.written > 0);
    assert!(head.body_allowed);
    assert_eq!(
        connection.encode_response_head(input, &headers, plain(200), &mut output),
        Err(ServerError::BadState)
    );
    assert_eq!(
        connection.encode_response_body(b"x", &mut output),
        Err(ServerError::WouldBlock)
    );
    assert_eq!(connection.advance_response_write(head.written), Ok(0));
    let finished = connection.finish_response_body(&[], &mut output).unwrap();
    assert_eq!((finished.written, finished.required), (0, 0));
    assert!(connection.can_reuse());
    connection.reset().unwrap();
    assert!(!connection.can_reuse());
}

#[test]
fn write_overrun_poisons_the_connection() {
    let mut connection = ServerConnection::new();
    let input = get();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let mut output = [0u8; 256];
    let head = connection
        .encode_response_head(input, &headers, plain(200), &mut output)
        .unwrap();
    assert_eq!(
        connection.advance_response_write(head.written + 1),
        Err(ServerError::WriteOverrun)
    );
    assert_eq!(
        connection.encode_response_body(b"x", &mut output),
        Err(ServerError::WriteOverrun)
    );
    assert_eq!(
        connection.finish_response_body(&[], &mut output),
        Err(ServerError::WriteOverrun)
    );
    assert!(!connection.can_reuse());
}

#[test]
fn fixed_body_tracks_remaining_bytes() {
    let mut connection = ServerConnection::new();
    let input = post();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    connection
        .admit_body(input, &headers, &[], &[], true)
        .unwrap();
    let mut trailers = [Header::default(); 4];
    let data = connection.read_body(b"abc", false, &mut trailers).unwrap();
    assert_eq!(data.kind, crate::BodyKind::Data);
    assert_eq!(data.consumed, 3);
    let complete = connection.read_body(&[], false, &mut trailers).unwrap();
    assert_eq!(complete.kind, crate::BodyKind::Complete);
    let head_params = crate::ResponseHeadParams {
        content_length: 5,
        ..plain(200)
    };
    let mut output = [0u8; 256];
    let head = connection
        .encode_response_head(input, &headers, head_params, &mut output)
        .unwrap();
    assert!(head.body_allowed);
    assert_eq!(
        connection.encode_response_body(b"abc", &mut output),
        Err(ServerError::WouldBlock)
    );
    connection.advance_response_write(head.written).unwrap();
    assert!(!connection.can_reuse());
    let body = connection
        .encode_response_body(b"abc", &mut output)
        .unwrap();
    assert_eq!((body.consumed, body.written), (3, 3));
    assert_eq!(connection.advance_response_write(body.written), Ok(0));
    assert_eq!(
        connection.encode_response_body(b"def", &mut output),
        Err(ServerError::BodyTooLong)
    );
    let empty = connection.encode_response_body(b"", &mut output).unwrap();
    assert_eq!((empty.consumed, empty.written), (0, 0));
    let body = connection.encode_response_body(b"de", &mut output).unwrap();
    connection.advance_response_write(body.written).unwrap();
    let finished = connection.finish_response_body(&[], &mut output).unwrap();
    assert_eq!((finished.written, finished.required), (0, 0));
    assert!(connection.can_reuse());
}

#[test]
fn fixed_short_finish_poisons_the_connection() {
    let mut connection = ServerConnection::new();
    let input = post();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let params = crate::ResponseHeadParams {
        content_length: 5,
        ..plain(200)
    };
    let mut output = [0u8; 256];
    let head = connection
        .encode_response_head(input, &headers, params, &mut output)
        .unwrap();
    connection.advance_response_write(head.written).unwrap();
    let body = connection.encode_response_body(b"ab", &mut output).unwrap();
    connection.advance_response_write(body.written).unwrap();
    assert_eq!(
        connection.finish_response_body(&[], &mut output),
        Err(ServerError::ProtocolError)
    );
    assert_eq!(
        connection.encode_response_body(b"c", &mut output),
        Err(ServerError::ProtocolError)
    );
    assert!(!connection.can_reuse());
}

#[test]
fn chunked_body_round_trips_with_trailers() {
    let mut connection = ServerConnection::new();
    let input = post();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let params = crate::ResponseHeadParams {
        framing: ResponseFraming::Chunked,
        ..plain(200)
    };
    let mut output = [0u8; 256];
    let head = connection
        .encode_response_head(input, &headers, params, &mut output)
        .unwrap();
    connection.advance_response_write(head.written).unwrap();
    assert_eq!(
        connection.encode_response_body(b"", &mut output),
        Err(ServerError::InvalidArgument)
    );
    let body = connection.encode_response_body(b"hi", &mut output).unwrap();
    assert_eq!(&output[..body.written], b"2\r\nhi\r\n");
    connection.advance_response_write(body.written).unwrap();
    let trailers = [crate::Field {
        name: b"x-sum",
        value: b"42",
    }];
    let finished = connection
        .finish_response_body(&trailers, &mut output)
        .unwrap();
    assert!(finished.written > 0);
    connection.advance_response_write(finished.written).unwrap();
    assert!(connection.can_reuse());
}

#[test]
fn interim_responses_return_to_idle() {
    let mut connection = ServerConnection::new();
    let input = get();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let mut output = [0u8; 256];
    let interim = connection
        .encode_response_head(input, &headers, plain(103), &mut output)
        .unwrap();
    assert!(!connection.can_reuse());
    connection.advance_response_write(interim.written).unwrap();
    assert!(!connection.can_reuse());
    let head = connection
        .encode_response_head(input, &headers, plain(200), &mut output)
        .unwrap();
    connection.advance_response_write(head.written).unwrap();
    let finished = connection.finish_response_body(&[], &mut output).unwrap();
    assert_eq!((finished.written, finished.required), (0, 0));
    assert!(connection.can_reuse());
}

#[test]
fn upgrade_and_close_verdicts_are_not_reusable() {
    let mut connection = ServerConnection::new();
    let input: &[u8] =
        b"GET /chat HTTP/1.1\r\nHost: example.test\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\n";
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let mut output = [0u8; 256];
    assert_eq!(
        connection.encode_response_head(input, &headers, plain(101), &mut output),
        Err(ServerError::InvalidArgument)
    );
    let upgrade = [crate::Field {
        name: b"Upgrade",
        value: b"websocket",
    }];
    let head_params = crate::ResponseHeadParams {
        status: 101,
        reason: b"Switching Protocols",
        fields: &upgrade,
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        request_body_complete: true,
        response_close: false,
    };
    let head = connection
        .encode_response_head(input, &headers, head_params, &mut output)
        .unwrap();
    connection.advance_response_write(head.written).unwrap();
    assert!(!connection.can_reuse());

    let mut connection = ServerConnection::new();
    let input = get();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let params = crate::ResponseHeadParams {
        response_close: true,
        ..plain(200)
    };
    let mut output = [0u8; 256];
    let head = connection
        .encode_response_head(input, &headers, params, &mut output)
        .unwrap();
    connection.advance_response_write(head.written).unwrap();
    assert!(!connection.can_reuse());
}

#[test]
fn reuse_verdict_follows_shared_keep_alive_policy() {
    // NOTE: drift alarm for the converged keep-alive policy: can_reuse must agree with
    // veylor_http1::can_reuse_after_response on every row below, so a verdict edit upstream
    // fails here until this crate follows. Only the cancelled/incomplete/no-response gates
    // stay local (engine state machine, not policy).
    use veylor_http1::{ConnectionHeader, can_reuse_after_response, response_permits_reuse};

    
    let mut connection = ServerConnection::new();
    let input = get();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let mut output = [0u8; 256];
    let head = connection
        .encode_response_head(input, &headers, plain(200), &mut output)
        .unwrap();
    connection.advance_response_write(head.written).unwrap();
    assert!(!connection.can_reuse());
    connection.finish_response_body(&[], &mut output).unwrap();
    assert!(connection.can_reuse());
    assert_eq!(
        connection.can_reuse(),
        can_reuse_after_response(false, ConnectionHeader::Omit, false, true)
    );

    
    let mut connection = ServerConnection::new();
    let input = post();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    connection
        .admit_body(input, &headers, &[], &[], true)
        .unwrap();
    let mut output = [0u8; 256];
    let params = crate::ResponseHeadParams {
        content_length: 0,
        request_body_complete: false,
        ..plain(200)
    };
    let head = connection
        .encode_response_head(input, &headers, params, &mut output)
        .unwrap();
    connection.advance_response_write(head.written).unwrap();
    connection.finish_response_body(&[], &mut output).unwrap();
    assert!(!connection.can_reuse());
    assert_eq!(
        connection.can_reuse(),
        can_reuse_after_response(false, ConnectionHeader::Close, false, false)
    );

    
    let mut connection = ServerConnection::new();
    let input = get();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let mut output = [0u8; 256];
    let params = crate::ResponseHeadParams {
        response_close: true,
        ..plain(200)
    };
    let head = connection
        .encode_response_head(input, &headers, params, &mut output)
        .unwrap();
    connection.advance_response_write(head.written).unwrap();
    connection.finish_response_body(&[], &mut output).unwrap();
    assert!(!connection.can_reuse());
    assert_eq!(
        connection.can_reuse(),
        can_reuse_after_response(false, ConnectionHeader::Close, false, true)
    );

    
    assert!(can_reuse_after_response(
        false,
        ConnectionHeader::Omit,
        true,
        false
    ));
    assert!(response_permits_reuse(false, ConnectionHeader::Omit));
}

#[test]
fn connect_tunnel_completion_never_reuses() {
    // NOTE: drift alarm for the deliberate upgrade split: the shared verdict maps a
    // CONNECT-tunnel 2xx to Omit on the wire, but only the upgraded input flips reuse shut.
    // This crate passes its live upgraded flag, so a tunnel never reuses; the managed pump
    // passes upgraded=false structurally (it never serves 101/CONNECT) and enforces the rest
    // through its drain phase, so unifying the flag would change pump wire behavior.
    use veylor_http1::{ConnectionHeader, can_reuse_after_response, response_permits_reuse};

    let mut connection = ServerConnection::new();
    let input: &[u8] = b"CONNECT example.test:443 HTTP/1.1\r\nHost: example.test\r\n\r\n";
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let mut output = [0u8; 256];
    let head = connection
        .encode_response_head(input, &headers, plain(200), &mut output)
        .unwrap();
    connection.advance_response_write(head.written).unwrap();
    // NOTE: a CONNECT-tunnel 2xx carries no body, so the head advance lands in Complete
    // with no finish step; the exchange is complete yet still not reusable (upgraded).
    assert!(!connection.can_reuse());
    assert_eq!(
        connection.can_reuse(),
        can_reuse_after_response(true, ConnectionHeader::Omit, false, true)
    );
    assert!(!can_reuse_after_response(
        true,
        ConnectionHeader::Omit,
        false,
        true
    ));
    assert!(response_permits_reuse(false, ConnectionHeader::Omit));
}

#[test]
fn duplicate_upgrade_headers_are_rejected() {
    let mut connection = ServerConnection::new();
    let input: &[u8] =
        b"GET /chat HTTP/1.1\r\nHost: example.test\r\nUpgrade: a\r\nUpgrade: b\r\n\r\n";
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let mut output = [0u8; 256];
    assert_eq!(
        connection.encode_response_head(input, &headers, plain(200), &mut output),
        Err(ServerError::InvalidArgument)
    );
}

#[test]
fn small_outputs_report_required_capacity() {
    let mut connection = ServerConnection::new();
    let input = get();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let mut output = [0u8; 4];
    let error = connection
        .encode_response_head(input, &headers, plain(200), &mut output)
        .unwrap_err();
    let crate::ServerError::BufferTooSmall { required } = error else {
        panic!("expected BufferTooSmall, got {error:?}");
    };
    assert!(required > 4);
}

#[test]
fn cancel_and_reset_shape_the_exchange() {
    let mut connection = ServerConnection::new();
    let input = get();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    connection.cancel();
    let mut output = [0u8; 256];
    assert_eq!(
        connection.encode_response_head(input, &headers, plain(200), &mut output),
        Err(ServerError::Cancelled)
    );
    assert_eq!(connection.reset(), Err(ServerError::Cancelled));

    let mut connection = ServerConnection::new();
    connection.reset().unwrap();
    let headers = [Header::default(); 4];
    assert_eq!(
        connection.encode_response_head(input, &headers, plain(200), &mut output),
        Err(ServerError::BadState)
    );
}
