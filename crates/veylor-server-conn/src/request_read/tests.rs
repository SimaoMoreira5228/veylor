use crate::{Admission, BodyKind, Header, ServerConnection, ServerError, Span};

fn get() -> &'static [u8] {
    b"GET /health HTTP/1.1\r\nHost: example.test\r\nAccept: */*\r\n\r\n"
}

fn post_fixed() -> &'static [u8] {
    b"POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\n\r\n"
}

fn post_continue() -> &'static [u8] {
    b"POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\nExpect: 100-continue\r\n\r\n"
}

fn parse<'a>(
    connection: &mut ServerConnection<'a>,
    input: &'a [u8],
    headers: &mut [Header],
) -> crate::RequestHead {
    connection.parse_head(input, headers).unwrap()
}

#[test]
fn parses_split_request_heads_and_resets() {
    let mut connection = ServerConnection::new();
    let input = get();
    let prefix = b"GET /health HTTP/1.1\r\nHost: example.test\r\nAcc".len();
    let mut headers = [Header::default(); 4];
    assert_eq!(
        connection.parse_head(&input[..prefix], &mut headers),
        Err(ServerError::NeedMore)
    );
    let head = connection.parse_head(input, &mut headers).unwrap();
    assert_eq!(head.header_count, 2);
    assert_eq!(
        &input[head.method.offset..head.method.offset + head.method.length],
        b"GET"
    );
    connection.reset().unwrap();
    let head = connection.parse_head(input, &mut headers).unwrap();
    assert_eq!(head.header_count, 2);
}

#[test]
fn reports_header_storage_requirements() {
    let mut connection = ServerConnection::new();
    let input = get();
    let mut headers = [Header::default(); 2];
    assert_eq!(
        connection.parse_head(input, &mut headers[..1]),
        Err(ServerError::HeaderCapacity {
            headers_required: 2,
            trailers_required: 0
        })
    );
    let head = connection.parse_head(input, &mut headers).unwrap();
    assert_eq!(head.header_count, 2);
}

#[test]
fn rejects_parse_after_response_started_or_body_admitted() {
    let mut connection = ServerConnection::new();
    let input = post_fixed();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    connection
        .admit_body(input, &headers, &[], &[], true)
        .unwrap();
    let mut headers_retry = [Header::default(); 4];
    assert_eq!(
        connection.parse_head(input, &mut headers_retry),
        Err(ServerError::BadState)
    );
}

#[test]
fn admits_ready_send_continue_and_rejected_bodies() {
    let mut connection = ServerConnection::new();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, get(), &mut headers);
    assert_eq!(
        connection.admit_body(get(), &headers, &[], &[], true),
        Ok(Admission::Ready)
    );

    let mut connection = ServerConnection::new();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, post_continue(), &mut headers);
    assert_eq!(
        connection.admit_body(post_continue(), &headers, &[], &[], true),
        Ok(Admission::SendContinue)
    );
    assert_eq!(
        connection.admit_body(post_continue(), &headers, &[], &[], true),
        Err(ServerError::BadState)
    );

    let mut connection = ServerConnection::new();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, post_fixed(), &mut headers);
    assert_eq!(
        connection.admit_body(post_fixed(), &headers, &[], &[], false),
        Ok(Admission::Rejected)
    );
    let mut trailers = [Header::default(); 4];
    assert_eq!(
        connection.read_body(b"abc", false, &mut trailers),
        Err(ServerError::BadState)
    );
}

#[test]
fn guards_admission_buffer_identity() {
    let mut connection = ServerConnection::new();
    let input = post_fixed();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    let moved_input = input.to_vec();
    assert_eq!(
        connection.admit_body(&moved_input, &headers, &[], &[], true),
        Err(ServerError::BadState)
    );
    let short = &input[..input.len() - 1];
    assert_eq!(
        connection.admit_body(short, &headers, &[], &[], true),
        Err(ServerError::BadState)
    );
}

#[test]
fn reads_fixed_body_to_completion() {
    let mut connection = ServerConnection::new();
    let input = post_fixed();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    connection
        .admit_body(input, &headers, &[], &[], true)
        .unwrap();
    let mut trailers = [Header::default(); 4];
    let first = connection.read_body(b"ab", false, &mut trailers).unwrap();
    assert_eq!(first.kind, BodyKind::Data);
    assert_eq!(first.consumed, 2);
    assert_eq!(&b"ab"[first.data.offset..][..first.data.length], b"ab");
    let done = connection.read_body(b"c", false, &mut trailers).unwrap();
    assert_eq!(done.kind, BodyKind::Data);
    assert_eq!(done.consumed, 1);
    let complete = connection.read_body(&[], false, &mut trailers).unwrap();
    assert_eq!(complete.kind, BodyKind::Complete);
}

#[test]
fn reads_chunked_body_with_trailers() {
    let mut connection = ServerConnection::new();
    let input: &[u8] =
        b"POST /stream HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\nTrailer: X-Sum\r\n\r\n";
    let mut headers = [Header::default(); 4];
    let head = connection.parse_head(input, &mut headers).unwrap();
    assert_eq!(head.header_count, 3);
    let trailer_names: &[u8] = b"x-sum";
    let permitted = [Span {
        offset: 0,
        length: 5,
    }];
    assert_eq!(
        connection.admit_body(input, &headers, trailer_names, &permitted, true),
        Ok(Admission::Ready)
    );
    let mut trailers = [Header::default(); 4];
    let raw: &[u8] = b"3\r\nabc\r\n0\r\nX-Sum: 42\r\n\r\n";
    let chunk = connection.read_body(raw, false, &mut trailers).unwrap();
    assert_eq!(chunk.kind, BodyKind::Data);
    let rest = &raw[chunk.consumed..];
    let end = connection.read_body(rest, false, &mut trailers).unwrap();
    assert_eq!(end.kind, BodyKind::NeedMore);
    let rest = &rest[end.consumed..];
    let line = connection.read_body(rest, false, &mut trailers).unwrap();
    assert_eq!(line.kind, BodyKind::NeedMore);
    let rest = &rest[line.consumed..];
    let event = connection.read_body(rest, false, &mut trailers).unwrap();
    assert_eq!(event.kind, BodyKind::Trailers);
    assert_eq!(event.trailer_count, 1);
    let name = &rest[trailers[0].name.offset..][..trailers[0].name.length];
    let value = &rest[trailers[0].value.offset..][..trailers[0].value.length];
    assert_eq!((name, value), (b"X-Sum".as_slice(), b"42".as_slice()));
    let done = connection.read_body(&[], false, &mut trailers).unwrap();
    assert_eq!(done.kind, BodyKind::Complete);
}

#[test]
fn rejects_unlisted_trailers() {
    let mut connection = ServerConnection::new();
    let input: &[u8] =
        b"POST /stream HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\n\r\n";
    let mut headers = [Header::default(); 4];
    connection.parse_head(input, &mut headers).unwrap();
    assert_eq!(
        connection.admit_body(input, &headers, &[], &[], true),
        Ok(Admission::Ready)
    );
    let mut trailers = [Header::default(); 4];
    let raw: &[u8] = b"3\r\nabc\r\n0\r\nX-Sum: 42\r\n\r\n";
    let chunk = connection.read_body(raw, false, &mut trailers).unwrap();
    assert_eq!(chunk.kind, BodyKind::Data);
    let rest = &raw[chunk.consumed..];
    let need = connection.read_body(rest, false, &mut trailers).unwrap();
    assert_eq!(need.kind, BodyKind::NeedMore);
    let rest = &rest[need.consumed..];
    let line = connection.read_body(rest, false, &mut trailers).unwrap();
    assert_eq!(line.kind, BodyKind::NeedMore);
    let rest = &rest[line.consumed..];
    assert_eq!(
        connection.read_body(rest, false, &mut trailers),
        Err(ServerError::ProtocolError)
    );
}

#[test]
fn continue_handshake_gates_body_reads() {
    let mut connection = ServerConnection::new();
    let input = post_continue();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    assert_eq!(
        connection.admit_body(input, &headers, &[], &[], true),
        Ok(Admission::SendContinue)
    );
    let mut trailers = [Header::default(); 4];
    assert_eq!(
        connection.read_body(b"abc", false, &mut trailers),
        Err(ServerError::BadState)
    );
    assert_eq!(connection.mark_continue_sent(), Err(ServerError::BadState));
    let mut output = [0u8; 256];
    let params = crate::ResponseHeadParams {
        status: 100,
        reason: b"Continue",
        fields: &[],
        framing: crate::ResponseFraming::FixedLength,
        content_length: 0,
        request_body_complete: false,
        response_close: false,
    };
    let head = connection
        .encode_response_head(input, &headers, params, &mut output)
        .unwrap();
    assert!(head.written > 0);
    assert_eq!(connection.advance_response_write(head.written), Ok(0));
    connection.mark_continue_sent().unwrap();
    assert_eq!(connection.mark_continue_sent(), Err(ServerError::BadState));
    let body = connection.read_body(b"abc", false, &mut trailers).unwrap();
    assert_eq!(body.kind, BodyKind::Data);
    let complete = connection.read_body(&[], false, &mut trailers).unwrap();
    assert_eq!(complete.kind, BodyKind::Complete);
}

#[test]
fn admission_mapping_follows_shared_continue_policy() {
    // NOTE: drift alarm for the converged 100-continue policy: the mapping in admit_body must
    // agree with veylor_http1::admission_needs_interim / admission_rejects_body, so a verdict
    // edit upstream fails here until this crate follows.
    use veylor_http1::{RequestBodyAdmission, admission_needs_interim, admission_rejects_body};

    let mut connection = ServerConnection::new();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, get(), &mut headers);
    assert_eq!(
        connection.admit_body(get(), &headers, &[], &[], true),
        Ok(Admission::Ready)
    );
    assert!(!admission_needs_interim(RequestBodyAdmission::ReadyToRead));
    assert!(!admission_rejects_body(RequestBodyAdmission::ReadyToRead));

    let mut connection = ServerConnection::new();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, post_continue(), &mut headers);
    assert_eq!(
        connection.admit_body(post_continue(), &headers, &[], &[], true),
        Ok(Admission::SendContinue)
    );
    assert!(admission_needs_interim(RequestBodyAdmission::SendContinue));
    assert!(!admission_rejects_body(RequestBodyAdmission::SendContinue));

    let mut connection = ServerConnection::new();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, post_fixed(), &mut headers);
    assert_eq!(
        connection.admit_body(post_fixed(), &headers, &[], &[], false),
        Ok(Admission::Rejected)
    );
    assert!(!admission_needs_interim(RequestBodyAdmission::Rejected));
    assert!(admission_rejects_body(RequestBodyAdmission::Rejected));
}

#[test]
fn unsupported_expectation_never_admits_or_arms_interim() {
    // NOTE: drift alarm for the 417 precedence half of the converged policy: the shared head
    // parser rejects an unsupported Expect before admission ever runs, so neither engine may
    // stage an interim. The managed pump renders this terminal as 417+close; this crate maps
    // the failed head to ProtocolError (host renders the final) and every follow-up fails
    // closed instead of admitting. The shared rule pinned here is terminality, not the
    // status codomain, which stays engine-local by design.
    let mut connection = ServerConnection::new();
    let input: &[u8] = b"POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\nExpect: magic\r\n\r\n";
    let mut headers = [Header::default(); 4];
    assert_eq!(
        connection.parse_head(input, &mut headers),
        Err(ServerError::ProtocolError)
    );
    assert_eq!(
        connection.admit_body(input, &headers, &[], &[], true),
        Err(ServerError::BadState)
    );
    let mut output = [0u8; 256];
    let params = crate::ResponseHeadParams {
        status: 100,
        reason: b"Continue",
        fields: &[],
        framing: crate::ResponseFraming::FixedLength,
        content_length: 0,
        request_body_complete: false,
        response_close: false,
    };
    assert_eq!(
        connection.encode_response_head(input, &headers, params, &mut output),
        Err(ServerError::BadState)
    );
    let mut trailers = [Header::default(); 4];
    assert_eq!(
        connection.read_body(b"abc", false, &mut trailers),
        Err(ServerError::BadState)
    );
    assert_eq!(connection.mark_continue_sent(), Err(ServerError::BadState));
    assert!(!connection.can_reuse());
}

#[test]
fn cancel_wins_over_pending_work() {
    let mut connection = ServerConnection::new();
    let input = post_continue();
    let mut headers = [Header::default(); 4];
    parse(&mut connection, input, &mut headers);
    connection
        .admit_body(input, &headers, &[], &[], true)
        .unwrap();
    connection.cancel();
    assert!(connection.is_cancelled());
    let mut headers_retry = [Header::default(); 4];
    assert_eq!(
        connection.parse_head(input, &mut headers_retry),
        Err(ServerError::Cancelled)
    );
    let mut trailers = [Header::default(); 4];
    assert_eq!(
        connection.read_body(b"abc", false, &mut trailers),
        Err(ServerError::Cancelled)
    );
    assert_eq!(connection.mark_continue_sent(), Err(ServerError::Cancelled));
    assert_eq!(connection.reset(), Err(ServerError::Cancelled));
    assert!(!connection.can_reuse());
}

#[test]
fn head_error_corpus_matches_managed_status_table() {
    // NOTE: drift alarm for H1-6 (error precedence), conn-crate half: parse_head collapses every
    // terminal head error to ProtocolError (the host renders the final status) while the managed
    // pump renders per-variant statuses (connection/mod.rs parse_exchange). The pinned agreement
    // is per-row — same input bytes => managed status X <=> this outcome:
    // bad Expect => 417 (server_tests unsupported_expectation_rejected_with_417_then_close),
    // oversized section => 431 (server_tests oversized_header_section_closes_with_431),
    // long target => 414 (server_tests oversized_request_target_answers_414), long method and
    // garbage line => 400 (server_tests oversized_request_method_stays_400,
    // malformed_request_line_closes_with_400). Storage here is roomy (128 slots, past the
    // 100-count section limit) so decoder verdicts — not caller capacity — decide, mirroring the
    // pump's HEADER_SLOTS == Limits::DEFAULT_HEADER_COUNT sizing. Two deliberate splits stay:
    // a full buffer with no complete head => NeedMore here (host owns its buffers) while the
    // pump attributes 414/400 from INPUT_CAP (server_tests full_buffer_without_head_answers_400
    // pins the pump half), and undersized caller storage => HeaderCapacity grow-and-retry here
    // while the pump can never surface it at head stage (the count limit trips first) and would
    // answer 400.
    use super::map_parse_error;
    use veylor_http1::ParseError;

    // NOTE: body-stage half of the same table: the managed pump never matches these decoder
    // variants (over-cap declared bodies reject 413 as a route-cap verdict in parked.rs, chunked
    // overruns close mid-stream with no status), so the agreement is trigger-family, not shared
    // code: BodyTooLarge => BodyTooLong here <=> 413-or-close there; AdmissionPending |
    // BodyRejected | Cancelled => BadState here <=> no-interim / failed-closed there; a stray
    // variant => ProtocolError on both render paths.
    assert_eq!(
        map_parse_error(ParseError::BodyTooLarge, 0),
        ServerError::BodyTooLong
    );
    for error in [
        ParseError::AdmissionPending,
        ParseError::BodyRejected,
        ParseError::Cancelled,
    ] {
        assert_eq!(map_parse_error(error, 7), ServerError::BadState);
    }
    assert_eq!(
        map_parse_error(ParseError::HeaderCapacity, 7),
        ServerError::BufferTooSmall { required: 7 }
    );
    for error in [
        ParseError::Invalid,
        ParseError::RequestLineTooLarge,
        ParseError::StatusLineTooLarge,
        ParseError::HeaderSectionTooLarge,
        ParseError::BufferChanged,
        ParseError::ExpectationFailed,
        ParseError::Incomplete,
    ] {
        assert_eq!(map_parse_error(error, 0), ServerError::ProtocolError);
    }

    fn head_error(input: &[u8]) -> ServerError {
        let mut connection = ServerConnection::new();
        // NOTE: Box keeps the 128-slot array off the test stack; Header: Copy + Default.
        let mut headers = Box::new([Header::default(); 128]);
        match connection.parse_head(input, &mut headers[..]) {
            Err(error) => error,
            Ok(_) => panic!("corpus input must fail head parse"),
        }
    }

    let split = b"GET /health HTTP/1.1\r\nHost: example.test\r\nAcc";
    let mut connection = ServerConnection::new();
    let mut headers = [Header::default(); 4];
    assert_eq!(
        connection.parse_head(split, &mut headers),
        Err(ServerError::NeedMore)
    );

    // NOTE: undersized caller storage is the grow-and-retry path, not a protocol verdict.
    let mut connection = ServerConnection::new();
    let mut headers = [Header::default(); 4];
    assert_eq!(
        connection.parse_head(get(), &mut headers[..1]),
        Err(ServerError::HeaderCapacity {
            headers_required: 2,
            trailers_required: 0
        })
    );

    assert_eq!(
        head_error(b"BAD REQUEST\r\n\r\n"),
        ServerError::ProtocolError
    );
    assert_eq!(
        head_error(b"POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\nExpect: magic\r\n\r\n"),
        ServerError::ProtocolError
    );
    let mut section = b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n".to_vec();
    for index in 0..120 {
        section.extend_from_slice(format!("X-Pad-{index:03}: abcdefgh\r\n").as_bytes());
    }
    section.extend_from_slice(b"\r\n");
    assert_eq!(head_error(&section), ServerError::ProtocolError);
    let target = format!("/{}", "a".repeat(9000));
    let long_target = format!("GET {target} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    assert_eq!(
        head_error(long_target.as_bytes()),
        ServerError::ProtocolError
    );
    let method = "A".repeat(9000);
    let long_method = format!("{method} /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    assert_eq!(
        head_error(long_method.as_bytes()),
        ServerError::ProtocolError
    );

    // NOTE: combined evil inputs pin the shared decoder line order from the other side:
    // section-overflow-before-Expect renders 431 in the pump, early-Expect renders 417, and both
    // collapse to ProtocolError here with no interim admitted afterwards.
    let mut pads_before_expect = b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n".to_vec();
    for index in 0..120 {
        pads_before_expect.extend_from_slice(format!("X-Pad-{index:03}: abcdefgh\r\n").as_bytes());
    }
    pads_before_expect.extend_from_slice(b"Expect: magic\r\n\r\n");
    assert_eq!(head_error(&pads_before_expect), ServerError::ProtocolError);
    let mut expect_before_pads =
        b"POST /upload HTTP/1.1\r\nHost: example.test\r\nExpect: magic\r\n".to_vec();
    for index in 0..120 {
        expect_before_pads.extend_from_slice(format!("X-Pad-{index:03}: abcdefgh\r\n").as_bytes());
    }
    expect_before_pads.extend_from_slice(b"\r\n");
    assert_eq!(head_error(&expect_before_pads), ServerError::ProtocolError);
}
