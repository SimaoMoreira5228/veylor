use super::*;

fn parse_all(input: &[u8]) -> Result<(RequestHead, Vec<Header>), ParseError> {
    let mut parser = Parser::new();
    let mut storage = [Header::default(); 16];
    let head = parser
        .parse(input, &mut storage, Limits::default())?
        .ok_or(ParseError::Invalid)?;
    let headers = storage[..head.header_count].to_vec();
    Ok((head, headers))
}

#[test]
fn parses_request_and_preserves_header_spans() {
    let input = b"GET /health HTTP/1.1\r\nHost: example.test\r\nAccept: */*\r\n\r\n";
    let (head, headers) = parse_all(input).unwrap();

    assert_eq!(head.method(input), Some(&b"GET"[..]));
    assert_eq!(head.target(input), Some(&b"/health"[..]));
    assert_eq!(head.version(input), Some(&b"HTTP/1.1"[..]));
    assert_eq!(
        &input[headers[0].name.offset..headers[0].name.offset + headers[0].name.length],
        b"Host"
    );
    assert_eq!(
        &input[headers[0].value.offset..headers[0].value.offset + headers[0].value.length],
        b"example.test"
    );
    assert_eq!(head.length, input.len());
}

#[test]
fn incremental_input_requires_stable_base() {
    let input = *b"GET / HTTP/1.1\r\nHost: test\r\n\r\n";
    let prefix_length = b"GET / HTTP/1.1\r\nHo".len();
    let mut parser = Parser::new();
    let mut storage = [Header::default(); 4];

    assert_eq!(
        parser.parse(&input[..prefix_length], &mut storage, Limits::default()),
        Ok(None)
    );
    assert!(
        parser
            .parse(&input, &mut storage, Limits::default())
            .unwrap()
            .is_some()
    );
}

#[test]
fn resumes_after_caller_header_storage_grows() {
    let input = b"GET / HTTP/1.1\r\nHost: test\r\nAccept: */*\r\n\r\n";
    let mut parser = Parser::new();
    let mut storage = [Header::default(); 2];

    assert_eq!(
        parser.parse(input, &mut storage[..1], Limits::default()),
        Err(ParseError::HeaderCapacity)
    );
    assert_eq!(parser.required_header_capacity(), 2);
    let head = parser
        .parse(input, &mut storage, Limits::default())
        .unwrap()
        .unwrap();

    assert_eq!(head.header_count, 2);
}

#[test]
fn rejects_bad_line_endings_and_field_names() {
    assert_eq!(parse_all(b"GET / HTTP/1.1\n\n"), Err(ParseError::Invalid));
    assert_eq!(
        parse_all(b"GET / HTTP/1.1\r\nBad Name: x\r\n\r\n"),
        Err(ParseError::Invalid)
    );
}

#[test]
fn validates_http_1_1_host_count() {
    assert_eq!(
        parse_all(b"GET / HTTP/1.1\r\n\r\n"),
        Err(ParseError::Invalid)
    );
    assert_eq!(
        parse_all(b"GET / HTTP/1.1\r\nHost: one\r\nhost: two\r\n\r\n"),
        Err(ParseError::Invalid)
    );
    assert!(parse_all(b"GET / HTTP/1.0\r\n\r\n").is_ok());
}

#[test]
fn parses_content_length_and_chunked_framing() {
    let (head, _) =
        parse_all(b"POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5, 005\r\n\r\n")
            .unwrap();
    assert_eq!(head.content_length, Some(5));
    assert!(!head.is_chunked);

    let (head, _) =
        parse_all(b"POST / HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: Chunked\r\n\r\n")
            .unwrap();
    assert_eq!(head.content_length, None);
    assert!(head.is_chunked);

    for request in [
        &b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5, 6\r\n\r\n"[..],
        &b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 18446744073709551616\r\n\r\n"[..],
        &b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
        &b"POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
    ] {
        assert_eq!(parse_all(request), Err(ParseError::Invalid));
    }
}

#[test]
fn parses_connection_tokens_without_allocating() {
    let (head, _) =
        parse_all(b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: keep-alive, CLOSE\r\n\r\n")
            .unwrap();
    assert!(head.connection.close);
    assert!(head.connection.keep_alive);
    assert!(!head.connection.upgrade);

    assert_eq!(
        parse_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close,\r\n\r\n"),
        Err(ParseError::Invalid)
    );
}

#[test]
fn enforces_raw_header_field_limits() {
    let request = b"GET / HTTP/1.1\r\nHost: x\r\nX:       \r\n\r\n";
    let mut parser = Parser::new();
    let mut storage = [Header::default(); 4];
    let limits = Limits {
        field_value_bytes: 4,
        ..Limits::default()
    };
    assert_eq!(
        parser.parse(request, &mut storage, limits),
        Err(ParseError::HeaderSectionTooLarge)
    );

    let partial = b"GET / HTTP/1.1\r\nHost: x\r\nLong-Name";
    parser.reset();
    let limits = Limits {
        field_name_bytes: 4,
        ..Limits::default()
    };
    assert_eq!(
        parser.parse(partial, &mut storage, limits),
        Err(ParseError::HeaderSectionTooLarge)
    );
}

#[test]
fn parses_http_1_1_expectations() {
    let (head, _) =
        parse_all(b"POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\n\r\n").unwrap();
    assert!(head.expect_continue);

    assert_eq!(
        parse_all(b"POST / HTTP/1.1\r\nHost: x\r\nExpect: magic\r\n\r\n"),
        Err(ParseError::ExpectationFailed)
    );
    assert_eq!(
        parse_all(b"POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue,,\r\n\r\n"),
        Err(ParseError::Invalid)
    );
    assert!(parse_all(b"POST / HTTP/1.0\r\nExpect: magic\r\n\r\n").is_ok());
}
#[test]
fn colon_less_line_past_caller_capacity_fails_instead_of_livelocking_resume() {
    




    let input = b"GET / HTTP/1.1\r\nHost: x\r\nNoColonLine\r\n\r\n";
    let mut parser = Parser::new();
    let mut storage = [Header::default(); 1];
    assert_eq!(
        parser.parse(input, &mut storage, Limits::default()),
        Err(ParseError::Invalid)
    );

    

    let input = b"GET / HTTP/1.1\r\n*Host: x\r\nX-A: \t  value withHTTP/0.1\r\n*Host: xx\r\nX-A: \t  value withHTTP/0\r\nX-A spaces\t \r\n\r\n";
    let mut parser = Parser::new();
    let mut small = [Header::default(); 4];
    let _ = parser.parse(&input[..input.len() / 2], &mut small, Limits::default());
    let result = parser.parse(input, &mut small, Limits::default());
    match result {
        Err(ParseError::HeaderCapacity) => {
            assert!(parser.required_header_capacity() > small.len());
        }
        Err(_) => {}
        Ok(_) => panic!("colon-less line must not parse successfully"),
    }
}

#[test]
fn head_error_precedence_pins_shared_decoder_ordering() {
    // NOTE: drift alarm for H1-6 (error precedence): the which-error-wins ordering lives in the
    // shared head-parser scan (request_head/parser.rs), not in either engine's mapper — the
    // managed pump arms (connection/exchange.rs parse_exchange) and the conn-crate map_parse_error
    // (server-conn request_read.rs) are disjoint patterns over a single error value, so a
    // canonical rank function would only relabel ParseError with zero call-site deletion. This
    // corpus pins the decoder verdict each evil input must yield; the per-engine renderings
    // pinned alongside must stay in agreement (managed status <=> conn ServerError):
    // ExpectationFailed <=> 417 / ProtocolError
    // (server_tests unsupported_expectation_rejected_with_417_then_close; server-conn
    // request_read/tests unsupported_expectation_never_admits_or_arms_interim),
    // HeaderSectionTooLarge <=> 431 / ProtocolError
    // (server_tests oversized_header_section_closes_with_431), RequestLineTooLarge with a long
    // target <=> 414 / ProtocolError (server_tests oversized_request_target_answers_414),
    // RequestLineTooLarge on method/version <=> 400 / ProtocolError
    // (server_tests oversized_request_method_stays_400), Invalid <=> 400 / ProtocolError
    // (server_tests malformed_request_line_closes_with_400). The full-buffer Ok(None) row stays
    // a deliberate split: the pump owns INPUT_CAP and attributes 414/400 while the host-driven
    // conn crate returns NeedMore (host owns its buffers).
    fn parse_fresh(input: &[u8]) -> Result<Option<RequestHead>, ParseError> {
        let mut parser = Parser::new();
        let mut storage = [Header::default(); 128];
        parser.parse(input, &mut storage, Limits::default())
    }

    let split = b"GET /health HTTP/1.1\r\nHost: example.test\r\nAcc".to_vec();
    assert_eq!(parse_fresh(&split), Ok(None));

    assert_eq!(
        parse_fresh(b"BAD REQUEST\r\n\r\n"),
        Err(ParseError::Invalid)
    );
    assert_eq!(
        parse_fresh(b"POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\nExpect: magic\r\n\r\n"),
        Err(ParseError::ExpectationFailed)
    );

    let mut section = b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n".to_vec();
    for index in 0..120 {
        section.extend_from_slice(format!("X-Pad-{index:03}: abcdefgh\r\n").as_bytes());
    }
    section.extend_from_slice(b"\r\n");
    assert_eq!(
        parse_fresh(&section),
        Err(ParseError::HeaderSectionTooLarge)
    );

    let target = format!("/{}", "a".repeat(9000));
    let long_target = format!("GET {target} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    assert_eq!(
        parse_fresh(long_target.as_bytes()),
        Err(ParseError::RequestLineTooLarge)
    );
    let method = "A".repeat(9000);
    let long_method = format!("{method} /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    assert_eq!(
        parse_fresh(long_method.as_bytes()),
        Err(ParseError::RequestLineTooLarge)
    );

    // NOTE: combined evil inputs pin the decoder line order both engines inherit: section
    // overflow scanned before the Expect line wins as HeaderSectionTooLarge (managed 431), while
    // an early Expect line wins as ExpectationFailed (managed 417). Neither engine re-decides
    // this; the parser does.
    let mut pads_before_expect = b"GET /hello HTTP/1.1\r\nHost: 127.0.0.1\r\n".to_vec();
    for index in 0..120 {
        pads_before_expect.extend_from_slice(format!("X-Pad-{index:03}: abcdefgh\r\n").as_bytes());
    }
    pads_before_expect.extend_from_slice(b"Expect: magic\r\n\r\n");
    assert_eq!(
        parse_fresh(&pads_before_expect),
        Err(ParseError::HeaderSectionTooLarge)
    );
    let mut expect_before_pads =
        b"POST /upload HTTP/1.1\r\nHost: example.test\r\nExpect: magic\r\n".to_vec();
    for index in 0..120 {
        expect_before_pads.extend_from_slice(format!("X-Pad-{index:03}: abcdefgh\r\n").as_bytes());
    }
    expect_before_pads.extend_from_slice(b"\r\n");
    assert_eq!(
        parse_fresh(&expect_before_pads),
        Err(ParseError::ExpectationFailed)
    );
}
