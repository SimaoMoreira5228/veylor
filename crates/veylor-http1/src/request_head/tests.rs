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
