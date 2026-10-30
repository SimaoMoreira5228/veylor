use super::{RequestMethod, ResponseBody, ResponseKind, ResponseParser};
use crate::request_head::{Header, Limits, ParseError};

fn parse_all(
    input: &[u8],
    method: RequestMethod,
) -> Result<(super::ResponseHead, Vec<Header>), ParseError> {
    parse_all_with_upgrade(input, method, b"")
}

fn parse_all_with_upgrade(
    input: &[u8],
    method: RequestMethod,
    requested_upgrade: &[u8],
) -> Result<(super::ResponseHead, Vec<Header>), ParseError> {
    let mut parser = ResponseParser::new(method, requested_upgrade);
    let mut storage = [Header::default(); 8];
    let head = parser
        .parse(input, &mut storage, Limits::default())?
        .ok_or(ParseError::Invalid)?;
    Ok((head, storage[..head.header_count].to_vec()))
}

#[test]
fn parses_response_head_and_selects_body_framing() {
    let input = b"HTTP/1.1 200 OK\r\nContent-Length: 5, 005\r\nX-Test: yes\r\n\r\nhello";
    let (head, headers) = parse_all(input, RequestMethod::Other).unwrap();
    assert_eq!(head.status, 200);
    assert_eq!(head.reason(input), Some(&b"OK"[..]));
    assert_eq!(head.body, ResponseBody::Fixed(5));
    assert!(head.reusable);
    assert_eq!(head.header_count, 2);
    assert_eq!(
        headers[1].name.offset,
        input.windows(6).position(|w| w == b"X-Test").unwrap()
    );

    let mut storage = [0; 96];
    storage[..input.len()].copy_from_slice(input);
    for split in 0..head.end_offset {
        let mut parser = ResponseParser::new(RequestMethod::Other, b"");
        let mut headers = [Header::default(); 8];
        assert_eq!(
            parser.parse(&storage[..split], &mut headers, Limits::default()),
            Ok(None)
        );
        let incremental = parser
            .parse(&storage[..input.len()], &mut headers, Limits::default())
            .unwrap()
            .unwrap();
        assert_eq!(incremental, head);
    }
}

#[test]
fn resumes_after_caller_header_storage_grows() {
    let input = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Test: yes\r\n\r\n";
    let mut parser = ResponseParser::new(RequestMethod::Other, b"");
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
    assert_eq!(head.body, ResponseBody::Fixed(5));
}

#[test]
fn colon_less_line_past_caller_capacity_fails_instead_of_livelocking_resume() {
    let input = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nNoColonLine\r\n\r\n";
    let mut parser = ResponseParser::new(RequestMethod::Other, b"");
    let mut storage = [Header::default(); 1];
    assert_eq!(
        parser.parse(input, &mut storage, Limits::default()),
        Err(ParseError::Invalid)
    );
}

#[test]
fn honors_no_body_and_upgrade_response_rules() {
    let (head, _) = parse_all(
        b"HTTP/1.1 200 OK\r\nContent-Length: 42\r\n\r\n",
        RequestMethod::Head,
    )
    .unwrap();
    assert_eq!(head.body, ResponseBody::None);

    let (head, _) = parse_all(b"HTTP/1.1 200 OK\r\n\r\n", RequestMethod::Other).unwrap();
    assert_eq!(head.body, ResponseBody::CloseDelimited);
    assert!(!head.reusable);

    let (head, _) = parse_all(
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        RequestMethod::Other,
    )
    .unwrap();
    assert!(!head.reusable);

    let (head, _) = parse_all(
        b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n",
        RequestMethod::Other,
    )
    .unwrap();
    assert!(head.reusable);

    let (head, _) = parse_all(b"HTTP/1.1 103 Early Hints\r\n\r\n", RequestMethod::Other).unwrap();
    assert_eq!(head.kind, ResponseKind::Informational);

    let (head, _) = parse_all_with_upgrade(
        b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\n",
        RequestMethod::Other,
        b"websocket",
    )
    .unwrap();
    assert_eq!(head.kind, ResponseKind::Upgrade);
    assert!(!head.reusable);

    let input = b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\nframe";
    let (head, _) = parse_all_with_upgrade(input, RequestMethod::Other, b"websocket").unwrap();
    assert_eq!(head.upgrade_bytes(input), Some(&b"frame"[..]));

    let connect = b"HTTP/1.1 200 Connection Established\r\n\r\ntunnel";
    let (head, _) = parse_all(connect, RequestMethod::Connect).unwrap();
    assert_eq!(head.upgrade_bytes(connect), Some(&b"tunnel"[..]));

    let (head, _) = parse_all(
        b"HTTP/1.1 200 Connection Established\r\n\r\n",
        RequestMethod::Connect,
    )
    .unwrap();
    assert_eq!(head.kind, ResponseKind::Upgrade);
    assert_eq!(head.body, ResponseBody::None);
}

#[test]
fn parses_incremental_heads_and_advances_past_informational_response() {
    let mut input = [0; 128];
    let bytes = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n";
    input[..bytes.len()].copy_from_slice(bytes);
    let mut parser = ResponseParser::new(RequestMethod::Other, b"");
    let mut headers = [Header::default(); 4];

    assert_eq!(
        parser.parse(&input[..12], &mut headers, Limits::default()),
        Ok(None)
    );
    let first = parser
        .parse(&input[..bytes.len()], &mut headers, Limits::default())
        .unwrap()
        .unwrap();
    assert_eq!(first.kind, ResponseKind::Informational);
    parser
        .advance(&input[..bytes.len()], first.end_offset)
        .unwrap();
    let second = parser
        .parse(&input[..bytes.len()], &mut headers, Limits::default())
        .unwrap()
        .unwrap();
    assert_eq!(second.status, 204);
    assert_eq!(second.length, b"HTTP/1.1 204 No Content\r\n\r\n".len());
    assert_eq!(second.end_offset, bytes.len());
    assert_eq!(second.kind, ResponseKind::Final);
    assert_eq!(second.body, ResponseBody::None);
}

#[test]
fn refuses_to_advance_past_a_final_response_head() {
    let input = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndata";
    let mut parser = ResponseParser::new(RequestMethod::Other, b"");
    let mut headers = [Header::default(); 2];
    let head = parser
        .parse(input, &mut headers, Limits::default())
        .unwrap()
        .unwrap();

    assert_eq!(head.kind, ResponseKind::Final);
    assert_eq!(
        parser.advance(input, head.end_offset),
        Err(ParseError::Invalid)
    );
}

#[test]
fn handles_split_crlf_and_rejects_oversized_partial_fields() {
    let mut input = [0; 96];
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    input[..response.len()].copy_from_slice(response);
    let split = b"HTTP/1.1 200 OK\r".len();
    let mut parser = ResponseParser::new(RequestMethod::Other, b"");
    let mut headers = [Header::default(); 4];
    assert_eq!(
        parser.parse(&input[..split], &mut headers, Limits::default()),
        Ok(None)
    );
    assert!(
        parser
            .parse(&input[..response.len()], &mut headers, Limits::default())
            .unwrap()
            .is_some()
    );

    let limits = Limits {
        field_name_bytes: 3,
        ..Limits::default()
    };
    let partial = b"HTTP/1.1 200 OK\r\nLong";
    let mut parser = ResponseParser::new(RequestMethod::Other, b"");
    assert_eq!(
        parser.parse(partial, &mut headers, limits),
        Err(ParseError::HeaderSectionTooLarge)
    );

    let mut malformed = [0; 32];
    let prefix = b"HTTP/1.1 200 OK\r";
    malformed[..prefix.len()].copy_from_slice(prefix);
    let mut parser = ResponseParser::new(RequestMethod::Other, b"");
    assert_eq!(
        parser.parse(&malformed[..prefix.len()], &mut headers, Limits::default()),
        Ok(None)
    );
    malformed[prefix.len()] = b'X';
    assert_eq!(
        parser.parse(
            &malformed[..prefix.len() + 1],
            &mut headers,
            Limits::default()
        ),
        Err(ParseError::Invalid)
    );
}

#[test]
fn rejects_ambiguous_and_invalid_response_framing() {
    for response in [
        &b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
        &b"HTTP/1.0 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
        &b"HTTP/1.1 200 OK\r\nContent-Length: 1, 2\r\n\r\n"[..],
        &b"HTTP/1.1 200 OK\r\nBad Name: x\r\n\r\n"[..],
    ] {
        assert_eq!(
            parse_all(response, RequestMethod::Other),
            Err(ParseError::Invalid)
        );
    }

    let response =
        b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: websocket/13\r\n\r\n";
    assert_eq!(
        parse_all_with_upgrade(response, RequestMethod::Other, b"h2c"),
        Err(ParseError::Invalid)
    );
    assert!(parse_all_with_upgrade(response, RequestMethod::Other, b"WebSocket/13, h2c").is_ok());

    let (head, _) = parse_all(
        b"HTTP/1.1 204 No Content\r\nContent-Length: invalid\r\n\r\n",
        RequestMethod::Other,
    )
    .unwrap();
    assert_eq!(head.body, ResponseBody::None);

    let (head, _) = parse_all(
            b"HTTP/1.1 200 Connection Established\r\nContent-Length: invalid\r\nTransfer-Encoding: other\r\n\r\n",
            RequestMethod::Connect,
        )
        .unwrap();
    assert_eq!(head.kind, ResponseKind::Upgrade);

    for response in [
        &b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n"[..],
        &b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\n\r\n"[..],
        &b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: websocket,\r\n\r\n"
            [..],
    ] {
        assert_eq!(
            parse_all(response, RequestMethod::Other),
            Err(ParseError::Invalid)
        );
    }
}
