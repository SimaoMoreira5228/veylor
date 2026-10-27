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
