use crate::{ClientConnection, ClientError, ClientReadKind, Header, RequestMethod, ResponseBody};

fn connection() -> ClientConnection<'static> {
    ClientConnection::new(RequestMethod::Other, b"")
}

#[test]
fn split_head_reports_need_more_then_head_and_reusable_body() {
    let mut connection = connection();
    let input = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc";
    let mut headers = [Header::default(); 8];
    let mut trailers = [Header::default(); 8];
    let prefix = b"HTTP/1.1 200 OK\r\nContent-Len".len();
    let first = connection
        .read_response(&input[..prefix], false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(first.kind, ClientReadKind::NeedMore);
    assert_eq!(first.consumed, 0);
    let head_end = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n".len();
    let head = connection
        .read_response(&input[..head_end], false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(head.kind, ClientReadKind::Head);
    assert_eq!(head.consumed, head_end);
    assert!(!head.can_reuse);
    let parsed = head.head.unwrap();
    assert_eq!(parsed.status, 200);
    assert_eq!(parsed.header_count, 1);
    assert_eq!(parsed.body, ResponseBody::Fixed(3));
    let body = connection
        .read_response(&input[head_end..], false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(body.kind, ClientReadKind::BodyData);
    assert_eq!(
        &input[head_end..][body.body_data.offset..][..body.body_data.length],
        b"abc"
    );
    let done = connection
        .read_response(&[], false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(done.kind, ClientReadKind::Complete);
    assert!(done.can_reuse);
    connection.reset().unwrap();
}

#[test]
fn informational_responses_precede_the_final_head() {
    let mut connection = connection();
    let input = b"HTTP/1.1 103 Early Hints\r\nLink: </style.css>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    let mut headers = [Header::default(); 8];
    let mut trailers = [Header::default(); 8];
    let interim = connection
        .read_response(input, false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(interim.kind, ClientReadKind::Informational);
    assert_eq!(interim.head.unwrap().status, 103);
    let final_head = connection
        .read_response(
            &input[interim.consumed..],
            false,
            &mut headers,
            &mut trailers,
        )
        .unwrap();
    assert_eq!(final_head.kind, ClientReadKind::Head);
    assert_eq!(final_head.head.unwrap().status, 200);
    let done = connection
        .read_response(&[], false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(done.kind, ClientReadKind::Complete);
}

#[test]
fn header_capacity_reports_requirements_and_resumes_on_the_same_base() {
    let mut connection = connection();
    let input = b"HTTP/1.1 200 OK\r\nA: 1\r\nB: 2\r\n\r\n";
    let mut headers = [Header::default(); 8];
    let mut trailers = [Header::default(); 8];
    let short = connection.read_response(input, false, &mut headers[..1], &mut trailers);
    assert_eq!(
        short.unwrap_err(),
        ClientError::HeaderCapacity {
            headers_required: 2,
            trailers_required: 0
        }
    );
    let head = connection
        .read_response(input, false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(head.kind, ClientReadKind::Head);
    assert_eq!(head.head.unwrap().header_count, 2);
}

#[test]
fn close_delimited_body_completes_at_eof_without_reuse() {
    let mut connection = connection();
    let input = b"HTTP/1.1 200 OK\r\n\r\nhello";
    let mut headers = [Header::default(); 8];
    let mut trailers = [Header::default(); 8];
    let head_end = b"HTTP/1.1 200 OK\r\n\r\n".len();
    let head = connection
        .read_response(&input[..head_end], false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(head.kind, ClientReadKind::Head);
    assert_eq!(head.head.unwrap().body, ResponseBody::CloseDelimited);
    let body = connection
        .read_response(&input[head_end..], false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(body.kind, ClientReadKind::BodyData);
    let done = connection
        .read_response(&[], true, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(done.kind, ClientReadKind::Complete);
    assert!(!done.can_reuse);
}

#[test]
fn protocol_errors_stick_and_cancel_wins() {
    let mut failed = connection();
    let mut headers = [Header::default(); 8];
    let mut trailers = [Header::default(); 8];
    assert_eq!(
        failed
            .read_response(b"bogus\r\n\r\n", false, &mut headers, &mut trailers)
            .unwrap_err(),
        ClientError::ProtocolError
    );
    assert_eq!(
        failed
            .read_response(b"", false, &mut headers, &mut trailers)
            .unwrap_err(),
        ClientError::ProtocolError
    );

    let mut cancelled = connection();
    cancelled.cancel();
    assert_eq!(
        cancelled
            .read_response(
                b"HTTP/1.1 200 OK\r\n\r\n",
                false,
                &mut headers,
                &mut trailers
            )
            .unwrap_err(),
        ClientError::Cancelled
    );
}
