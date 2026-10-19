use super::{BodyDriver, BodyResult, BodyState};
use crate::body::BodyLimits;
use crate::connection::{IoCompletion, IoOperation, IoResult};
use crate::request_body::RequestBodyAdmission;
use crate::request_head::{Header, Limits, ParseError, Parser, RequestHead, Span};

fn parse_request(input: &[u8]) -> (RequestHead, [Header; 8]) {
    let mut parser = Parser::new();
    let mut headers = [Header::default(); 8];
    let head = parser
        .parse(input, &mut headers, Limits::default())
        .unwrap()
        .unwrap();
    (head, headers)
}

fn read_completion(connection_id: u64, operation_id: u64, result: IoResult) -> IoCompletion {
    IoCompletion {
        connection_id,
        operation_id,
        operation: IoOperation::Read,
        result,
    }
}

#[test]
fn streams_fixed_body_and_preserves_pipelined_bytes() {
    let request = b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\n";
    let (head, headers) = parse_request(request);
    let mut driver = BodyDriver::new(
        51,
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    assert_eq!(
        driver.admit_body(true),
        Ok(RequestBodyAdmission::ReadyToRead)
    );

    let body = b"helloNEXT";
    let event = driver.consume(
        read_completion(
            51,
            1,
            IoResult::Progress {
                transferred_bytes: body.len(),
            },
        ),
        body,
        &mut [Header::default(); 4],
        Limits::default(),
    );
    assert_eq!(event.result, BodyResult::Complete);
    assert!(event.body_complete);
    assert_eq!(event.consumed, 5);
    assert_eq!(
        event.span,
        Some(Span {
            offset: 0,
            length: 5,
        })
    );
    assert_eq!(&body[event.consumed..], b"NEXT");
    assert_eq!(driver.state(), BodyState::Complete);
}

#[test]
fn completes_bodyless_requests_without_a_transport_read() {
    let request = b"GET / HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, headers) = parse_request(request);
    let mut driver = BodyDriver::new(
        59,
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    assert_eq!(
        driver.admit_body(true),
        Ok(RequestBodyAdmission::ReadyToRead)
    );
    let event = driver.consume_buffered(b"", &mut [], Limits::default());
    assert_eq!(event.result, BodyResult::Complete);
    assert!(event.body_complete);
    assert_eq!(event.consumed, 0);
}

#[test]
fn chunked_body_yields_data_trailers_and_completion_with_tail_preserved() {
    let request = b"POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n";
    let (head, headers) = parse_request(request);
    let permitted = [b"Digest".as_slice()];
    let mut driver = BodyDriver::new(
        52,
        head,
        request,
        &headers[..head.header_count],
        &permitted,
        BodyLimits::default(),
    )
    .unwrap();
    driver.admit_body(true).unwrap();

    let body = b"4\r\nWiki\r\n0\r\nDigest: sha-256=x\r\n\r\nNEXT";
    let mut trailers = [Header::default(); 4];
    let event = driver.consume(
        read_completion(
            52,
            1,
            IoResult::Progress {
                transferred_bytes: 2,
            },
        ),
        &body[..2],
        &mut trailers,
        Limits::default(),
    );
    assert_eq!(event.result, BodyResult::NeedMore);

    let event = driver.consume(
        read_completion(
            52,
            2,
            IoResult::Progress {
                transferred_bytes: body.len() - 2,
            },
        ),
        body,
        &mut trailers,
        Limits::default(),
    );
    assert_eq!(event.result, BodyResult::Data);
    assert_eq!(
        event.span,
        Some(Span {
            offset: 3,
            length: 4,
        })
    );
    assert_eq!(&body[3..7], b"Wiki");

    let after_data = &body[event.consumed..];
    let event = driver.consume_buffered(after_data, &mut trailers, Limits::default());
    assert_eq!(event.result, BodyResult::NeedMore);
    let after_chunk = &after_data[event.consumed..];

    let event = driver.consume_buffered(after_chunk, &mut trailers, Limits::default());
    assert_eq!(event.result, BodyResult::NeedMore);
    let after_trailer_line = &after_chunk[event.consumed..];

    let event = driver.consume_buffered(after_trailer_line, &mut trailers, Limits::default());
    assert_eq!(event.result, BodyResult::Trailers);
    assert_eq!(event.trailer_count, 1);
    assert_eq!(
        &after_trailer_line
            [trailers[0].name.offset..trailers[0].name.offset + trailers[0].name.length],
        b"Digest"
    );
    assert_eq!(
        &after_trailer_line
            [trailers[0].value.offset..trailers[0].value.offset + trailers[0].value.length],
        b"sha-256=x"
    );
    let tail = &after_trailer_line[event.consumed..];
    assert_eq!(tail, b"NEXT");

    let event = driver.consume_buffered(tail, &mut trailers, Limits::default());
    assert_eq!(event.result, BodyResult::Complete);
    assert!(event.body_complete);
    assert_eq!(event.consumed, 0);
    assert_eq!(driver.state(), BodyState::Complete);
}

#[test]
fn read_ahead_waits_for_route_admission_and_continue_flush() {
    let input =
        b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 3\r\nExpect: 100-continue\r\n\r\nabc";
    let (head, headers) = parse_request(input);
    let mut driver = BodyDriver::new(
        53,
        head,
        input,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    let body = &input[head.length..];
    assert_eq!(
        driver
            .consume_buffered(body, &mut [], Limits::default())
            .result,
        BodyResult::AdmissionPending
    );
    assert_eq!(
        driver.admit_body(true),
        Ok(RequestBodyAdmission::SendContinue)
    );
    assert_eq!(
        driver
            .consume_buffered(body, &mut [], Limits::default())
            .result,
        BodyResult::AdmissionPending
    );
    driver.continue_sent().unwrap();

    let event = driver.consume_buffered(body, &mut [], Limits::default());
    assert_eq!(event.result, BodyResult::Complete);
    assert_eq!(event.consumed, 3);
    assert_eq!(event.span.unwrap().length, 3);
}

#[test]
fn rejects_stale_wrong_and_inconsistent_read_completions() {
    let request = b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\n";
    let (head, headers) = parse_request(request);
    let mut driver = BodyDriver::new(
        54,
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    driver.admit_body(true).unwrap();

    let wrong_connection = read_completion(55, 1, IoResult::WouldBlock);
    assert_eq!(
        driver
            .consume(wrong_connection, b"", &mut [], Limits::default())
            .result,
        BodyResult::Invalid
    );

    let (head, headers) = parse_request(request);
    let mut driver = BodyDriver::new(
        54,
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    driver.admit_body(true).unwrap();
    let mut wrong_operation = read_completion(54, 1, IoResult::WouldBlock);
    wrong_operation.operation = IoOperation::Write;
    assert_eq!(
        driver
            .consume(wrong_operation, b"", &mut [], Limits::default())
            .result,
        BodyResult::Invalid
    );

    let (head, headers) = parse_request(request);
    let mut driver = BodyDriver::new(
        54,
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    driver.admit_body(true).unwrap();
    let partial = b"ab";
    let completion = read_completion(
        54,
        1,
        IoResult::Progress {
            transferred_bytes: partial.len(),
        },
    );
    assert_eq!(
        driver
            .consume(completion, partial, &mut [], Limits::default())
            .result,
        BodyResult::Data
    );
    assert_eq!(
        driver
            .consume(completion, partial, &mut [], Limits::default())
            .result,
        BodyResult::Invalid
    );

    let (head, headers) = parse_request(request);
    let mut driver = BodyDriver::new(
        54,
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    driver.admit_body(true).unwrap();
    assert_eq!(
        driver
            .consume(
                read_completion(
                    54,
                    1,
                    IoResult::Progress {
                        transferred_bytes: 1
                    }
                ),
                b"",
                &mut [],
                Limits::default(),
            )
            .result,
        BodyResult::Invalid
    );
}

#[test]
fn reports_incomplete_eof_and_admission_rejection_without_reading() {
    let request = b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\n";
    let (head, headers) = parse_request(request);
    let mut driver = BodyDriver::new(
        56,
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    assert_eq!(driver.admit_body(false), Ok(RequestBodyAdmission::Rejected));
    assert_eq!(driver.state(), BodyState::Rejected);
    assert_eq!(
        driver
            .consume(
                read_completion(
                    56,
                    1,
                    IoResult::Progress {
                        transferred_bytes: 1
                    }
                ),
                b"x",
                &mut [],
                Limits::default(),
            )
            .result,
        BodyResult::Rejected
    );

    let (head, headers) = parse_request(request);
    let mut driver = BodyDriver::new(
        57,
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    driver.admit_body(true).unwrap();
    let event = driver.consume(
        read_completion(57, 1, IoResult::EndOfStream),
        b"",
        &mut [],
        Limits::default(),
    );
    assert_eq!(event.result, BodyResult::PeerClosed);
    assert_eq!(driver.state(), BodyState::Failed);

    let (head, headers) = parse_request(request);
    let mut driver = BodyDriver::new(
        60,
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    driver.admit_body(true).unwrap();
    assert_eq!(
        driver
            .consume(
                read_completion(60, 1, IoResult::Cancelled),
                b"",
                &mut [],
                Limits::default(),
            )
            .result,
        BodyResult::Cancelled
    );
    assert_eq!(driver.state(), BodyState::Cancelled);
}

#[test]
fn rejects_a_request_head_extent_outside_its_input() {
    let request = b"POST / HTTP/1.1\r\nHost: test\r\n\r\n";
    let (mut head, headers) = parse_request(request);
    head.length += 1;
    assert_eq!(
        BodyDriver::new(
            58,
            head,
            request,
            &headers[..head.header_count],
            &[],
            BodyLimits::default(),
        )
        .err(),
        Some(ParseError::Invalid)
    );
}
