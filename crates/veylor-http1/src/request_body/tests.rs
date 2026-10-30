use super::{
    RequestBodyAdmission, RequestBodyDecoder, admission_needs_interim, admission_rejects_body,
};
use crate::body::{BodyEvent, BodyLimits};
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

fn admit_body(decoder: &mut RequestBodyDecoder<'_>) {
    assert_eq!(
        decoder.admit_body(true),
        Ok(RequestBodyAdmission::ReadyToRead)
    );
}

#[test]
fn streams_fixed_request_body_and_preserves_following_bytes() {
    let head_bytes = b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\n";
    let (head, headers) = parse_request(head_bytes);
    let mut decoder = RequestBodyDecoder::new(
        head,
        head_bytes,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    admit_body(&mut decoder);
    let mut trailers = [Header::default(); 4];

    let progress = decoder
        .next(b"helloNEXT", false, &mut trailers, Limits::default())
        .unwrap();
    assert_eq!(progress.consumed, 5);
    assert_eq!(
        progress.event,
        Some(BodyEvent::Data(Span {
            offset: 0,
            length: 5,
        }))
    );
    assert_eq!(
        decoder
            .next(b"NEXT", false, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Complete)
    );
}

#[test]
fn accepts_allowlisted_request_trailers_and_rejects_connection_nominations() {
    let request = b"POST /upload HTTP/1.1\r\nHost: test\r\nConnection: X-Private\r\nTransfer-Encoding: chunked\r\n\r\n";
    let (head, headers) = parse_request(request);
    let allowed = [b"Digest".as_slice(), b"X-Private"];
    let mut decoder = RequestBodyDecoder::new(
        head,
        request,
        &headers[..head.header_count],
        &allowed,
        BodyLimits::default(),
    )
    .unwrap();
    admit_body(&mut decoder);
    let mut trailers = [Header::default(); 4];
    let first = decoder
        .next(
            b"0\r\nDigest: ok\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert_eq!(first.consumed, 3);
    let next = decoder
        .next(
            b"Digest: ok\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert_eq!(next.event, None);
    let trailers_done = decoder
        .next(
            b"Digest: ok\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert_eq!(trailers_done.event, Some(BodyEvent::Trailers { count: 1 }));
    assert_eq!(
        decoder
            .next(b"", false, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Complete)
    );

    let mut decoder = RequestBodyDecoder::new(
        head,
        request,
        &headers[..head.header_count],
        &allowed,
        BodyLimits::default(),
    )
    .unwrap();
    admit_body(&mut decoder);
    assert_eq!(
        decoder.next(
            b"0\r\nX-Private: secret\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        ),
        Ok(crate::body::BodyProgress {
            event: None,
            consumed: 3,
        })
    );
    assert_eq!(
        decoder
            .next(
                b"X-Private: secret\r\n\r\n",
                false,
                &mut trailers,
                Limits::default(),
            )
            .unwrap()
            .event,
        None
    );
    assert_eq!(
        decoder.next(
            b"X-Private: secret\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        ),
        Err(ParseError::Invalid)
    );
    assert_eq!(
        decoder.next(b"", false, &mut trailers, Limits::default()),
        Err(ParseError::Invalid)
    );
}

#[test]
fn delays_expect_continue_body_reads_until_admission_and_continue_write() {
    let request =
        b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n";
    let (head, headers) = parse_request(request);
    assert!(head.expect_continue);
    let mut decoder = RequestBodyDecoder::new(
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    let mut trailers = [Header::default(); 4];
    assert_eq!(
        decoder.next(b"data", false, &mut trailers, Limits::default()),
        Err(ParseError::AdmissionPending)
    );
    assert_eq!(
        decoder.admit_body(true),
        Ok(RequestBodyAdmission::SendContinue)
    );
    assert_eq!(
        decoder.next(b"data", false, &mut trailers, Limits::default()),
        Err(ParseError::AdmissionPending)
    );
    decoder.continue_sent().unwrap();
    assert_eq!(
        decoder
            .next(b"data", false, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Data(Span {
            offset: 0,
            length: 4,
        }))
    );

    let request = b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\n";
    let (head, headers) = parse_request(request);
    let mut decoder = RequestBodyDecoder::new(
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    assert_eq!(
        decoder.admit_body(false),
        Ok(RequestBodyAdmission::Rejected)
    );
    assert_eq!(
        decoder.next(b"data", false, &mut trailers, Limits::default()),
        Err(ParseError::BodyRejected)
    );
}

#[test]
fn requires_a_trailer_allowlist_and_enforces_body_quotas() {
    let request = b"POST /upload HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n";
    let (head, headers) = parse_request(request);
    let mut decoder = RequestBodyDecoder::new(
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    admit_body(&mut decoder);
    let mut trailers = [Header::default(); 4];
    assert_eq!(
        decoder.next(
            b"0\r\nDigest: ok\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        ),
        Ok(crate::body::BodyProgress {
            event: None,
            consumed: 3,
        })
    );
    assert_eq!(
        decoder
            .next(
                b"Digest: ok\r\n\r\n",
                false,
                &mut trailers,
                Limits::default(),
            )
            .unwrap()
            .event,
        None
    );
    assert_eq!(
        decoder.next(
            b"Digest: ok\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        ),
        Err(ParseError::Invalid)
    );

    let (head, headers) =
        parse_request(b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\n");
    let request = b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\n";
    let limits = BodyLimits {
        total_body_bytes: 3,
        ..BodyLimits::default()
    };
    assert_eq!(
        RequestBodyDecoder::new(head, request, &headers[..head.header_count], &[], limits,).err(),
        Some(ParseError::BodyTooLarge)
    );

    let request = b"POST /upload HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n";
    let (head, headers) = parse_request(request);
    let limits = BodyLimits {
        total_body_bytes: 3,
        ..BodyLimits::default()
    };
    let mut decoder =
        RequestBodyDecoder::new(head, request, &headers[..head.header_count], &[], limits).unwrap();
    admit_body(&mut decoder);
    let mut trailers = [Header::default(); 4];
    assert_eq!(
        decoder.next(b"4\r\n", false, &mut trailers, Limits::default()),
        Err(ParseError::BodyTooLarge)
    );

    let limits = BodyLimits {
        extension_bytes: 2,
        ..BodyLimits::default()
    };
    let mut decoder =
        RequestBodyDecoder::new(head, request, &headers[..head.header_count], &[], limits).unwrap();
    admit_body(&mut decoder);
    assert_eq!(
        decoder.next(b"1;x=1\r\n", false, &mut trailers, Limits::default()),
        Err(ParseError::HeaderSectionTooLarge)
    );
}

#[test]
fn rejects_prohibited_request_trailer_fields() {
    let request = b"POST /upload HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n";
    let (head, headers) = parse_request(request);
    let allowed = [b"Content-Length".as_slice()];
    let mut decoder = RequestBodyDecoder::new(
        head,
        request,
        &headers[..head.header_count],
        &allowed,
        BodyLimits::default(),
    )
    .unwrap();
    admit_body(&mut decoder);
    let mut trailers = [Header::default(); 4];
    decoder
        .next(
            b"0\r\nContent-Length: 1\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert_eq!(
        decoder.next(
            b"Content-Length: 1\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        ),
        Err(ParseError::Invalid)
    );
}

#[test]
fn matches_borrowed_trailer_name_spans_without_copying() {
    let request = b"POST /upload HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n";
    let (head, headers) = parse_request(request);
    let allowed_names = b"Digest";
    let allowed_spans = [Span {
        offset: 0,
        length: allowed_names.len(),
    }];
    let mut decoder = RequestBodyDecoder::new_with_trailer_spans(
        head,
        request,
        &headers[..head.header_count],
        allowed_names,
        &allowed_spans,
        BodyLimits::default(),
    )
    .unwrap();
    admit_body(&mut decoder);
    let mut trailers = [Header::default(); 1];

    decoder
        .next(b"0\r\n", false, &mut trailers, Limits::default())
        .unwrap();
    let progress = decoder
        .next(
            b"Digest: ok\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert_eq!(progress.event, None);
    let progress = decoder
        .next(
            b"Digest: ok\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();

    assert_eq!(progress.event, Some(BodyEvent::Trailers { count: 1 }));
}

#[test]
fn retries_trailer_capacity_after_reporting_required_count() {
    let request = b"POST /upload HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n";
    let (head, headers) = parse_request(request);
    let allowed_names = b"Digest";
    let allowed_spans = [Span {
        offset: 0,
        length: allowed_names.len(),
    }];
    let mut decoder = RequestBodyDecoder::new_with_trailer_spans(
        head,
        request,
        &headers[..head.header_count],
        allowed_names,
        &allowed_spans,
        BodyLimits::default(),
    )
    .unwrap();
    admit_body(&mut decoder);
    let mut no_trailers = [];

    let framing = decoder
        .next(
            b"0\r\nDigest: ok\r\n\r\n",
            false,
            &mut no_trailers,
            Limits::default(),
        )
        .unwrap();
    assert_eq!(framing.consumed, 3);

    assert_eq!(
        decoder.next(
            b"Digest: ok\r\n\r\n",
            false,
            &mut no_trailers,
            Limits::default(),
        ),
        Err(ParseError::HeaderCapacity)
    );
    assert_eq!(decoder.required_trailer_capacity(), 1);

    let mut trailers = [Header::default(); 1];
    let progress = decoder
        .next(
            b"Digest: ok\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert_eq!(progress.event, None);
    let progress = decoder
        .next(
            b"Digest: ok\r\n\r\n",
            false,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert_eq!(progress.event, Some(BodyEvent::Trailers { count: 1 }));
}

#[test]
fn rejects_invalid_borrowed_trailer_name_spans() {
    let request = b"POST /upload HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n";
    let (head, headers) = parse_request(request);
    let allowed_spans = [Span {
        offset: 1,
        length: 2,
    }];

    assert_eq!(
        RequestBodyDecoder::new_with_trailer_spans(
            head,
            request,
            &headers[..head.header_count],
            b"x",
            &allowed_spans,
            BodyLimits::default(),
        )
        .err(),
        Some(ParseError::Invalid)
    );
}

#[test]
fn admission_verdicts_match_shared_continue_policy() {
    // NOTE: drift alarm for the converged 100-continue policy: the decoder owns the
    // Expect-to-admission mapping and both engines read admission_needs_interim /
    // admission_rejects_body instead of matching variants locally, so any verdict edit
    // here must update the predicates and both consumers together.
    // Expect with a declared body arms the interim; refusal never does, even with Expect.
    let request =
        b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n";
    let (head, headers) = parse_request(request);
    let mut decoder = RequestBodyDecoder::new(
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    let verdict = decoder.admit_body(true).unwrap();
    assert_eq!(verdict, RequestBodyAdmission::SendContinue);
    assert!(admission_needs_interim(verdict));
    assert!(!admission_rejects_body(verdict));

    let mut decoder = RequestBodyDecoder::new(
        head,
        request,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    let verdict = decoder.admit_body(false).unwrap();
    assert_eq!(verdict, RequestBodyAdmission::Rejected);
    assert!(!admission_needs_interim(verdict));
    assert!(admission_rejects_body(verdict));

    let chunked =
        b"POST /upload HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n";
    let (head, headers) = parse_request(chunked);
    let mut decoder = RequestBodyDecoder::new(
        head,
        chunked,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    let verdict = decoder.admit_body(true).unwrap();
    assert_eq!(verdict, RequestBodyAdmission::SendContinue);
    assert!(admission_needs_interim(verdict));

    let plain = b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\n";
    let (head, headers) = parse_request(plain);
    let mut decoder = RequestBodyDecoder::new(
        head,
        plain,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    let verdict = decoder.admit_body(true).unwrap();
    assert_eq!(verdict, RequestBodyAdmission::ReadyToRead);
    assert!(!admission_needs_interim(verdict));
    assert!(!admission_rejects_body(verdict));

    let bodyless = b"GET /status HTTP/1.1\r\nHost: test\r\nExpect: 100-continue\r\n\r\n";
    let (head, headers) = parse_request(bodyless);
    assert!(head.expect_continue);
    let mut decoder = RequestBodyDecoder::new(
        head,
        bodyless,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    let verdict = decoder.admit_body(true).unwrap();
    assert_eq!(verdict, RequestBodyAdmission::ReadyToRead);
    assert!(!admission_needs_interim(verdict));
    assert!(!admission_rejects_body(verdict));
}
