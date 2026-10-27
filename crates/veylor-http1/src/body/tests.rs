use super::{BodyDecoder, BodyEvent};
use crate::request_head::{Header, Limits, ParseError};
use crate::response_head::ResponseBody;

#[test]
fn streams_fixed_and_close_delimited_content() {
    let mut trailers = [Header::default(); 4];
    let mut partial = BodyDecoder::new(ResponseBody::Fixed(4));
    assert_eq!(
        partial
            .next(b"ab", false, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Data(crate::request_head::Span {
            offset: 0,
            length: 2,
        }))
    );
    assert_eq!(
        partial
            .next(b"cdNEXT", false, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Data(crate::request_head::Span {
            offset: 0,
            length: 2,
        }))
    );
    assert_eq!(
        partial
            .next(b"NEXT", false, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Complete)
    );

    let mut fixed = BodyDecoder::new(ResponseBody::Fixed(3));
    let progress = fixed
        .next(b"abcNEXT", false, &mut trailers, Limits::default())
        .unwrap();
    assert_eq!(progress.consumed, 3);
    assert_eq!(
        progress.event,
        Some(BodyEvent::Data(crate::request_head::Span {
            offset: 0,
            length: 3,
        }))
    );
    assert_eq!(
        fixed
            .next(b"NEXT", false, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Complete)
    );

    let mut close = BodyDecoder::new(ResponseBody::CloseDelimited);
    assert_eq!(
        close
            .next(b"abc", false, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Data(crate::request_head::Span {
            offset: 0,
            length: 3,
        }))
    );
    assert_eq!(
        close
            .next(b"de", true, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Data(crate::request_head::Span {
            offset: 0,
            length: 2,
        }))
    );
    assert_eq!(
        close
            .next(b"", true, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Complete)
    );
}

#[test]
fn decodes_chunked_content_and_trailers_across_single_byte_feeds() {
    let wire = b"3;foo=\"bar\"\r\nabc\r\n0\r\nDigest: ok\r\n\r\n";
    let mut decoder = BodyDecoder::new(ResponseBody::Chunked);
    let mut input = [0; 96];
    let mut input_length = 0;
    let mut trailers = [Header::default(); 4];
    let mut body = [0; 3];
    let mut body_length = 0;
    let mut complete = false;

    for byte in wire {
        input[input_length] = *byte;
        input_length += 1;
        loop {
            let progress = decoder
                .next(
                    &input[..input_length],
                    false,
                    &mut trailers,
                    Limits::default(),
                )
                .unwrap();
            if let Some(BodyEvent::Data(span)) = progress.event {
                let end = span.offset + span.length;
                body[body_length..body_length + span.length]
                    .copy_from_slice(&input[span.offset..end]);
                body_length += span.length;
            }
            if let Some(BodyEvent::Trailers { count }) = progress.event {
                assert_eq!(count, 1);
                assert_eq!(
                    &input[trailers[0].name.offset
                        ..trailers[0].name.offset + trailers[0].name.length],
                    b"Digest"
                );
                assert_eq!(
                    &input[trailers[0].value.offset
                        ..trailers[0].value.offset + trailers[0].value.length],
                    b"ok"
                );
            }
            if progress.event == Some(BodyEvent::Complete) {
                complete = true;
            }
            if progress.consumed > 0 {
                input.copy_within(progress.consumed..input_length, 0);
                input_length -= progress.consumed;
            }
            if complete || (progress.event.is_none() && progress.consumed == 0) {
                break;
            }
        }
        if complete {
            break;
        }
    }

    assert!(complete);
    assert_eq!(&body[..body_length], b"abc");
}

#[test]
fn rejects_bad_chunk_framing_trailers_and_truncated_bodies() {
    let mut trailers = [Header::default(); 4];
    let mut decoder = BodyDecoder::new(ResponseBody::Chunked);
    assert_eq!(
        decoder.next(
            b"10000000000000000\r\n",
            false,
            &mut trailers,
            Limits::default()
        ),
        Err(ParseError::Invalid)
    );

    let mut decoder = BodyDecoder::new(ResponseBody::Chunked);
    assert_eq!(
        decoder.next(
            b"0\r\nContent-Length: 0\r\n\r\n",
            false,
            &mut trailers,
            Limits::default()
        ),
        Ok(super::BodyProgress {
            event: None,
            consumed: 3,
        })
    );
    assert_eq!(
        decoder.next(
            b"Content-Length: 0\r\n\r\n",
            false,
            &mut trailers,
            Limits::default()
        ),
        Err(ParseError::Invalid)
    );

    let mut fixed = BodyDecoder::new(ResponseBody::Fixed(4));
    assert_eq!(
        fixed
            .next(b"abc", true, &mut trailers, Limits::default())
            .unwrap()
            .event,
        Some(BodyEvent::Data(crate::request_head::Span {
            offset: 0,
            length: 3,
        }))
    );
    assert_eq!(
        fixed.next(b"", true, &mut trailers, Limits::default()),
        Err(ParseError::Incomplete)
    );

    let mut chunked = BodyDecoder::new(ResponseBody::Chunked);
    let progress = chunked
        .next(b"1\r\na", false, &mut trailers, Limits::default())
        .unwrap();
    assert_eq!(progress.consumed, 4);
    assert!(matches!(progress.event, Some(BodyEvent::Data(_))));
    assert_eq!(
        chunked.next(b"", true, &mut trailers, Limits::default()),
        Err(ParseError::Incomplete)
    );
}

#[test]
fn request_trailer_prohibited_set_is_exactly_the_fifteen() {
    // NOTE: drift alarm for the converged trailer policy: the managed H1 response-terminal
    // stage and the H2 request-trailer gate import this predicate, so any membership edit
    // here changes both servers and must fail this test first.
    use crate::prohibited_request_trailer;
    const PROHIBITED: [&[u8]; 15] = [
        b"authorization",
        b"connection",
        b"content-encoding",
        b"content-length",
        b"content-range",
        b"content-type",
        b"expect",
        b"host",
        b"keep-alive",
        b"proxy-authorization",
        b"proxy-connection",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
    ];
    for name in PROHIBITED {
        assert!(prohibited_request_trailer(name), "must refuse {name:?}");
        let mut upper = name.to_vec();
        upper.make_ascii_uppercase();
        assert!(
            prohibited_request_trailer(&upper),
            "must refuse case-insensitively {upper:?}"
        );
    }
    for name in [
        b"x-checksum".as_slice(),
        b"digest",
        b"content-lengthx",
        b"x-te",
        b"",
    ] {
        assert!(!prohibited_request_trailer(name), "must allow {name:?}");
    }
}

#[test]
fn decoder_default_trailer_caps_stay_wider_than_staged_caps() {
    // NOTE: pins the deliberate non-unification: the decoder admits up to 32 trailers /
    // 8 KiB while host staging fails first at 8 / 1 KiB (veylor-server parked bounds).
    assert_eq!(crate::BodyLimits::DEFAULT.trailer_count, 32);
    assert_eq!(crate::BodyLimits::DEFAULT.trailer_bytes, 8 * 1024);
}
