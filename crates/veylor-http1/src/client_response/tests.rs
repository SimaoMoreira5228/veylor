use super::{ClientResponse, ClientResponseEvent};
use crate::body::BodyEvent;
use crate::request_head::{Header, Limits, ParseError};
use crate::response_head::RequestMethod;

#[test]
fn sequences_informational_response_head_body_and_reuse() {
    let mut input = [0; 128];
    let bytes = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc";
    input[..bytes.len()].copy_from_slice(bytes);
    let mut length = bytes.len();
    let mut client = ClientResponse::new(RequestMethod::Other, b"");
    let mut headers = [Header::default(); 4];
    let mut trailers = [Header::default(); 4];

    let informational = client
        .next(
            &input[..length],
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    let Some(ClientResponseEvent::Informational(head)) = informational.event else {
        panic!("expected an informational head")
    };
    assert_eq!(head.status, 100);
    input.copy_within(informational.consumed..length, 0);
    length -= informational.consumed;

    let final_head = client
        .next(
            &input[..length],
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    let Some(ClientResponseEvent::Head(head)) = final_head.event else {
        panic!("expected a final response head")
    };
    assert_eq!(head.status, 200);
    input.copy_within(final_head.consumed..length, 0);
    length -= final_head.consumed;
    assert!(!client.can_reuse());

    let data = client
        .next(
            &input[..length],
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert_eq!(
        data.event,
        Some(ClientResponseEvent::Body(BodyEvent::Data(
            crate::request_head::Span {
                offset: 0,
                length: 3,
            }
        )))
    );
    input.copy_within(data.consumed..length, 0);
    length -= data.consumed;

    assert_eq!(length, 0);
    assert_eq!(
        client
            .next(
                &input[..length],
                false,
                &mut headers,
                &mut trailers,
                Limits::default(),
            )
            .unwrap()
            .event,
        Some(ClientResponseEvent::Body(BodyEvent::Complete))
    );
    assert!(client.can_reuse());
}

#[test]
fn fails_closed_after_bad_framing_unexpected_bytes_or_truncation() {
    let mut client = ClientResponse::new(RequestMethod::Other, b"");
    let mut headers = [Header::default(); 4];
    let mut trailers = [Header::default(); 4];
    let malformed = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n";
    assert_eq!(
        client.next(
            malformed,
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        ),
        Err(ParseError::Invalid)
    );
    assert!(!client.can_reuse());
    assert_eq!(
        client.next(b"", false, &mut headers, &mut trailers, Limits::default(),),
        Err(ParseError::Invalid)
    );

    let mut client = ClientResponse::new(RequestMethod::Other, b"");
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\nextra";
    let head = client
        .next(
            response,
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert!(matches!(head.event, Some(ClientResponseEvent::Head(_))));
    assert_eq!(
        client.next(
            b"extra",
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        ),
        Err(ParseError::Invalid)
    );
    assert!(!client.can_reuse());

    let mut client = ClientResponse::new(RequestMethod::Other, b"");
    let head = client
        .next(
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nabc",
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert!(matches!(head.event, Some(ClientResponseEvent::Head(_))));
    let data = client
        .next(b"abc", true, &mut headers, &mut trailers, Limits::default())
        .unwrap();
    assert!(matches!(
        data.event,
        Some(ClientResponseEvent::Body(BodyEvent::Data(_)))
    ));
    assert_eq!(
        client.next(b"", true, &mut headers, &mut trailers, Limits::default(),),
        Err(ParseError::Incomplete)
    );
    assert!(!client.can_reuse());
}

#[test]
fn returns_upgrade_tail_and_never_marks_tunnel_reusable_as_http() {
    let response = b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\nframe";
    let mut client = ClientResponse::new(RequestMethod::Other, b"websocket");
    let mut headers = [Header::default(); 4];
    let mut trailers = [Header::default(); 4];
    let progress = client
        .next(
            response,
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    let Some(ClientResponseEvent::Head(head)) = progress.event else {
        panic!("expected an upgrade head")
    };
    assert_eq!(head.upgrade_bytes(response), Some(&b"frame"[..]));
    assert_eq!(progress.consumed, head.end_offset);
    assert!(client.is_upgraded());
    assert!(!client.can_reuse());
}

#[test]
fn cancellation_during_response_is_terminal_and_prevents_reuse() {
    let mut client = ClientResponse::new(RequestMethod::Other, b"");
    let mut headers = [Header::default(); 4];
    let mut trailers = [Header::default(); 4];
    let head = client
        .next(
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n",
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    assert!(matches!(head.event, Some(ClientResponseEvent::Head(_))));

    client.cancel();
    assert!(!client.can_reuse());
    assert_eq!(
        client.next(
            b"payload",
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        ),
        Err(ParseError::Cancelled)
    );
}

#[test]
fn connection_close_and_eof_prevent_keep_alive_reuse() {
    let mut headers = [Header::default(); 4];
    let mut trailers = [Header::default(); 4];
    let mut close = ClientResponse::new(RequestMethod::Other, b"");
    close
        .next(
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            false,
            &mut headers,
            &mut trailers,
            Limits::default(),
        )
        .unwrap();
    close
        .next(b"", false, &mut headers, &mut trailers, Limits::default())
        .unwrap();
    assert!(!close.can_reuse());

    let mut eof = ClientResponse::new(RequestMethod::Other, b"");
    eof.next(
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        false,
        &mut headers,
        &mut trailers,
        Limits::default(),
    )
    .unwrap();
    eof.next(b"", true, &mut headers, &mut trailers, Limits::default())
        .unwrap();
    assert!(!eof.can_reuse());
}
