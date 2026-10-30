use crate::{BodyFraming, ClientConnection, ClientError, Field, RequestMethod};

fn connection() -> ClientConnection<'static> {
    ClientConnection::new(RequestMethod::Other, b"")
}

fn host_fields() -> [Field<'static>; 1] {
    [Field {
        name: b"host",
        value: b"example.test",
    }]
}

#[test]
fn bodiless_request_completes_once_head_is_acknowledged() {
    let mut connection = connection();
    let fields = host_fields();
    let mut output = [0u8; 256];
    let encoded = connection
        .encode_request_head(b"GET", b"/", &fields, BodyFraming::None, &mut output)
        .unwrap();
    assert!(encoded.written > 0);
    assert!(output[..encoded.written].starts_with(b"GET / HTTP/1.1\r\n"));
    assert_eq!(
        connection
            .encode_request_head(b"GET", b"/", &fields, BodyFraming::None, &mut output)
            .unwrap_err(),
        ClientError::BadState
    );
    assert_eq!(
        connection
            .encode_request_body(b"x", &mut output)
            .unwrap_err(),
        ClientError::WouldBlock
    );
    let pending = connection
        .advance_request_write(encoded.written - 1)
        .unwrap();
    assert_eq!(pending, 1);
    assert_eq!(
        connection.advance_request_write(2).unwrap_err(),
        ClientError::WriteOverrun
    );
    assert_eq!(connection.advance_request_write(1).unwrap(), 0);
    assert_eq!(
        connection
            .encode_request_body(b"x", &mut output)
            .unwrap_err(),
        ClientError::BadState
    );
    assert_eq!(
        connection
            .finish_request_body(&[], &mut output)
            .unwrap_err(),
        ClientError::BadState
    );
}

#[test]
fn fixed_body_tracks_remaining_bytes() {
    let mut connection = connection();
    let fields = host_fields();
    let mut output = [0u8; 256];
    let encoded = connection
        .encode_request_head(
            b"POST",
            b"/upload",
            &fields,
            BodyFraming::Fixed(5),
            &mut output,
        )
        .unwrap();
    connection.advance_request_write(encoded.written).unwrap();
    assert_eq!(
        connection
            .encode_request_body(b"abc", &mut output)
            .unwrap()
            .written,
        3
    );
    assert_eq!(
        connection
            .encode_request_body(b"de", &mut output)
            .unwrap_err(),
        ClientError::WouldBlock
    );
    assert_eq!(connection.advance_request_write(3).unwrap(), 0);
    assert_eq!(
        connection
            .encode_request_body(b"def", &mut output)
            .unwrap_err(),
        ClientError::BodyTooLong
    );
    let empty = connection.encode_request_body(b"", &mut output).unwrap();
    assert_eq!((empty.written, empty.required), (0, 0));
    assert_eq!(
        connection.encode_request_body(b"de", &mut []).unwrap_err(),
        ClientError::BufferTooSmall { required: 2 }
    );
    let encoded = connection.encode_request_body(b"de", &mut output).unwrap();
    assert_eq!((encoded.written, encoded.required), (2, 2));
    assert_eq!(&output[..2], b"de");
    connection.advance_request_write(2).unwrap();
    assert_eq!(
        connection
            .encode_request_body(b"x", &mut output)
            .unwrap_err(),
        ClientError::BodyTooLong
    );
}

#[test]
fn chunked_body_round_trips_with_trailers() {
    let mut connection = connection();
    let fields = host_fields();
    let mut output = [0u8; 256];
    let encoded = connection
        .encode_request_head(
            b"PUT",
            b"/stream",
            &fields,
            BodyFraming::Chunked,
            &mut output,
        )
        .unwrap();
    connection.advance_request_write(encoded.written).unwrap();
    assert_eq!(
        connection
            .finish_request_body(&[], &mut [0u8; 2])
            .unwrap_err(),
        ClientError::BufferTooSmall { required: 5 }
    );
    assert_eq!(
        connection
            .encode_request_body(b"", &mut output)
            .unwrap_err(),
        ClientError::InvalidArgument
    );
    let encoded = connection.encode_request_body(b"hi", &mut output).unwrap();
    assert_eq!(&output[..encoded.written], b"2\r\nhi\r\n");
    connection.advance_request_write(encoded.written).unwrap();
    let trailers = [Field {
        name: b"x-sum",
        value: b"42",
    }];
    let finished = connection
        .finish_request_body(&trailers, &mut output)
        .unwrap();
    assert!(finished.written > 0);
    assert_eq!(finished.required, finished.written);
    connection.advance_request_write(finished.written).unwrap();
    assert_eq!(
        connection
            .encode_request_body(b"hi", &mut output)
            .unwrap_err(),
        ClientError::BadState
    );
}

#[test]
fn method_gate_cancel_and_reset_shape_the_write_half() {
    let mut headed = ClientConnection::new(RequestMethod::Head, b"");
    let fields = host_fields();
    let mut output = [0u8; 256];
    assert_eq!(
        headed
            .encode_request_head(b"GET", b"/", &fields, BodyFraming::None, &mut output)
            .unwrap_err(),
        ClientError::InvalidArgument
    );
    headed
        .encode_request_head(b"HEAD", b"/", &fields, BodyFraming::None, &mut output)
        .unwrap();
    assert_eq!(headed.reset().unwrap_err(), ClientError::BadState);
    headed.cancel();
    assert!(headed.is_cancelled());
    assert_eq!(
        headed
            .encode_request_head(b"HEAD", b"/", &fields, BodyFraming::None, &mut output)
            .unwrap_err(),
        ClientError::Cancelled
    );
    assert_eq!(
        headed.advance_request_write(0).unwrap_err(),
        ClientError::Cancelled
    );
    assert_eq!(headed.reset().unwrap_err(), ClientError::Cancelled);

    let mut fresh = connection();
    assert!(!fresh.is_cancelled());
    fresh.reset().unwrap();
    let oversized = [Field {
        name: b"a",
        value: b"b",
    }; crate::MAX_REQUEST_FIELDS + 1];
    assert_eq!(
        fresh
            .encode_request_head(b"GET", b"/", &oversized, BodyFraming::None, &mut output)
            .unwrap_err(),
        ClientError::Capacity
    );
}

#[test]
fn fixed_body_completes_and_allows_reset_for_reuse() {
    let mut conn = connection();
    let fields = host_fields();
    let mut output = [0u8; 256];
    let head = conn
        .encode_request_head(
            b"POST",
            b"/upload",
            &fields,
            BodyFraming::Fixed(3),
            &mut output,
        )
        .unwrap();
    assert_eq!(conn.advance_request_write(head.written), Ok(0));
    let wrote = conn.encode_request_body(b"abc", &mut output).unwrap();
    assert_eq!(wrote.written, 3);
    assert_eq!(conn.advance_request_write(wrote.written), Ok(0));
    // NOTE: the fixed body is fully staged; finish marks the stream complete with no
    // terminal bytes (unlike chunked's zero-chunk), so reset() can park the next exchange.
    let finished = conn.finish_request_body(&[], &mut output).unwrap();
    assert_eq!(finished.written, 0);

    let mut headers = [crate::Header::default(); 8];
    let mut trailers = [crate::Header::default(); 8];
    let input = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    let head_end = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n".len();
    let head = conn
        .read_response(&input[..head_end], false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(head.kind, crate::ClientReadKind::Head);
    let body = conn
        .read_response(&input[..], false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(body.kind, crate::ClientReadKind::BodyData);
    let done = conn
        .read_response(&[], false, &mut headers, &mut trailers)
        .unwrap();
    assert_eq!(done.kind, crate::ClientReadKind::Complete);
    assert!(conn.reset().is_ok());
    assert!(!conn.is_cancelled());
}
