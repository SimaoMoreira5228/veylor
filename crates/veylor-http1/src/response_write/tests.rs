use super::{
    ConnectionHeader, FixedResponseBodyWriter, ResponseEncodeError, ResponseFraming,
    ResponseToWrite, ResponseWriteCursor, connection_header_for_response, encode_response_chunk,
    encode_response_chunk_with_required_length, encode_response_final_chunk,
    encode_response_final_chunk_with_required_length, encode_response_head,
};
use crate::request_head::{Header, Limits, Parser};
use crate::request_write::Field;

fn parse_request(input: &[u8]) -> crate::request_head::RequestHead {
    let mut parser = Parser::new();
    let mut headers = [Header::default(); 4];
    parser
        .parse(input, &mut headers, Limits::default())
        .unwrap()
        .unwrap()
}

#[test]
fn encodes_fixed_response_head_into_caller_storage() {
    let fields = [Field {
        name: b"Content-Type",
        value: b"text/plain",
    }];
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 200,
        reason: b"OK",
        fields: &fields,
        framing: ResponseFraming::FixedLength,
        content_length: 5,
        connection: ConnectionHeader::Close,
    };
    let mut output = [0; 128];
    let encoded = encode_response_head(&response, &mut output).unwrap();

    assert!(encoded.body_allowed);
    assert_eq!(
            &output[..encoded.length],
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\nContent-Length: 5\r\n\r\n"
        );
}

#[test]
fn applies_head_status_and_connect_body_rules() {
    let mut output = [0; 128];
    let head = ResponseToWrite {
        request_method: b"HEAD",
        requested_upgrade: b"",
        status: 200,
        reason: b"OK",
        fields: &[],
        framing: ResponseFraming::FixedLength,
        content_length: 123,
        connection: ConnectionHeader::Omit,
    };
    let encoded = encode_response_head(&head, &mut output).unwrap();
    assert!(!encoded.body_allowed);
    assert_eq!(
        &output[..encoded.length],
        b"HTTP/1.1 200 OK\r\nContent-Length: 123\r\n\r\n"
    );

    let no_content = ResponseToWrite {
        status: 204,
        content_length: 0,
        ..head
    };
    let encoded = encode_response_head(&no_content, &mut output).unwrap();
    assert!(!encoded.body_allowed);
    assert_eq!(&output[..encoded.length], b"HTTP/1.1 204 OK\r\n\r\n");

    let tunnel = ResponseToWrite {
        request_method: b"CONNECT",
        status: 200,
        reason: b"Connection Established",
        content_length: 0,
        ..head
    };
    let encoded = encode_response_head(&tunnel, &mut output).unwrap();
    assert!(!encoded.body_allowed);
    assert_eq!(
        &output[..encoded.length],
        b"HTTP/1.1 200 Connection Established\r\n\r\n"
    );

    let not_modified = ResponseToWrite {
        request_method: b"GET",
        status: 304,
        framing: ResponseFraming::Chunked,
        content_length: 0,
        ..head
    };
    let encoded = encode_response_head(&not_modified, &mut output).unwrap();
    assert!(!encoded.body_allowed);
    assert_eq!(
        &output[..encoded.length],
        b"HTTP/1.1 304 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
    );
}

#[test]
fn rejects_invalid_framing_and_leaves_short_buffers_untouched() {
    let mut output = [0xa5; 128];
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 204,
        reason: b"No Content",
        fields: &[],
        framing: ResponseFraming::Chunked,
        content_length: 0,
        connection: ConnectionHeader::Omit,
    };
    assert_eq!(
        encode_response_head(&response, &mut output),
        Err(ResponseEncodeError::Invalid)
    );

    let fixed = ResponseToWrite {
        status: 200,
        framing: ResponseFraming::FixedLength,
        content_length: 5,
        ..response
    };
    assert_eq!(
        encode_response_head(&fixed, &mut output[..8]),
        Err(ResponseEncodeError::BufferTooSmall)
    );
    assert!(output.iter().all(|byte| *byte == 0xa5));

    let fields = [Field {
        name: b"Content-Length",
        value: b"5",
    }];
    let invalid = ResponseToWrite {
        fields: &fields,
        ..fixed
    };
    assert_eq!(
        encode_response_head(&invalid, &mut output),
        Err(ResponseEncodeError::InvalidFramingField)
    );

    let invalid_reason = ResponseToWrite {
        reason: b"OK\r\nInjected: yes",
        ..fixed
    };
    assert_eq!(
        encode_response_head(&invalid_reason, &mut output),
        Err(ResponseEncodeError::Invalid)
    );

    let invalid_value_fields = [Field {
        name: b"X-Test",
        value: b"ok\r\nInjected: yes",
    }];
    let invalid_value = ResponseToWrite {
        fields: &invalid_value_fields,
        ..fixed
    };
    assert_eq!(
        encode_response_head(&invalid_value, &mut output),
        Err(ResponseEncodeError::InvalidField)
    );
}

#[test]
fn close_delimited_responses_require_close_and_no_body_statuses_omit_framing() {
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 200,
        reason: b"OK",
        fields: &[],
        framing: ResponseFraming::CloseDelimited,
        content_length: 0,
        connection: ConnectionHeader::Close,
    };
    let mut output = [0; 128];
    let encoded = encode_response_head(&response, &mut output).unwrap();
    assert!(encoded.body_allowed);
    assert_eq!(
        &output[..encoded.length],
        b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n"
    );

    let invalid_connection = ResponseToWrite {
        connection: ConnectionHeader::Omit,
        ..response
    };
    assert_eq!(
        encode_response_head(&invalid_connection, &mut output),
        Err(ResponseEncodeError::Invalid)
    );

    let informational = ResponseToWrite {
        status: 100,
        reason: b"Continue",
        framing: ResponseFraming::FixedLength,
        connection: ConnectionHeader::Omit,
        ..response
    };
    let encoded = encode_response_head(&informational, &mut output).unwrap();
    assert!(!encoded.body_allowed);
    assert_eq!(&output[..encoded.length], b"HTTP/1.1 100 Continue\r\n\r\n");

    let reset_content = ResponseToWrite {
        status: 205,
        reason: b"Reset Content",
        framing: ResponseFraming::FixedLength,
        ..response
    };
    let encoded = encode_response_head(&reset_content, &mut output).unwrap();
    assert!(!encoded.body_allowed);
    assert_eq!(
        &output[..encoded.length],
        b"HTTP/1.1 205 Reset Content\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    );
}

#[test]
fn encodes_protocol_upgrade_response_without_http_body_framing() {
    let upgrade = [Field {
        name: b"Upgrade",
        value: b"websocket",
    }];
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"websocket",
        status: 101,
        reason: b"Switching Protocols",
        fields: &upgrade,
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        connection: ConnectionHeader::Upgrade,
    };
    let mut output = [0; 128];
    let encoded = encode_response_head(&response, &mut output).unwrap();
    assert!(!encoded.body_allowed);
    assert_eq!(
        &output[..encoded.length],
        b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\r\n"
    );

    let invalid = ResponseToWrite {
        connection: ConnectionHeader::Omit,
        ..response
    };
    assert_eq!(
        encode_response_head(&invalid, &mut output),
        Err(ResponseEncodeError::Invalid)
    );

    let unoffered = ResponseToWrite {
        requested_upgrade: b"h2",
        ..response
    };
    assert_eq!(
        encode_response_head(&unoffered, &mut output),
        Err(ResponseEncodeError::Invalid)
    );
}

#[test]
fn resumes_encoded_head_and_chunk_frames_after_short_writes() {
    let response = ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 200,
        reason: b"OK",
        fields: &[],
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        connection: ConnectionHeader::Omit,
    };
    let mut output = [0; 64];
    let encoded = encode_response_head(&response, &mut output).unwrap();
    let mut cursor = ResponseWriteCursor::new(&output[..encoded.length]);
    assert_eq!(cursor.advance_write(5), Ok(false));
    assert_eq!(cursor.pending(), &output[5..encoded.length]);
    assert_eq!(cursor.advance_write(encoded.length - 5), Ok(true));
    assert!(cursor.is_complete());

    let chunk_length = encode_response_chunk(b"data", &mut output).unwrap();
    let mut cursor = ResponseWriteCursor::new(&output[..chunk_length]);
    assert_eq!(cursor.advance_write(2), Ok(false));
    assert_eq!(cursor.pending(), b"\ndata\r\n");
    assert_eq!(cursor.advance_write(7), Ok(true));
}

#[test]
fn response_write_cursor_poisoning_prevents_reuse_after_overrun() {
    let bytes = b"head";
    let mut cursor = ResponseWriteCursor::new(bytes);
    assert_eq!(
        cursor.advance_write(5),
        Err(ResponseEncodeError::WriteOverrun)
    );
    assert!(cursor.pending().is_empty());
    assert!(!cursor.is_complete());
    assert_eq!(
        cursor.advance_write(1),
        Err(ResponseEncodeError::InvalidState)
    );
}

#[test]
fn streams_borrowed_fixed_body_chunks_across_partial_writes() {
    let mut writer = FixedResponseBodyWriter::new(5, true);
    writer.queue(b"abc").unwrap();
    assert_eq!(writer.pending(), b"abc");
    assert_eq!(writer.advance_write(2), Ok(false));
    assert_eq!(writer.pending(), b"c");
    writer.queue(b"de").unwrap_err();
    assert_eq!(writer.advance_write(1), Ok(false));
    writer.queue(b"de").unwrap();
    assert_eq!(writer.finish(), Ok(false));
    assert_eq!(writer.advance_write(1), Ok(false));
    assert_eq!(writer.pending(), b"e");
    assert_eq!(writer.advance_write(1), Ok(true));
    assert!(writer.finish().unwrap());
}

#[test]
fn fixed_body_writer_rejects_overrun_short_finish_and_suppressed_body() {
    let mut overrun = FixedResponseBodyWriter::new(2, true);
    assert_eq!(overrun.queue(b"abc"), Err(ResponseEncodeError::BodyOverrun));

    let mut short = FixedResponseBodyWriter::new(4, true);
    short.queue(b"abc").unwrap();
    assert_eq!(short.finish(), Ok(false));
    assert_eq!(short.advance_write(3), Err(ResponseEncodeError::ShortBody));

    let mut suppressed = FixedResponseBodyWriter::new(9, false);
    assert!(suppressed.finish().unwrap());
    assert_eq!(
        suppressed.queue(b"forbidden"),
        Err(ResponseEncodeError::InvalidState)
    );
}

#[test]
fn persistence_requires_complete_request_and_self_delimited_response() {
    let request_bytes = b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    let request = parse_request(request_bytes);
    assert_eq!(
        connection_header_for_response(
            request,
            request_bytes,
            200,
            ResponseFraming::FixedLength,
            true,
            false,
        ),
        ConnectionHeader::Omit
    );
    assert_eq!(
        connection_header_for_response(
            request,
            request_bytes,
            200,
            ResponseFraming::FixedLength,
            false,
            false,
        ),
        ConnectionHeader::Close
    );
    assert_eq!(
        connection_header_for_response(
            request,
            request_bytes,
            200,
            ResponseFraming::CloseDelimited,
            true,
            false,
        ),
        ConnectionHeader::Close
    );
    assert_eq!(
        connection_header_for_response(
            request,
            request_bytes,
            200,
            ResponseFraming::FixedLength,
            true,
            true,
        ),
        ConnectionHeader::Close
    );

    let keep_alive_bytes = b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n";
    let keep_alive_request = parse_request(keep_alive_bytes);
    assert_eq!(
        connection_header_for_response(
            keep_alive_request,
            keep_alive_bytes,
            200,
            ResponseFraming::FixedLength,
            true,
            false,
        ),
        ConnectionHeader::KeepAlive
    );

    let close_bytes = b"GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n";
    let close_request = parse_request(close_bytes);
    assert_eq!(
        connection_header_for_response(
            close_request,
            close_bytes,
            200,
            ResponseFraming::FixedLength,
            true,
            false,
        ),
        ConnectionHeader::Close
    );
}

#[test]
fn encodes_chunked_response_data_and_safe_trailers() {
    let mut output = [0; 64];
    let length = encode_response_chunk(b"Wiki", &mut output).unwrap();
    assert_eq!(&output[..length], b"4\r\nWiki\r\n");

    let trailers = [Field {
        name: b"Digest",
        value: b"sha-256=abc",
    }];
    let length = encode_response_final_chunk(&trailers, &mut output).unwrap();
    assert_eq!(&output[..length], b"0\r\nDigest: sha-256=abc\r\n\r\n");

    let forbidden = [Field {
        name: b"Authorization",
        value: b"secret",
    }];
    output.fill(0xa5);
    assert_eq!(
        encode_response_final_chunk(&forbidden, &mut output),
        Err(ResponseEncodeError::InvalidFramingField)
    );
    assert!(output.iter().all(|byte| *byte == 0xa5));
    assert_eq!(
        encode_response_chunk(b"Wiki", &mut output[..4]),
        Err(ResponseEncodeError::BufferTooSmall)
    );
    assert!(output.iter().all(|byte| *byte == 0xa5));
}

#[test]
fn reports_required_lengths_for_chunk_data_and_trailers() {
    let mut output = [0xa5; 16];
    let mut required = usize::MAX;
    assert_eq!(
        encode_response_chunk_with_required_length(b"Wiki", &mut output[..7], &mut required),
        Err(ResponseEncodeError::BufferTooSmall)
    );
    assert_eq!(required, 9);
    assert!(output.iter().all(|byte| *byte == 0xa5));

    let trailers = [Field {
        name: b"Digest",
        value: b"ok",
    }];
    required = usize::MAX;
    assert_eq!(
        encode_response_final_chunk_with_required_length(
            &trailers,
            &mut output[..16],
            &mut required,
        ),
        Err(ResponseEncodeError::BufferTooSmall)
    );
    assert_eq!(required, 17);
    assert!(output.iter().all(|byte| *byte == 0xa5));
}
