use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use veylor_http1::{
    BodyDriver, BodyEvent, BodyFraming as RequestBodyFraming, BodyLimits, BodyResult,
    ClientReadResult, ClientRequestDriver, ClientResponseDriver, ClientResponseEvent,
    ConnectionHeader, Field, HeadDriver, HeadResult, Header, IoCompletion, IoOperation, IoResult,
    Limits, Request, RequestBodyAdmission, ResponseFraming, ResponseRequestMethod, ResponseToWrite,
    ResponseWriteDriver, WriteState,
};

#[test]
fn sends_two_requests_and_reads_two_responses_on_one_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let mut output = [0; 128];
        let mut operation_id = 0;
        read_request(&mut stream, 2, &mut operation_id, b"POST", b"/one", b"abc");
        {
            let response = ResponseToWrite {
                request_method: b"POST",
                requested_upgrade: b"",
                status: 200,
                reason: b"OK",
                fields: &[],
                framing: ResponseFraming::FixedLength,
                content_length: 2,
                connection: ConnectionHeader::Omit,
            };
            let mut writer = ResponseWriteDriver::new(2, response, &mut output).unwrap();
            write_response_pending(&mut stream, 2, &mut operation_id, &mut writer);
            writer.queue_fixed(b"ok").unwrap();
            write_response_pending(&mut stream, 2, &mut operation_id, &mut writer);
            assert_eq!(writer.state(), WriteState::Complete);
        }

        read_request(&mut stream, 2, &mut operation_id, b"GET", b"/two", b"");
        let response = ResponseToWrite {
            request_method: b"GET",
            requested_upgrade: b"",
            status: 204,
            reason: b"No Content",
            fields: &[],
            framing: ResponseFraming::FixedLength,
            content_length: 0,
            connection: ConnectionHeader::Omit,
        };
        let mut writer = ResponseWriteDriver::new(2, response, &mut output).unwrap();
        write_response_pending(&mut stream, 2, &mut operation_id, &mut writer);
        assert_eq!(writer.state(), WriteState::Complete);
    });

    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let host = [Field {
        name: b"Host",
        value: b"local.test",
    }];
    let mut operation_id = 0;
    let first_request = Request {
        method: b"POST",
        target: b"/one",
        fields: &host,
        body: RequestBodyFraming::Fixed(3),
    };
    send_request(
        &mut stream,
        1,
        &mut operation_id,
        &first_request,
        Some(b"abc"),
    );

    let (status, body, reusable) = read_response(&mut stream, 1, &mut operation_id);
    assert_eq!(status, 200);
    assert_eq!(body, b"ok");
    assert!(reusable);

    let second_request = Request {
        method: b"GET",
        target: b"/two",
        fields: &host,
        body: RequestBodyFraming::None,
    };
    send_request(&mut stream, 1, &mut operation_id, &second_request, None);

    let (status, body, _) = read_response(&mut stream, 1, &mut operation_id);
    assert_eq!(status, 204);
    assert!(body.is_empty());

    server.join().unwrap();
}

fn send_request(
    stream: &mut TcpStream,
    connection_id: u64,
    operation_id: &mut u64,
    request: &Request<'_>,
    body: Option<&[u8]>,
) {
    let mut output = [0; 128];
    let mut writer = ClientRequestDriver::new(connection_id, request, &mut output).unwrap();
    write_pending(stream, connection_id, operation_id, &mut writer);

    if let Some(body) = body {
        let accepted = writer.queue_fixed(body).unwrap();
        assert_eq!(accepted, body.len());
        write_pending(stream, connection_id, operation_id, &mut writer);
    }

    assert_eq!(writer.state(), WriteState::Complete);
}

fn read_request(
    stream: &mut TcpStream,
    connection_id: u64,
    operation_id: &mut u64,
    expected_method: &[u8],
    expected_target: &[u8],
    body: &[u8],
) {
    let mut input = [0; 128];
    let mut input_length = 0;
    let mut headers = [Header::default(); 8];
    let mut head_driver = HeadDriver::new(connection_id);
    let head = loop {
        assert!(input_length < input.len());
        let bytes_read = stream
            .read(&mut input[input_length..input_length + 1])
            .unwrap();
        assert_eq!(bytes_read, 1);
        input_length += 1;
        *operation_id += 1;
        let event = head_driver.consume(
            IoCompletion {
                connection_id,
                operation_id: *operation_id,
                operation: IoOperation::Read,
                result: IoResult::Progress {
                    transferred_bytes: bytes_read,
                },
            },
            &input[..input_length],
            &mut headers,
            Limits::default(),
        );
        match event.result {
            HeadResult::Ready => break event.head.unwrap(),
            HeadResult::NeedMore => {}
            result => panic!("unexpected request-head result: {result:?}"),
        }
    };
    assert_eq!(head.method(&input[..head.length]), Some(expected_method));
    assert_eq!(head.target(&input[..head.length]), Some(expected_target));

    let mut decoder = BodyDriver::new(
        connection_id,
        head,
        &input[..head.length],
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    assert_eq!(
        decoder.admit_body(true),
        Ok(RequestBodyAdmission::ReadyToRead)
    );

    let mut body_input = [0; 64];
    let mut body_input_length = 0;
    let mut received_body = [0; 64];
    let mut received_body_length = 0;
    let mut trailers = [Header::default(); 4];
    let mut completion = None;
    loop {
        let event = if let Some(completion) = completion.take() {
            decoder.consume(
                completion,
                &body_input[..body_input_length],
                &mut trailers,
                Limits::default(),
            )
        } else {
            decoder.consume_buffered(
                &body_input[..body_input_length],
                &mut trailers,
                Limits::default(),
            )
        };
        if let Some(span) = event.span {
            let end = span.offset + span.length;
            received_body[received_body_length..received_body_length + span.length]
                .copy_from_slice(&body_input[span.offset..end]);
            received_body_length += span.length;
        }
        if event.consumed > 0 {
            body_input.copy_within(event.consumed..body_input_length, 0);
            body_input_length -= event.consumed;
        }
        if event.result == BodyResult::Complete {
            break;
        }
        if event.result == BodyResult::NeedMore {
            let bytes_read = stream.read(&mut body_input[body_input_length..]).unwrap();
            *operation_id += 1;
            let result = if bytes_read == 0 {
                IoResult::EndOfStream
            } else {
                body_input_length += bytes_read;
                IoResult::Progress {
                    transferred_bytes: bytes_read,
                }
            };
            completion = Some(IoCompletion {
                connection_id,
                operation_id: *operation_id,
                operation: IoOperation::Read,
                result,
            });
        }
        assert_ne!(event.result, BodyResult::Invalid);
        assert_ne!(event.result, BodyResult::PeerClosed);
        assert_ne!(event.result, BodyResult::TooLarge);
    }
    assert_eq!(&received_body[..received_body_length], body);
}

fn write_pending(
    stream: &mut TcpStream,
    connection_id: u64,
    operation_id: &mut u64,
    writer: &mut ClientRequestDriver<'_>,
) {
    while !writer.pending().is_empty() {
        let length = writer.pending().len().min(2);
        let written = stream.write(&writer.pending()[..length]).unwrap();
        assert!(written > 0);
        *operation_id += 1;
        let event = writer.consume(IoCompletion {
            connection_id,
            operation_id: *operation_id,
            operation: IoOperation::Write,
            result: IoResult::Progress {
                transferred_bytes: written,
            },
        });
        assert_eq!(event.accepted_bytes, written);
    }
}

fn write_response_pending(
    stream: &mut TcpStream,
    connection_id: u64,
    operation_id: &mut u64,
    writer: &mut ResponseWriteDriver<'_, '_>,
) {
    while !writer.pending().is_empty() {
        let length = writer.pending().len().min(2);
        let written = stream.write(&writer.pending()[..length]).unwrap();
        assert!(written > 0);
        *operation_id += 1;
        let event = writer.consume(IoCompletion {
            connection_id,
            operation_id: *operation_id,
            operation: IoOperation::Write,
            result: IoResult::Progress {
                transferred_bytes: written,
            },
        });
        assert_eq!(event.accepted_bytes, written);
    }
}

fn read_response(
    stream: &mut TcpStream,
    connection_id: u64,
    operation_id: &mut u64,
) -> (u16, Vec<u8>, bool) {
    let mut input = [0; 128];
    let mut input_length = 0;
    let mut headers = [Header::default(); 8];
    let mut trailers = [Header::default(); 8];
    let mut status = None;
    let mut body = Vec::new();
    let mut driver = ClientResponseDriver::new(
        connection_id,
        ResponseRequestMethod::Other,
        b"",
        BodyLimits::default(),
    );
    let mut completion = None;

    loop {
        let event = if let Some(completion) = completion.take() {
            driver.consume(
                completion,
                &input[..input_length],
                &mut headers,
                &mut trailers,
                Limits::default(),
            )
        } else {
            driver.consume_buffered(
                &input[..input_length],
                &mut headers,
                &mut trailers,
                Limits::default(),
            )
        };

        record_response(event.response, &input, &mut status, &mut body);
        if event.result == ClientReadResult::Complete {
            return (status.unwrap(), body, driver.can_reuse());
        }

        if event.consumed > 0 {
            input.copy_within(event.consumed..input_length, 0);
            input_length -= event.consumed;
        }
        if event.result == ClientReadResult::NeedMore {
            let bytes_read = stream.read(&mut input[input_length..]).unwrap();
            *operation_id += 1;
            let result = if bytes_read == 0 {
                IoResult::EndOfStream
            } else {
                input_length += bytes_read;
                IoResult::Progress {
                    transferred_bytes: bytes_read,
                }
            };
            completion = Some(IoCompletion {
                connection_id,
                operation_id: *operation_id,
                operation: IoOperation::Read,
                result,
            });
        }
        assert_ne!(event.result, ClientReadResult::Invalid);
        assert_ne!(event.result, ClientReadResult::Incomplete);
        assert_ne!(event.result, ClientReadResult::TooLarge);
    }
}

fn record_response(
    event: Option<ClientResponseEvent>,
    input: &[u8],
    status: &mut Option<u16>,
    body: &mut Vec<u8>,
) {
    match event {
        Some(ClientResponseEvent::Head(head)) => *status = Some(head.status),
        Some(ClientResponseEvent::Body(BodyEvent::Data(span))) => {
            let end = span.offset + span.length;
            body.extend_from_slice(&input[span.offset..end]);
        }
        Some(ClientResponseEvent::Informational(_))
        | Some(ClientResponseEvent::Body(BodyEvent::Trailers { .. }))
        | Some(ClientResponseEvent::Body(BodyEvent::Complete))
        | None => {}
    }
}
