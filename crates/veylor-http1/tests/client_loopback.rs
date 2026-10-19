use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use veylor_http1::{
    BodyEvent, BodyFraming as RequestBodyFraming, BodyLimits, ClientRequestWriter, ClientResponse,
    ClientResponseEvent, ConnectionHeader, Field, Header, Limits, Parser, Request,
    RequestBodyDecoder, ResponseFraming, ResponseRequestMethod, ResponseToWrite,
    encode_response_head,
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

        read_request(&mut stream, b"POST", b"/one", b"abc");
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
        let mut output = [0; 128];
        let head = encode_response_head(&response, &mut output).unwrap();
        stream.write_all(&output[..head.length]).unwrap();
        stream.write_all(b"ok").unwrap();

        read_request(&mut stream, b"GET", b"/two", b"");
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
        let head = encode_response_head(&response, &mut output).unwrap();
        stream.write_all(&output[..head.length]).unwrap();
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
    let first_request = Request {
        method: b"POST",
        target: b"/one",
        fields: &host,
        body: RequestBodyFraming::Fixed(3),
    };
    send_request(&mut stream, &first_request, Some(b"abc"));

    let mut first_response = ClientResponse::new(ResponseRequestMethod::Other, b"");
    let (status, body) = read_response(&mut stream, &mut first_response);
    assert_eq!(status, 200);
    assert_eq!(body, b"ok");
    assert!(first_response.can_reuse());

    let second_request = Request {
        method: b"GET",
        target: b"/two",
        fields: &host,
        body: RequestBodyFraming::None,
    };
    send_request(&mut stream, &second_request, None);

    let mut second_response = ClientResponse::new(ResponseRequestMethod::Other, b"");
    let (status, body) = read_response(&mut stream, &mut second_response);
    assert_eq!(status, 204);
    assert!(body.is_empty());

    server.join().unwrap();
}

fn send_request(stream: &mut TcpStream, request: &Request<'_>, body: Option<&[u8]>) {
    let mut output = [0; 128];
    let mut writer = ClientRequestWriter::new(request, &mut output).unwrap();
    write_pending(stream, &mut writer);

    if let Some(body) = body {
        let accepted = writer.queue_fixed(body).unwrap();
        assert_eq!(accepted, body.len());
        write_pending(stream, &mut writer);
    }

    assert!(writer.is_complete());
}

fn read_request(
    stream: &mut TcpStream,
    expected_method: &[u8],
    expected_target: &[u8],
    body: &[u8],
) {
    let mut input = [0; 128];
    let mut input_length = 0;
    let mut parser = Parser::new();
    let mut headers = [Header::default(); 8];
    let head = loop {
        assert!(input_length < input.len());
        stream
            .read_exact(&mut input[input_length..input_length + 1])
            .unwrap();
        input_length += 1;
        if let Some(head) = parser
            .parse(&input[..input_length], &mut headers, Limits::default())
            .unwrap()
        {
            break head;
        }
    };
    assert_eq!(head.method(&input[..head.length]), Some(expected_method));
    assert_eq!(head.target(&input[..head.length]), Some(expected_target));

    let request_head = &input[..head.length];
    let mut decoder = RequestBodyDecoder::new(
        head,
        request_head,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    assert_eq!(
        decoder.admit_body(true).unwrap(),
        veylor_http1::RequestBodyAdmission::ReadyToRead
    );

    let mut body_input = [0; 64];
    let mut body_input_length = 0;
    let mut received_body = [0; 64];
    let mut received_body_length = 0;
    let mut trailers = [Header::default(); 4];
    loop {
        let progress = decoder
            .next(
                &body_input[..body_input_length],
                false,
                &mut trailers,
                Limits::default(),
            )
            .unwrap();
        if let Some(BodyEvent::Data(span)) = progress.event {
            let end = span.offset + span.length;
            received_body[received_body_length..received_body_length + span.length]
                .copy_from_slice(&body_input[span.offset..end]);
            received_body_length += span.length;
        }
        if progress.consumed > 0 {
            body_input.copy_within(progress.consumed..body_input_length, 0);
            body_input_length -= progress.consumed;
        }
        if progress.event == Some(BodyEvent::Complete) {
            break;
        }
        if progress.event.is_none() && progress.consumed == 0 {
            let bytes_read = stream.read(&mut body_input[body_input_length..]).unwrap();
            assert!(bytes_read > 0);
            body_input_length += bytes_read;
        }
    }
    assert_eq!(&received_body[..received_body_length], body);
}

fn write_pending(stream: &mut TcpStream, writer: &mut ClientRequestWriter<'_>) {
    while !writer.pending().is_empty() {
        let length = writer.pending().len().min(2);
        let written = stream.write(&writer.pending()[..length]).unwrap();
        writer.advance_write(written).unwrap();
    }
}

fn read_response(stream: &mut TcpStream, response: &mut ClientResponse<'_>) -> (u16, Vec<u8>) {
    let mut input = [0; 128];
    let mut input_length = 0;
    let mut headers = [Header::default(); 8];
    let mut trailers = [Header::default(); 8];
    let mut status = None;
    let mut body = Vec::new();

    loop {
        let progress = response
            .next(
                &input[..input_length],
                false,
                &mut headers,
                &mut trailers,
                Limits::default(),
            )
            .unwrap();

        match progress.event {
            Some(ClientResponseEvent::Head(head)) => status = Some(head.status),
            Some(ClientResponseEvent::Body(BodyEvent::Data(span))) => {
                let end = span.offset + span.length;
                body.extend_from_slice(&input[span.offset..end]);
            }
            Some(ClientResponseEvent::Body(BodyEvent::Complete)) => {
                return (status.unwrap(), body);
            }
            Some(
                ClientResponseEvent::Informational(_)
                | ClientResponseEvent::Body(BodyEvent::Trailers { .. }),
            )
            | None => {}
        }

        if progress.consumed > 0 {
            input.copy_within(progress.consumed..input_length, 0);
            input_length -= progress.consumed;
        }
        if progress.event.is_none() && progress.consumed == 0 {
            let bytes_read = stream.read(&mut input[input_length..]).unwrap();
            assert!(bytes_read > 0);
            input_length += bytes_read;
        }
    }
}
