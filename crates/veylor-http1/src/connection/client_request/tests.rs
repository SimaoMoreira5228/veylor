use super::ClientRequestDriver;
use crate::connection::{IoCompletion, IoOperation, IoResult, WriteResult, WriteState};
use crate::request_write::{BodyFraming, Field, Request, RequestEncodeError};

fn write_completion(connection_id: u64, operation_id: u64, result: IoResult) -> IoCompletion {
    IoCompletion {
        connection_id,
        operation_id,
        operation: IoOperation::Write,
        result,
    }
}

fn request<'a>(body: BodyFraming, fields: &'a [Field<'a>]) -> Request<'a> {
    Request {
        method: b"POST",
        target: b"/upload",
        fields,
        body,
    }
}

#[test]
fn fixed_request_waits_for_head_and_body_write_completion() {
    let fields = [Field {
        name: b"Host",
        value: b"example.test",
    }];
    let request = request(BodyFraming::Fixed(5), &fields);
    let mut output = [0; 128];
    let mut driver = ClientRequestDriver::new(80, &request, &mut output).unwrap();
    let head_length = driver.pending().len();
    assert!(driver.pending().starts_with(b"POST /upload HTTP/1.1\r\n"));
    assert_eq!(
        driver.queue_fixed(b"hello"),
        Err(RequestEncodeError::OutputPending)
    );

    assert_eq!(
        driver
            .consume(write_completion(
                80,
                1,
                IoResult::Progress {
                    transferred_bytes: 5,
                },
            ))
            .result,
        WriteResult::Progress
    );
    assert_eq!(
        driver
            .consume(write_completion(80, 2, IoResult::WouldBlock))
            .result,
        WriteResult::WouldBlock
    );
    assert_eq!(
        driver.queue_fixed(b"hello"),
        Err(RequestEncodeError::OutputPending)
    );

    assert_eq!(
        driver
            .consume(write_completion(
                80,
                3,
                IoResult::Progress {
                    transferred_bytes: head_length - 5,
                },
            ))
            .result,
        WriteResult::Progress
    );
    assert_eq!(driver.queue_fixed(b"hello"), Ok(5));
    assert_eq!(driver.pending(), b"hello");
    assert_eq!(
        driver
            .consume(write_completion(
                80,
                4,
                IoResult::Progress {
                    transferred_bytes: 2,
                },
            ))
            .result,
        WriteResult::Progress
    );
    assert_eq!(driver.pending(), b"llo");
    assert_eq!(
        driver.queue_fixed(b""),
        Err(RequestEncodeError::OutputPending)
    );
    let event = driver.consume(write_completion(
        80,
        5,
        IoResult::Progress {
            transferred_bytes: 3,
        },
    ));
    assert_eq!(event.result, WriteResult::Complete);
    assert_eq!(driver.state(), WriteState::Complete);
}

#[test]
fn chunked_request_writes_chunks_and_trailers_through_the_same_driver() {
    let fields = [Field {
        name: b"Host",
        value: b"example.test",
    }];
    let request = request(BodyFraming::Chunked, &fields);
    let mut output = [0; 128];
    let mut driver = ClientRequestDriver::new(81, &request, &mut output).unwrap();
    let head_length = driver.pending().len();
    assert_eq!(
        driver
            .consume(write_completion(
                81,
                1,
                IoResult::Progress {
                    transferred_bytes: head_length,
                },
            ))
            .result,
        WriteResult::Progress
    );

    driver.queue_chunk(b"Wiki").unwrap();
    assert_eq!(driver.pending(), b"4\r\nWiki\r\n");
    assert_eq!(
        driver
            .consume(write_completion(
                81,
                2,
                IoResult::Progress {
                    transferred_bytes: 4,
                },
            ))
            .result,
        WriteResult::Progress
    );
    assert_eq!(driver.pending(), b"iki\r\n");
    assert_eq!(
        driver
            .consume(write_completion(
                81,
                3,
                IoResult::Progress {
                    transferred_bytes: 5,
                },
            ))
            .result,
        WriteResult::Progress
    );

    driver
        .finish_chunked(&[Field {
            name: b"Digest",
            value: b"sha-256=x",
        }])
        .unwrap();
    assert_eq!(driver.pending(), b"0\r\nDigest: sha-256=x\r\n\r\n");
    let final_length = driver.pending().len();
    let event = driver.consume(write_completion(
        81,
        4,
        IoResult::Progress {
            transferred_bytes: final_length,
        },
    ));
    assert_eq!(event.result, WriteResult::Complete);
    assert_eq!(driver.state(), WriteState::Complete);
}

#[test]
fn request_write_rejects_stale_completions_and_cancels_cleanly() {
    let fields = [Field {
        name: b"Host",
        value: b"example.test",
    }];
    let request = request(BodyFraming::None, &fields);
    let mut output = [0; 128];
    let mut driver = ClientRequestDriver::new(82, &request, &mut output).unwrap();
    let wrong_connection = write_completion(83, 1, IoResult::WouldBlock);
    assert_eq!(
        driver.consume(wrong_connection).result,
        WriteResult::Invalid
    );
    assert_eq!(driver.state(), WriteState::Failed);

    let mut output = [0; 128];
    let mut driver = ClientRequestDriver::new(82, &request, &mut output).unwrap();
    assert!(!driver.cancel());
    assert_eq!(driver.state(), WriteState::Cancelled);
    assert!(driver.pending().is_empty());
    assert_eq!(
        driver.queue_chunk(b"data"),
        Err(RequestEncodeError::InvalidState)
    );

    let mut output = [0; 128];
    let mut driver = ClientRequestDriver::new(82, &request, &mut output).unwrap();
    assert_eq!(
        driver
            .consume(write_completion(
                82,
                1,
                IoResult::Progress {
                    transferred_bytes: 1,
                },
            ))
            .result,
        WriteResult::Progress
    );
    assert!(driver.cancel());
    assert_eq!(driver.state(), WriteState::Cancelled);
}
