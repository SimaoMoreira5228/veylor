use super::{
    HeadDriver, HeadResult, HeadState, IoCompletion, IoOperation, IoResult, WriteDriver,
    WriteResult, WriteState,
};

fn completion(operation_id: u64, result: IoResult) -> IoCompletion {
    IoCompletion {
        connection_id: 7,
        operation_id,
        operation: IoOperation::Write,
        result,
    }
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
fn request_head_reads_accumulate_input_and_preserve_read_ahead() {
    let mut driver = HeadDriver::new(12);
    let mut input = [0; 128];
    let mut headers = [crate::Header::default(); 4];
    let first = b"GET / HT";
    input[..first.len()].copy_from_slice(first);

    let event = driver.consume(
        read_completion(
            12,
            1,
            IoResult::Progress {
                transferred_bytes: first.len(),
            },
        ),
        &input[..first.len()],
        &mut headers,
        crate::Limits::default(),
    );
    assert_eq!(event.result, HeadResult::NeedMore);

    let tail = b"TP/1.1\r\nHost: example.test\r\n\r\nbody";
    input[first.len()..first.len() + tail.len()].copy_from_slice(tail);
    let input_length = first.len() + tail.len();
    let event = driver.consume(
        read_completion(
            12,
            2,
            IoResult::Progress {
                transferred_bytes: tail.len(),
            },
        ),
        &input[..input_length],
        &mut headers,
        crate::Limits::default(),
    );
    let head = event.head.unwrap();
    assert_eq!(event.result, HeadResult::Ready);
    assert_eq!(head.length, input_length - b"body".len());
    assert_eq!(&input[head.length..input_length], b"body");
}

#[test]
fn request_head_read_checks_completion_identity_and_byte_count() {
    let mut driver = HeadDriver::new(21);
    let mut headers = [crate::Header::default(); 2];
    let wrong_connection = read_completion(22, 1, IoResult::WouldBlock);
    assert_eq!(
        driver
            .consume(
                wrong_connection,
                b"",
                &mut headers,
                crate::Limits::default(),
            )
            .result,
        HeadResult::Invalid
    );

    let mut driver = HeadDriver::new(21);
    let wrong_operation = IoCompletion {
        operation: IoOperation::Write,
        ..read_completion(21, 1, IoResult::WouldBlock)
    };
    assert_eq!(
        driver
            .consume(wrong_operation, b"", &mut headers, crate::Limits::default(),)
            .result,
        HeadResult::Invalid
    );

    let mut driver = HeadDriver::new(21);
    assert_eq!(
        driver
            .consume(
                read_completion(
                    21,
                    1,
                    IoResult::Progress {
                        transferred_bytes: 1,
                    },
                ),
                b"",
                &mut headers,
                crate::Limits::default(),
            )
            .result,
        HeadResult::Invalid
    );
}

#[test]
fn request_head_read_preserves_backpressure_and_rejects_partial_eof() {
    let mut driver = HeadDriver::new(30);
    let mut headers = [crate::Header::default(); 2];
    assert_eq!(
        driver
            .consume(
                read_completion(30, 1, IoResult::WouldBlock),
                b"",
                &mut headers,
                crate::Limits::default(),
            )
            .result,
        HeadResult::WouldBlock
    );
    assert_eq!(driver.state(), HeadState::Reading);

    let input = b"GET / HTTP/1.1\r\nHost:";
    assert_eq!(
        driver
            .consume(
                read_completion(
                    30,
                    2,
                    IoResult::Progress {
                        transferred_bytes: input.len(),
                    },
                ),
                input,
                &mut headers,
                crate::Limits::default(),
            )
            .result,
        HeadResult::NeedMore
    );
    assert_eq!(
        driver
            .consume(
                read_completion(30, 3, IoResult::EndOfStream),
                input,
                &mut headers,
                crate::Limits::default(),
            )
            .result,
        HeadResult::Invalid
    );
    assert_eq!(driver.state(), HeadState::Failed);
}

#[test]
fn request_head_read_reports_clean_eof_and_cancellation() {
    let mut headers = [crate::Header::default(); 2];
    let mut driver = HeadDriver::new(35);
    assert_eq!(
        driver
            .consume(
                read_completion(35, 1, IoResult::EndOfStream),
                b"",
                &mut headers,
                crate::Limits::default(),
            )
            .result,
        HeadResult::PeerClosed
    );
    assert_eq!(driver.state(), HeadState::Closed);

    let mut driver = HeadDriver::new(36);
    assert_eq!(
        driver
            .consume(
                read_completion(36, 1, IoResult::Cancelled),
                b"",
                &mut headers,
                crate::Limits::default(),
            )
            .result,
        HeadResult::Cancelled
    );
    assert_eq!(driver.state(), HeadState::Cancelled);
}

#[test]
fn ready_request_head_keeps_the_original_header_storage_contract() {
    let request = b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    let mut driver = HeadDriver::new(40);
    let mut headers = [crate::Header::default(); 2];
    let event = driver.consume(
        read_completion(
            40,
            1,
            IoResult::Progress {
                transferred_bytes: request.len(),
            },
        ),
        request,
        &mut headers,
        crate::Limits::default(),
    );
    assert_eq!(event.result, HeadResult::Ready);
    assert_eq!(event.head.unwrap().header_count, 1);

    let event = driver.consume(
        read_completion(40, 2, IoResult::WouldBlock),
        request,
        &mut [crate::Header::default(); 2],
        crate::Limits::default(),
    );
    assert_eq!(event.result, HeadResult::BufferChanged);
    assert_eq!(headers[0].name.length, 4);
}

#[test]
fn request_head_read_returns_parser_limits_and_transport_errors() {
    let request = b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    let mut headers = [crate::Header::default(); 2];
    let mut driver = HeadDriver::new(41);
    let limits = crate::Limits {
        request_line_bytes: b"GET / HTTP/1.1".len() - 1,
        ..crate::Limits::default()
    };
    assert_eq!(
        driver
            .consume(
                read_completion(
                    41,
                    1,
                    IoResult::Progress {
                        transferred_bytes: request.len(),
                    },
                ),
                request,
                &mut headers,
                limits,
            )
            .result,
        HeadResult::RequestLineTooLarge
    );

    let mut driver = HeadDriver::new(42);
    let event = driver.consume(
        read_completion(42, 1, IoResult::Error { code: 5 }),
        b"",
        &mut headers,
        crate::Limits::default(),
    );
    assert_eq!(event.result, HeadResult::TransportError);
    assert_eq!(event.error_code, 5);
    assert_eq!(driver.state(), HeadState::Failed);
}

#[test]
fn request_head_read_maps_capacity_expectation_and_input_contract_errors() {
    let request = b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    let mut driver = HeadDriver::new(50);
    assert_eq!(
        driver
            .consume(
                read_completion(
                    50,
                    1,
                    IoResult::Progress {
                        transferred_bytes: request.len(),
                    },
                ),
                request,
                &mut [],
                crate::Limits::default(),
            )
            .result,
        HeadResult::HeaderCapacity
    );

    let unsupported_expectation = b"GET / HTTP/1.1\r\nHost: example.test\r\nExpect: magic\r\n\r\n";
    let mut driver = HeadDriver::new(51);
    assert_eq!(
        driver
            .consume(
                read_completion(
                    51,
                    1,
                    IoResult::Progress {
                        transferred_bytes: unsupported_expectation.len(),
                    },
                ),
                unsupported_expectation,
                &mut [crate::Header::default(); 2],
                crate::Limits::default(),
            )
            .result,
        HeadResult::ExpectationFailed
    );

    let mut driver = HeadDriver::new(52);
    let first_input = *b"GET / HT";
    assert_eq!(
        driver
            .consume(
                read_completion(
                    52,
                    1,
                    IoResult::Progress {
                        transferred_bytes: first_input.len(),
                    },
                ),
                &first_input,
                &mut [crate::Header::default(); 2],
                crate::Limits::default(),
            )
            .result,
        HeadResult::NeedMore
    );
    let second_input = *b"GET / HTTP";
    let event = driver.consume(
        read_completion(
            52,
            2,
            IoResult::Progress {
                transferred_bytes: 2,
            },
        ),
        &second_input,
        &mut [crate::Header::default(); 2],
        crate::Limits::default(),
    );
    assert_eq!(event.result, HeadResult::BufferChanged);
    assert_eq!(first_input[0], b'G');
}

#[test]
fn partial_writes_resume_after_would_block_and_finish_without_duplication() {
    let bytes = b"HTTP/1.1 200 OK\r\n\r\nbody";
    let mut driver = WriteDriver::new(7, bytes);
    assert_eq!(driver.pending(), bytes);

    assert_eq!(
        driver.consume(completion(
            1,
            IoResult::Progress {
                transferred_bytes: 9
            }
        )),
        super::WriteEvent {
            accepted_bytes: 9,
            result: WriteResult::Progress,
            error_code: 0,
        }
    );
    assert_eq!(
        driver.consume(completion(2, IoResult::WouldBlock)).result,
        WriteResult::WouldBlock
    );
    assert_eq!(driver.pending(), &bytes[9..]);

    let event = driver.consume(completion(
        3,
        IoResult::Progress {
            transferred_bytes: bytes.len() - 9,
        },
    ));
    assert_eq!(event.accepted_bytes, bytes.len() - 9);
    assert_eq!(event.result, WriteResult::Complete);
    assert_eq!(driver.pending(), b"");
    assert_eq!(driver.state(), WriteState::Complete);
}

#[test]
fn rejects_foreign_stale_zero_and_overrun_completions() {
    let mut driver = WriteDriver::new(7, b"response");
    let mut foreign = completion(
        1,
        IoResult::Progress {
            transferred_bytes: 1,
        },
    );
    foreign.connection_id = 8;
    assert_eq!(driver.consume(foreign).result, WriteResult::Invalid);

    let mut driver = WriteDriver::new(7, b"response");
    let mut wrong_operation = completion(
        1,
        IoResult::Progress {
            transferred_bytes: 1,
        },
    );
    wrong_operation.operation = IoOperation::Read;
    assert_eq!(driver.consume(wrong_operation).result, WriteResult::Invalid);

    let mut driver = WriteDriver::new(7, b"response");
    assert_eq!(
        driver
            .consume(completion(
                1,
                IoResult::Progress {
                    transferred_bytes: 1
                }
            ))
            .result,
        WriteResult::Progress
    );
    assert_eq!(
        driver.consume(completion(1, IoResult::WouldBlock)).result,
        WriteResult::Invalid
    );

    let mut driver = WriteDriver::new(7, b"response");
    assert_eq!(
        driver
            .consume(completion(
                1,
                IoResult::Progress {
                    transferred_bytes: 0
                }
            ))
            .result,
        WriteResult::Invalid
    );

    let mut driver = WriteDriver::new(7, b"response");
    assert_eq!(
        driver
            .consume(completion(
                1,
                IoResult::Progress {
                    transferred_bytes: 9
                }
            ))
            .result,
        WriteResult::Invalid
    );
}

#[test]
fn reports_transport_failure_cancellation_and_closed_peer() {
    let mut driver = WriteDriver::new(7, b"response");
    let event = driver.consume(completion(1, IoResult::Error { code: 32 }));
    assert_eq!(event.result, WriteResult::TransportError);
    assert_eq!(event.error_code, 32);
    assert_eq!(driver.state(), WriteState::Failed);

    let mut driver = WriteDriver::new(7, b"response");
    assert_eq!(
        driver.consume(completion(1, IoResult::Cancelled)).result,
        WriteResult::Cancelled
    );
    assert_eq!(driver.state(), WriteState::Cancelled);

    let mut driver = WriteDriver::new(7, b"response");
    assert_eq!(
        driver.consume(completion(1, IoResult::Closed)).result,
        WriteResult::PeerClosed
    );
    assert_eq!(driver.state(), WriteState::Closed);
}

#[test]
fn empty_write_is_complete_without_transport_activity() {
    let driver = WriteDriver::new(7, b"");
    assert_eq!(driver.state(), WriteState::Complete);
    assert_eq!(driver.pending(), b"");
}
