use super::ResponseWriteDriver;
use crate::RequestVersion;
use crate::connection::{IoCompletion, IoOperation, IoResult, WriteResult, WriteState};
use crate::request_write::Field;
use crate::response_write::{
    ConnectionHeader, ResponseEncodeError, ResponseFraming, ResponseToWrite,
};

fn completion(connection_id: u64, operation_id: u64, transferred_bytes: usize) -> IoCompletion {
    IoCompletion {
        connection_id,
        operation_id,
        operation: IoOperation::Write,
        result: IoResult::Progress { transferred_bytes },
    }
}

fn response(
    request_method: &'static [u8],
    framing: ResponseFraming,
    content_length: u64,
    connection: ConnectionHeader,
) -> ResponseToWrite<'static> {
    ResponseToWrite {
        request_method,
        request_version: RequestVersion::Http11,
        requested_upgrade: b"",
        status: 200,
        reason: b"OK",
        fields: &[],
        framing,
        content_length,
        connection,
    }
}

fn write_pending_head(driver: &mut ResponseWriteDriver<'_, '_>, connection_id: u64) {
    let mut operation_id = 0;
    while !driver.pending().is_empty() {
        let pending = driver.pending().len();
        operation_id += 1;
        let event = driver.consume(completion(connection_id, operation_id, pending));
        assert_eq!(event.result, WriteResult::Progress);
    }
}

#[test]
fn fixed_response_streams_borrowed_chunks_and_finishes_after_partial_writes() {
    let mut output = [0; 128];
    let mut driver = ResponseWriteDriver::new(
        91,
        response(
            b"POST",
            ResponseFraming::FixedLength,
            5,
            ConnectionHeader::Omit,
        ),
        &mut output,
    )
    .unwrap();
    let head_length = driver.pending().len();
    assert!(driver.pending().starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert_eq!(
        driver.consume(completion(91, 1, 4)).result,
        WriteResult::Progress
    );
    assert_eq!(driver.pending().len(), head_length - 4);
    assert_eq!(
        driver.consume(completion(91, 2, head_length - 4)).result,
        WriteResult::Progress
    );

    driver.queue_fixed(b"abc").unwrap();
    assert_eq!(driver.pending(), b"abc");
    assert_eq!(
        driver.consume(completion(91, 3, 2)).result,
        WriteResult::Progress
    );
    assert_eq!(driver.pending(), b"c");
    assert_eq!(
        driver.queue_fixed(b"de"),
        Err(ResponseEncodeError::InvalidState)
    );
    assert_eq!(
        driver.consume(completion(91, 4, 1)).result,
        WriteResult::Progress
    );
    driver.queue_fixed(b"de").unwrap();
    assert_eq!(driver.finish_fixed(), Ok(false));
    assert_eq!(
        driver.consume(completion(91, 5, 1)).result,
        WriteResult::Progress
    );
    let event = driver.consume(completion(91, 6, 1));
    assert_eq!(event.result, WriteResult::Complete);
    assert_eq!(driver.state(), WriteState::Complete);
}

#[test]
fn chunked_response_writes_chunk_frames_and_final_trailers() {
    let mut output = [0; 128];
    let mut driver = ResponseWriteDriver::new(
        92,
        response(b"GET", ResponseFraming::Chunked, 0, ConnectionHeader::Omit),
        &mut output,
    )
    .unwrap();
    write_pending_head(&mut driver, 92);

    driver.queue_chunk(b"Wiki").unwrap();
    assert_eq!(driver.pending(), b"4\r\nWiki\r\n");
    assert_eq!(
        driver.consume(completion(92, 2, 3)).result,
        WriteResult::Progress
    );
    assert_eq!(driver.pending(), b"Wiki\r\n");
    assert_eq!(
        driver.consume(completion(92, 3, 6)).result,
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
    let event = driver.consume(completion(92, 4, final_length));
    assert_eq!(event.result, WriteResult::Complete);
    assert_eq!(driver.state(), WriteState::Complete);
}

#[test]
fn close_delimited_response_finishes_after_queued_body_is_written() {
    let mut output = [0; 128];
    let mut driver = ResponseWriteDriver::new(
        93,
        response(
            b"GET",
            ResponseFraming::CloseDelimited,
            0,
            ConnectionHeader::Close,
        ),
        &mut output,
    )
    .unwrap();
    write_pending_head(&mut driver, 93);

    driver.queue_close_delimited(b"body").unwrap();
    assert_eq!(driver.pending(), b"body");
    assert_eq!(driver.finish_close_delimited(), Ok(false));
    assert_eq!(
        driver.consume(completion(93, 2, 2)).result,
        WriteResult::Progress
    );
    assert_eq!(driver.pending(), b"dy");
    let event = driver.consume(completion(93, 3, 2));
    assert_eq!(event.result, WriteResult::Complete);
    assert_eq!(driver.state(), WriteState::Complete);
}

#[test]
fn suppressed_response_body_completes_at_the_head_and_overrun_fails_closed() {
    let mut output = [0; 128];
    let mut head_only = ResponseWriteDriver::new(
        94,
        response(
            b"HEAD",
            ResponseFraming::FixedLength,
            5,
            ConnectionHeader::Omit,
        ),
        &mut output,
    )
    .unwrap();
    let head_length = head_only.pending().len();
    let event = head_only.consume(completion(94, 1, head_length));
    assert_eq!(event.result, WriteResult::Complete);
    assert_eq!(
        head_only.queue_fixed(b"forbidden"),
        Err(ResponseEncodeError::InvalidState)
    );

    let mut output = [0; 128];
    let mut overrun = ResponseWriteDriver::new(
        95,
        response(
            b"POST",
            ResponseFraming::FixedLength,
            2,
            ConnectionHeader::Omit,
        ),
        &mut output,
    )
    .unwrap();
    write_pending_head(&mut overrun, 95);
    assert_eq!(
        overrun.queue_fixed(b"too long"),
        Err(ResponseEncodeError::BodyOverrun)
    );
    assert_eq!(overrun.state(), WriteState::Failed);
}

#[test]
fn short_fixed_body_and_cancellation_are_terminal() {
    let mut output = [0; 128];
    let mut short = ResponseWriteDriver::new(
        96,
        response(
            b"GET",
            ResponseFraming::FixedLength,
            4,
            ConnectionHeader::Omit,
        ),
        &mut output,
    )
    .unwrap();
    write_pending_head(&mut short, 96);
    assert_eq!(short.queue_fixed(b"abc"), Ok(()));
    assert_eq!(short.finish_fixed(), Ok(false));
    assert_eq!(
        short.consume(completion(96, 2, 3)).result,
        WriteResult::Invalid
    );
    assert_eq!(short.state(), WriteState::Failed);

    let mut output = [0; 128];
    let mut cancelled = ResponseWriteDriver::new(
        97,
        response(
            b"GET",
            ResponseFraming::FixedLength,
            4,
            ConnectionHeader::Omit,
        ),
        &mut output,
    )
    .unwrap();
    write_pending_head(&mut cancelled, 97);
    cancelled.queue_fixed(b"body").unwrap();
    cancelled.cancel();
    assert_eq!(cancelled.pending(), b"");
    assert_eq!(cancelled.state(), WriteState::Cancelled);
    assert_eq!(
        cancelled
            .consume(IoCompletion {
                connection_id: 97,
                operation_id: 2,
                operation: IoOperation::Write,
                result: IoResult::Cancelled,
            })
            .result,
        WriteResult::Cancelled
    );
}
