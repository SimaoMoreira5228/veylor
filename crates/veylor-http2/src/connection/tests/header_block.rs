use super::*;
use crate::frame::{CONTINUATION_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM};
use crate::hpack::decode_header_block;

#[test]
fn oversized_compressed_block_fails_connection_before_sibling_stream() {
    let mut slots = [StreamSlot::EMPTY; 2];
    let mut header_block = [0; 32];
    let mut streams = StreamTable::new(&mut slots, H2Role::Server, &mut header_block, 32);
    establish(&mut streams);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut decoder_storage = [0; 128];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut encoder_storage = [0; 128];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_storage, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    let mut block = vec![
        0x82, 0x86, 0x84, 0x01, 0x09, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', 0x40,
        0x07, b'x', b'-', b's', b't', b'a', b't', b'e', 0x05, b's', b'a', b'v', b'e', b'd',
    ];
    for _ in 0..2 {
        block.extend_from_slice(&[0x00, 0x05, b'x', b'-', b'p', b'a', b'd', 0x40]);
        block.extend_from_slice(&[b'v'; 64]);
    }

    let mut proof_entries = [HpackEntrySlot::EMPTY; 4];
    let mut proof_storage = [0; 256];
    let mut proof_decoder = HpackTable::new(&mut proof_entries, &mut proof_storage, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 7];
    let (decoded_count, decoded_length) =
        decode_header_block(&block, &mut proof_decoder, &mut decoded).unwrap();
    let inserted = proof_decoder.get(62).unwrap();

    assert_eq!(decoded_count, 7);
    assert_eq!(decoded_length, block.len());
    assert_eq!(proof_decoder.resolve(inserted.name), Some(&b"x-state"[..]));
    assert_eq!(proof_decoder.resolve(inserted.value), Some(&b"saved"[..]));

    let insertion_end = 29;
    let mut first_frame = [0; FRAME_HEADER_LEN + 32];
    let first_length = frame(
        &mut first_frame,
        FrameType::Headers,
        HEADERS_FLAG_END_STREAM,
        1,
        &block[..insertion_end],
    );
    let mut actions = [None; 2];
    let mut headers = [HpackHeader::EMPTY; 8];
    let first = driver.feed(&first_frame[..first_length], &mut actions, &mut headers);

    assert_eq!(first.status, FeedStatus::NeedMoreInput);
    assert_eq!(first.actions, 0);
    assert_eq!(first.consumed, first_length);

    let mut continuation_frame = [0; FRAME_HEADER_LEN + 256];
    let continuation_length = frame(
        &mut continuation_frame,
        FrameType::Continuation,
        CONTINUATION_FLAG_END_HEADERS,
        1,
        &block[insertion_end..],
    );
    let overflow = driver.feed(
        &continuation_frame[..continuation_length],
        &mut actions,
        &mut headers,
    );

    assert_eq!(overflow.status, FeedStatus::ConnectionFailed);
    assert_eq!(overflow.error, Some(ErrorCode::CompressionError));
    assert_eq!(overflow.actions, 1);
    assert_eq!(
        actions[0],
        Some(H2FrameAction::ConnectionError {
            error_code: ErrorCode::CompressionError,
        })
    );

    let mut sibling_frame = [0; FRAME_HEADER_LEN + 17];
    let sibling_length = frame(
        &mut sibling_frame,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        3,
        &[
            0x82, 0x86, 0x84, 0x01, 0x09, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't',
        ],
    );
    let sibling = driver.feed(&sibling_frame[..sibling_length], &mut actions, &mut headers);

    assert_eq!(sibling.status, FeedStatus::ConnectionFailed);
    assert_eq!(sibling.consumed, 0);
    assert_eq!(sibling.actions, 0);
    assert_eq!(sibling.error, Some(ErrorCode::CompressionError));
}
