use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::time::{Duration, Instant};
use veylor_http2::{
    CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM, ErrorCode, FrameType, HpackEntrySlot,
    HpackFieldToEncode, HpackRepresentation, HpackTable, SETTINGS_MAX_HEADER_LIST_SIZE,
    decode_goaway_payload,
};
use veylor_server::{H2C_MAX_HEADER_LIST_SIZE, h2::HEADER_BLOCK_CAP};

#[test]
fn h2_oversize_request_header_block_fails_connection_with_compression_error() {
    let server = managed_h2_body_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    let mut names = Vec::new();
    let mut values = Vec::new();
    for index in 0..400usize {
        names.push(format!("x-pad-{index:04}"));
        values.push("v".repeat(40));
    }
    let mut fields: Vec<(&[u8], &[u8])> = vec![
        (&b":method"[..], &b"POST"[..]),
        (b":scheme", b"http"),
        (b":path", b"/upload"),
        (b":authority", b"localhost"),
    ];
    for (name, value) in names.iter().zip(values.iter()) {
        fields.push((name.as_bytes(), value.as_bytes()));
    }
    send_fragmented_headers(&mut stream, 1, &fields);
    let (_, code) = read_goaway(&mut stream);
    assert_eq!(code, ErrorCode::CompressionError);
    read_eof(&mut stream);
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 0, "counts: {counts:?}");
}

#[test]
fn h2_advertises_and_enforces_header_list_limit() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    let advertised = handshake_advertised_setting(&mut stream, SETTINGS_MAX_HEADER_LIST_SIZE);
    assert_eq!(advertised, H2C_MAX_HEADER_LIST_SIZE);

    let names: Vec<String> = (0..38).map(|index| format!("x-pad-{index:02}")).collect();
    let values = vec!["v".repeat(400); names.len()];
    let mut fields = vec![
        field(b":method", b"GET"),
        field(b":scheme", b"http"),
        field(b":path", b"/hello"),
        field(b":authority", b"localhost"),
    ];
    for (name, value) in names.iter().zip(values.iter()) {
        fields.push(field(name.as_bytes(), value.as_bytes()));
    }

    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 16_384];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; HEADER_BLOCK_CAP];
    send_encoded_headers(&mut stream, 1, &fields, true, &mut encoder, &mut block);
    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 1);
    assert_eq!(code, ErrorCode::RefusedStream);

    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

#[test]
fn h2_header_slot_overflow_resets_stream_and_keeps_hpack_state() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    let names: Vec<String> = (0..60).map(|index| format!("x-{index:02}")).collect();
    let mut fields = vec![
        field(b":method", b"GET"),
        field(b":scheme", b"http"),
        field(b":path", b"/hello"),
        field(b":authority", b"localhost"),
    ];
    for name in &names {
        fields.push(field(name.as_bytes(), b""));
    }
    fields.push(HpackFieldToEncode {
        name: b"x-state",
        value: b"preserved",
        representation: HpackRepresentation::Incremental,
        huffman_name: false,
        huffman_value: false,
    });

    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 2_048];
    send_encoded_headers(&mut stream, 1, &fields, true, &mut encoder, &mut block);

    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 1);
    assert_eq!(code, ErrorCode::RefusedStream);

    let sibling_fields = [
        field(b":method", b"GET"),
        field(b":scheme", b"http"),
        field(b":path", b"/hello"),
        field(b":authority", b"localhost"),
        HpackFieldToEncode {
            name: b"x-state",
            value: b"preserved",
            representation: HpackRepresentation::Default,
            huffman_name: false,
            huffman_value: false,
        },
    ];
    send_encoded_headers(
        &mut stream,
        3,
        &sibling_fields,
        true,
        &mut encoder,
        &mut block,
    );
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);

    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

#[test]
fn h2_reset_flood_goes_away_with_enhance_your_calm() {
    let server = managed_h2_body_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/hello", true);
    let hello = read_response(&mut stream, 1);
    assert_eq!(hello.status, 200);
    for _ in 0..300 {
        write_frame(&mut stream, FrameType::RstStream, 0, 1, &[0, 0, 0, 8]);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if Instant::now() > deadline {
            panic!("no GOAWAY for reset flood");
        }
        match try_read_frame(&mut stream) {
            Some((header, payload)) if header.frame_type == FrameType::GoAway => {
                let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
                assert_eq!(goaway.error_code, ErrorCode::EnhanceYourCalm);
                return;
            }
            Some(_) | None => {}
        }
    }
}

#[test]
fn h2_settings_flood_goes_away_with_enhance_your_calm() {
    let server = managed_h2_body_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/hello", true);
    let hello = read_response(&mut stream, 1);
    assert_eq!(hello.status, 200);
    // NOTE: frames are written one at a time so the guard samples the flood across several
    // turns. A peer that closes mid-flood turns a later write into a transport error; that is
    // the interleaving where the verdict already landed. A close may also turn the GOAWAY
    // into a reset, so the wire code is checked when it survives and the counted reason below
    // carries the proof either way.
    for _ in 0..60 {
        if try_write_frame(&mut stream, FrameType::Settings, 0, 0, &[]).is_err() {
            break;
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut goaway_code = None;
    while Instant::now() < deadline {
        let Some((header, payload)) = try_read_frame(&mut stream) else {
            break;
        };
        if header.frame_type == FrameType::GoAway {
            let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
            goaway_code = Some(goaway.error_code);
            break;
        }
    }
    if let Some(code) = goaway_code {
        assert_eq!(code, ErrorCode::EnhanceYourCalm);
    }
    let counts = stop_join(server);
    assert_eq!(counts.abused, 1, "counts: {counts:?}");
}

#[test]
fn h2_occasional_resets_do_not_trip_flood_guard() {
    let server = managed_h2_body_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/hello", false);
    write_frame(&mut stream, FrameType::RstStream, 0, 1, &[0, 0, 0, 8]);
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

// NOTE: adversarial flow-control wire pins (RFC 9113 Phase 5 exit): every violation below
// drives raw frames at a live server and asserts the exact wire outcome plus the fail-closed
// tail (a clean sibling 200 on a surviving connection, or GOAWAY plus EOF where the RFC
// mandates connection teardown). No partial state may linger: the follow-up proves it.

// NOTE: RFC 9113 section 6.9 receiver MAY (stream FLOW_CONTROL_ERROR when unable to accept)
// plus section 8.1.1 (length mismatch is stream-only): DATA past the admitted body window
// RSTs only its stream and the sibling serves cleanly on the surviving connection.
#[test]
fn h2_adversarial_data_over_body_resets_protocol_error() {
    let server = managed_h2_body_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers_len(&mut stream, 1, b"POST", b"/upload", Some(4), false);
    write_frame(
        &mut stream,
        FrameType::Data,
        DATA_FLAG_END_STREAM,
        1,
        b"12345678",
    );
    let (id, code) = read_rst(&mut stream);
    assert_eq!(id, 1);
    assert_eq!(code, ErrorCode::ProtocolError);
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let sibling = read_response(&mut stream, 3);
    assert_eq!(sibling.status, 200);
    assert_eq!(sibling.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_refused, 1, "counts: {counts:?}");
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}

// NOTE: RFC 9113 section 4.3 requires decompression even for discarded blocks. The 12,000
// plus 5,000 fragment octets exceed the assembly cap before completion, so the connection
// fails with COMPRESSION_ERROR instead of continuing with HPACK state out of sync.
#[test]
fn h2_adversarial_oversized_header_block_fails_connection() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    let first = vec![0x82; 12_000];
    write_frame(&mut stream, FrameType::Headers, 0, 1, &first);
    let second = vec![0x84; 5_000];
    write_frame(
        &mut stream,
        FrameType::Continuation,
        CONTINUATION_FLAG_END_HEADERS,
        1,
        &second,
    );
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(last, 1);
    assert_eq!(code, ErrorCode::CompressionError);
    read_eof(&mut stream);
    stop_join(server);
}

// NOTE: RFC 9113 section 6.10 (stray CONTINUATION is a PROTOCOL_ERROR connection error):
// no HEADERS opened any block, so the connection dies with nothing served.
#[test]
fn h2_adversarial_stray_continuation_goes_away() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    write_frame(
        &mut stream,
        FrameType::Continuation,
        CONTINUATION_FLAG_END_HEADERS,
        1,
        &[0x82],
    );
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(code, ErrorCode::ProtocolError);
    assert_eq!(last, 0);
    read_eof(&mut stream);
    stop_join(server);
}

// NOTE: RFC 9113 sections 6.10 and 4.3 (field blocks are contiguous; any other frame or
// stream mid-block is a PROTOCOL_ERROR connection error): stream 3 HEADERS interrupts the
// stream 1 fragments and tears the connection down.
#[test]
fn h2_adversarial_continuation_out_of_order_goes_away() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    write_frame(&mut stream, FrameType::Headers, 0, 1, &[0x82]);
    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(code, ErrorCode::ProtocolError);
    assert_eq!(last, 1);
    read_eof(&mut stream);
    stop_join(server);
}

// NOTE: RFC 9113 section 5.1.1 (client streams are odd; an unexpected id is a PROTOCOL_ERROR
// connection error): HEADERS on server-initiated stream 2 dies with last 0, so no stream was
// half-created by the violation.
#[test]
fn h2_adversarial_even_stream_id_goes_away() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 2, b"GET", b"/hello", true);
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(code, ErrorCode::ProtocolError);
    assert_eq!(last, 0);
    read_eof(&mut stream);
    stop_join(server);
}

// NOTE: RFC 9113 section 6.1 (DATA on stream 0 is a PROTOCOL_ERROR connection error): the
// connection dies before any stream exists.
#[test]
fn h2_adversarial_zero_stream_data_goes_away() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    write_frame(&mut stream, FrameType::Data, 0, 0, b"junk");
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(code, ErrorCode::ProtocolError);
    assert_eq!(last, 0);
    read_eof(&mut stream);
    stop_join(server);
}

// NOTE: RFC 9113 section 5.1 (PRIORITY may ride any stream state, creating none): the frame
// on idle stream 5 is a no-op, the sibling serves, and graceful shutdown stays NoError.
#[test]
fn h2_adversarial_priority_idle_is_noop() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    write_frame(&mut stream, FrameType::Priority, 0, 5, &[0, 0, 0, 0, 16]);
    send_headers(&mut stream, 1, b"GET", b"/hello", true);
    let response = read_response(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"hello");
    server.stop_accepting();
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(code, ErrorCode::NoError);
    assert_eq!(last, 1);
    read_eof(&mut stream);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 1);
}

// NOTE: RFC 9113 section 5.1 half-closed(remote) (frames past END_STREAM answer stream error
// STREAM_CLOSED) with section 8.1 trailer shape: the late HEADERS RSTs only its stream and
// the sibling serves on the surviving connection.
#[test]
fn h2_adversarial_trailer_after_close_goes_away_stream_closed() {
    let server = h2_server();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/hello", true);
    let first = read_response(&mut stream, 1);
    assert_eq!(first.status, 200);
    assert_eq!(first.body, b"hello");
    send_trailers(&mut stream, 1, &[(b"x-checksum", b"yes")]);
    let (last, code) = read_goaway(&mut stream);
    assert_eq!(last, 1);
    assert_eq!(code, ErrorCode::StreamClosed);
    read_eof(&mut stream);
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}
