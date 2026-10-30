use veylor_http2::{
    ControlFrameWriter, DEFAULT_MAX_FRAME_SIZE, ErrorCode, H2ClientDriver, H2FrameAction,
    HpackEntrySlot, HpackFieldToEncode, HpackHeader, HpackRepresentation, HpackTable, StreamSlot,
    StreamTable,
};

use super::{
    BatchStatus, FeedScratch, H2ServerConnection, H2ServerError, H2ServerTables, HEADER_BLOCK,
    HPACK_ENTRIES, HPACK_SCRATCH, SETTINGS_ACKS, STREAM_SLOTS,
};

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

macro_rules! fixture {
    ($tables:ident, $conn:ident) => {
        let mut slots = [StreamSlot::EMPTY; STREAM_SLOTS];
        let mut header_block = [0; HEADER_BLOCK];
        let mut decoder_entries = [HpackEntrySlot::EMPTY; HPACK_ENTRIES];
        let mut decoder_scratch = [0; HPACK_SCRATCH];
        let mut encoder_entries = [HpackEntrySlot::EMPTY; HPACK_ENTRIES];
        let mut encoder_scratch = [0; HPACK_SCRATCH];
        let mut ack_limits = [0; SETTINGS_ACKS];
        let mut $tables = H2ServerTables::new(
            &mut slots,
            &mut header_block,
            &mut decoder_entries,
            &mut decoder_scratch,
            &mut encoder_entries,
            &mut encoder_scratch,
            &mut ack_limits,
        );
        let mut $conn = $tables.connect().expect("connect");
    };
}

macro_rules! peer {
    ($client:ident, $cblock:ident) => {
        let mut cslots = [StreamSlot::EMPTY; STREAM_SLOTS];
        let mut cheader_block = [0; HEADER_BLOCK];
        let mut cdecoder_entries = [HpackEntrySlot::EMPTY; HPACK_ENTRIES];
        let mut cdecoder_scratch = [0; HPACK_SCRATCH];
        let mut cencoder_entries = [HpackEntrySlot::EMPTY; HPACK_ENTRIES];
        let mut cencoder_scratch = [0; HPACK_SCRATCH];
        let mut cack_limits = [0; SETTINGS_ACKS];
        let mut cstreams = StreamTable::new(
            &mut cslots,
            veylor_http2::H2Role::Client,
            &mut cheader_block,
            HEADER_BLOCK as u32,
        );
        let mut cdecoder = HpackTable::new(&mut cdecoder_entries, &mut cdecoder_scratch, 4_096);
        let mut cencoder = HpackTable::new(&mut cencoder_entries, &mut cencoder_scratch, 4_096);
        let mut $client = H2ClientDriver::new(
            &mut cstreams,
            &mut cdecoder,
            &mut cencoder,
            &mut cack_limits,
        )
        .expect("peer connect");
        let mut $cblock = [0u8; HEADER_BLOCK];
    };
}

fn field<'f>(name: &'f [u8], value: &'f [u8]) -> HpackFieldToEncode<'f> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

fn request_fields<'f>() -> [HpackFieldToEncode<'f>; 4] {
    [
        field(b":method", b"POST"),
        field(b":scheme", b"https"),
        field(b":path", b"/upload"),
        field(b":authority", b"example.com"),
    ]
}

fn empty_settings_ack_frame() -> [u8; 9] {
    [0, 0, 0, 0x04, 0x01, 0, 0, 0, 0]
}

fn client_settings_handshake(
    server: &mut H2ServerConnection<'_, '_>,
    client: &mut H2ClientDriver<'_, '_, '_, '_>,
) {
    let mut output = [0u8; 64];
    let written = {
        let mut writer = ControlFrameWriter::new(&mut output[..], DEFAULT_MAX_FRAME_SIZE);
        writer.queue_settings(&[]).expect("queue peer settings")
    };
    client
        .driver_mut()
        .register_fully_written_local_settings(&[])
        .expect("register peer settings");

    let mut scratch = FeedScratch::new();
    let batch = server.poll_feed(PREFACE, &mut scratch).expect("preface");
    assert_eq!(batch.status, BatchStatus::NeedMore);
    assert_eq!(batch.consumed, PREFACE.len());

    let batch = server
        .poll_feed(&output[..written], &mut scratch)
        .expect("settings");
    assert_eq!(batch.status, BatchStatus::Ready);
    assert_eq!(batch.action_count, 1);
    assert!(matches!(
        batch.actions[0],
        Some(H2FrameAction::SettingsAckRequired)
    ));
    server.finish_batch();

    let mut ack = [0u8; 32];
    let out = server.send_settings_ack(&mut ack).expect("settings ack");
    assert_eq!(out.written, 9);
    assert_eq!(server.acknowledge(out.written), Ok(0));

    let mut settings = [0u8; 64];
    let out = server.send_settings(&[], &mut settings).expect("settings");
    assert_eq!(
        &settings[..out.written],
        &[0, 0, 6, 0x04, 0, 0, 0, 0, 0, 0, 6, 0, 0, 8, 64]
    );
    assert_eq!(server.acknowledge(out.written), Ok(0));

    let mut cactions = [None; 16];
    let mut cheaders = [HpackHeader::EMPTY; 64];
    let fed = client
        .driver_mut()
        .feed(&settings[..out.written], &mut cactions, &mut cheaders);
    assert_eq!(fed.actions, 1);
    assert!(matches!(
        cactions[0],
        Some(H2FrameAction::SettingsAckRequired)
    ));

    let ack_frame = empty_settings_ack_frame();
    let batch = server
        .poll_feed(&ack_frame, &mut scratch)
        .expect("client ack");
    assert_eq!(batch.status, BatchStatus::NeedMore);
}

#[test]
fn server_settings_match_fixed_header_field_capacity() {
    fixture!(tables, server);
    let mut output = [0u8; 128];
    let too_large = [veylor_http2::SettingsParameter {
        id: veylor_http2::SETTINGS_MAX_HEADER_LIST_SIZE,
        value: super::MAX_HEADER_LIST_SIZE + 1,
    }];
    assert_eq!(
        server.send_settings(&too_large, &mut output),
        Err(H2ServerError::InvalidArgument)
    );

    let sent = server
        .send_settings(&[], &mut output)
        .expect("default settings");
    assert_eq!(sent.written, 15);
    assert_eq!(
        &output[..sent.written],
        &[0, 0, 6, 0x04, 0, 0, 0, 0, 0, 0, 6, 0, 0, 8, 64]
    );
    assert_eq!(server.acknowledge(sent.written), Ok(0));
    assert_eq!(
        server
            .driver
            .driver()
            .streams()
            .local_settings()
            .max_header_list_size,
        super::MAX_HEADER_LIST_SIZE
    );

    let full = [veylor_http2::SettingsParameter {
        id: veylor_http2::SETTINGS_HEADER_TABLE_SIZE,
        value: 4_096,
    }; super::MAX_SETTINGS];
    let sent = server
        .send_settings(&full, &mut output)
        .expect("full caller settings plus default")
        .written;
    assert_eq!(sent, 63);
    assert_eq!(server.acknowledge(sent), Ok(0));
}

fn acked(server: &mut H2ServerConnection<'_, '_>, written: usize) {
    assert_eq!(server.acknowledge(written), Ok(0));
}

#[test]
fn handshake_accept_response_and_data_round_trip() {
    fixture!(tables, server);
    peer!(client, cblock);
    client_settings_handshake(&mut server, &mut client);

    let fields = request_fields();
    let mut request = [0u8; 1_024];
    let started = client
        .start_request(&fields, &mut cblock, &mut request, false)
        .expect("start request");
    assert_eq!(started.stream_id, 1);
    let request_length = started.frame.output_length;
    let request = &request[..request_length];

    let mut scratch = FeedScratch::new();
    let batch = server.poll_feed(request, &mut scratch).expect("poll");
    assert_eq!(batch.status, BatchStatus::Ready);
    assert_eq!(batch.action_count, 1);
    let (stream_id, end_stream, header_count) = match &batch.actions[0] {
        Some(H2FrameAction::Headers {
            stream_id,
            headers,
            end_stream,
        }) => (*stream_id, *end_stream, headers.count),
        action => panic!("expected headers, got {action:?}"),
    };
    assert_eq!(stream_id, 1);
    assert!(!end_stream);
    assert_eq!(header_count, 4);
    let validated = server
        .accept_request(1, &batch.headers[..header_count])
        .expect("accept");
    assert_eq!(validated.method, Some(0));
    assert_eq!(validated.content_length, None);
    let (name, value) = server
        .resolve_header(batch.headers[validated.method.expect("method")])
        .expect("resolve");
    assert_eq!((name, value), (b":method".as_slice(), b"POST".as_slice()));
    server.finish_batch();

    let mut update = [0u8; 32];
    let out = server
        .send_window_update(1, 1_024, &mut update)
        .expect("window update");
    acked(&mut server, out.written);

    let response = [field(b":status", b"200")];
    let mut encoded = [0u8; 1_024];
    let out = server
        .send_response(1, &response, true, &mut encoded)
        .expect("send response");
    acked(&mut server, out.written);

    let mut actions = [None; 16];
    let mut cheaders = [HpackHeader::EMPTY; 64];
    let fed = client
        .driver_mut()
        .feed(&encoded[..out.written], &mut actions, &mut cheaders);
    assert_eq!(fed.actions, 1);
    let (stream_id, end_stream) = match &actions[0] {
        Some(H2FrameAction::Headers {
            stream_id,
            headers: range,
            end_stream,
        }) => {
            let mut status = None;
            for header in &cheaders[range.start..range.start + range.count] {
                let (name, value) = client.driver().resolve_header(*header).expect("resolve");
                if name == b":status" {
                    status = Some(value.to_vec());
                }
            }
            assert_eq!(status.as_deref(), Some(b"200".as_slice()));
            (*stream_id, *end_stream)
        }
        action => panic!("expected headers, got {action:?}"),
    };
    assert_eq!(stream_id, 1);
    assert!(end_stream);
}

#[test]
fn refuse_reset_goaway_ping_and_window_update() {
    fixture!(tables, server);
    peer!(client, cblock);
    client_settings_handshake(&mut server, &mut client);

    let fields = request_fields();
    let mut request = [0u8; 1_024];
    let started = client
        .start_request(&fields, &mut cblock, &mut request, false)
        .expect("start request");
    let request_length = started.frame.output_length;
    let mut scratch = FeedScratch::new();
    let batch = server
        .poll_feed(&request[..request_length], &mut scratch)
        .expect("poll");
    assert_eq!(batch.action_count, 1);
    server
        .accept_request(1, &batch.headers[..batch.header_count])
        .expect("accept");
    server.finish_batch();

    let mut refused = [0u8; 32];
    let out = server.refuse(1, &mut refused).expect("refuse");
    acked(&mut server, out.written);
    let mut actions = [None; 16];
    let mut headers = [HpackHeader::EMPTY; 64];
    let fed = client
        .driver_mut()
        .feed(&refused[..out.written], &mut actions, &mut headers);
    assert_eq!(fed.actions, 1);
    assert!(matches!(
        actions[0],
        Some(H2FrameAction::StreamReset {
            error_code: ErrorCode::RefusedStream,
            ..
        })
    ));

    let mut reset = [0u8; 32];
    assert_eq!(
        server.send_reset(1, ErrorCode::Cancel, &mut reset),
        Err(H2ServerError::BadState)
    );

    let opaque = [1, 2, 3, 4, 5, 6, 7, 8];
    let mut ping = [0u8; 32];
    let out = server.send_ping_ack(&opaque, &mut ping).expect("ping");
    assert_eq!(out.written, 17);
    acked(&mut server, out.written);

    let mut goaway = [0u8; 32];
    let out = server.goaway_drain(0, &mut goaway).expect("goaway");
    assert_eq!(out.written, 17);
    acked(&mut server, out.written);

    assert_eq!(
        server.send_window_update(0, 0, &mut ping),
        Err(H2ServerError::InvalidArgument)
    );
    assert_eq!(
        server.send_window_update(99, 1, &mut ping),
        Err(H2ServerError::InvalidArgument)
    );
}

#[test]
fn rejects_invalid_inputs_and_pending_state() {
    fixture!(tables, server);
    assert_eq!(
        server.accept_request(1, &[]),
        Err(H2ServerError::InvalidArgument)
    );
    let response = [field(b":status", b"200")];
    let mut output = [0u8; 1_024];
    assert_eq!(
        server.send_response(1, &response, true, &mut output),
        Err(H2ServerError::InvalidArgument)
    );
    let oversized = [field(b"x", b"y"); 65];
    assert_eq!(
        server.send_response(1, &oversized, true, &mut output),
        Err(H2ServerError::Capacity)
    );
    assert_eq!(
        server.send_settings(&[], &mut [0u8; 2]),
        Err(H2ServerError::BufferTooSmall { required: 15 })
    );
    let out = server.send_settings(&[], &mut output).expect("settings");
    assert_eq!(
        server.send_settings(&[], &mut output),
        Err(H2ServerError::WouldBlock)
    );
    assert_eq!(
        server.acknowledge(out.written + 1),
        Err(H2ServerError::WriteOverrun)
    );
    assert_eq!(server.acknowledge(1), Ok(out.written - 1));
    assert_eq!(server.acknowledge(out.written - 1), Ok(0));
    assert_eq!(server.acknowledge(0), Err(H2ServerError::BadState));

    let mut scratch = FeedScratch::new();
    assert!(matches!(
        server.poll_retry(&mut scratch),
        Err(H2ServerError::BadState)
    ));
    let batch = server.poll_feed(&[], &mut scratch).expect("empty");
    assert_eq!(batch.status, BatchStatus::NeedMore);
    assert_eq!(server.feed_header_count(), 0);

    server.cancel();
    assert!(server.is_cancelled());
    assert_eq!(
        server.send_settings(&[], &mut output),
        Err(H2ServerError::Cancelled)
    );
    assert!(matches!(
        server.poll_feed(&[], &mut scratch),
        Err(H2ServerError::Cancelled)
    ));
    assert!(matches!(
        server.accept_request(1, &[]),
        Err(H2ServerError::Cancelled)
    ));
}
