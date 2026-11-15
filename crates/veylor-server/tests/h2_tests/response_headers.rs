use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use veylor_http2::{
    ErrorCode, FrameType, HpackEntrySlot, HpackFieldToEncode, HpackRepresentation, HpackTable,
    encode_header_block,
};
use veylor_server::RespondOutcome;

// NOTE: policy-adoption golden with Date: :status stays the static-table indexed form (0x88),
// date rides a 32-byte literal without indexing (name index 33, 29-byte value, zero table
// insertions since date is not a stable name), content-length stays the 4-byte literal, so
// the repeat response costs the same 37 bytes and the dynamic table stays untouched.
#[test]
fn h2_policy_indexed_status_wire_golden() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let (header, first) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(first.len(), 37);
    assert_eq!(&first[..4], &[0x88, 0x0f, 0x12, 0x1d]);
    date_value_shape(&first[4..33]);
    assert_eq!(&first[33..], &[0x0f, 0x0d, 0x01, b'5']);
    let response = decode_response(&first);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_length, 5);
    date_shape(&response);
    assert_date_first(&response.order, true);
    let (header, body) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(body, b"hello");
    send_headers(&mut client, 3, b"GET", b"/empty", true);
    let (header, second) = read_frame(&mut client);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(second.len(), 37);
    assert_eq!(&second[..4], &[0x88, 0x0f, 0x12, 0x1d]);
    date_value_shape(&second[4..33]);
    assert_eq!(&second[33..], &[0x0f, 0x0d, 0x01, b'0']);
    let response = decode_response(&second);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_length, 0);
    date_shape(&response);
    assert_date_first(&response.order, true);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 2);
    assert_eq!(counts.shutdown, 1);
}

// NOTE: explicit kinds bypass the policy: the same :status value encodes indexed under
// Default but stays a 5-byte literal under WithoutIndexing (the pre-adoption wire form).
#[test]
fn h2_explicit_literal_bypasses_policy() {
    let legacy = [field(b":status", b"200"), field(b"content-length", b"5")];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&legacy, &mut encoder, &mut block).expect("encode legacy");
    assert_eq!(len, 9);
    assert_eq!(
        &block[..len],
        &[0x08, 0x03, b'2', b'0', b'0', 0x0f, 0x0d, 0x01, b'5']
    );
    let policy = [
        HpackFieldToEncode {
            name: b":status",
            value: b"200",
            representation: HpackRepresentation::Default,
            huffman_name: false,
            huffman_value: false,
        },
        HpackFieldToEncode {
            name: b"content-length",
            value: b"5",
            representation: HpackRepresentation::Default,
            huffman_name: false,
            huffman_value: false,
        },
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&policy, &mut encoder, &mut block).expect("encode policy");
    assert_eq!(len, 5);
    assert_eq!(&block[..len], &[0x88, 0x0f, 0x0d, 0x01, b'5']);
}

// NOTE: H2 Date parity pin (H1 h1_date_present_and_shaped_across_static_paths mirror): every
// HEADERS emission carries exactly one IMF-fixdate Date first after :status; bodies, lengths,
// and statuses stay byte-exact, and multiplexed siblings each carry their own Date.
#[test]
fn h2_date_static_200_first_shaped_exact_elsewhere() {
    let server = h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let first = read_response(&mut client, 1);
    assert_eq!(first.status, 200);
    assert_eq!(first.content_length, 5);
    assert_eq!(first.body, b"hello");
    date_shape(&first);
    assert_date_first(&first.order, true);
    send_headers(&mut client, 3, b"GET", b"/empty", true);
    let second = read_response(&mut client, 3);
    assert_eq!(second.status, 200);
    assert_eq!(second.content_length, 0);
    assert!(second.body.is_empty());
    date_shape(&second);
    assert_date_first(&second.order, true);
    send_headers(&mut client, 5, b"GET", b"/missing", true);
    let missing = read_response(&mut client, 5);
    assert_eq!(missing.status, 404);
    assert_eq!(missing.content_length, 9);
    assert_eq!(missing.body, b"Not Found");
    date_shape(&missing);
    assert_date_first(&missing.order, true);
    server.stop_accepting();
    let (last, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    assert_eq!(last, 5);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 3);
    assert_eq!(counts.shutdown, 1);
}

// NOTE: native Date pin through the transport-neutral rig: handler 200 carries Date first
// after :status with the handler body and length byte-exact.
#[test]
fn h2_date_native_200_first_shaped() {
    let view = h2n_view(&H2N_OK);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"GET", b"/native", true);
        let outcome = pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        assert_eq!(outcome.served, 1);
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.content_length, 12);
        assert_eq!(response.body, b"native-hello");
        date_shape(&response);
        assert_date_first(&response.order, true);
    });
}

// NOTE: file Date pin over one multiplexed connection with a stateful decoder (dynamic-table
// continuity proves the 32-byte literal Date inserts nothing): 200/304/416 all carry Date
// first after :status, Last-Modified/Content-Range stay exact, bodies stay byte-exact.
#[test]
fn h2_date_file_200_304_416_first_shaped() {
    let root = file_mount_root("date");
    std::fs::write(root.join("hello.txt"), b"file-content").expect("mount file");
    let server = file_mount_server(&root);
    let mut client = h2_connect(&server);
    handshake(&mut client);
    let mut dec_entries = [HpackEntrySlot::EMPTY; 64];
    let mut dec_scratch = [0; 4096];
    let mut dec = HpackTable::new(&mut dec_entries, &mut dec_scratch, 4_096);
    send_headers(&mut client, 1, b"GET", b"/assets/hello.txt", true);
    let got = read_response_stateful(&mut client, 1, &mut dec);
    assert_eq!(got.status, 200);
    assert_eq!(got.content_length, 12);
    assert_eq!(got.body, b"file-content");
    assert_eq!(got.content_type, b"text/plain");
    assert!(!got.last_modified.is_empty());
    date_shape(&got);
    assert_date_first(&got.order, true);
    assert_eq!(
        got.order,
        vec![
            b":status".to_vec(),
            b"date".to_vec(),
            b"content-length".to_vec(),
            b"content-type".to_vec(),
            b"accept-ranges".to_vec(),
            b"last-modified".to_vec(),
            b"vary".to_vec()
        ]
    );
    let since: &[u8] = &got.last_modified;
    send_headers_extra(
        &mut client,
        3,
        b"GET",
        b"/assets/hello.txt",
        &[(b"if-modified-since".as_slice(), since)],
        true,
    );
    let fresh = read_response_stateful(&mut client, 3, &mut dec);
    assert_eq!(fresh.status, 304);
    assert_eq!(fresh.content_length, 0);
    assert!(fresh.body.is_empty());
    assert_eq!(fresh.last_modified, got.last_modified);
    date_shape(&fresh);
    assert_date_first(&fresh.order, false);
    assert_eq!(
        fresh.order,
        vec![
            b":status".to_vec(),
            b"date".to_vec(),
            b"last-modified".to_vec(),
            b"vary".to_vec()
        ]
    );
    send_headers_extra(
        &mut client,
        5,
        b"GET",
        b"/assets/hello.txt",
        &[(b"range".as_slice(), b"bytes=5000-6000".as_slice())],
        true,
    );
    let bad = read_response_stateful(&mut client, 5, &mut dec);
    assert_eq!(bad.status, 416);
    assert!(bad.body.is_empty());
    assert_eq!(bad.content_range, b"bytes */12");
    date_shape(&bad);
    assert_date_first(&bad.order, true);
    assert_eq!(
        bad.order,
        vec![
            b":status".to_vec(),
            b"date".to_vec(),
            b"content-length".to_vec(),
            b"content-range".to_vec(),
            b"vary".to_vec()
        ]
    );
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 3, "counts: {counts:?}");
    let _ = std::fs::remove_dir_all(&root);
}

// NOTE: managed-Begin Date pin (H1 streams.rs Begin-arm parity): the length-unknown Begin
// HEADERS carries :status plus a shaped Date only, END_STREAM stays clear, and the DATA
// that follows still frames the host bytes byte-exact.
#[test]
fn h2_date_managed_begin_first_shaped() {
    let server = managed_h2_server();
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);
    send_headers(&mut stream, 1, b"GET", b"/work", true);
    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(
        host.write_stream(event.token, b"managed-work"),
        RespondOutcome::Ok
    );
    spin_until(|| host.is_writable(event.token));
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (response, trailers) = read_response_with_trailers(&mut stream, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"managed-work");
    assert!(trailers.is_empty());
    date_shape(&response);
    assert_date_first(&response.order, false);
    assert_eq!(
        response.order.len(),
        2,
        "plain Begin carries status plus date only"
    );
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
}
