use std::io::{Read, Write};
use std::net::TcpStream;
use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM,
    DEFAULT_MAX_FRAME_SIZE, FrameHeader, FrameType, HEADERS_FLAG_END_HEADERS,
    HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackFieldToEncode, HpackHeader, HpackRepresentation,
    HpackTable, SETTINGS_ACK, decode_frame_header, decode_header_block, encode_frame_header,
    encode_header_block,
};

pub(crate) fn h2_write_frame(
    stream: &mut TcpStream,
    frame_type: FrameType,
    flags: u8,
    id: u32,
    payload: &[u8],
) {
    let header = FrameHeader {
        payload_length: payload.len() as u32,
        frame_type,
        flags,
        stream_id: id,
    };
    let mut encoded = [0; 9];
    encode_frame_header(header, &mut encoded, DEFAULT_MAX_FRAME_SIZE).expect("encode header");
    stream.write_all(&encoded).expect("write header");
    stream.write_all(payload).expect("write payload");
}

pub(crate) fn h2_read_frame(stream: &mut TcpStream) -> (FrameHeader, Vec<u8>) {
    let mut encoded = [0; 9];
    stream.read_exact(&mut encoded).expect("read header");
    let header = decode_frame_header(&encoded, DEFAULT_MAX_FRAME_SIZE).expect("decode header");
    let mut payload = vec![0; header.payload_length as usize];
    stream.read_exact(&mut payload).expect("read payload");
    (header, payload)
}

pub(crate) fn h2_field(name: &'static [u8], value: &'static [u8]) -> HpackFieldToEncode<'static> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

pub(crate) fn h2_handshake(stream: &mut TcpStream) {
    stream
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write magic");
    h2_write_frame(stream, FrameType::Settings, 0, 0, &[]);
    let (header, _) = h2_read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.stream_id, 0);
    h2_write_frame(stream, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = h2_read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);
}

pub(crate) fn h2_send_headers(stream: &mut TcpStream, id: u32, fields: &[HpackFieldToEncode<'_>]) {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(fields, &mut encoder, &mut block).expect("encode block");
    h2_write_frame(
        stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        id,
        &block[..len],
    );
}

pub(crate) fn h2_send_get(stream: &mut TcpStream, id: u32, path: &'static [u8]) {
    let fields = [
        h2_field(b":method", b"GET"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", path),
        h2_field(b":authority", b"localhost"),
    ];
    h2_send_headers(stream, id, &fields);
}

pub(crate) fn h2_send_lying_get(stream: &mut TcpStream, id: u32, path: &'static [u8]) {
    let fields = [
        h2_field(b":method", b"GET"),
        h2_field(b":scheme", b"http"),
        h2_field(b":path", path),
        h2_field(b":authority", b"localhost"),
        h2_field(b"content-length", b"5"),
    ];
    h2_send_headers(stream, id, &fields);
}

pub(crate) fn h2_status(
    stream: &mut TcpStream,
    decoder: &mut HpackTable<'_>,
    id: u32,
    decoded: &mut [HpackHeader; 32],
) -> u16 {
    let (header, payload) = h2_read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, id);
    let (count, _) = decode_header_block(&payload, decoder, decoded).expect("decode block");
    let mut status = 0;
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    status
}

pub(crate) fn h2_data(stream: &mut TcpStream, id: u32) -> Vec<u8> {
    let (header, body) = h2_read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Data);
    assert_eq!(header.stream_id, id);
    assert_eq!(header.flags & DATA_FLAG_END_STREAM, DATA_FLAG_END_STREAM);
    body
}

pub(crate) fn h2_expect_rst(stream: &mut TcpStream, id: u32) {
    let (header, payload) = h2_read_frame(stream);
    assert_eq!(header.frame_type, FrameType::RstStream);
    assert_eq!(header.stream_id, id);
    assert_eq!(
        payload,
        1u32.to_be_bytes(),
        "framing lie must RST PROTOCOL_ERROR"
    );
}

pub(crate) fn h2_decoder<'a>(
    entries: &'a mut [HpackEntrySlot; 8],
    scratch: &'a mut [u8],
) -> HpackTable<'a> {
    HpackTable::new(entries, scratch, 4_096)
}

pub(crate) fn h2_send_headers_open(
    stream: &mut TcpStream,
    id: u32,
    fields: &[HpackFieldToEncode<'_>],
) {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(fields, &mut encoder, &mut block).expect("encode block");
    h2_write_frame(
        stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS,
        id,
        &block[..len],
    );
}

pub(crate) fn h2_write_data(stream: &mut TcpStream, id: u32, bytes: &[u8], end: bool) {
    let flags = if end { DATA_FLAG_END_STREAM } else { 0 };
    h2_write_frame(stream, FrameType::Data, flags, id, bytes);
}

pub(crate) fn h2_next_non_grant(stream: &mut TcpStream) -> (FrameHeader, Vec<u8>) {
    loop {
        let (header, payload) = h2_read_frame(stream);
        if header.frame_type == FrameType::WindowUpdate {
            continue;
        }
        return (header, payload);
    }
}

pub(crate) fn h2_send_trailers(stream: &mut TcpStream, id: u32, fields: &[HpackFieldToEncode<'_>]) {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(fields, &mut encoder, &mut block).expect("encode block");
    h2_write_frame(
        stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        id,
        &block[..len],
    );
}

pub(crate) fn h2_decode_status(
    decoder: &mut HpackTable<'_>,
    decoded: &mut [HpackHeader; 32],
    block: &[u8],
) -> u16 {
    let (count, _) = decode_header_block(block, decoder, decoded).expect("decode block");
    let mut status = 0;
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    status
}

pub(crate) fn h2_decode_pairs(
    decoder: &mut HpackTable<'_>,
    decoded: &mut [HpackHeader; 32],
    block: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (count, _) = decode_header_block(block, decoder, decoded).expect("decode block");
    decoded
        .iter()
        .take(count)
        .map(|header| {
            let name = decoder.resolve(header.field.name).expect("name").to_vec();
            let value = decoder.resolve(header.field.value).expect("value").to_vec();
            (name, value)
        })
        .collect()
}

pub(crate) fn h2_read_block_skipping_grants(stream: &mut TcpStream, id: u32) -> (Vec<u8>, bool) {
    let (header, payload) = h2_next_non_grant(stream);
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, id);
    let mut block = payload;
    let ended = header.flags & HEADERS_FLAG_END_STREAM != 0;
    let mut flags = header.flags;
    while flags & HEADERS_FLAG_END_HEADERS == 0 {
        let (next, payload) = h2_next_non_grant(stream);
        assert_eq!(next.frame_type, FrameType::Continuation);
        assert_eq!(next.stream_id, id);
        flags = next.flags;
        block.extend_from_slice(&payload);
        if flags & CONTINUATION_FLAG_END_HEADERS != 0 {
            break;
        }
    }
    (block, ended)
}

pub(crate) fn h2c_read_simple(
    stream: &mut TcpStream,
    decoder: &mut HpackTable<'_>,
    decoded: &mut [HpackHeader; 32],
    id: u32,
) -> (u16, Vec<u8>) {
    let (block, ended) = h2_read_block_skipping_grants(stream, id);
    let status = h2_decode_status(decoder, decoded, &block);
    let mut body = Vec::new();
    if !ended {
        loop {
            let (header, payload) = h2_next_non_grant(stream);
            assert_eq!(header.stream_id, id);
            assert_eq!(header.frame_type, FrameType::Data);
            body.extend_from_slice(&payload);
            if header.flags & DATA_FLAG_END_STREAM != 0 {
                break;
            }
        }
    }
    (status, body)
}

pub(crate) fn h2c_read_streamed_with_trailers(
    stream: &mut TcpStream,
    decoder: &mut HpackTable<'_>,
    decoded: &mut [HpackHeader; 32],
    id: u32,
) -> H2TraileredResponse {
    let (block, ended) = h2_read_block_skipping_grants(stream, id);
    assert!(!ended, "response HEADERS must not end a trailered stream");
    let status = h2_decode_status(decoder, decoded, &block);
    let mut body = Vec::new();
    let mut trailers = Vec::new();
    loop {
        let (header, payload) = h2_next_non_grant(stream);
        assert_eq!(header.stream_id, id);
        match header.frame_type {
            FrameType::Data => {
                body.extend_from_slice(&payload);
                if header.flags & DATA_FLAG_END_STREAM != 0 {
                    panic!("DATA must not end a trailered stream");
                }
            }
            FrameType::Headers => {
                let mut block = payload;
                let mut flags = header.flags;
                while flags & HEADERS_FLAG_END_HEADERS == 0 {
                    let (next, payload) = h2_next_non_grant(stream);
                    assert_eq!(next.frame_type, FrameType::Continuation);
                    assert_eq!(next.stream_id, id);
                    flags = next.flags;
                    block.extend_from_slice(&payload);
                }
                let end = header.flags & HEADERS_FLAG_END_STREAM != 0;
                trailers.extend(h2_decode_pairs(decoder, decoded, &block));
                assert!(end, "terminal HEADERS must end the stream");
                break;
            }
            found => panic!("unexpected frame: {found:?}"),
        }
    }
    (status, body, trailers)
}

pub(crate) type H2TraileredResponse = (u16, Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>);

pub(crate) fn h2_expect_rst_h2(stream: &mut TcpStream, id: u32, code: u32) {
    loop {
        let (header, payload) = h2_read_frame(stream);
        if header.frame_type == FrameType::WindowUpdate {
            continue;
        }
        assert_eq!(header.frame_type, FrameType::RstStream);
        assert_eq!(header.stream_id, id);
        assert_eq!(payload, code.to_be_bytes(), "unexpected RST code");
        return;
    }
}

// NOTE: file-mount-over-H2C coverage through the managed C ABI. Enabling H2C forces the
// start-time rebind (api.rs gates the direct path on no-H2C plus single worker), so the
// mount below exercises the ReadyRouteOp::FileMount replay (mod.rs ReadyRouteOp::apply
// into Server::register_file_mount); every stream then pins the H2 file dispatch shapes
// (h2/file.rs plan_file/emit_file, h2/routes.rs send_file_head) against the H1 matrix
// above. One stateful HPACK decoder rides the connection because response blocks index.

pub(crate) fn h2_file_field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

pub(crate) fn h2_send_file(
    stream: &mut TcpStream,
    id: u32,
    method: &[u8],
    path: &[u8],
    extra: &[(&[u8], &[u8])],
) {
    let mut fields = Vec::with_capacity(4 + extra.len());
    fields.push(h2_file_field(b":method", method));
    fields.push(h2_file_field(b":scheme", b"http"));
    fields.push(h2_file_field(b":path", path));
    fields.push(h2_file_field(b":authority", b"localhost"));
    for (name, value) in extra {
        fields.push(h2_file_field(name, value));
    }
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 1024];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    h2_write_frame(
        stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        id,
        &block[..len],
    );
}

pub(crate) type H2FileResponse = (u16, Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>);

pub(crate) fn h2c_read_full(
    stream: &mut TcpStream,
    decoder: &mut HpackTable<'_>,
    decoded: &mut [HpackHeader; 32],
    id: u32,
) -> H2FileResponse {
    let (block, ended) = h2_read_block_skipping_grants(stream, id);
    let pairs = h2_decode_pairs(decoder, decoded, &block);
    let status = pairs
        .iter()
        .find(|(name, _)| name == b":status")
        .map(|(_, value)| {
            std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int")
        })
        .expect(":status");
    let mut body = Vec::new();
    if !ended {
        loop {
            let (header, payload) = h2_next_non_grant(stream);
            assert_eq!(header.stream_id, id);
            assert_eq!(header.frame_type, FrameType::Data);
            body.extend_from_slice(&payload);
            if header.flags & DATA_FLAG_END_STREAM != 0 {
                break;
            }
        }
    }
    (status, pairs, body)
}

pub(crate) fn h2_header<'a>(headers: &'a [(Vec<u8>, Vec<u8>)], name: &str) -> Option<&'a str> {
    headers.iter().find_map(|(found, value)| {
        (found == name.as_bytes()).then(|| std::str::from_utf8(value).expect("header utf8"))
    })
}
