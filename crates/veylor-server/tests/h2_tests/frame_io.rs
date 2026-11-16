use crate::h2_peer::*;
use crate::h2_response::*;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;
use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM,
    DEFAULT_MAX_FRAME_SIZE, ErrorCode, FRAME_HEADER_LEN, FrameHeader, FrameType,
    HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackFieldToEncode,
    HpackHeader, HpackRepresentation, HpackTable, SETTINGS_ACK, SETTINGS_ENTRY_LEN,
    decode_frame_header, decode_header_block, decode_rst_stream_payload, encode_frame_header,
    encode_header_block,
};
use veylor_server::h2::HEADER_BLOCK_CAP;

pub(crate) fn field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

pub(crate) fn write_frame(
    stream: &mut TcpStream,
    frame_type: FrameType,
    flags: u8,
    id: u32,
    payload: &[u8],
) {
    try_write_frame(stream, frame_type, flags, id, payload).expect("write frame");
}

// NOTE: a peer that closed early turns a frame write into a transport error; flood tests
// treat that as the interleaving where the verdict already landed, not as a failure.
pub(crate) fn try_write_frame(
    stream: &mut TcpStream,
    frame_type: FrameType,
    flags: u8,
    id: u32,
    payload: &[u8],
) -> std::io::Result<()> {
    let header = FrameHeader {
        payload_length: payload.len() as u32,
        frame_type,
        flags,
        stream_id: id,
    };
    let mut encoded = [0; FRAME_HEADER_LEN];
    encode_frame_header(header, &mut encoded, DEFAULT_MAX_FRAME_SIZE).expect("encode header");
    stream.write_all(&encoded)?;
    stream.write_all(payload)
}

pub(crate) fn read_frame(stream: &mut TcpStream) -> (FrameHeader, Vec<u8>) {
    let mut encoded = [0; FRAME_HEADER_LEN];
    stream.read_exact(&mut encoded).expect("read header");
    let header = decode_frame_header(&encoded, DEFAULT_MAX_FRAME_SIZE).expect("decode header");
    let mut payload = vec![0; header.payload_length as usize];
    stream.read_exact(&mut payload).expect("read payload");
    (header, payload)
}

pub(crate) fn handshake(stream: &mut TcpStream) {
    stream
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write magic");
    write_frame(stream, FrameType::Settings, 0, 0, &[]);
    let (header, _) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.stream_id, 0);
    write_frame(stream, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);
}

pub(crate) fn try_read_frame(stream: &mut TcpStream) -> Option<(FrameHeader, Vec<u8>)> {
    let mut encoded = [0; FRAME_HEADER_LEN];
    stream.read_exact(&mut encoded).ok()?;
    let header = decode_frame_header(&encoded, DEFAULT_MAX_FRAME_SIZE).ok()?;
    let mut payload = vec![0; header.payload_length as usize];
    stream.read_exact(&mut payload).ok()?;
    Some((header, payload))
}

pub(crate) fn handshake_advertised_setting(stream: &mut TcpStream, setting_id: u16) -> u32 {
    stream
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write magic");
    write_frame(stream, FrameType::Settings, 0, 0, &[]);
    let (header, payload) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.stream_id, 0);
    let mut advertised = None;
    for entry in payload.as_chunks::<SETTINGS_ENTRY_LEN>().0 {
        let id = u16::from_be_bytes([entry[0], entry[1]]);
        let value = u32::from_be_bytes([entry[2], entry[3], entry[4], entry[5]]);
        if id == setting_id {
            advertised = Some(value);
        }
    }
    write_frame(stream, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);
    advertised.expect("server advertises requested setting")
}

pub(crate) fn send_headers(
    stream: &mut TcpStream,
    id: u32,
    method: &'static [u8],
    path: &'static [u8],
    end: bool,
) {
    send_headers_extra(stream, id, method, path, &[], end);
}

// NOTE: long-:path sender beside send_headers_extra: its 2 KiB block cannot stage an
// over-long :path, so this one sizes the block at the header-block cap (one HEADERS frame,
// still under the 16 KiB max frame, HPACK literal overhead included).
pub(crate) fn send_long_headers(
    stream: &mut TcpStream,
    id: u32,
    method: &[u8],
    path: &[u8],
    end: bool,
) {
    let fields = vec![
        field(b":method", method),
        field(b":scheme", b"http"),
        field(b":path", path),
        field(b":authority", b"localhost"),
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = vec![0; HEADER_BLOCK_CAP];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    let mut flags = HEADERS_FLAG_END_HEADERS;
    if end {
        flags |= HEADERS_FLAG_END_STREAM;
    }
    write_frame(stream, FrameType::Headers, flags, id, &block[..len]);
}

pub(crate) fn send_headers_extra(
    stream: &mut TcpStream,
    id: u32,
    method: &[u8],
    path: &[u8],
    extra: &[(&[u8], &[u8])],
    end: bool,
) {
    let mut fields = vec![
        field(b":method", method),
        field(b":scheme", b"http"),
        field(b":path", path),
        field(b":authority", b"localhost"),
    ];
    for (name, value) in extra {
        fields.push(field(name, value));
    }
    send_header_fields(stream, id, &fields, end);
}

pub(crate) fn send_headers_no_authority(
    stream: &mut TcpStream,
    id: u32,
    method: &[u8],
    path: &[u8],
) {
    let fields = vec![
        field(b":method", method),
        field(b":scheme", b"http"),
        field(b":path", path),
    ];
    send_header_fields(stream, id, &fields, true);
}

pub(crate) fn send_header_fields(
    stream: &mut TcpStream,
    id: u32,
    fields: &[HpackFieldToEncode<'_>],
    end: bool,
) {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 2048];
    send_encoded_headers(stream, id, fields, end, &mut encoder, &mut block);
}

pub(crate) fn send_encoded_headers(
    stream: &mut TcpStream,
    id: u32,
    fields: &[HpackFieldToEncode<'_>],
    end: bool,
    encoder: &mut HpackTable<'_>,
    block: &mut [u8],
) {
    let len = encode_header_block(fields, encoder, block).expect("encode block");
    let mut flags = HEADERS_FLAG_END_HEADERS;
    if end {
        flags |= HEADERS_FLAG_END_STREAM;
    }
    write_frame(stream, FrameType::Headers, flags, id, &block[..len]);
}

pub(crate) fn send_headers_len(
    stream: &mut TcpStream,
    id: u32,
    method: &'static [u8],
    path: &'static [u8],
    length: Option<u64>,
    end: bool,
) {
    let length_text = length.map(|value| value.to_string());
    let mut fields = vec![
        field(b":method", method),
        field(b":scheme", b"http"),
        field(b":path", path),
        field(b":authority", b"localhost"),
    ];
    if let Some(text) = length_text.as_ref() {
        fields.push(HpackFieldToEncode {
            name: b"content-length",
            value: text.as_bytes(),
            representation: HpackRepresentation::WithoutIndexing,
            huffman_name: false,
            huffman_value: false,
        });
    }
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    let mut flags = HEADERS_FLAG_END_HEADERS;
    if end {
        flags |= HEADERS_FLAG_END_STREAM;
    }
    write_frame(stream, FrameType::Headers, flags, id, &block[..len]);
}

pub(crate) fn send_trailers(
    stream: &mut TcpStream,
    id: u32,
    fields: &[(&'static [u8], &'static [u8])],
) {
    let encoded: Vec<HpackFieldToEncode<'_>> = fields
        .iter()
        .map(|(name, value)| field(name, value))
        .collect();
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&encoded, &mut encoder, &mut block).expect("encode block");
    let flags = HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM;
    write_frame(stream, FrameType::Headers, flags, id, &block[..len]);
}

pub(crate) fn send_open_trailers(
    stream: &mut TcpStream,
    id: u32,
    fields: &[(&'static [u8], &'static [u8])],
) {
    let encoded: Vec<HpackFieldToEncode<'_>> = fields
        .iter()
        .map(|(name, value)| field(name, value))
        .collect();
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 512];
    let len = encode_header_block(&encoded, &mut encoder, &mut block).expect("encode block");
    write_frame(
        stream,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS,
        id,
        &block[..len],
    );
}

pub(crate) fn send_fragmented_headers(stream: &mut TcpStream, id: u32, fields: &[(&[u8], &[u8])]) {
    let encoded: Vec<HpackFieldToEncode<'_>> = fields
        .iter()
        .map(|(name, value)| field(name, value))
        .collect();
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 24_576];
    let len = encode_header_block(&encoded, &mut encoder, &mut block).expect("encode block");
    assert!(
        len > 16_384,
        "block must exceed the assembly cap, got {len}"
    );
    let mut chunks = block[..len].chunks(8_000);
    let first = chunks.next().expect("block");
    write_frame(stream, FrameType::Headers, 0, id, first);
    for chunk in chunks {
        write_frame(stream, FrameType::Continuation, 0, id, chunk);
    }
}

pub(crate) fn read_rst(stream: &mut TcpStream) -> (u32, ErrorCode) {
    let (header, payload) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::RstStream);
    let code = decode_rst_stream_payload(header, &payload).expect("decode rst");
    (header.stream_id, code)
}

pub(crate) fn post_upload(stream: &mut TcpStream, id: u32, body: &[u8]) {
    send_headers_len(
        stream,
        id,
        b"POST",
        b"/upload",
        Some(body.len() as u64),
        false,
    );
    let mut offset = 0;
    while offset < body.len() {
        let take = (body.len() - offset).min(4096);
        let last = offset + take == body.len();
        let flags = if last { DATA_FLAG_END_STREAM } else { 0 };
        write_frame(
            stream,
            FrameType::Data,
            flags,
            id,
            &body[offset..offset + take],
        );
        offset += take;
    }
}

pub(crate) fn spin_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + DEADLINE;
    while !ready() {
        assert!(Instant::now() < deadline, "spun waiting for host state");
        std::thread::yield_now();
    }
}

pub(crate) fn post_upload_open(stream: &mut TcpStream, id: u32, body: &[u8]) {
    send_headers_len(
        stream,
        id,
        b"POST",
        b"/upload",
        Some(body.len() as u64),
        false,
    );
    write_frame(stream, FrameType::Data, 0, id, body);
}

pub(crate) fn read_block(
    stream: &mut TcpStream,
    id: u32,
    first: (FrameHeader, Vec<u8>),
) -> (Vec<u8>, bool) {
    let (header, mut block) = first;
    assert_eq!(header.frame_type, FrameType::Headers);
    assert_eq!(header.stream_id, id);
    let mut ended = header.flags & HEADERS_FLAG_END_STREAM != 0;
    let mut flags = header.flags;
    while flags & HEADERS_FLAG_END_HEADERS == 0 {
        let (next, payload) = read_frame(stream);
        assert_eq!(next.frame_type, FrameType::Continuation);
        assert_eq!(next.stream_id, id);
        flags = next.flags;
        block.extend_from_slice(&payload);
        ended = ended || flags & HEADERS_FLAG_END_STREAM != 0;
        if flags & CONTINUATION_FLAG_END_HEADERS != 0 {
            break;
        }
    }
    (block, ended)
}

pub(crate) fn decode_fields(block: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (count, _) = decode_header_block(block, &mut decoder, &mut decoded).expect("decode block");
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

// NOTE: RFC 9113 section 8.2.2 length-match pin: a length-unknown Begin must ride :status-only
// HEADERS with END_STREAM clear; a content-length: 0 would turn the first DATA into a real-client
// PROTOCOL_ERROR, so every streaming test reads its head through here and the absence is asserted.
// Date rides first after :status here too (H1 Begin parity), shaped and exactly once; managed
// headers, when the host stages any, follow Date.
pub(crate) fn read_begin_block(stream: &mut TcpStream, id: u32) -> Vec<u8> {
    let first = loop {
        let frame = read_frame(stream);
        if frame.0.frame_type == FrameType::WindowUpdate {
            continue;
        }
        break frame;
    };
    let (block, ended) = read_block(stream, id, first);
    assert!(!ended, "streaming Begin HEADERS must leave the stream open");
    let fields = decode_fields(&block);
    assert!(fields.iter().any(|(name, _)| name.as_slice() == b":status"));
    assert!(
        fields
            .iter()
            .all(|(name, _)| name.as_slice() != b"content-length"),
        "length-unknown Begin omits content-length"
    );
    assert_eq!(fields[0].0, b":status", "status first on Begin");
    assert_eq!(fields[1].0, b"date", "date first after :status on Begin");
    date_value_shape(&fields[1].1);
    block
}

pub(crate) fn send_window_update(stream: &mut TcpStream, id: u32, increment: u32) {
    write_frame(
        stream,
        FrameType::WindowUpdate,
        0,
        id,
        &increment.to_be_bytes(),
    );
}
