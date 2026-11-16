use crate::frame_io::*;
use crate::h2_peer::*;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::net::TcpStream;
use std::time::Instant;
use veylor_http2::{
    CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM, ErrorCode, FrameType,
    HEADERS_FLAG_END_HEADERS, HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackHeader, HpackTable,
    decode_goaway_payload, decode_header_block,
};
use veylor_runtime::ConnectionToken;
use veylor_server::{HostHandle, RespondOutcome};

pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) content_length: u64,
    pub(crate) content_type: Vec<u8>,
    pub(crate) content_encoding: Vec<u8>,
    pub(crate) vary: Vec<u8>,
    pub(crate) last_modified: Vec<u8>,
    pub(crate) content_range: Vec<u8>,
    pub(crate) allow: Vec<u8>,
    pub(crate) date: Vec<u8>,
    pub(crate) order: Vec<Vec<u8>>,
    pub(crate) body: Vec<u8>,
}

pub(crate) fn read_response(stream: &mut TcpStream, id: u32) -> Response {
    let (mut response, headers_ended_stream) = read_response_headers(stream, id);
    let mut body = Vec::new();
    if !headers_ended_stream {
        loop {
            let (header, payload) = read_frame(stream);
            assert_eq!(header.stream_id, id);
            assert_eq!(header.frame_type, FrameType::Data);
            body.extend_from_slice(&payload);
            if header.flags & DATA_FLAG_END_STREAM != 0 {
                break;
            }
        }
    }
    response.body = body;
    response
}

pub(crate) fn read_response_body(stream: &mut TcpStream, id: u32) -> Vec<u8> {
    let mut body = Vec::new();

    loop {
        let (header, payload) = read_frame(stream);
        assert_eq!(header.stream_id, id);
        assert_eq!(header.frame_type, FrameType::Data);
        body.extend_from_slice(&payload);

        if header.flags & DATA_FLAG_END_STREAM != 0 {
            return body;
        }
    }
}

pub(crate) fn read_response_bodies(
    stream: &mut TcpStream,
    streams: &[u32],
) -> HashMap<u32, Vec<u8>> {
    let mut bodies = HashMap::new();
    let mut ended = HashSet::new();

    while ended.len() < streams.len() {
        let (header, payload) = read_frame(stream);
        if matches!(
            header.frame_type,
            FrameType::Settings | FrameType::WindowUpdate
        ) {
            continue;
        }

        assert!(streams.contains(&header.stream_id));
        assert_eq!(header.frame_type, FrameType::Data);
        bodies
            .entry(header.stream_id)
            .or_insert_with(Vec::new)
            .extend_from_slice(&payload);

        if header.flags & DATA_FLAG_END_STREAM != 0 {
            ended.insert(header.stream_id);
        }
    }

    bodies
}

pub(crate) fn read_response_headers(stream: &mut TcpStream, id: u32) -> (Response, bool) {
    let mut block = Vec::new();
    let mut headers_ended_stream = false;
    loop {
        let (header, payload) = read_frame(stream);

        if matches!(
            header.frame_type,
            FrameType::Settings | FrameType::WindowUpdate
        ) {
            continue;
        }

        assert_eq!(header.stream_id, id);
        match header.frame_type {
            FrameType::Headers => {
                headers_ended_stream = header.flags & HEADERS_FLAG_END_STREAM != 0;
                block.extend_from_slice(&payload);
                if header.flags & HEADERS_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            FrameType::Continuation => {
                block.extend_from_slice(&payload);
                if header.flags & CONTINUATION_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            found => panic!("expected HEADERS, got {found:?}"),
        }
    }
    let mut response = decode_response(&block);
    response.body.clear();
    (response, headers_ended_stream)
}

pub(crate) fn decode_response(block: &[u8]) -> Response {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    decode_with(&mut decoder, block)
}

// NOTE: stateful HPACK decode for connections whose responses use the dynamic table (file
// headers index content-type incrementally): pass one table per connection, since a fresh
// table per response wipes the entries and desyncs after the first insertion.
pub(crate) fn decode_with(decoder: &mut HpackTable<'_>, block: &[u8]) -> Response {
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (count, _) = decode_header_block(block, decoder, &mut decoded).expect("decode block");
    let mut status = 0;
    let mut content_length = 0;
    let mut content_type = Vec::new();
    let mut content_encoding = Vec::new();
    let mut vary = Vec::new();
    let mut last_modified = Vec::new();
    let mut content_range = Vec::new();
    let mut allow = Vec::new();
    let mut date = Vec::new();
    let mut order = Vec::new();
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        let value = decoder.resolve(header.field.value).expect("value");
        order.push(name.to_vec());
        if name == b":status" {
            status = std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        } else if name == b"content-length" {
            content_length = std::str::from_utf8(value)
                .expect("length utf8")
                .parse()
                .expect("length int");
        } else if name == b"content-type" {
            content_type = value.to_vec();
        } else if name == b"content-encoding" {
            content_encoding = value.to_vec();
        } else if name == b"vary" {
            vary = value.to_vec();
        } else if name == b"last-modified" {
            last_modified = value.to_vec();
        } else if name == b"content-range" {
            content_range = value.to_vec();
        } else if name == b"allow" {
            allow = value.to_vec();
        } else if name == b"date" {
            date = value.to_vec();
        }
    }
    assert_ne!(status, 0, "response carries :status");
    Response {
        status,
        content_length,
        content_type,
        content_encoding,
        vary,
        last_modified,
        content_range,
        allow,
        date,
        order,
        body: Vec::new(),
    }
}

// NOTE: H1 date_shape mirror (server_tests.rs): 29-byte IMF-fixdate, GMT only, weekday
// comma, exactly one Date per HEADERS emission.
pub(crate) fn date_shape(response: &Response) -> String {
    let seen = std::str::from_utf8(&response.date)
        .expect("date utf8")
        .to_string();
    assert_eq!(seen.len(), 29, "IMF-fixdate width: {seen}");
    assert!(seen.ends_with(" GMT"), "GMT only: {seen}");
    assert_eq!(seen.as_bytes()[3], b',', "weekday comma: {seen}");
    let count = response
        .order
        .iter()
        .filter(|name| name.as_slice() == b"date")
        .count();
    assert_eq!(count, 1, "exactly one date");
    seen
}

// NOTE: Date-first pin mirroring H1 (Date rides first on every H1 assembly): :status then
// date, then the length pin per shape (length-unknown Begin/204/304 omit content-length).
pub(crate) fn assert_date_first(order: &[Vec<u8>], with_length: bool) {
    assert!(order.len() >= 2, "status plus date: {order:?}");
    assert_eq!(order[0], b":status", "status first: {order:?}");
    assert_eq!(order[1], b"date", "date first after :status: {order:?}");
    let lengths = order
        .iter()
        .filter(|name| name.as_slice() == b"content-length")
        .count();
    assert_eq!(
        lengths,
        usize::from(with_length),
        "content-length pin: {order:?}"
    );
}

// NOTE: raw field-list Date shape for :status-plus-date-only emissions (204/Begin), where
// the block decodes through decode_fields instead of the Response struct.
pub(crate) fn date_value_shape(value: &[u8]) -> String {
    let seen = std::str::from_utf8(value).expect("date utf8").to_string();
    assert_eq!(seen.len(), 29, "IMF-fixdate width: {seen}");
    assert!(seen.ends_with(" GMT"), "GMT only: {seen}");
    assert_eq!(seen.as_bytes()[3], b',', "weekday comma: {seen}");
    seen
}

pub(crate) fn read_response_stateful(
    stream: &mut TcpStream,
    id: u32,
    decoder: &mut HpackTable<'_>,
) -> Response {
    read_response_stateful_block(stream, id, decoder).0
}

// NOTE: same decode as `read_response_stateful`, but the raw header block comes back too, so a
// test can pin the leading dynamic table size update a recycled encoder must send.
pub(crate) fn read_response_stateful_block(
    stream: &mut TcpStream,
    id: u32,
    decoder: &mut HpackTable<'_>,
) -> (Response, Vec<u8>) {
    let mut block = Vec::new();
    let mut headers_ended_stream = false;
    loop {
        let (header, payload) = read_frame(stream);
        assert_eq!(header.stream_id, id);
        match header.frame_type {
            FrameType::Headers => {
                headers_ended_stream = header.flags & HEADERS_FLAG_END_STREAM != 0;
                block.extend_from_slice(&payload);
                if header.flags & HEADERS_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            FrameType::Continuation => {
                block.extend_from_slice(&payload);
                if header.flags & CONTINUATION_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            found => panic!("expected HEADERS, got {found:?}"),
        }
    }
    let mut response = decode_with(decoder, &block);
    let mut body = Vec::new();
    if !headers_ended_stream {
        loop {
            let (header, payload) = read_frame(stream);

            if header.stream_id == 0
                && matches!(
                    header.frame_type,
                    FrameType::Settings | FrameType::WindowUpdate
                )
            {
                continue;
            }

            assert_eq!(header.stream_id, id);
            assert_eq!(header.frame_type, FrameType::Data);
            body.extend_from_slice(&payload);
            if header.flags & DATA_FLAG_END_STREAM != 0 {
                break;
            }
        }
    }
    response.body = body;
    (response, block)
}

pub(crate) fn read_eof(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        other => panic!("expected EOF, got {other:?}"),
    }
}

// NOTE: a force close races in-flight bytes, so the kernel may deliver RST
// instead of FIN; either proves the connection is dead.
pub(crate) fn read_closed(stream: &mut TcpStream) {
    let mut byte = [0; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Err(_) => {}
        Ok(_) => panic!("expected close, got data"),
    }
}

pub(crate) fn read_goaway(stream: &mut TcpStream) -> (u32, ErrorCode) {
    let (header, payload) = read_frame(stream);
    assert_eq!(header.frame_type, FrameType::GoAway);
    let goaway = decode_goaway_payload(header, &payload).expect("decode goaway");
    (goaway.last_stream_id, goaway.error_code)
}

pub(crate) fn read_h1_body(stream: &mut TcpStream) -> (u16, Vec<u8>) {
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(error) => panic!("read head: {error:?}"),
        }
        assert!(head.len() < 65536, "head overflow");
    }
    let text = String::from_utf8(head).expect("head utf8");
    let mut lines = text.split("\r\n");
    let status = lines
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status int");
    let mut content_length = 0;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').expect("header colon");
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().expect("content length int");
        }
    }
    let mut body = vec![0; content_length];
    stream.read_exact(&mut body).expect("read body");
    (status, body)
}

pub(crate) fn pull_all(host: &HostHandle, token: ConnectionToken) -> Vec<u8> {
    let deadline = Instant::now() + DEADLINE;
    let mut body = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        let (outcome, copied, done) = host.pull_body(token, &mut chunk);
        assert_eq!(outcome, RespondOutcome::Ok);
        body.extend_from_slice(&chunk[..copied]);
        if done {
            return body;
        }
        assert!(Instant::now() < deadline, "staged body never completed");
    }
}

pub(crate) fn read_response_with_grants(
    stream: &mut TcpStream,
    id: u32,
) -> (Vec<(u32, u32)>, Response) {
    let mut grants = Vec::new();
    let mut block = Vec::new();
    let mut headers_ended_stream = false;
    loop {
        let (header, payload) = read_frame(stream);
        match header.frame_type {
            FrameType::WindowUpdate => {
                assert_eq!(payload.len(), 4);
                let increment =
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
                        & 0x7FFF_FFFF;
                grants.push((header.stream_id, increment));
            }
            FrameType::Headers => {
                assert_eq!(header.stream_id, id);
                headers_ended_stream = header.flags & HEADERS_FLAG_END_STREAM != 0;
                block.extend_from_slice(&payload);
                if header.flags & HEADERS_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            FrameType::Continuation => {
                block.extend_from_slice(&payload);
                if header.flags & CONTINUATION_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            found => panic!("expected HEADERS/WINDOW_UPDATE, got {found:?}"),
        }
    }
    let mut response = decode_response(&block);
    let mut body = Vec::new();
    if !headers_ended_stream {
        loop {
            let (header, payload) = read_frame(stream);
            assert_eq!(header.stream_id, id);
            assert_eq!(header.frame_type, FrameType::Data);
            body.extend_from_slice(&payload);
            if header.flags & DATA_FLAG_END_STREAM != 0 {
                break;
            }
        }
    }
    response.body = body;
    (grants, response)
}

pub(crate) fn pull_trailers_ready(host: &HostHandle, token: ConnectionToken) -> (Vec<u8>, Vec<u8>) {
    let deadline = Instant::now() + DEADLINE;
    let mut names = vec![0; 256];
    let mut values = vec![0; 1024];
    loop {
        let report = host.pull_trailers(token, &mut names, &mut values);
        match report.outcome {
            veylor_server::TrailerOutcome::Ok => {
                names.truncate(report.names_copied);
                values.truncate(report.values_copied);
                return (names, values);
            }
            veylor_server::TrailerOutcome::NotReady | veylor_server::TrailerOutcome::Stale => {}
            outcome => panic!("unexpected trailer outcome: {outcome:?}"),
        }
        assert!(Instant::now() < deadline, "trailers never staged");
        std::thread::yield_now();
    }
}

pub(crate) fn read_response_with_trailers(
    stream: &mut TcpStream,
    id: u32,
) -> (Response, TrailerFields) {
    let block = read_begin_block(stream, id);
    let mut ended = false;
    let mut response = decode_response(&block);
    let mut body = Vec::new();
    let mut trailers = Vec::new();
    while !ended {
        let (header, payload) = read_frame(stream);
        assert_eq!(header.stream_id, id);
        match header.frame_type {
            FrameType::Data => {
                body.extend_from_slice(&payload);
                ended = header.flags & DATA_FLAG_END_STREAM != 0;
            }
            FrameType::Headers => {
                let (block, end) = read_block(stream, id, (header, payload));
                ended = end;
                trailers.extend(decode_fields(&block));
            }
            FrameType::WindowUpdate => {}
            found => panic!("unexpected frame: {found:?}"),
        }
    }
    response.body = body;
    (response, trailers)
}

// NOTE: paired trailer reader: concurrent trailered streams interleave DATA/HEADERS frame by
// frame (one stream command applies per turn), so sequential per-stream readers would misroute
// siblings. This demuxes both streams in one loop until each sees its terminal HEADERS.
pub(crate) fn read_trailer_pair(
    stream: &mut TcpStream,
    ids: [u32; 2],
) -> [(Response, TrailerFields); 2] {
    let mut heads: [Option<Response>; 2] = [None, None];
    let mut bodies: [Vec<u8>; 2] = [Vec::new(), Vec::new()];
    let mut trailers: [TrailerFields; 2] = [Vec::new(), Vec::new()];
    let mut blocks: [Vec<u8>; 2] = [Vec::new(), Vec::new()];
    let mut ended = [false, false];
    let deadline = Instant::now() + DEADLINE;
    while ended != [true, true] {
        assert!(Instant::now() < deadline, "trailer pair never completed");
        let (header, payload) = read_frame(stream);
        if header.frame_type == FrameType::WindowUpdate {
            continue;
        }
        let index = ids
            .iter()
            .position(|id| *id == header.stream_id)
            .expect("pair stream");
        match header.frame_type {
            FrameType::Headers | FrameType::Continuation => {
                blocks[index].extend_from_slice(&payload);
                let complete = if header.frame_type == FrameType::Headers {
                    header.flags & HEADERS_FLAG_END_HEADERS != 0
                } else {
                    header.flags & CONTINUATION_FLAG_END_HEADERS != 0
                };
                if !complete {
                    continue;
                }
                let end = header.flags & HEADERS_FLAG_END_STREAM != 0;
                let block = std::mem::take(&mut blocks[index]);
                if heads[index].is_none() {
                    assert!(!end, "response HEADERS must not end a trailered stream");
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
                    heads[index] = Some(decode_response(&block));
                } else {
                    trailers[index].extend(decode_fields(&block));
                    ended[index] = end;
                }
            }
            FrameType::Data => {
                bodies[index].extend_from_slice(&payload);
                ended[index] = header.flags & DATA_FLAG_END_STREAM != 0;
            }
            found => panic!("unexpected frame: {found:?}"),
        }
    }
    let [first, second] = heads;
    let mut first = first.expect("first head");
    first.body = std::mem::take(&mut bodies[0]);
    let mut second = second.expect("second head");
    second.body = std::mem::take(&mut bodies[1]);
    [
        (first, std::mem::take(&mut trailers[0])),
        (second, std::mem::take(&mut trailers[1])),
    ]
}
