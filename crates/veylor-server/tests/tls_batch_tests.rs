use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use veylor_http2::{
    CLIENT_CONNECTION_PREFACE, CONTINUATION_FLAG_END_HEADERS, DATA_FLAG_END_STREAM,
    DEFAULT_MAX_FRAME_SIZE, FRAME_HEADER_LEN, FrameHeader, FrameType, HEADERS_FLAG_END_HEADERS,
    HEADERS_FLAG_END_STREAM, HpackEntrySlot, HpackFieldToEncode, HpackHeader, HpackRepresentation,
    HpackTable, SETTINGS_ACK, decode_frame_header, decode_header_block, encode_frame_header,
    encode_header_block,
};
use veylor_runtime::ConnectionToken;
use veylor_server::{
    EventSlot, HostEventKind, HostHandle, MAX_STATIC_BODY_BYTES, RespondOutcome, RunningServer,
    Server, ServerConfig, TlsConfig,
};
use veylor_tls::{TlsConnector, TlsStream};

const CERT_DER: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_cert.der");
const KEY_DER: &[u8] = include_bytes!("../../veylor-tls/tests/testdata/server_key.der");

const DEADLINE: Duration = Duration::from_secs(5);
// NOTE: longer than SLAB_HOLD_NS plus the two worker sweeps the release gate also needs.
const HOLD_WINDOW: Duration = Duration::from_millis(140);
const UPLOAD_CAP: u64 = 1 << 20;

type Tls = TlsStream<TcpStream>;

fn connector() -> TlsConnector {
    TlsConnector::builder()
        .tls13_only(true)
        .pin_certificate(CERT_DER)
        .expect("pin fixture")
        .alpn(&[b"h2"])
        .build()
        .expect("connector")
}

fn server_config() -> ServerConfig {
    ServerConfig {
        tls: Some(TlsConfig {
            cert_chain: vec![veylor_server::Certificate::from(CERT_DER.to_vec())],
            key: veylor_server::PrivateKey::Pkcs8(KEY_DER.to_vec()),
            max_handshakes: 16,
        }),
        ..ServerConfig::default()
    }
}

fn tls_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, server_config()).expect("bind tls");

    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register hello");
    server
        .register_managed(b"POST", b"/upload", UPLOAD_CAP)
        .expect("register upload");
    server.start().expect("start tls")
}

fn connect(running: &RunningServer) -> Tls {
    let stream = TcpStream::connect(running.addr()).expect("connect");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(DEADLINE))
        .expect("write timeout");
    let conn = connector()
        .connect("localhost")
        .expect("client builds handshake");
    let mut client = TlsStream::new(stream, conn);

    client.handshake().expect("handshake");

    client
}

fn open_h2(client: &mut Tls) -> Vec<u8> {
    assert_eq!(client.alpn(), Some(b"h2".as_slice()), "client observes h2");

    let mut stash = Vec::new();
    client
        .write_all(CLIENT_CONNECTION_PREFACE)
        .expect("write preface");
    write_frame(client, FrameType::Settings, 0, 0, &[]);

    let (header, _) = read_frame(client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);

    write_frame(client, FrameType::Settings, SETTINGS_ACK, 0, &[]);
    let (header, _) = read_frame(client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);
    assert_eq!(header.flags & SETTINGS_ACK, SETTINGS_ACK);

    stash
}

fn write_frame(client: &mut Tls, frame_type: FrameType, flags: u8, id: u32, payload: &[u8]) {
    let mut wire = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    append_frame(&mut wire, frame_type, flags, id, payload);
    client.write_all(&wire).expect("write frame");
}

fn append_frame(out: &mut Vec<u8>, frame_type: FrameType, flags: u8, id: u32, payload: &[u8]) {
    let header = FrameHeader {
        payload_length: payload.len() as u32,
        frame_type,
        flags,
        stream_id: id,
    };
    let mut encoded = [0; FRAME_HEADER_LEN];
    encode_frame_header(header, &mut encoded, DEFAULT_MAX_FRAME_SIZE).expect("encode header");
    out.extend_from_slice(&encoded);
    out.extend_from_slice(payload);
}

fn read_frame(client: &mut Tls, stash: &mut Vec<u8>) -> (FrameHeader, Vec<u8>) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if stash.len() >= FRAME_HEADER_LEN {
            let header = decode_frame_header(&stash[..FRAME_HEADER_LEN], DEFAULT_MAX_FRAME_SIZE)
                .expect("decode header");
            let total = FRAME_HEADER_LEN + header.payload_length as usize;
            if stash.len() >= total {
                let payload = stash[FRAME_HEADER_LEN..total].to_vec();
                stash.drain(..total);
                return (header, payload);
            }
        }
        assert!(Instant::now() < deadline, "h2 frame read timed out");
        let mut chunk = [0u8; 4096];
        match client.read(&mut chunk) {
            Ok(0) => panic!("eof while reading h2 frame"),
            Ok(n) => stash.extend_from_slice(&chunk[..n]),
            Err(error) => panic!("wire read during h2 frame: {error:?}"),
        }
    }
}

fn field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }
}

fn send_headers(client: &mut Tls, id: u32, method: &[u8], path: &[u8], end_stream: bool) {
    let block = request_block(method, path);
    let mut flags = HEADERS_FLAG_END_HEADERS;
    if end_stream {
        flags |= HEADERS_FLAG_END_STREAM;
    }
    write_frame(client, FrameType::Headers, flags, id, &block);
}

fn request_block(method: &[u8], path: &[u8]) -> Vec<u8> {
    let fields = vec![
        field(b":method", method),
        field(b":scheme", b"https"),
        field(b":path", path),
        field(b":authority", b"localhost"),
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut encoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut block = [0; 2048];
    let len = encode_header_block(&fields, &mut encoder, &mut block).expect("encode block");
    block[..len].to_vec()
}

fn decode_status(block: &[u8]) -> u16 {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut decoder = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut decoded = [HpackHeader::EMPTY; 32];
    let (count, _) = decode_header_block(block, &mut decoder, &mut decoded).expect("decode block");
    for header in decoded.iter().take(count) {
        let name = decoder.resolve(header.field.name).expect("name");
        if name == b":status" {
            let value = decoder.resolve(header.field.value).expect("value");
            return std::str::from_utf8(value)
                .expect("status utf8")
                .parse()
                .expect("status int");
        }
    }
    panic!("response block has no :status");
}

// NOTE: uploads make the server grant window credit, so WINDOW_UPDATE frames arrive between
// the response frames; a SETTINGS ACK can land there too. Both are skipped here and the
// response order is still enforced.
fn read_response(client: &mut Tls, stash: &mut Vec<u8>, id: u32) -> (u16, Vec<u8>) {
    let mut block = Vec::new();
    let mut headers_ended_stream = false;
    loop {
        let (header, payload) = read_frame(client, stash);
        match header.frame_type {
            FrameType::WindowUpdate | FrameType::Settings => {}
            FrameType::Headers => {
                assert_eq!(header.stream_id, id);
                headers_ended_stream = header.flags & HEADERS_FLAG_END_STREAM != 0;
                block.extend_from_slice(&payload);
                if header.flags & HEADERS_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            FrameType::Continuation => {
                assert_eq!(header.stream_id, id);
                block.extend_from_slice(&payload);
                if header.flags & CONTINUATION_FLAG_END_HEADERS != 0 {
                    break;
                }
            }
            found => panic!("expected HEADERS, got {found:?}"),
        }
    }
    let status = decode_status(&block);
    let mut body = Vec::new();
    if !headers_ended_stream {
        loop {
            let (header, payload) = read_frame(client, stash);
            match header.frame_type {
                FrameType::WindowUpdate | FrameType::Settings => {}
                FrameType::Data => {
                    assert_eq!(header.stream_id, id);
                    body.extend_from_slice(&payload);
                    if header.flags & DATA_FLAG_END_STREAM != 0 {
                        break;
                    }
                }
                found => panic!("expected DATA, got {found:?}"),
            }
        }
    }
    (status, body)
}

fn empty_slot() -> EventSlot {
    EventSlot {
        kind: HostEventKind::Shutdown,
        token: ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 0,
            generation: 0,
        },
        route_id: 0,
        route_token: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    }
}

fn poll_request(host: &HostHandle) -> EventSlot {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let mut events = vec![empty_slot(); 8];
        let mut methods = vec![0; 64];
        let mut paths = vec![0; 4096];
        let report = host.poll(100_000_000, &mut events, &mut methods, &mut paths);
        for event in events.iter().take(report.copied) {
            if event.kind == HostEventKind::Request {
                return *event;
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting for request");
    }
}

fn pull_all(host: &HostHandle, token: ConnectionToken) -> Vec<u8> {
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

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

// NOTE: a 30 KiB body spans several TLS records and several ciphertext turns, so the
// ciphertext read, the provider's partial-record queue and the ordered plaintext delivery
// are all exercised in one request. The managed body limit keeps the aggregate under
// H2_BODY_CAP, which is what a body without a declared length is admitted against.
#[test]
fn h2_tls_upload_spanning_records_arrives_intact() {
    let running = tls_server();
    let host = running.host();
    let mut client = connect(&running);
    let mut stash = open_h2(&mut client);

    let body = pattern(30_000);
    send_headers(&mut client, 1, b"POST", b"/upload", false);
    let frames = body.chunks(3_000).collect::<Vec<_>>();
    for (index, part) in frames.iter().enumerate() {
        let end = index + 1 == frames.len();
        let flags = if end { DATA_FLAG_END_STREAM } else { 0 };
        write_frame(&mut client, FrameType::Data, flags, 1, part);
    }

    let event = poll_request(&host);
    assert_eq!(pull_all(&host, event.token), body);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"stored"),
        RespondOutcome::Ok
    );

    let (status, response) = read_response(&mut client, &mut stash, 1);
    assert_eq!(status, 200);
    assert_eq!(response, b"stored");

    running.stop_accepting();
    let counts = running.join().expect("join clean");
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.transport_error, 0, "counts: {counts:?}");
}

// NOTE: the client sends its preface, SETTINGS and a complete request before it reads
// anything, so those bytes are already queued when the moved TLS slot gets its first
// readiness edge. That edge is the only proof the batch needs to read them.
#[test]
fn h2_tls_first_flight_is_served_from_the_queued_bytes() {
    let running = tls_server();
    let mut client = connect(&running);
    assert_eq!(client.alpn(), Some(b"h2".as_slice()), "client observes h2");

    let mut flight = Vec::new();
    flight.extend_from_slice(CLIENT_CONNECTION_PREFACE);
    append_frame(&mut flight, FrameType::Settings, 0, 0, &[]);
    let block = request_block(b"GET", b"/hello");
    append_frame(
        &mut flight,
        FrameType::Headers,
        HEADERS_FLAG_END_HEADERS | HEADERS_FLAG_END_STREAM,
        1,
        &block,
    );
    client.write_all(&flight).expect("write first flight");

    let mut stash = Vec::new();
    let (header, _) = read_frame(&mut client, &mut stash);
    assert_eq!(header.frame_type, FrameType::Settings);

    let (status, response) = read_response(&mut client, &mut stash, 1);
    assert_eq!(status, 200);
    assert_eq!(response, b"hello");

    running.stop_accepting();
    let counts = running.join().expect("join clean");
    assert_eq!(counts.h2_served, 1, "counts: {counts:?}");
    assert_eq!(counts.transport_error, 0, "counts: {counts:?}");
}

#[test]
fn h2_tls_keep_alive_survives_the_slab_hold_window() {
    let running = tls_server();
    let mut client = connect(&running);
    let mut stash = open_h2(&mut client);

    for (index, stream) in [1u32, 3, 5].iter().enumerate() {
        if index > 0 {
            std::thread::sleep(HOLD_WINDOW);
        }
        send_headers(&mut client, *stream, b"GET", b"/hello", true);
        let (status, body) = read_response(&mut client, &mut stash, *stream);
        assert_eq!(status, 200);
        assert_eq!(body, b"hello");
    }

    running.stop_accepting();
    let counts = running.join().expect("join clean");
    assert_eq!(counts.h2_served, 3, "counts: {counts:?}");
    assert_eq!(counts.transport_error, 0, "counts: {counts:?}");
}

#[test]
fn h2_tls_close_notify_closes_without_a_transport_error() {
    let running = tls_server();
    let mut client = connect(&running);
    let mut stash = open_h2(&mut client);

    send_headers(&mut client, 1, b"GET", b"/hello", true);
    let (status, body) = read_response(&mut client, &mut stash, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello");

    client.close_notify().expect("send close_notify");
    drop(client);

    std::thread::sleep(HOLD_WINDOW);

    running.stop_accepting();
    let counts = running.join().expect("join clean");
    assert_eq!(counts.peer_closed, 1, "counts: {counts:?}");
    assert_eq!(counts.transport_error, 0, "counts: {counts:?}");
}
