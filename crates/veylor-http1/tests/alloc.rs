use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use veylor_http1::{
    BodyEvent, BodyLimits, ConnectionHeader, FixedResponseBodyWriter, Header, Limits, Parser,
    RequestBodyAdmission, RequestBodyDecoder, RequestHead, ResponseFraming, ResponseToWrite,
    encode_response_head,
};

struct CountAlloc;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static DEALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[allow(unsafe_code)]
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::SeqCst);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        DEALLOCATIONS.fetch_add(1, Ordering::SeqCst);
    }
}

#[global_allocator]
static A: CountAlloc = CountAlloc;

fn reset_counts() {
    ALLOCATIONS.store(0, Ordering::SeqCst);
    ALLOCATED_BYTES.store(0, Ordering::SeqCst);
    DEALLOCATIONS.store(0, Ordering::SeqCst);
}

fn load_counts() -> (usize, usize, usize) {
    (
        ALLOCATIONS.load(Ordering::SeqCst),
        ALLOCATED_BYTES.load(Ordering::SeqCst),
        DEALLOCATIONS.load(Ordering::SeqCst),
    )
}

const REQUEST: &[u8] = b"GET /route/00042 HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n";
const ITERATIONS: usize = 64;

fn response_to_write() -> ResponseToWrite<'static> {
    ResponseToWrite {
        request_method: b"GET",
        requested_upgrade: b"",
        status: 200,
        reason: b"OK",
        fields: &[],
        framing: ResponseFraming::FixedLength,
        content_length: 2,
        connection: ConnectionHeader::Omit,
    }
}

fn run_once() -> usize {
    let split = REQUEST.len() / 2;
    let mut parser = Parser::new();
    let mut headers = [Header::default(); 8];
    let partial = parser
        .parse(&REQUEST[..split], &mut headers, Limits::default())
        .unwrap();
    assert!(partial.is_none());
    let head = parser
        .parse(REQUEST, &mut headers, Limits::default())
        .unwrap()
        .unwrap();
    assert_eq!(head.method(REQUEST), Some(b"GET".as_slice()));
    assert_eq!(head.target(REQUEST), Some(b"/route/00042".as_slice()));

    let response = response_to_write();
    let mut output = [0; 256];
    let encoded = encode_response_head(&response, &mut output).unwrap();
    assert!(encoded.length > 0);
    assert!(encoded.body_allowed);

    let mut body = FixedResponseBodyWriter::new(response.content_length, encoded.body_allowed);
    body.queue(b"ok").unwrap();
    assert!(body.advance_write(2).unwrap());
    encoded.length + 2
}

const FIXED_HEAD: &[u8] = b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 32\r\n\r\n";
const FIXED_BODY: &[u8] = b"0123456789abcdef0123456789abcdef";
const CHUNKED_HEAD: &[u8] =
    b"POST /upload HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n";
const CHUNKED_WIRE: &[u8] = b"3;foo=\"bar\"\r\nabc\r\n2;x=1\r\nde\r\n0\r\nDigest: ok\r\n\r\n";
const CHUNKED_BODY_LEN: usize = 5;
const ALLOWED_TRAILERS: [&[u8]; 1] = [b"Digest"];

fn parse_head(input: &[u8], headers: &mut [Header]) -> RequestHead {
    let mut parser = Parser::new();
    let partial = parser
        .parse(&input[..input.len() / 2], headers, Limits::default())
        .unwrap();
    assert!(partial.is_none());
    parser
        .parse(input, headers, Limits::default())
        .unwrap()
        .unwrap()
}

fn run_fixed_body_once() -> usize {
    let mut headers = [Header::default(); 8];
    let head = parse_head(FIXED_HEAD, &mut headers);
    let mut decoder = RequestBodyDecoder::new(
        head,
        FIXED_HEAD,
        &headers[..head.header_count],
        &[],
        BodyLimits::default(),
    )
    .unwrap();
    assert_eq!(
        decoder.admit_body(true),
        Ok(RequestBodyAdmission::ReadyToRead)
    );
    let mut trailers = [Header::default(); 4];

    let first = decoder
        .next(&FIXED_BODY[..16], false, &mut trailers, Limits::default())
        .unwrap();
    assert_eq!(first.consumed, 16);
    assert!(matches!(first.event, Some(BodyEvent::Data(_))));

    let second = decoder
        .next(&FIXED_BODY[16..], false, &mut trailers, Limits::default())
        .unwrap();
    assert_eq!(second.consumed, 16);
    assert!(matches!(second.event, Some(BodyEvent::Data(_))));

    let done = decoder
        .next(b"", false, &mut trailers, Limits::default())
        .unwrap();
    assert_eq!(done.event, Some(BodyEvent::Complete));
    FIXED_BODY.len()
}

fn run_chunked_once() -> usize {
    let mut headers = [Header::default(); 8];
    let head = parse_head(CHUNKED_HEAD, &mut headers);
    let mut decoder = RequestBodyDecoder::new(
        head,
        CHUNKED_HEAD,
        &headers[..head.header_count],
        &ALLOWED_TRAILERS,
        BodyLimits::default(),
    )
    .unwrap();
    assert_eq!(
        decoder.admit_body(true),
        Ok(RequestBodyAdmission::ReadyToRead)
    );

    let mut trailers = [Header::default(); 4];
    let mut staging = [0; 128];
    let mut staged = 0;
    let mut body_bytes = 0;
    let mut complete = false;
    let split = CHUNKED_WIRE.len() / 2;

    for half in [&CHUNKED_WIRE[..split], &CHUNKED_WIRE[split..]] {
        staging[staged..staged + half.len()].copy_from_slice(half);
        staged += half.len();
        let mut stalled = false;
        loop {
            let progress = decoder
                .next(&staging[..staged], false, &mut trailers, Limits::default())
                .unwrap();
            if let Some(BodyEvent::Data(span)) = progress.event {
                body_bytes += span.length;
            }
            if let Some(BodyEvent::Trailers { count }) = progress.event {
                assert_eq!(count, 1);
                let name = &staging
                    [trailers[0].name.offset..trailers[0].name.offset + trailers[0].name.length];
                let value = &staging
                    [trailers[0].value.offset..trailers[0].value.offset + trailers[0].value.length];
                assert_eq!(name, &b"Digest"[..]);
                assert_eq!(value, &b"ok"[..]);
            }
            if progress.event == Some(BodyEvent::Complete) {
                complete = true;
            }
            if progress.consumed > 0 {
                staging.copy_within(progress.consumed..staged, 0);
                staged -= progress.consumed;
            }
            if complete {
                break;
            }
            if progress.event.is_none() && progress.consumed == 0 {
                if stalled {
                    break;
                }
                stalled = true;
            } else {
                stalled = false;
            }
        }
        if complete {
            break;
        }
    }

    assert!(complete);
    assert_eq!(body_bytes, CHUNKED_BODY_LEN);
    body_bytes
}

// NOTE: unbounded allocator use on network-controlled input is a DoS vector.
// This gate pins the bounded-memory contract for the H1 request/response hot path.
#[test]
fn http1_hot_path_performs_no_heap_allocation() {
    let warmed_bytes = run_once();
    assert!(warmed_bytes > 0);
    assert_eq!(run_fixed_body_once(), FIXED_BODY.len());
    assert_eq!(run_chunked_once(), CHUNKED_BODY_LEN);

    reset_counts();

    let mut total_bytes = 0;
    for _ in 0..ITERATIONS {
        total_bytes += run_once();
    }
    let (hot_allocs, hot_bytes, hot_deallocs) = load_counts();

    reset_counts();

    let mut fixed_bytes = 0;
    for _ in 0..ITERATIONS {
        fixed_bytes += run_fixed_body_once();
    }
    let (fixed_allocs, fixed_allocated, fixed_deallocs) = load_counts();

    reset_counts();

    let mut chunked_bytes = 0;
    for _ in 0..ITERATIONS {
        chunked_bytes += run_chunked_once();
    }
    let (chunked_allocs, chunked_allocated, chunked_deallocs) = load_counts();

    assert_eq!(hot_allocs, 0);
    assert_eq!(hot_bytes, 0);
    assert_eq!(hot_deallocs, 0);
    assert_eq!(fixed_allocs, 0);
    assert_eq!(fixed_allocated, 0);
    assert_eq!(fixed_deallocs, 0);
    assert_eq!(chunked_allocs, 0);
    assert_eq!(chunked_allocated, 0);
    assert_eq!(chunked_deallocs, 0);

    println!(
        "http1: {ITERATIONS} iterations x {warmed_bytes} response bytes = {total_bytes} total bytes, {hot_allocs} allocs"
    );
    println!(
        "http1 fixed body: {ITERATIONS} iterations x {} body bytes = {fixed_bytes} total bytes, {fixed_allocs} allocs",
        FIXED_BODY.len()
    );
    println!(
        "http1 chunked body: {ITERATIONS} iterations x {CHUNKED_BODY_LEN} body bytes = {chunked_bytes} total bytes, {chunked_allocs} allocs"
    );
}
