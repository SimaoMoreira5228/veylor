use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use veylor_http1::{
    ConnectionHeader, FixedResponseBodyWriter, Header, Limits, Parser, ResponseFraming,
    ResponseToWrite, encode_response_head,
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

// NOTE: unbounded allocator use on network-controlled input is a DoS vector.
// This gate pins the bounded-memory contract for the H1 request/response hot path.
#[test]
fn http1_hot_path_performs_no_heap_allocation() {
    let warmed_bytes = run_once();
    assert!(warmed_bytes > 0);

    reset_counts();

    let mut total_bytes = 0;
    for _ in 0..ITERATIONS {
        total_bytes += run_once();
    }

    let allocations = ALLOCATIONS.load(Ordering::SeqCst);
    let allocated_bytes = ALLOCATED_BYTES.load(Ordering::SeqCst);
    let deallocations = DEALLOCATIONS.load(Ordering::SeqCst);
    assert_eq!(allocations, 0);
    assert_eq!(allocated_bytes, 0);
    assert_eq!(deallocations, 0);

    println!(
        "http1: {ITERATIONS} iterations x {warmed_bytes} response bytes = {total_bytes} total bytes, {allocations} allocs"
    );
}
