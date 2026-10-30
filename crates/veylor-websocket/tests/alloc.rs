use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use veylor_http1::{Header, Limits as HttpLimits, Parser};
use veylor_websocket::{
    Limits, OpCode, WebSocket, encode_frame, encode_server_handshake, server_accept,
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

fn reset() {
    ALLOCATIONS.store(0, Ordering::SeqCst);
    ALLOCATED_BYTES.store(0, Ordering::SeqCst);
    DEALLOCATIONS.store(0, Ordering::SeqCst);
}

fn counts() -> (usize, usize, usize) {
    (
        ALLOCATIONS.load(Ordering::SeqCst),
        ALLOCATED_BYTES.load(Ordering::SeqCst),
        DEALLOCATIONS.load(Ordering::SeqCst),
    )
}

const REQUEST: &[u8] = b"GET /chat HTTP/1.1\r\nHost: server.example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
const MASK: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];

#[test]
fn handshake_and_frame_paths_do_not_allocate() {
    let mut headers = [Header::default(); 8];
    let mut parser = Parser::new();
    let head = parser
        .parse(REQUEST, &mut headers, HttpLimits::default())
        .unwrap()
        .unwrap();

    let mut handshake_out = [0u8; 256];
    reset();
    let handshake = server_accept(head, REQUEST, &headers[..head.header_count]).unwrap();
    let written = encode_server_handshake(&handshake, &mut handshake_out).unwrap();
    assert!(written > 0);
    assert_eq!(counts(), (0, 0, 0));

    let mut websocket = WebSocket::server(Limits::default());
    let mut frame = [0u8; 64];
    let length = encode_frame(true, OpCode::Text, Some(MASK), b"hello", &mut frame).unwrap();

    let mut response = [0u8; 64];
    reset();
    let feed = websocket.feed(&mut frame[..length]).unwrap();
    assert!(feed.event.is_some());
    let written = websocket
        .encode(true, OpCode::Text, None, b"hello", &mut response)
        .unwrap();
    assert!(written > 0);
    assert_eq!(counts(), (0, 0, 0));
}
