use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use veylor_http1::{Header, Limits, Parser, RequestHead};
use veylor_router::{RouteBodyPolicy, RouteCapture, RouteConfig, RouteKind, RouteSlot, RouteTable};

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

const LOOKUPS: usize = 1_000;

struct FrozenCase {
    table: RouteTable<'static, 'static>,
    head: RequestHead,
    request: Vec<u8>,
    target: Vec<u8>,
    route_count: usize,
}

fn parse_head(input: &[u8]) -> RequestHead {
    let mut parser = Parser::new();
    let mut headers = [Header::default(); 16];
    parser
        .parse(input, &mut headers, Limits::default())
        .unwrap()
        .unwrap()
}

fn lookup(table: &RouteTable<'static, 'static>, head: RequestHead, request: &[u8], target: &[u8]) {
    let mut captures = [RouteCapture::default(); 8];
    let matched = table.find_request(head, request, &mut captures).unwrap();
    assert_eq!(matched.pattern, target);
}

// NOTE: unbounded allocator use on network-controlled input is a DoS vector.
// This gate pins the bounded-memory contract for exact-match routing lookups.
#[test]
fn router_exact_match_performs_no_heap_allocation() {
    let paths: Vec<String> = (0..10_000)
        .map(|index| format!("/route/{index:05}"))
        .collect();
    let leaked: &'static [String] = Box::leak(paths.into_boxed_slice());

    let slots_small: &'static mut [RouteSlot<'static>] =
        Box::leak(vec![RouteSlot::EMPTY; 8].into_boxed_slice());
    let slots_medium: &'static mut [RouteSlot<'static>] =
        Box::leak(vec![RouteSlot::EMPTY; 100].into_boxed_slice());
    let slots_large: &'static mut [RouteSlot<'static>] =
        Box::leak(vec![RouteSlot::EMPTY; 10_000].into_boxed_slice());

    let mut cases: Vec<FrozenCase> = Vec::new();
    for (slots, count) in [(slots_small, 8), (slots_medium, 100), (slots_large, 10_000)] {
        let mut table = RouteTable::new(slots);
        for path in &leaked[..count] {
            let path_bytes: &'static [u8] = Box::leak(path.as_bytes().to_vec().into_boxed_slice());
            let config = RouteConfig {
                kind: RouteKind::Static,
                token: 0,
                body: RouteBodyPolicy::Reject,
            };
            table.register(b"GET", path_bytes, config).unwrap();
        }
        table.freeze().unwrap();

        let target = leaked[count - 1].as_bytes().to_vec();
        let mut request = b"GET ".to_vec();
        request.extend_from_slice(&target);
        request.extend_from_slice(b" HTTP/1.1\r\nHost: test\r\n\r\n");
        let head = parse_head(&request);
        let route_count = count;
        cases.push(FrozenCase {
            table,
            head,
            request,
            target,
            route_count,
        });
    }

    for case in &cases {
        lookup(&case.table, case.head, &case.request, &case.target);
    }

    reset_counts();

    let mut nanos = [0u128; 3];
    for (index, case) in cases.iter().enumerate() {
        let start = Instant::now();
        for _ in 0..LOOKUPS {
            lookup(&case.table, case.head, &case.request, &case.target);
        }
        nanos[index] = start.elapsed().as_nanos();
    }

    let allocations = ALLOCATIONS.load(Ordering::SeqCst);
    let allocated_bytes = ALLOCATED_BYTES.load(Ordering::SeqCst);
    let deallocations = DEALLOCATIONS.load(Ordering::SeqCst);
    assert_eq!(allocations, 0);
    assert_eq!(allocated_bytes, 0);
    assert_eq!(deallocations, 0);

    for (case, elapsed) in cases.iter().zip(nanos.iter()) {
        let per_lookup = elapsed / LOOKUPS as u128;
        println!(
            "router: {} routes, {LOOKUPS} lookups, {per_lookup} ns/lookup, {allocations} allocs",
            case.route_count
        );
    }
}
