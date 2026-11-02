#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use veylor_web::{
    Handler, Headers, HeadersArg, Param, Path, PathStateQueryArg, Query, QueryArg, Request, State,
};

struct Counting;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

struct Base(i64);

fn query_only(Query(query): Query<'_>) -> &'static str {
    if query.parse::<i64>("a") == Some(7) {
        "ok"
    } else {
        "no"
    }
}

fn headers_only(Headers(headers): Headers<'_>) -> &'static str {
    if headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case(b"x-token") && *value == b"abc")
    {
        "ok"
    } else {
        "no"
    }
}

fn statement(
    Path(account): Path<i64>,
    State(base): State<Base>,
    Query(query): Query<'_>,
) -> &'static str {
    let multiplier = query.parse::<i64>("m").unwrap_or(1);
    if (account + base.0) * multiplier == 426 {
        "ok"
    } else {
        "no"
    }
}

#[test]
fn borrowing_extractors_allocate_nothing() {
    static BASE: Base = Base(100);

    let headers: [(&[u8], &[u8]); 1] = [(b"x-token", b"abc")];
    let params: [Param; 1] = [Param {
        name: b"account",
        value: b"42",
    }];
    let request = Request::new(
        b"GET",
        b"/accounts/42/statement?m=3&a=7",
        &headers,
        &[],
        &params,
        &BASE,
    );

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let response = <_ as Handler<QueryArg, Base>>::call(&query_only, &request, &BASE);
    let by_query = ALLOCATIONS.load(Ordering::Relaxed) - before;

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let response_headers = <_ as Handler<HeadersArg, Base>>::call(&headers_only, &request, &BASE);
    let by_headers = ALLOCATIONS.load(Ordering::Relaxed) - before;

    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let response_statement =
        <_ as Handler<PathStateQueryArg<i64>, Base>>::call(&statement, &request, &BASE);
    let by_statement = ALLOCATIONS.load(Ordering::Relaxed) - before;

    assert_eq!(by_query, 0);
    assert_eq!(by_headers, 0);
    assert_eq!(by_statement, 0);
    assert_eq!(response.body().as_bytes(), b"ok");
    assert_eq!(response_headers.body().as_bytes(), b"ok");
    assert_eq!(response_statement.body().as_bytes(), b"ok");
}
