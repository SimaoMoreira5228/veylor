# veylor-web

A router and handler layer over `veylor-server`. One app serves H1, H2, H3, WebSocket and
SSE.

```rust
let app = Router::with_state(store)
    .get("/accounts/:account/statement", statement)
    .post("/accounts/:account/transactions", transaction)
    .route("/static/*path", files("./public"))
    .ws_echo("/ws")
    .sse("/events", events);

Server::bind(addr, config)?.serve(app)?;
```

Handlers are plain functions. Borrowing extractors (`Query<'_>`, `Headers<'_>`) and owned
ones (`Path<i64>`, `State<Store>`, `QueryOwned<T>`, `HeadersOwned`, `Bytes`, `Form<T>`,
`Json<T>`) both work, or take a `Request` when you want the raw bytes. `files(dir)` registers
the engine's file mount at the route's prefix, so ranges, gzip twins and ETags keep working.

Two paths, picked per route:

- **Native.** GET/HEAD/DELETE/OPTIONS go through `register_native`. No park, no future. Path
  params arrive as spans the engine already matched, and the response body is copied straight
  into the worker's output slab. Keep hot routes here.
- **Managed.** Anything with a body, or anything marked `managed()` or `*_async`, parks on
  the host and runs on one dispatcher per worker. The dispatcher keeps up to 128 in-flight
  requests per app and advances every one each turn, so one slow upload does not stall the
  rest. Async handlers run there too, on a thread-parking executor. Fine for database work,
  wrong for the hot path. You don't write the dispatch loop.

State is one `&'static` reference handed out through `State<T>`. It is never cloned per
request.

`ws_echo(path)` upgrades to the engine's echo tunnel. `sse(path, handler)` takes
`Fn(SseSender)` or `Fn(State<S>, SseSender)` and streams `text/event-stream`; the handler owns
its own thread per connection, so it can sleep between events and send heartbeats with
`comment`. SSE works on H1, H2 and H3.

No tower, on purpose. `tower::Service` is async, and putting the native tier behind it means
boxing a future for every request. If you need middleware, `layer()` wraps the handler on both
tiers; a tower bridge can sit on the managed path later without touching the native one.

The low-level API is still there. `Server`, `register_*` and `HostHandle` didn't move.
