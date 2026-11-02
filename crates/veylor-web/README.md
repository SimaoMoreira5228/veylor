# veylor-web

A router and handler layer over `veylor-server`. One app serves H1, H2 and H3.

```rust
let app = Router::with_state(store)
    .get("/accounts/:account/statement", statement)
    .post("/accounts/:account/transactions", transaction)
    .mount("/static/", "./public");

Server::bind(addr, config)?.serve(app)?;
```

Handlers are plain functions. They can take extractors (`Path<i64>`, `Query<QueryValues>`,
`State<Store>`, `Headers`, `Body`, `Json<T>`) or a `Request` when you want the raw bytes.

Two paths, picked per route:

- **Native.** GET/HEAD/DELETE/OPTIONS go through `register_native`. No park, no future.
  Path params are parsed and the response body is copied straight into the worker's output
  slab. Keep hot routes here.
- **Managed.** Anything with a body, or anything marked `managed()` or `*_async`, parks on
  the host and runs on one dispatcher thread that buffers the body first. Async handlers run
  there too, on a tiny thread-parking executor. Fine for database work, wrong for the hot
  path. You don't write the dispatch loop.

State is one `&'static` reference handed out through `State<T>`. It is never cloned per
request.

No tower, on purpose. `tower::Service` is async, and putting the native tier behind it means
boxing a future for every request. If you need middleware, wrap the handler. A tower bridge
can sit on the managed path later without touching the native one.

No `files()` helper either. `mount(prefix, dir)` is the engine's own file mount, so ranges,
gzip twins and ETags keep working.

The low-level API is still there. `Server`, `register_*` and `HostHandle` didn't move.
