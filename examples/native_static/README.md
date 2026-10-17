# Native static server

Build the Linux example from the repository root:

```sh
just build-native-static
```

Run it for 120 seconds on port 8080 with four workers:

```sh
./build/veylor-static 120 8080 4
```

It serves `GET /health` and `GET`/`HEAD /assets/<path>` on loopback. File paths are opened beneath
`examples/native_static/public`, with symlinks and traversal rejected. Small files use the caller-owned
response buffer; larger bodies stream with `sendfile`.
Run it from the repository root so that directory resolves. The arguments set run time, port, and worker
count, in that order. All are optional; the worker count defaults to one and accepts values from one to
four. With multiple workers, each gets a separate `SO_REUSEPORT` listener on the same port and keeps its
own connection state and buffers. On exit it prints aggregate counters, per-worker accept and close
counts, workspace peaks, and nonzero close reasons. The server drains active connections for up to five
seconds before it stops.

## Limits in this sample

These are fixed values in `server_storage.odin`, not public Veylor defaults. The server has no
body-consuming handler or host event queue yet, so this table covers only the static server path.

| Limit | Per-worker value | When it is reached |
| --- | ---: | --- |
| Connection slots | 128 | The listener pauses; new clients queue while the kernel backlog has room. |
| Listen backlog hint | 128 | Linux may cap the effective backlog below this value. |
| Request workspaces | 8 | A readable request that cannot acquire a workspace is closed without a response. |
| Request-head input | 16 KiB per workspace | The worker returns 431 at this cap; request lines over 8 KiB get 414. |
| Header fields | 100 | The worker returns 431 and closes the connection. |
| Header field name/value | 256 B / 8 KiB | The parser returns 431 when either per-field limit is exceeded. |
| Response output | 1 KiB per workspace | The worker closes before writing a partial response. |
| Static file body | Files that fit the 1 KiB response buffer are copied there; larger files use 64 KiB `sendfile` turns. | `HEAD` sends headers and the file length only. |
| Declared request body | 16 MiB | A larger `Content-Length` on a matched static route gets 413 before body reads. |
| Events per worker turn | 128 | Remaining level-triggered events stay ready for a later turn. |
| Read/write bytes per connection turn | 64 KiB | The worker resumes the connection on a later turn. |
| Accept attempts per turn | 16 | Remaining clients stay in the listener backlog. |
| Timer entries | 130 | One per connection slot, plus two entries for server and listener deadlines. |
| Request-head and response-write deadlines | 10 seconds each | The connection closes when its deadline expires. |
| Keep-alive idle deadline | 5 seconds | The idle connection closes when its deadline expires. |
| Poll wait | 100 ms maximum | The worker wakes to check timers and control events. |
| Shutdown drain | 5 seconds | Remaining connections are force-closed at the deadline. |

The static route does not read request bodies. A body at or below the declared-size limit can still
leave unread bytes, in which case the response closes the connection. The fixed-length and chunked
body decoders have separate 16 MiB defaults, but they are not wired into this server path.

Use ApacheBench from another terminal for a fixed static response workload:

```sh
ab -n 50000 -c 8 -k http://127.0.0.1:8080/health
```

Keep the server's run time longer than the benchmark. ApacheBench reports throughput, latency
percentiles, and errors. At sub-millisecond latency, its percentile output may round away useful
differences. Its machine and client settings also affect the results. Compare worker counts on the same
machine and with the same client settings; a multi-worker result is useful only if the workers actually
receive connections and the client has spare CPU.
