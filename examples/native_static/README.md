# Native static server

Build the Linux example from the repository root:

```sh
just build-native-static
```

Run it for 120 seconds on port 8080 with four workers:

```sh
./build/veylor-static 120 8080 4
```

It serves `GET /health` on loopback. The arguments set run time, port, and worker count, in that order.
All are optional; the worker count defaults to one and accepts values from one to four. With multiple
workers, each gets a separate `SO_REUSEPORT` listener on the same port and keeps its own connection
state and buffers. On exit it prints aggregate counters and the accepted-connection count for each
worker. The server drains active connections for up to five seconds before it stops.

Use ApacheBench from another terminal for a fixed static response workload:

```sh
ab -n 50000 -c 8 -k http://127.0.0.1:8080/health
```

Keep the server's run time longer than the benchmark. ApacheBench reports throughput, latency
percentiles, and errors. At sub-millisecond latency, its percentile output may round away useful
differences. Its machine and client settings also affect the results. Compare worker counts on the same
machine and with the same client settings; a multi-worker result is useful only if the workers actually
receive connections and the client has spare CPU.
