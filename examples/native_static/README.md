# Native static server

Build the Linux example from the repository root:

```sh
just build-native-static
```

Run it for 120 seconds on port 8080:

```sh
./build/veylor-static 120 8080
```

It serves `GET /health` on loopback. The first argument sets the run time in seconds; the second sets
the port. Both are optional. The server drains active connections for up to five seconds before it
stops.

Use ApacheBench from another terminal for a fixed static response workload:

```sh
ab -n 50000 -c 8 -k http://127.0.0.1:8080/health
```

Keep the server's run time longer than the benchmark. ApacheBench reports throughput, latency
percentiles, and errors. At sub-millisecond latency, its percentile output may round away useful
differences. Its machine and client settings also affect the results.
