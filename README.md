# Veylor

An HTTP engine for Rust. One connection core underneath HTTP/1.1, HTTP/2, HTTP/3, TLS and
WebSocket. The managed server owns the sockets; `veylor-web` adds routing and a small
handler layer on top.

This is a prototype, not a release. It builds, the conformance suites pass, and the
benchmarks run, but there is no versioning promise yet.

## What is in here

- `veylor-http1`, `veylor-http2`, `veylor-http3`: protocol state machines over caller-owned
  buffers.
- `veylor-server`: the managed worker and poller server, with H1, H2C, TLS H2 and H3.
- `veylor-web`: `Router`, native and managed tiers, borrowed extractors, file mounts,
  WebSocket echo and SSE. `veylor-web-tokio` runs async handlers on Tokio.
- `veylor-runtime-tokio`, `veylor-runtime-smol`: host-driven connection adapters.
- `veylor-ffi`: the `vyl_*` C ABI.

## Build and test

```sh
cargo test --workspace
just verify
```

`just verify` runs formatting, clippy, tests and the C ABI integrations. The conformance
scripts under `scripts/` need the Nix dev shell and Docker.

## Status

The H1 path and the managed server are the stable parts. H2 and H3 pass their conformance
suites apart from one TLS case. The web facade and the runtime adapters are newer. Expect
breaking changes.

## License

Apache-2.0.
