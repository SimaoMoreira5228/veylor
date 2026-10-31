#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TOOLS_DIR="${TOOLS_DIR:-$ROOT/local/tools}"
LOG_DIR="${LOG_DIR:-$ROOT/local/conformance}"
BIN="$TOOLS_DIR/bin"
mkdir -p "$BIN" "$LOG_DIR"

WORKERS="${WORKERS:-16}"
H2LOAD_CONNS="${H2LOAD_CONNS:-8}"
H2LOAD_STREAMS="${H2LOAD_STREAMS:-16}"
failures=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; failures=$((failures + 1)); }
skip() { printf 'SKIP  %s\n' "$1"; }

cargo build --release -p veylor-conformance >/dev/null

CERT="$LOG_DIR/cert.pem"
KEY="$LOG_DIR/key.pem"
if command -v openssl >/dev/null 2>&1 && [ ! -f "$CERT" ]; then
    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$KEY" -out "$CERT" -days 1 \
        -subj "/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1
fi

SERVER_LOG="$LOG_DIR/server.log"
"$ROOT/target/release/veylor-conformance" \
    --address 127.0.0.1:0 \
    --h2c-port 0 \
    --h3-port 0 \
    --workers "$WORKERS" \
    --tls-cert "$CERT" \
    --tls-key "$KEY" \
    >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
cleanup() {
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 100); do
    if grep -q '^READY ' "$SERVER_LOG"; then
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        cat "$SERVER_LOG"
        exit 1
    fi
    sleep 0.1
done
READY="$(grep -m1 '^READY ' "$SERVER_LOG")"
H1="$(printf '%s\n' "$READY" | sed -n 's/.* h1=\([^ ]*\).*/\1/p')"
H2C="$(printf '%s\n' "$READY" | sed -n 's/.* h2c=\([^ ]*\).*/\1/p')"
H2C_PORT="${H2C##*:}"
H3="$(printf '%s\n' "$READY" | sed -n 's/.* h3=\([^ ]*\).*/\1/p')"
TLS_ENABLED="$(printf '%s\n' "$READY" | sed -n 's/.* tls=\([^ ]*\).*/\1/p')"
printf 'server %s\n' "$READY"

if [ "$TLS_ENABLED" = "1" ]; then
    H1_URL="https://$H1/hello"
    H1_FLAGS="-k"
else
    H1_URL="http://$H1/hello"
    H1_FLAGS=""
fi
if curl -fsS --max-time 5 $H1_FLAGS --http1.1 "$H1_URL" >/dev/null; then
    pass "curl HTTP/1.1"
else
    fail "curl HTTP/1.1"
fi

if curl -fsS --max-time 5 --http2-prior-knowledge "http://$H2C/hello" >/dev/null; then
    pass "curl HTTP/2 prior knowledge"
else
    fail "curl HTTP/2 prior knowledge"
fi

if curl -kfsS --max-time 5 --http2 "https://$H1/hello" >/dev/null; then
    pass "curl HTTP/2 over TLS ALPN"
else
    fail "curl HTTP/2 over TLS ALPN"
fi

H2SPEC="$(command -v h2spec || true)"
if [ -z "$H2SPEC" ] && [ -x "$BIN/h2spec" ]; then
    H2SPEC="$BIN/h2spec"
fi
if [ -z "$H2SPEC" ] && command -v go >/dev/null 2>&1; then
    GOBIN="$BIN" go install github.com/summerwind/h2spec/cmd/h2spec@latest >/dev/null 2>&1 || true
    if [ -x "$BIN/h2spec" ]; then
        H2SPEC="$BIN/h2spec"
    fi
fi
if [ -n "$H2SPEC" ]; then
    if timeout 180 "$H2SPEC" -h 127.0.0.1 -p "$H2C_PORT" >"$LOG_DIR/h2spec.log" 2>&1; then
        pass "h2spec"
    else
        fail "h2spec"
        tail -40 "$LOG_DIR/h2spec.log"
    fi
else
    skip "h2spec (install with: GOBIN=$BIN go install github.com/summerwind/h2spec/cmd/h2spec@latest)"
fi

H2LOAD="$(command -v h2load || true)"
if [ -n "$H2LOAD" ]; then
    if timeout 120 "$H2LOAD" -n 10000 -c "$H2LOAD_CONNS" -m "$H2LOAD_STREAMS" \
        "http://$H2C/hello" >"$LOG_DIR/h2load.log" 2>&1; then
        if grep -Eq '[1-9][0-9]* (failed|errored)' "$LOG_DIR/h2load.log"; then
            fail "h2load (requests failed or errored)"
            grep -E 'finished in|requests:|status codes:' "$LOG_DIR/h2load.log" || true
        else
            pass "h2load 10000 requests, $H2LOAD_CONNS connections, $H2LOAD_STREAMS streams"
            grep -E 'finished in|requests:|status codes:' "$LOG_DIR/h2load.log" || true
        fi
    else
        fail "h2load"
        tail -40 "$LOG_DIR/h2load.log"
    fi
else
    skip "h2load (install the nghttp2 tools package)"
fi

if [ "$H3" != "-" ] && [ -n "$H3" ]; then
    if curl --version | grep -q 'ngtcp2\|nghttp3\|HTTP3'; then
        if curl -kfsS --max-time 5 --http3-only "https://$H3/hello" >/dev/null; then
            pass "curl HTTP/3"
        else
            fail "curl HTTP/3"
        fi
    else
        skip "curl HTTP/3 (curl built without HTTP/3)"
    fi
else
    skip "curl HTTP/3 (server started without --h3-port)"
fi

H3SPEC="$(command -v h3spec || true)"
if [ -n "$H3SPEC" ] && [ "$H3" != "-" ] && [ -n "$H3" ]; then
    H3_PORT="${H3##*:}"
    if timeout 180 "$H3SPEC" 127.0.0.1 "$H3_PORT" -n \
        -s "missing_extension TLS alert" >"$LOG_DIR/h3spec.log" 2>&1; then
        pass "h3spec (76/77; TLS 8.2 skipped: rustls/quinn gap)"
    else
        fail "h3spec"
        tail -40 "$LOG_DIR/h3spec.log"
    fi
else
    skip "h3spec (not installed)"
fi

printf '\n%d failure(s); logs in %s\n' "$failures" "$LOG_DIR"
exit "$failures"
