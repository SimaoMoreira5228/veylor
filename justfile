ABI_CFLAGS := "-std=c11 -Wall -Wextra -Werror"

default: test

check:
    cargo check --workspace --all-targets

check-linux:
    cargo check --workspace --target x86_64-unknown-linux-gnu

# Cross compiler for the Windows-GNU target (aws-lc-sys drives it as CC).
# Override on machines where it lives elsewhere: `just windows-cc=... check-windows`.
windows-cc := "x86_64-w64-mingw32-gcc"

check-windows:
    CC_x86_64_pc_windows_gnu={{windows-cc}} cargo check --workspace --all-targets --target x86_64-pc-windows-gnu

format:
    cargo fmt --all

lint:
    cargo clippy --workspace --all-targets -- -D warnings

doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

test:
    cargo test --workspace

test-linux: test

# Full gate: format, lint, docs, tests, then the C ABI suite.
verify: format lint doc test integration-linux

build:
    cargo build --workspace

integration-linux: test-linux integration-rust-ffi-linux

# Full sanitizer screen (mirrors local/sanitizers/REPORT.md): sanitized cdylib,
# both C build flavors, all binaries under ASan+UBSan(+LSan), log verdicts.
sanitize:
    CARGO_TARGET_DIR=$PWD/local/sanitizers/target RUSTFLAGS="-Zsanitizer=address" cargo +nightly-x86_64-unknown-linux-gnu build -Zbuild-std --target x86_64-unknown-linux-gnu -p veylor-ffi
    ./local/sanitizers/build.sh
    ./local/sanitizers/build-int.sh
    mkdir -p local/sanitizers/logs local/sanitizers/logs-int
    for b in local/sanitizers/build/test-rust-ffi-* local/sanitizers/build/test-foreign-threads; do n=$(basename $b); ASAN_OPTIONS="detect_leaks=1:print_stacktrace=1:abort_on_error=0:halt_on_error=0" UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=0" timeout 90 ./$b > local/sanitizers/logs/$n.log 2>&1 || echo "EXIT $? (see log): $n"; done
    for b in local/sanitizers/build-int/*; do n=$(basename $b); ASAN_OPTIONS="detect_leaks=1:print_stacktrace=1:abort_on_error=0:halt_on_error=0" UBSAN_OPTIONS="print_stacktrace=1:halt_on_error=0" timeout 90 ./$b > local/sanitizers/logs-int/$n.log 2>&1 || echo "EXIT $? (see log): $n"; done
    grep -rIlE "ERROR|runtime error|WARNING|leaked|Sanitizer" local/sanitizers/logs local/sanitizers/logs-int && (echo "SANITIZER FINDINGS ABOVE"; exit 1) || echo "sanitize: all logs clean"

integration-rust-ffi-linux:
    mkdir -p build
    cargo build -p veylor-ffi
    clang {{ ABI_CFLAGS }} -Iinclude tests/rust_ffi_http1.c \
        -Ltarget/debug -lveylor_ffi -Wl,-rpath,$PWD/target/debug \
        -o build/test-rust-ffi-http1
    clang {{ ABI_CFLAGS }} -Iinclude tests/rust_ffi_client_response.c \
        -Ltarget/debug -lveylor_ffi -Wl,-rpath,$PWD/target/debug \
        -o build/test-rust-ffi-client-response
    clang {{ ABI_CFLAGS }} -Iinclude tests/rust_ffi_server_body.c \
        -Ltarget/debug -lveylor_ffi -Wl,-rpath,$PWD/target/debug \
        -o build/test-rust-ffi-server-body
    clang {{ ABI_CFLAGS }} -Iinclude tests/rust_ffi_server_response.c \
        -Ltarget/debug -lveylor_ffi -Wl,-rpath,$PWD/target/debug \
        -o build/test-rust-ffi-server-response
    clang {{ ABI_CFLAGS }} -Iinclude tests/rust_ffi_server_cancel.c \
        -Ltarget/debug -lveylor_ffi -Wl,-rpath,$PWD/target/debug \
        -o build/test-rust-ffi-server-cancel
    clang {{ ABI_CFLAGS }} -Iinclude tests/rust_ffi_h2_exchange.c \
        -Ltarget/debug -lveylor_ffi -Wl,-rpath,$PWD/target/debug \
        -o build/test-rust-ffi-h2-exchange
    clang {{ ABI_CFLAGS }} -Iinclude tests/rust_ffi_managed_static.c \
        -Ltarget/debug -lveylor_ffi -Wl,-rpath,$PWD/target/debug \
        -o build/test-rust-ffi-managed-static
    clang {{ ABI_CFLAGS }} -Iinclude tests/rust_ffi_managed_dispatch.c \
        -Ltarget/debug -lveylor_ffi -Wl,-rpath,$PWD/target/debug \
        -o build/test-rust-ffi-managed-dispatch
    clang {{ ABI_CFLAGS }} -Iinclude tests/rust_ffi_managed_native.c \
        -Ltarget/debug -lveylor_ffi -Wl,-rpath,$PWD/target/debug \
        -o build/test-rust-ffi-managed-native
    clang {{ ABI_CFLAGS }} -pthread -Iinclude tests/foreign_threads.c \
        -Ltarget/debug -lveylor_ffi -Wl,-rpath,$PWD/target/debug -pthread \
        -o build/test-foreign-threads
    build/test-rust-ffi-http1
    build/test-rust-ffi-client-response
    build/test-rust-ffi-server-body
    build/test-rust-ffi-server-response
    build/test-rust-ffi-server-cancel
    build/test-rust-ffi-h2-exchange
    build/test-rust-ffi-managed-static
    build/test-rust-ffi-managed-dispatch
    build/test-rust-ffi-managed-native
    build/test-foreign-threads
