ABI_CFLAGS := "-std=c11 -Wall -Wextra -Werror"

default: test

check:
    cargo check --workspace --all-targets

check-linux:
    cargo check --workspace --target x86_64-unknown-linux-gnu

format:
    cargo fmt --all

lint:
    cargo clippy --workspace --all-targets -- -D warnings

doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

test:
    cargo test --workspace

test-linux: test

build:
    cargo build --workspace

integration-linux: test-linux integration-rust-ffi-linux

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
