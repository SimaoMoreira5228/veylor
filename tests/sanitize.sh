#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
sanitize_dir="$PWD/target/sanitizers"
lib_dir="$sanitize_dir/target/x86_64-unknown-linux-gnu/debug"

CARGO_TARGET_DIR="$sanitize_dir/target" RUSTFLAGS="-Zsanitizer=address" \
    cargo +nightly-x86_64-unknown-linux-gnu build -Zbuild-std \
    --target x86_64-unknown-linux-gnu -p veylor-ffi

for flavor in base signed-overflow; do
    output_dir="$sanitize_dir/$flavor"
    mkdir -p "$output_dir"

    sanitizers=address,undefined
    if [[ "$flavor" == signed-overflow ]]; then
        sanitizers+=,signed-integer-overflow
    fi

    for source in tests/rust_ffi_*.c tests/foreign_threads.c; do
        name="$(basename "$source" .c)"
        binary="$output_dir/$name"

        clang -std=c11 -Wall -Wextra -Werror -Iinclude \
            "-fsanitize=$sanitizers" -fno-omit-frame-pointer -g -pthread \
            "$source" -L"$lib_dir" -lveylor_ffi "-Wl,-rpath,$lib_dir" \
            -o "$binary"

        if ! ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
            UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 \
            timeout 90 "$binary" > "$binary.log" 2>&1; then
            cat "$binary.log"
            exit 1
        fi

        if grep -Eq 'ERROR|runtime error|WARNING|leaked|Sanitizer' "$binary.log"; then
            cat "$binary.log"
            exit 1
        fi
    done
done

printf 'sanitize: all tests passed in both build flavors\n'
