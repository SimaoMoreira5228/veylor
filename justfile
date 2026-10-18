ABI_CFLAGS := "-std=c11 -Wall -Wextra -Werror"

default: test

check:
    odin check src/runtime/connections -vet -no-entry-point -collection:veylor=src
    odin check src/runtime/shutdown -vet -no-entry-point -collection:veylor=src
    odin check src/runtime/workers -vet -no-entry-point -collection:veylor=src
    odin check src/runtime/host -vet -no-entry-point -collection:veylor=src
    odin check src/router -vet -no-entry-point -collection:veylor=src
    odin check src/http2 -vet -no-entry-point -collection:veylor=src
    odin check src/http1 -vet -no-entry-point -collection:veylor=src
    odin check src/http1/body -vet -no-entry-point -collection:veylor=src
    odin check src/http1/response -vet -no-entry-point -collection:veylor=src
    odin check src/http1/connection -vet -no-entry-point -collection:veylor=src
    odin check src/transport -vet -no-entry-point

format:
	odinfmt src -w
	odinfmt examples -w

test:
    odin test src/http1/connection -all-packages -collection:veylor=src -out:build/test-http1-connection
    odin test src/runtime/connections -all-packages -collection:veylor=src -out:build/test-runtime-connections
    odin test src/runtime/shutdown -all-packages -collection:veylor=src -out:build/test-runtime-shutdown
    odin test src/runtime/workers -all-packages -collection:veylor=src -out:build/test-runtime-workers
    odin test src/runtime/host -all-packages -collection:veylor=src -out:build/test-runtime-host
    odin test src/router -all-packages -collection:veylor=src -out:build/test-router
    odin test src/http2 -all-packages -collection:veylor=src -out:build/test-http2

check-linux:
    odin check src/transport/linux -vet -no-entry-point -collection:veylor=src
    odin check src/transport/linux/socket -vet -no-entry-point -collection:veylor=src
    odin check src/runtime/linux -vet -no-entry-point -collection:veylor=src

test-linux:
    odin test src/runtime/linux -all-packages -collection:veylor=src -out:build/test-runtime-linux

integration-linux: check-linux test-linux

build-native-static:
	odin build examples/native_static -collection:veylor=src -o:speed -out:build/veylor-static

integration-abi-linux:
	odin build src/abi -build-mode:shared -collection:veylor=src -o:speed -out:build/libveylor.so
	clang {{ABI_CFLAGS}} -Iinclude tests/abi_static_server.c -Lbuild -lveylor -o build/test-abi-static-server
	LD_LIBRARY_PATH=build build/test-abi-static-server
