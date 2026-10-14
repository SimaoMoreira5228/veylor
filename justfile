default: test

check:
    odin check src/runtime/connections -vet -no-entry-point -collection:veylor=src
    odin check src/runtime/shutdown -vet -no-entry-point -collection:veylor=src
    odin check src/runtime/workers -vet -no-entry-point -collection:veylor=src
    odin check src/router -vet -no-entry-point -collection:veylor=src
    odin check src/http1 -vet -no-entry-point -collection:veylor=src
    odin check src/http1/body -vet -no-entry-point -collection:veylor=src
    odin check src/http1/response -vet -no-entry-point -collection:veylor=src
    odin check src/http1/connection -vet -no-entry-point -collection:veylor=src
    odin check src/transport -vet -no-entry-point

format:
	odinfmt src -w

test:
    odin test src/http1/connection -all-packages -collection:veylor=src -out:build/test-http1-connection
    odin test src/runtime/connections -all-packages -collection:veylor=src -out:build/test-runtime-connections
    odin test src/runtime/shutdown -all-packages -collection:veylor=src -out:build/test-runtime-shutdown
    odin test src/runtime/workers -all-packages -collection:veylor=src -out:build/test-runtime-workers
    odin test src/router -all-packages -collection:veylor=src -out:build/test-router

check-linux:
    odin check src/transport/linux -vet -no-entry-point -collection:veylor=src
    odin check src/transport/linux/socket -vet -no-entry-point -collection:veylor=src
    odin check src/runtime/linux -vet -no-entry-point -collection:veylor=src

test-linux:
    odin test src/runtime/linux -all-packages -collection:veylor=src -out:build/test-runtime-linux

integration-linux: check-linux test-linux
