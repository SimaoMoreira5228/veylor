default: test

check:
    odin check src/runtime/connections -vet -no-entry-point -collection:veylor=src
    odin check src/http1 -vet -no-entry-point -collection:veylor=src
    odin check src/http1/body -vet -no-entry-point -collection:veylor=src
    odin check src/http1/response -vet -no-entry-point -collection:veylor=src
    odin check src/http1/connection -vet -no-entry-point -collection:veylor=src
    odin check src/transport -vet -no-entry-point

format:
	odinfmt src/runtime -w
	odinfmt src/http1 -w
	odinfmt src/transport -w
	odinfmt src/transport/linux -w

test:
    odin test src/http1/connection -all-packages -collection:veylor=src
    odin test src/runtime/connections -all-packages -collection:veylor=src

check-linux:
    odin check src/transport/linux -vet -no-entry-point -collection:veylor=src
    odin check src/transport/linux/socket -vet -no-entry-point -collection:veylor=src
    odin check src/runtime/linux -vet -no-entry-point -collection:veylor=src

test-linux:
    odin test src/runtime/linux -all-packages -collection:veylor=src

integration-linux: check-linux test-linux
