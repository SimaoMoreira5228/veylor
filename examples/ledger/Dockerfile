FROM rust:1.85-bookworm AS build

WORKDIR /source
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples

RUN cargo build --locked --release -p veylor-ledger

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 ledger \
    && useradd --uid 10001 --gid ledger --no-create-home ledger \
    && mkdir /data \
    && chown ledger:ledger /data

COPY --from=build /source/target/release/veylor-ledger /usr/local/bin/veylor-ledger

USER ledger:ledger
WORKDIR /data
EXPOSE 9999
STOPSIGNAL SIGTERM

ENTRYPOINT ["veylor-ledger"]
CMD ["--address", "0.0.0.0:9999", "--database", "/data/accounts.sqlite3", "--workers", "1"]
