# Build and runtime are separate so the shipped image carries no toolchain and
# no source: it is the binary, its certificates, and nothing else.
FROM rust:1.98-slim-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
      pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Dependencies first, in their own layer, so a source change does not rebuild
# the whole dependency tree.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs \
    && cargo build --release --locked 2>/dev/null || true \
    && rm -rf src

COPY src ./src
# Touch the entry points so cargo notices them after the stub build above.
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# An unprivileged user with an explicit id, and a group-writable home, so the
# image also runs under a platform that assigns an arbitrary user id.
RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin ichnos \
    && chgrp -R 0 /home/ichnos && chmod -R g=u /home/ichnos

COPY --from=build /src/target/release/ichnos /usr/local/bin/ichnos
COPY schema /opt/ichnos/schema

USER 10001
WORKDIR /home/ichnos
ENV ICHNOS_LOG=info

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ichnos"]
CMD ["serve"]
