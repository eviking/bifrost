# bifrost-bridge: standalone HTTP service embedding datafusion-loki.
#
# The default query mode for the Grafana plugin talks to this over HTTP. Build from
# the repo root:
#   docker build -f docker/bridge.Dockerfile -t eviking/bifrost-bridge .
#
# Configuration (env vars):
#   LOKI_URL             Loki base URL (default: http://localhost:3100)
#   LOKI_STREAM_SELECTOR Base LogQL selector for the "logs" table (default: {job="myapp"})
#   BRIDGE_ADDR           Bind address (default here: 0.0.0.0:8090, overriding the
#                         binary's own 127.0.0.1:8090 default so the container is
#                         actually reachable from outside)

FROM rust:1.88-bookworm AS builder
WORKDIR /work
COPY . .
# LTO is disabled for Docker builds specifically: the root Cargo.toml's
# [profile.release] lto = true is fine for local development, but LTO-linking a
# binary that statically includes all of DataFusion needs more memory than many
# CI runners / Docker Desktop VMs default to, and can fail with "cannot allocate
# memory" rather than just running slowly. This override doesn't touch Cargo.toml
# or local `cargo build --release` behavior.
ENV CARGO_PROFILE_RELEASE_LTO=false
RUN cargo build --release -p bifrost-bridge

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /work/target/release/bifrost-bridge /usr/local/bin/bifrost-bridge

ENV BRIDGE_ADDR=0.0.0.0:8090
ENV LOKI_URL=http://localhost:3100
ENV LOKI_STREAM_SELECTOR={job="myapp"}
EXPOSE 8090

ENTRYPOINT ["/usr/local/bin/bifrost-bridge"]
