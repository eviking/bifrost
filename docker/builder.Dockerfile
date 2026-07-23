# bifrost-builder: Rust + Go + cgo toolchain for building Bifrost's native artifacts.
#
# This exists because the in-process (FFI) query mode requires cgo, and cgo cannot
# cross-compile from macOS to Linux without a matching Linux C toolchain (see
# ARCHITECTURE.md's "In-process (FFI) mode" section). Building inside this container
# on any host produces genuine linux/${TARGETARCH} artifacts.
#
# What it can build, all under /work (bind-mount the repo root there):
#   - libbifrost_ffi_export.so           (cargo build --release -p bifrost-ffi-export)
#   - bifrost-bridge                     (cargo build --release -p bifrost-bridge)
#   - datafusion-go's own native library (make bundle, from a datafusion-go checkout)
#   - gpx_bifrost_linux_${TARGETARCH}    (go build ./pkg, in grafana-plugin/, cgo-enabled)
#
# See docker/README.md for full build recipes using this image.

FROM rust:1.88-bookworm

ARG TARGETARCH
ARG GO_VERSION=1.26.5

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    ca-certificates \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz" -o /tmp/go.tgz \
    && tar -C /usr/local -xzf /tmp/go.tgz \
    && rm /tmp/go.tgz

ENV PATH="/usr/local/go/bin:/usr/local/cargo/bin:${PATH}"
ENV CGO_ENABLED=1
ENV GOSUMDB=off

WORKDIR /work
CMD ["bash"]
