# bifrost-grafana-ffi: a Grafana image with the Bifrost plugin AND the in-process
# (FFI) query engine's native libraries pre-built and wired up, so "In-process (FFI)"
# query mode works immediately with no manual library mounting.
#
# Uses grafana/grafana:*-ubuntu (glibc) rather than the default Alpine/musl image --
# required because libbifrost_ffi_export.so and datafusion-go's native library are
# glibc-linked. See ARCHITECTURE.md's "In-process (FFI) mode" section for why.
#
# Build from the repo root:
#   docker build -f docker/grafana-ffi.Dockerfile -t eviking/bifrost-grafana-ffi .
#
# Run:
#   docker run -d --name grafana -p 3000:3000 eviking/bifrost-grafana-ffi
# then add a "Bifrost (DataFusion + Loki)" datasource, select "In-process (FFI)"
# query mode, and set the Loki URL / stream selector / labels fields.

ARG TARGETARCH
ARG DATAFUSION_GO_COMMIT=41c5568d891f8c97928649292d5a06ed817d5d2d

# --- frontend build ---
FROM node:20-bookworm AS frontend
WORKDIR /work
COPY grafana-plugin/package.json grafana-plugin/package-lock.json ./
# --legacy-peer-deps: see docker/grafana.Dockerfile for why this is needed
# (pre-existing react@19 devDependency vs. @grafana/data@13.1.1's react@^18 peer dep).
RUN npm ci --legacy-peer-deps
COPY grafana-plugin/ ./
RUN npm run build

# --- native libs + backend build ---
FROM rust:1.88-bookworm AS backend
ARG TARGETARCH
ARG GO_VERSION=1.26.5
ARG DATAFUSION_GO_COMMIT
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git curl ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz" -o /tmp/go.tgz \
    && tar -C /usr/local -xzf /tmp/go.tgz && rm /tmp/go.tgz
ENV PATH="/usr/local/go/bin:${PATH}"
ENV CGO_ENABLED=1
ENV GOSUMDB=off

WORKDIR /work
COPY . .

# LTO is disabled for Docker builds specifically: the root Cargo.toml's
# [profile.release] lto = true is fine for local development, but LTO-linking a
# binary that statically includes all of DataFusion needs more memory than many
# CI runners / Docker Desktop VMs default to, and can fail with "cannot allocate
# memory" rather than just running slowly. Applies to both cargo builds below
# (bifrost-ffi-export and datafusion-go's own vendored DataFusion dependency).
# This override doesn't touch Cargo.toml or local `cargo build --release` behavior.
ENV CARGO_PROFILE_RELEASE_LTO=false

# 1. Build the Rust FFI export library.
RUN cargo build --release -p bifrost-ffi-export

# 2. Build datafusion-go's own native library from source -- required because the
#    Go-side FFI registration feature this depends on isn't in a tagged release yet
#    (see ARCHITECTURE.md's "In-process (FFI) mode" section).
RUN git clone --quiet https://github.com/datafusion-contrib/datafusion-go.git /tmp/dfgo-src \
    && cd /tmp/dfgo-src \
    && git checkout --quiet ${DATAFUSION_GO_COMMIT} \
    && make bundle

# 3. Build the plugin binary linked against both native libraries.
RUN mkdir -p /native-libs \
    && cp /work/target/release/libbifrost_ffi_export.so /native-libs/ \
    && cp /tmp/dfgo-src/internal/native/lib/linux-${TARGETARCH}/libdatafusion_go.so /native-libs/
RUN cd grafana-plugin \
    && CGO_LDFLAGS="-L/native-libs" DATAFUSION_GO_LIBRARY=/native-libs/libdatafusion_go.so \
       go build -o dist/gpx_bifrost_linux_${TARGETARCH} ./pkg

# --- final image ---
FROM grafana/grafana:11.3.0-ubuntu
COPY --from=frontend /work/dist/module.js /work/dist/module.js.map /work/dist/plugin.json \
    /var/lib/grafana/plugins/bifrost-datafusion-datasource/
COPY --from=backend /work/grafana-plugin/dist/gpx_bifrost_linux_* \
    /var/lib/grafana/plugins/bifrost-datafusion-datasource/
COPY --from=backend /native-libs/ /native-libs/

ENV GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=bifrost-datafusion-datasource
ENV LD_LIBRARY_PATH=/native-libs
ENV DATAFUSION_GO_LIBRARY=/native-libs/libdatafusion_go.so
