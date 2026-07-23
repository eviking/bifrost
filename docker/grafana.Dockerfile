# bifrost-grafana: a standard Grafana image with the Bifrost datasource plugin
# pre-installed, defaulted to HTTP-bridge query mode.
#
# This DOES bundle libbifrost_ffi_export.so, even though it defaults to HTTP-bridge
# mode: the Go plugin's pkg/lokiffi package has an unconditional cgo directive
# (`#cgo LDFLAGS: -lbifrost_ffi_export`), so the binary dynamically links against
# that library at build AND run time regardless of which query mode a given
# datasource actually uses. There is currently no lighter "HTTP-only, zero FFI
# dependencies" build path -- that would require a Go build tag to make the cgo
# import conditional, which is a plugin source change, not a packaging one. See
# ARCHITECTURE.md's "In-process (FFI) mode" section.
#
# Because of that, and because libbifrost_ffi_export.so is glibc-linked, this image
# uses the same grafana/grafana:*-ubuntu (glibc) base as eviking/bifrost-grafana-ffi.
# The only real differences from that image are the default "queryMode" (http here,
# ffi there) and whether datafusion-go's own native library is pre-built and wired
# up for a zero-config FFI experience (only bifrost-grafana-ffi does that). If you
# select "In-process (FFI)" mode on this image's datasource, it will fail to
# initialize with a message pointing at DATAFUSION_GO_LIBRARY -- use
# eviking/bifrost-grafana-ffi instead if you want that path to work out of the box.
#
# Build from the repo root:
#   docker build -f docker/grafana.Dockerfile -t eviking/bifrost-grafana .
#
# Run alongside a bifrost-bridge container and a Loki instance:
#   docker run -d --name grafana -p 3000:3000 \
#     --add-host=host.docker.internal:host-gateway \
#     eviking/bifrost-grafana
# then add a "Bifrost (DataFusion + Loki)" datasource pointing at your bridge's URL.

ARG TARGETARCH

# --- frontend build ---
FROM node:20-bookworm AS frontend
WORKDIR /work
COPY grafana-plugin/package.json grafana-plugin/package-lock.json ./
# --legacy-peer-deps: grafana-plugin/package.json's devDependency on react@^19 conflicts
# with @grafana/data@13.1.1's peer dependency on react@^18 (plain `npm ci` fails with
# ERESOLVE). This is a pre-existing dependency conflict in the plugin's package.json,
# not something specific to this Docker build -- see grafana-plugin/README.md if it
# gets fixed upstream and this flag can be dropped.
RUN npm ci --legacy-peer-deps
COPY grafana-plugin/ ./
RUN npm run build

# --- backend build (cgo required: pkg/lokiffi is always compiled in, see above) ---
FROM rust:1.88-bookworm AS backend
ARG TARGETARCH
ARG GO_VERSION=1.26.5
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

# LTO disabled for the same reason as docker/bridge.Dockerfile: LTO-linking a
# binary that statically includes DataFusion needs more memory than many CI
# runners / Docker Desktop VMs have available.
ENV CARGO_PROFILE_RELEASE_LTO=false

# libbifrost_ffi_export.so is required at both build and runtime -- see the
# top-of-file note on pkg/lokiffi's unconditional cgo directive.
RUN cargo build --release -p bifrost-ffi-export
RUN mkdir -p /native-libs && cp /work/target/release/libbifrost_ffi_export.so /native-libs/
RUN cd grafana-plugin && CGO_LDFLAGS="-L/native-libs" \
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
