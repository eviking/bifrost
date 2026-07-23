# Docker images

Four Dockerfiles live here, all built from the repo root (they `COPY . .`, so the build
context is the whole repo — see `.dockerignore` for what's excluded). All are published
to Docker Hub under [`eviking/`](https://hub.docker.com/u/eviking).

| Dockerfile | Published as | What it is |
|---|---|---|
| `builder.Dockerfile` | `eviking/bifrost-builder` | Rust + Go + cgo toolchain, for reproducing the FFI native libraries and plugin binaries without installing anything locally |
| `bridge.Dockerfile` | `eviking/bifrost-bridge` | Standalone HTTP bridge process (`bridge/`) |
| `grafana.Dockerfile` | `eviking/bifrost-grafana` | Grafana + plugin, defaults to HTTP bridge query mode |
| `grafana-ffi.Dockerfile` | `eviking/bifrost-grafana-ffi` | Grafana + plugin, defaults to In-process (FFI) query mode with native libraries pre-wired |

## Building the images yourself

```sh
cd /path/to/Bifrost   # repo root, not docker/

docker build -f docker/bridge.Dockerfile      -t eviking/bifrost-bridge      .
docker build -f docker/grafana.Dockerfile     -t eviking/bifrost-grafana     .
docker build -f docker/grafana-ffi.Dockerfile -t eviking/bifrost-grafana-ffi .
docker build -f docker/builder.Dockerfile     -t eviking/bifrost-builder     .
```

Pass `--platform linux/arm64` or `--platform linux/amd64` to target a specific
architecture; all four Dockerfiles use `ARG TARGETARCH` and work with either.

**Memory note:** the root `Cargo.toml` sets `[profile.release] lto = true` for local
development builds. All three Dockerfiles that run `cargo build --release`
(`bridge.Dockerfile`, `grafana.Dockerfile`, `grafana-ffi.Dockerfile`) override that with
`CARGO_PROFILE_RELEASE_LTO=false`, because LTO-linking a binary that statically includes
DataFusion needs more memory than many CI runners or Docker Desktop's default VM size
have available — without the override, the link step can fail with `cannot allocate
memory` (or simply take 30+ minutes) rather than a normal compile error. This only
affects these Docker builds; `cargo build --release` locally still uses full LTO.

## Using `eviking/bifrost-builder` directly

The builder image is useful if you want the FFI native libraries or a `linux/arm64`
plugin binary without installing Rust, Go, or a C toolchain locally (e.g. building from
macOS, which can't cross-compile cgo binaries for Linux):

```sh
docker run --rm -v "$(pwd):/work" eviking/bifrost-builder \
  bash -c "cargo build --release -p bifrost-ffi-export"
```

The built library ends up at `target/release/libbifrost_ffi_export.so` on your host,
since `/work` is bind-mounted. From there, follow `ffi-export/README.md` or
`grafana-plugin/README.md`'s "Query mode: In-process (FFI)" section to link it into a
plugin binary.

## Taking the FFI libs and binary out of the build container and into a plain Grafana image

This is what `grafana-ffi.Dockerfile` automates, but the manual version (e.g. if you want
to mount the libraries into a Grafana image you don't control the Dockerfile for) is:

```sh
# 1. Build the Rust FFI export library and datafusion-go's native library inside the
#    builder container (see docker/builder.Dockerfile's header comment for what it can
#    build, and ARCHITECTURE.md's "In-process (FFI) mode" section for why datafusion-go
#    needs a from-source build).
mkdir -p /tmp/bifrost-native-libs
docker run --rm -v "$(pwd):/work" -v /tmp/bifrost-native-libs:/out eviking/bifrost-builder bash -c "
  cargo build --release -p bifrost-ffi-export &&
  cp target/release/libbifrost_ffi_export.so /out/ &&
  git clone --quiet https://github.com/datafusion-contrib/datafusion-go.git /tmp/dfgo-src &&
  cd /tmp/dfgo-src &&
  git checkout --quiet 41c5568d891f8c97928649292d5a06ed817d5d2d &&
  make bundle &&
  cp internal/native/lib/linux-\$(go env GOARCH)/libdatafusion_go.so /out/
"

# 2. Build the plugin binary linked against both libraries.
docker run --rm -v "$(pwd):/work" -v /tmp/bifrost-native-libs:/native-libs eviking/bifrost-builder bash -c "
  cd grafana-plugin &&
  CGO_LDFLAGS='-L/native-libs' DATAFUSION_GO_LIBRARY=/native-libs/libdatafusion_go.so \
    go build -o dist/gpx_bifrost_linux_\$(go env GOARCH) ./pkg
"

# 3. Mount both into any grafana/grafana:*-ubuntu container (glibc required -- see
#    ARCHITECTURE.md for why Alpine/musl won't load these).
docker run -d --name grafana -p 3000:3000 \
  -e GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=bifrost-datafusion-datasource \
  -e LD_LIBRARY_PATH=/native-libs \
  -e DATAFUSION_GO_LIBRARY=/native-libs/libdatafusion_go.so \
  -v "$(pwd)/grafana-plugin/dist:/var/lib/grafana/plugins/bifrost-datafusion-datasource" \
  -v /tmp/bifrost-native-libs:/native-libs \
  grafana/grafana:11.3.0-ubuntu
```

## Pushing to Docker Hub

```sh
docker login -u <your-username>   # use --password-stdin with an access token; a bare
                                   # `docker login` needs an interactive TTY for the
                                   # password prompt and will fail in non-interactive
                                   # shells (CI, some agent/automation contexts)

docker push eviking/bifrost-bridge:latest
docker push eviking/bifrost-grafana:latest
docker push eviking/bifrost-grafana-ffi:latest
docker push eviking/bifrost-builder:latest
```
