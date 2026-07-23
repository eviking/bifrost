# ffi-go-poc

Proves that a Go process can query Grafana Loki through Apache DataFusion
**in-process** — no `bridge/` HTTP server in between — by loading
`LokiTableProvider` across a C ABI boundary via
[`datafusion-ffi`](../ffi-export/) and
[`datafusion-go`](https://github.com/datafusion-contrib/datafusion-go)'s cgo
bindings.

**This is a prototype, not what the real Grafana plugin uses.** See
`../ffi-export/README.md` for why (short version: the Go-side FFI
registration feature isn't in a tagged `datafusion-go` release yet).

## Prerequisites

- Rust toolchain (to build `bifrost-ffi-export` and, once, `datafusion-go`'s
  own native library)
- Go 1.24+ with cgo enabled (a C compiler on `PATH`)
- A running Loki instance with some data in it — see the root README's
  "Live demo" section for `docker run ... grafana/loki` + `scripts/push_logs.py`

## One-time setup

**1. Build `bifrost-ffi-export`** (from the repo root):

```sh
cargo build --release -p bifrost-ffi-export
```

**2. Copy the built library somewhere without a space in the path** (cgo
can't link against `-L` paths containing spaces; skip this step if your
checkout path has no spaces):

```sh
mkdir -p /tmp/bifrost-ffi-lib
cp ../target/release/libbifrost_ffi_export.dylib /tmp/bifrost-ffi-lib/   # macOS
# cp ../target/release/libbifrost_ffi_export.so /tmp/bifrost-ffi-lib/   # Linux
```

**3. Build `datafusion-go`'s own native library from source.** Required
because we're pinned to an unreleased commit with no prebuilt release asset
(see `../ffi-export/README.md`):

```sh
git clone https://github.com/datafusion-contrib/datafusion-go.git /tmp/datafusion-go-src
cd /tmp/datafusion-go-src
git checkout 41c5568d891f8c97928649292d5a06ed817d5d2d   # must match go.mod's pinned commit
make bundle   # needs a Rust toolchain; builds internal/native/lib/<goos>-<goarch>/libdatafusion_go.{a,dylib}
```

## Running

```sh
CGO_LDFLAGS="-L/tmp/bifrost-ffi-lib" \
DYLD_LIBRARY_PATH=/tmp/bifrost-ffi-lib \
DATAFUSION_GO_LIBRARY=/tmp/datafusion-go-src/internal/native/lib/darwin-arm64/libdatafusion_go.dylib \
GOSUMDB=off \
go run .
```

(On Linux, use `LD_LIBRARY_PATH` instead of `DYLD_LIBRARY_PATH`, and
`linux-amd64`/`linux-arm64` in the `DATAFUSION_GO_LIBRARY` path.)

Configurable via environment variables, matching `bridge/`'s own convention:

| Variable | Default |
|---|---|
| `LOKI_URL` | `http://localhost:3100` |
| `LOKI_STREAM_SELECTOR` | `{job="myapp"}` |
| `LOKI_LABELS` | `job,level,env,pod` |
| `QUERY` | `SELECT level, COUNT(*) AS n FROM logs WHERE level = 'error' GROUP BY level` |

Expected output against the repo's live demo setup:

```
bifrost-ffi-export DataFusion version: 53.1.0
datafusion-go DataFusion version:       53.1.0
=== running query in-process (no HTTP bridge) ===
SELECT level, COUNT(*) AS n FROM logs WHERE level = 'error' GROUP BY level
[error 1316]
OK: 1 row(s) queried through DataFusion in-process, zero HTTP hops to a bridge process
```

You can confirm the query genuinely pushed down into LogQL (rather than
fetching everything and filtering client-side) by tailing the Loki
container's logs while this runs — the same verification technique used
throughout this repo:

```sh
docker logs -f loki-demo 2>&1 | grep 'msg="executing query"'
# should show: query="{job=\"myapp\", level=\"error\"}"
```

## Cross-compiling for Linux (e.g. to run inside Docker)

cgo cannot cross-compile without a matching cross-compiler installed for the
target OS/arch — running `GOOS=linux go build` from macOS with only the
macOS `cc` toolchain fails with libc-symbol errors like
`error: call to undeclared function 'clearenv'`, since it's compiling
Linux-targeted C code with a macOS C library. If you don't have a Linux
cross-compiler set up, build inside a Linux container instead:

```sh
docker run --rm -v "$(pwd)/..:/work" -v go-mod-cache:/root/go/pkg/mod \
  rust:1.88-bookworm bash -c "
    apt-get update && apt-get install -y golang-go &&
    cd /work/ffi-go-poc &&
    CGO_LDFLAGS='-L/work/target/release' GOSUMDB=off CGO_ENABLED=1 \
      GOOS=linux GOARCH=arm64 go build -o ffi-go-poc-linux ."
```

(Match the container's Go version to your host's if it matters to you; the
`rust:1.88-bookworm` image ships whatever Debian's `apt` has, which may lag
behind. Building both `bifrost-ffi-export` and `datafusion-go`'s native
library for Linux should happen inside the same container, for the same
reason.)

**Also watch for glibc vs. musl**: the official `grafana/grafana` Docker
image is Alpine-based (musl libc) and cannot load a glibc-linked
`libbifrost_ffi_export.so`/`libdatafusion_go.so` built by a standard
Rust/Go Linux toolchain — loading it fails at the dynamic linker level with
errors like `Error loading shared library libgcc_s.so.1: No such file or
directory`. If you're running this inside `grafana/grafana` specifically,
use the `-ubuntu` tag variant (e.g. `grafana/grafana:11.3.0-ubuntu`), which
is glibc-based, or cross-compile for `musl` yourself.
