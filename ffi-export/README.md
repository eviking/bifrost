# bifrost-ffi-export

Exposes `LokiTableProvider` as a `datafusion_ffi::FFI_TableProvider` behind a
small C ABI, so it can be loaded **in-process** by a non-Rust host — see
`../ffi-go-poc/` for a working Go proof-of-concept using this to query Loki
through DataFusion with no separate HTTP process in between.

## Status: prototype, not the supported path

The Grafana plugin (`../grafana-plugin/`) does **not** use this crate. It
still talks to `../bridge/`, a small HTTP server, exactly as before. This
crate exists to prove out and de-risk removing that HTTP hop, using
[`datafusion-go`](https://github.com/datafusion-contrib/datafusion-go), a
community Go binding for DataFusion.

**Why this is prototype-only right now**: the specific feature this crate
depends on — `datafusion-go`'s `RegisterFFITableProvider`, which registers a
foreign (Rust) `TableProvider` into a Go-driven DataFusion session — does not
exist in `datafusion-go`'s tagged `v0.530100.1` release. It only exists on an
unreleased commit (`41c5568d891f`, 2026-07-16). Consuming it today means:

- Pinning `ffi-go-poc/go.mod` to that exact untagged commit hash rather than
  a version tag, with Go module checksum verification disabled for this one
  module (`GOSUMDB=off`), since the sum database has no entry for it.
- Building `datafusion-go`'s own native library **from source** yourself
  (its `make bundle`, which needs a Rust toolchain) rather than downloading a
  prebuilt release asset, since prebuilt assets are only published for
  tagged releases.

Neither of these are things you'd want in a production dependency today.
Revisit once `RegisterFFITableProvider` ships in a tagged `datafusion-go`
release with a matching prebuilt native library.

## Why DataFusion is pinned to exactly `53.1.0`

`datafusion-go` performs a strict version handshake before it will even
dereference a foreign provider pointer: the Go side's compiled-in
`DataFusionVersion` constant must exactly equal the version string this
crate reports via `bifrost_ffi_datafusion_version()`. This is deliberately
stricter than `datafusion-ffi`'s own ABI contract, because DataFusion itself
is pre-1.0 and its internal struct layouts aren't guaranteed stable across
even patch versions. See `RegisterFFITableProvider`'s doc comment in
`datafusion-go` for the full rationale.

Practically: `datafusion-loki` (the root crate), `bridge/`, and this crate
all pin `datafusion = "=53.1.0"` in lockstep. Bumping any of them to a newer
DataFusion release requires bumping `datafusion-go`'s pin too, or this
crate's version check (in `ffi-go-poc/main.go`) will fail loudly rather than
silently misbehave.

## Building

```sh
cargo build --release -p bifrost-ffi-export
```

Produces `target/release/libbifrost_ffi_export.dylib` (macOS) /
`.so` (Linux). This is a genuinely large artifact (~100MB+ even in release
mode, since it statically links all of DataFusion) — it is never checked
into git (`target/` is gitignored) and must be built locally.

### A note on paths with spaces

If your checkout path contains a space (as this repo's default clone
location under "Grafana Labs" does), cgo's `#cgo LDFLAGS` directive cannot
reference it directly — Go's linker doesn't handle unescaped spaces in `-L`
flags. Copy or symlink the built `.dylib`/`.so` into a space-free directory
and point `CGO_LDFLAGS`/`DYLD_LIBRARY_PATH` (macOS) or `LD_LIBRARY_PATH`
(Linux) at that instead:

```sh
mkdir -p /tmp/bifrost-ffi-lib
cp target/release/libbifrost_ffi_export.dylib /tmp/bifrost-ffi-lib/
```

See `../ffi-go-poc/README.md` for the full end-to-end run instructions.

## API surface

Three exported C functions (`src/lib.rs`):

| Function | Purpose |
|---|---|
| `bifrost_ffi_create_provider(base_url, stream_selector, labels_csv)` | Builds a `LokiTableProvider` and wraps it as an `FFI_TableProvider*`. Caller (Go) takes ownership of the returned pointer. |
| `bifrost_ffi_free_provider(provider)` | Frees a pointer returned by `bifrost_ffi_create_provider`. Must be called exactly once. |
| `bifrost_ffi_datafusion_version()` | Returns this build's exact DataFusion version, for the version-handshake check described above. |

All three are `extern "C"`, `#[no_mangle]`, documented with their safety
contracts inline in `src/lib.rs`.
