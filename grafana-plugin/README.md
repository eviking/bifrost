# bifrost-datafusion-datasource (Grafana plugin)

A Grafana backend datasource plugin that lets panels query Loki with SQL. It never talks to
Loki directly — every query goes through Apache DataFusion via one of two interchangeable
**query engines**, selected per-datasource in the config UI:

| Query mode | How it works | Status |
|---|---|---|
| **HTTP bridge** (default) | SQL is forwarded to `bifrost-bridge` (`../bridge/`), a separate Rust process embedding `datafusion-loki`, over HTTP | Supported |
| **In-process (FFI)** | `LokiTableProvider` is loaded directly into this plugin's own process via `datafusion-ffi`/`datafusion-go` — no bridge process at all | Experimental |

Both modes share identical time-macro handling and frame-building logic (`pkg/plugin/datasource.go`,
`pkg/plugin/frames.go`) — see `queryEngine` in `pkg/plugin/engine.go` for the seam between
them. Switching modes on an existing datasource is just flipping the "Query mode" radio
button in its config page; no rebuild required, since both engines are compiled into the
same plugin binary.

## Fastest way to try it: pre-built Docker images

```sh
docker run -d --name grafana -p 3000:3000 \
  --add-host=host.docker.internal:host-gateway \
  eviking/bifrost-grafana          # HTTP bridge mode by default
# or: eviking/bifrost-grafana-ffi  # In-process (FFI) mode by default, native libs pre-wired
```

Both images already have this plugin built in — no need to `npm run build` or `go build`
anything. See the root [`docker/README.md`](../docker/README.md) for the full image list
and how they're built. The rest of this file covers building the plugin from source.

## Query mode: In-process (FFI)

Loads `LokiTableProvider` (the Rust `TableProvider` in the repo root) directly into this
Go process's memory via [`datafusion-ffi`](https://crates.io/crates/datafusion-ffi) and
[`datafusion-go`](https://github.com/datafusion-contrib/datafusion-go)'s cgo bindings — see
`pkg/lokiffi/`. Verified working end-to-end on both `darwin/arm64` (native) and
`linux/arm64` (inside the actual `grafana/grafana:11.3.0-ubuntu` Docker image), including
confirming via Loki's own query logs that predicate/time-range pushdown still reaches Loki
correctly through the FFI boundary.

**Marked experimental because**: the specific `datafusion-go` feature this depends on
(`RegisterFFITableProvider`) is not in any tagged `datafusion-go` release — only on an
unreleased commit (`41c5568d891f`, pinned exactly in `go.mod`). Using this mode means:

- The plugin binary must be built with `CGO_ENABLED=1` and linked against
  `libbifrost_ffi_export.{dylib,so}` (built from `../ffi-export/`).
- `datafusion-go`'s own native library must be built from source (its `make bundle`,
  requiring a Rust toolchain) rather than downloaded as a prebuilt release asset.
- **The Docker image matters**: the default `grafana/grafana` image is Alpine/musl-based
  and cannot load glibc-linked shared libraries built by a standard Rust/Go toolchain —
  use the `-ubuntu` tag variant (e.g. `grafana/grafana:11.3.0-ubuntu`), or cross-compile
  for musl yourself. This is *not* required for HTTP-bridge mode, which has no native
  library dependency at all.

If you don't need this mode, leave the datasource on "HTTP bridge" (the default) and none
of the above applies to you.

### Building and running the FFI mode locally

```sh
# 1. Build the Rust FFI export library (from the repo root)
cargo build --release -p bifrost-ffi-export

# 2. Build datafusion-go's own native library from source, pinned to the same commit
#    as this plugin's go.mod
git clone https://github.com/datafusion-contrib/datafusion-go.git /tmp/dfgo
cd /tmp/dfgo && git checkout 41c5568d891f8c97928649292d5a06ed817d5d2d && make bundle

# 3. Build the plugin itself with cgo enabled, linked against the FFI export library
#    (adjust the -L path if your checkout has no spaces in it -- see ffi-export/README.md
#    for why a space-free path matters)
cp ../target/release/libbifrost_ffi_export.dylib /tmp/bifrost-ffi-lib/   # macOS
CGO_LDFLAGS="-L/tmp/bifrost-ffi-lib" GOSUMDB=off CGO_ENABLED=1 \
  go build -o dist/gpx_bifrost_darwin_arm64 ./pkg

# 4. Restart Grafana with the native libraries on its library search path
docker run -d --name grafana-demo -p 3000:3000 \
  -e LD_LIBRARY_PATH=/native-libs \
  -e DATAFUSION_GO_LIBRARY=/native-libs/libdatafusion_go.so \
  -v "$(pwd)/dist:/var/lib/grafana/plugins/bifrost-datafusion-datasource" \
  -v /path/to/native-libs:/native-libs \
  grafana/grafana:11.3.0-ubuntu   # note: -ubuntu tag, not the default Alpine image
```

Then in the datasource config page, switch "Query mode" to "In-process (FFI)" and fill in
Loki URL / stream selector / labels (there's no bridge process to carry that config in this
mode, so it lives directly on the datasource).

## Important: the dashboard time picker does nothing by default

Grafana's time range picker ("Last 6 hours", "Last 5 minutes", etc.) does **not**
automatically constrain your SQL. The picker's selected range is only applied if your query
uses the `$__timeFilter(...)` macro explicitly:

```sql
SELECT date_trunc('minute', timestamp) AS time, level, COUNT(*) AS count
FROM logs
WHERE $__timeFilter(timestamp)
GROUP BY time, level
ORDER BY time
```

Without it, the query runs exactly as written regardless of what the picker shows — you can
select "Last 5 minutes" and still see hours of data (or, just as easily, select "Last 6
hours" and see a narrower window than expected, if the query happens to be implicitly
bounded some other way). This produces no error; it just silently ignores the picker, which
is easy to mistake for a broken connection or stale data rather than a missing macro.

This follows the same convention as Grafana's official SQL datasources (Postgres, MySQL,
ClickHouse): the query editor doesn't rewrite arbitrary SQL for you, you opt in with a macro.

### Supported macros

| Macro | Expands to |
|---|---|
| `$__timeFilter(column)` | `column >= '<from>' AND column <= '<to>'` |
| `$__timeFrom()` | a quoted RFC3339 literal for the range start |
| `$__timeTo()` | a quoted RFC3339 literal for the range end |

Substitution happens in the Go plugin (`pkg/plugin/datasource.go`,
`applyTimeRangeMacros`) before the SQL is sent to the bridge. Because Bifrost's timestamp
pushdown (`src/time_range.rs` in the main crate) recognizes RFC3339 string literals compared
against the `timestamp` column, a `$__timeFilter(timestamp)` bound is pushed all the way
down into Loki's `start`/`end` query params — it isn't just a client-side post-filter.

### If you're debugging "the time picker doesn't seem to work"

1. Check the panel's SQL for `$__timeFilter(...)` — if it's missing, that's almost always
   the cause.
2. If it's present and still not filtering, use **Query inspector** in the panel editor to
   see the actual SQL sent (after macro substitution) and confirm the timestamps look right.
3. Confirm the underlying data actually spans the range you expect — see the "stale demo
   data" note in the main README's Live demo section; a `WHERE` clause can't show data that
   was never pushed to Loki for that window.

## Rebuilding after a change

The Go module now always depends on `datafusion-go` (for the FFI query mode), which
requires cgo. If you only care about HTTP-bridge mode and don't want to deal with the FFI
build prerequisites from the section above, plain `go build` still works fine — cgo will
link against the `datafusion-go` native library lazily at runtime only if the FFI engine is
actually constructed (i.e. only if a datasource is configured with Query mode = "In-process
(FFI)"). You still need `CGO_ENABLED=1` at build time either way, since the import graph
includes cgo code regardless of which mode a given datasource ends up using at runtime.

```sh
npm run build   # frontend: dist/module.js

# pkg/lokiffi has an UNCONDITIONAL cgo directive (`#cgo LDFLAGS: -lbifrost_ffi_export`),
# so libbifrost_ffi_export.{dylib,so} must be on the linker path (and later, on
# LD_LIBRARY_PATH/DYLD_LIBRARY_PATH at runtime) even for HTTP-bridge-only usage --
# see ARCHITECTURE.md's "In-process (FFI) mode" section. Build it first if you haven't:
#   cargo build --release -p bifrost-ffi-export   (from the repo root)
GOSUMDB=off CGO_ENABLED=1 CGO_LDFLAGS="-L../target/release" \
  GOOS=linux  GOARCH=arm64 go build -o dist/gpx_bifrost_linux_arm64  ./pkg
GOSUMDB=off CGO_ENABLED=1 CGO_LDFLAGS="-L../target/release" \
  GOOS=darwin GOARCH=arm64 go build -o dist/gpx_bifrost_darwin_arm64 ./pkg
```

**If `npm ci` fails with an `ERESOLVE` error** about `react@^19` conflicting with
`@grafana/data`'s `react@^18` peer dependency: this is a real, pre-existing conflict in
`package.json` (the `react`/`react-dom` devDependencies were bumped to 19 without
updating `@grafana/data`/`@grafana/ui`/`@grafana/runtime`, which still peer-depend on
18). Use `npm ci --legacy-peer-deps` until that's reconciled upstream.

Cross-compiling the Linux binary from macOS requires a Linux cross-compiler cgo can invoke,
which this repo doesn't assume you have. If you hit `cc: error: ...undeclared function...`
trying to `GOOS=linux` from macOS, build inside a Linux container instead — either
`eviking/bifrost-builder` (see `docker/README.md`) or `ffi-go-poc/README.md`'s note on
the same problem for a minimal standalone reproduction.

Grafana does not hot-reload a backend plugin process on file change — restart the Grafana
container (or process) after rebuilding for changes to take effect:

```sh
docker restart grafana-demo
```
