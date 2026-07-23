# Architecture

Deep technical reference for Bifrost: how the `TableProvider` maps SQL onto LogQL, what
gets pushed down versus computed locally, schema modes, pagination/ordering semantics,
version pinning rationale, and the two Grafana query-engine paths. For "what is this and
why would I use it," see [README.md](README.md).

## How it works

`LokiTableProvider` implements DataFusion's `TableProvider` trait. On `scan()`, DataFusion
hands it a set of filter expressions and a projection; the provider walks those expressions
and translates whatever it can into a LogQL selector, line filter, and `start`/`end` range,
then issues `query_range` calls against Loki's HTTP API and streams the results back as
Arrow `RecordBatch`es via a custom `ExecutionPlan`. Anything DataFusion can't reduce to a
pushable shape is still evaluated correctly — just after the fetch, inside DataFusion,
rather than inside Loki.

```
   SQL query
       │
       ▼
┌──────────────────┐        pushable predicates       ┌──────────┐
│  LokiTableProvider │ ───── (labels, line, time, IN) ──▶│  Loki    │
│  (DataFusion       │                                   │ query_range│
│   TableProvider)   │◀──── Arrow RecordBatches ─────────│  HTTP API │
└──────────────────┘                                   └──────────┘
       │
       ▼
  non-pushable predicates, GROUP BY, JOIN,
  window functions, ORDER BY on computed
  expressions — evaluated by DataFusion itself
```

## Pushdown reference

| SQL | LogQL |
|---|---|
| `label = 'x'` (or `labels['label'] = 'x'` in map mode) | `{..., label="x"}` |
| `label != 'x'` | `{..., label!="x"}` |
| `label ~ 'regex'` | `{..., label=~"regex"}` |
| `label IN ('a', 'b', 'c')` | `{..., label=~"a\|b\|c"}` |
| `label NOT IN ('a', 'b', 'c')` | `{..., label!~"a\|b\|c"}` |
| `label = 'a' OR label = 'b' OR label = 'c'` (same column) | `{..., label=~"a\|b\|c"}` |
| `line = 'x'` | `\|= "x"` |
| `line LIKE '%x%'` | `\|= "x"` |
| `line != 'x'` | `!= "x"` |
| `line IN (...)` / same-column `OR` on `line` | `\|~ "a\|b\|c"` |
| `timestamp > / >= / < / <= ts` | `start` / `end` query params |
| `LIMIT n` | row cap across paginated fetches |

`IN`/`OR`-alternation pushdown only applies when every value is a string literal and
(for `OR`) every branch targets the *same* column — mixed-column `OR` trees, numeric
predicates, `NOT` wrapping a pushable predicate, UDFs, and anchored/wildcard `LIKE`
patterns are still evaluated correctly by DataFusion after the scan, just not sent to
Loki, so a scan may fetch more rows than strictly match the final result.

## Two schema modes

**Flattened (recommended when labels are known ahead of time)**

```rust
let provider = LokiTableProvider::new(config, vec!["job".into(), "env".into()]);
```

`WHERE job = 'foo'` maps directly to a LogQL label matcher and gets pushed down.

**Map column (for unknown/arbitrary label sets)**

```rust
let provider = LokiTableProvider::new_with_map_labels(config);
```

All labels land in a `labels` column of type `Map<Utf8, Utf8>`; query with
`WHERE labels['job'] = 'foo'`. No schema discovery call is required. Predicates
of the shape `labels['x'] <op> literal` (equality/inequality/regex/`IN`/OR)
are pushed down into LogQL the same way flattened label columns are; only
predicates DataFusion can't reduce to that shape fall back to fetching
everything matching the base `stream_selector` and filtering client-side.

There's also `LokiTableProvider::connect(config)`, which calls Loki's
`/loki/api/v1/labels` endpoint to discover the label set automatically and builds a
flattened-schema provider from it — convenient for interactive use, but the schema can
shift silently if new labels appear upstream, so prefer an explicit list in production.

## Features

- **Schema mapping**: `timestamp` (`Timestamp(Nanosecond)`) + `line` (`Utf8`) + labels,
  either flattened into individual columns or collapsed into a `Map<Utf8, Utf8>` column.
- **Predicate pushdown**: label equality/inequality/regex, line `LIKE`/`=`/`!=`/regex, and
  timestamp range bounds are translated into LogQL selectors, line filters, and
  `start`/`end` params respectively. Everything else is still evaluated correctly by
  DataFusion post-scan.
- **Limit pushdown**: `LIMIT n` caps rows fetched across paginated Loki calls instead of
  pulling the whole result set.
- **Automatic pagination**: transparently issues follow-up `query_range` calls when a page
  comes back at Loki's per-request cap, advancing the time window each time.
- **Async streaming execution**: results are streamed as an `ExecutionPlan`, not buffered
  entirely in memory before returning to DataFusion.

## Design notes

- Loki log queries only return `resultType: "streams"`; this provider errors clearly if
  a query somehow produces a metric/instant result instead (it's built for `query_range`
  log queries, not `query`/instant metric queries).
- Loki responses are naturally partitioned by stream (unique label set); each page's
  entries are sorted by timestamp (ascending for forward direction, descending for
  backward) before conversion to Arrow, so pages are internally ordered and — combined
  with direction-monotonic pagination — the overall stream is globally ordered without
  requiring an explicit `ORDER BY` in SQL.
- Pagination follows Loki's actual `[start, end)` window semantics (`start` inclusive,
  `end` exclusive — matching Loki's own `logcli` reference client): the boundary
  timestamp is deliberately re-included on the next page rather than skipped past, and
  entries already emitted at that exact timestamp are deduplicated via a
  `(timestamp, labels, line)` key. This avoids silently dropping entries when many log
  lines share the same nanosecond timestamp, which a naive "advance by 1ns" approach
  would do. `LokiConfig::max_pages` bounds total pages as a safety net against unbounded
  scans.
- Self-hosted multi-tenant Loki (`auth_enabled: true`) is supported via
  `LokiConfig::with_tenant_id`, which sets the `X-Scope-OrgID` header.
- **Grafana Cloud Loki** uses HTTP Basic Auth instead: username is your stack's numeric
  Loki instance ID, password is an access-policy token (`logs:read` scope). It does
  **not** use `X-Scope-OrgID` — tenancy there is already implied by the per-stack URL and
  instance ID. Use `LokiConfig::with_basic_auth(instance_id, token)`, not
  `with_bearer_token` or `with_tenant_id`. See
  <https://grafana.com/docs/loki/latest/reference/python-client-examples/> for the
  authoritative reference this is based on.

## Status / caveats

This is a from-scratch implementation, targeting DataFusion 53.1.0 / Arrow 58.4.0.

- `cargo check --all-targets`, `cargo test` (24 unit tests + 10 integration tests against
  a mocked Loki HTTP server via `wiremock`, including regressions for the pagination
  dedup logic and a boundary-tie infinite-loop guard), and `cargo test --doc` all pass
  cleanly with zero warnings. Also verified end-to-end against a real
  `grafana/loki:3.1.0` container, including confirming via Loki's own server logs that
  `IN`, same-column `OR`, and map-mode `labels['x']` predicates are pushed down into
  LogQL regex alternation exactly as designed.
- Basic auth is wired through `reqwest::RequestBuilder::basic_auth` per-request rather
  than as a default header, since `reqwest` doesn't support default Basic-auth headers
  as cleanly as Bearer.
- Only log queries (`query_range` → `resultType: streams`) are supported — no metric
  query support (`sum(rate(...))`, etc.) is included in this table provider.
- The scan is always a single `ExecutionPlan` partition (`Partitioning::UnknownPartitioning(1)`)
  — no intra-scan parallelism across DataFusion threads, even for large time ranges.
- `LokiTableProvider::connect()` freezes the label set at registration time; labels that
  appear in Loki afterward are invisible until the table is re-registered.
- `IN`/`OR`-alternation pushdown requires literal string values and (for `OR`) every
  branch to target the same column; mixed-column `OR`, numeric predicates, and `NOT`
  wrapping an otherwise-pushable predicate still fall back to client-side filtering.
- If more entries share one exact nanosecond timestamp than fit in a single page
  (`LokiConfig::query_limit`), pagination cannot safely make progress past that tie and
  returns an error asking you to raise `query_limit`, rather than silently dropping data
  or looping — this mirrors Loki's own `logcli` client's behavior at the same edge case.

### Performance envelope

Bifrost inherits *all* of Loki's storage and query characteristics — it owns no data,
no index, no compression. Every query is bounded by Loki's own `query_range` performance
plus sequential HTTP pagination on top. It does not attempt to compete with purpose-built
OLAP stores (e.g. ClickHouse) on raw query throughput at scale; seconds-scale interactive
analytics over data already in Loki is the intended envelope, not sub-second aggregation
over billions of rows. See the "Bifrost vs. ClickHouse" comparison in `docs/` (ask the
maintainer if it isn't in your checkout) for a fuller breakdown of where each tool fits.

## Grafana plugin: two query engines

`grafana-plugin/` is a real Grafana backend datasource plugin (Go + TypeScript). It never
talks to Loki directly — every query goes through Apache DataFusion via one of two
interchangeable **query engines**, selected per-datasource in the config UI:

| Query mode | How it works | Status |
|---|---|---|
| **HTTP bridge** (default) | SQL is forwarded to `bridge/`, a separate Rust process embedding `datafusion-loki`, over HTTP | Supported |
| **In-process (FFI)** | `LokiTableProvider` is loaded directly into the plugin's own process via `datafusion-ffi`/`datafusion-go` — no bridge process at all | Experimental |

Both modes share identical time-macro handling and frame-building logic
(`grafana-plugin/pkg/plugin/datasource.go`, `grafana-plugin/pkg/plugin/frames.go`) — see
`queryEngine` in `grafana-plugin/pkg/plugin/engine.go` for the seam between them.
Switching modes on an existing datasource is just flipping the "Query mode" radio button
in its config page; no rebuild required, since both engines are compiled into the same
plugin binary.

### In-process (FFI) mode

Loads `LokiTableProvider` directly into the Go plugin process's memory via
[`datafusion-ffi`](https://crates.io/crates/datafusion-ffi) and
[`datafusion-go`](https://github.com/datafusion-contrib/datafusion-go)'s cgo bindings —
see `grafana-plugin/pkg/lokiffi/`. Verified working end-to-end on both `darwin/arm64`
(native) and `linux/arm64` (inside a real `grafana/grafana` Docker container), including
confirming via Loki's own query logs that predicate/time-range pushdown still reaches
Loki correctly through the FFI boundary.

**Marked experimental because**: the specific `datafusion-go` feature this depends on
(`RegisterFFITableProvider`) is not in any tagged `datafusion-go` release — only on an
unreleased commit, pinned exactly in `grafana-plugin/go.mod`. Using this mode means:

- The plugin binary must be built with `CGO_ENABLED=1` and linked against
  `libbifrost_ffi_export.{dylib,so}` (built from `ffi-export/`). This is a *dynamic*
  link (`#cgo LDFLAGS: -lbifrost_ffi_export` in `pkg/lokiffi/lokiffi.go`), and it's
  unconditional — the library must be present at both build time and runtime
  regardless of which query mode a given datasource actually selects. There is
  currently no way to build a "pure HTTP-bridge, zero FFI dependencies" plugin binary
  without adding a Go build tag to make that cgo import conditional (not done, since
  the HTTP-only path already works fine with the library present but unused).
- `datafusion-go`'s own native library must be built from source (its `make bundle`,
  requiring a Rust toolchain) rather than downloaded as a prebuilt release asset.
- **The Docker base image matters**: the default `grafana/grafana` image is Alpine/musl-based
  and cannot load glibc-linked shared libraries built by a standard Rust/Go toolchain —
  use the `-ubuntu` tag variant (e.g. `grafana/grafana:11.3.0-ubuntu`), or cross-compile
  for musl yourself. This applies to the plugin binary itself, not just the FFI engine:
  because the Go module always imports `pkg/lokiffi` (so both engines compile into one
  binary), `CGO_ENABLED=1` and a glibc-compatible runtime are required even if a given
  datasource only ever uses HTTP-bridge mode.

If you don't need this mode, leave the datasource on "HTTP bridge" (the default) — see
`grafana-plugin/README.md` for full build/run steps, `ffi-export/README.md` for the Rust
FFI export crate, and `ffi-go-poc/README.md` for a minimal standalone reproduction.

### The dashboard time picker does nothing by default

Grafana's time range picker ("Last 6 hours", "Last 5 minutes", etc.) does **not**
automatically constrain your SQL. The picker's selected range is only applied if your
query uses the `$__timeFilter(...)` macro explicitly:

```sql
SELECT date_trunc('minute', timestamp) AS time, level, COUNT(*) AS count
FROM logs
WHERE $__timeFilter(timestamp)
GROUP BY time, level
ORDER BY time
```

This follows the same convention as Grafana's official SQL datasources (Postgres, MySQL,
ClickHouse): the query editor doesn't rewrite arbitrary SQL for you, you opt in with a
macro. Supported macros:

| Macro | Expands to |
|---|---|
| `$__timeFilter(column)` | `column >= '<from>' AND column <= '<to>'` |
| `$__timeFrom()` | a quoted RFC3339 literal for the range start |
| `$__timeTo()` | a quoted RFC3339 literal for the range end |

Substitution happens in the Go plugin (`grafana-plugin/pkg/plugin/datasource.go`,
`applyTimeRangeMacros`) before the SQL reaches either engine. Because Bifrost's timestamp
pushdown (`src/time_range.rs`) recognizes RFC3339 string literals compared against the
`timestamp` column, a `$__timeFilter(timestamp)` bound is pushed all the way down into
Loki's `start`/`end` query params — it isn't just a client-side post-filter.

## Version pinning rationale

The workspace pins `datafusion = "=53.1.0"` and `arrow = "=58.4.0"` exactly, rather than a
range, for two concrete reasons hit during development:

- DataFusion 53.1.0 made SQL parsing a separate opt-in `"sql"` feature (deliberate
  upstream change); omitting it from `default-features = false` breaks the build with a
  confusing `expected BinaryOperator, found Operator` error that looks like a sqlparser
  version-skew bug but isn't.
- An unpinned `arrow = "*"` resolves independently of what DataFusion 53.1.0 uses
  internally, producing "multiple different versions of crate" compile errors. Arrow must
  match exactly what that DataFusion version resolves.

`datafusion-go` (the Go FFI binding used by the experimental in-process query mode) is
pinned to the same DataFusion version for ABI compatibility across the C boundary that
`datafusion-ffi`'s `FFI_TableProvider` crosses.
