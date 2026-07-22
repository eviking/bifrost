# datafusion-loki

An [Apache DataFusion](https://datafusion.apache.org/) `TableProvider` for querying
[Grafana Loki](https://grafana.com/oss/loki/) log streams with SQL. Under the hood,
SQL is translated (as much as possible) into [LogQL](https://grafana.com/docs/loki/latest/query/)
and sent to Loki's `query_range` HTTP API; results stream back as Arrow `RecordBatch`es.

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

## Example

```rust,no_run
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_loki::{LokiConfig, LokiTableProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Grafana Cloud: Basic Auth with your stack's numeric Loki instance ID as
    // the username and an access-policy token (logs:read) as the password.
    // Find both on your stack's Loki service details page in the Cloud portal.
    let config = LokiConfig::new("https://logs-prod-006.grafana.net", r#"{job="myapp"}"#)
        .with_basic_auth("123456", "glc_your-access-policy-token");

    let provider = LokiTableProvider::new(config, vec!["job".into(), "level".into(), "pod".into()]);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider))?;

    let df = ctx
        .sql(r#"
            SELECT timestamp, line, level
            FROM logs
            WHERE level = 'error' AND line LIKE '%panic%'
            ORDER BY timestamp DESC
            LIMIT 100
        "#)
        .await?;

    df.show().await?;
    Ok(())
}
```

For self-hosted Loki with multi-tenancy enabled (`auth_enabled: true`), use
`with_tenant_id` (sends `X-Scope-OrgID`) instead — Grafana Cloud does not use that
header, since tenancy there is already implied by the per-stack URL and instance ID.

Run the bundled example against a local or remote Loki:

```sh
LOKI_URL=http://localhost:3100 cargo run --example query_loki
```

## Live demo (verified against a real Loki)

This was tested end-to-end against a real `grafana/loki:3.1.0` container, not just mocks.

```sh
# 1. Start Loki
docker run -d --name loki-demo -p 3100:3100 grafana/loki:3.1.0 \
  -config.file=/etc/loki/local-config.yaml

# 2. Push some sample log data (300 synthetic entries across job/level/env/pod streams)
python3 scripts/push_logs.py

# 3. Query it via SQL
LOKI_URL=http://localhost:3100 cargo run --example query_loki
LOKI_URL=http://localhost:3100 cargo run --example demo_map_and_agg
```

`examples/query_loki.rs` runs `WHERE level = 'error' AND line LIKE '%panic%' AND timestamp > now() - INTERVAL '1' HOUR`.
Loki's own server logs confirm exact pushdown — the received query was:

```
query="{job=\"myapp\", level=\"error\"} |= \"panic\""
```

i.e. both the label matcher and the line filter were translated into LogQL and evaluated
by Loki itself (`post_filter_lines` in Loki's stats shows it narrowing results server-side),
not fetched in bulk and filtered client-side.

`examples/demo_map_and_agg.rs` additionally exercises the `Map<Utf8,Utf8>` label schema
mode (`labels['level'] = 'error'`, pushed down into LogQL the same way a flattened label
column would be) and a `GROUP BY level, env` aggregation computed entirely by DataFusion
on top of the scanned rows — a 300-row synthetic dataset summed correctly across all
group combinations, confirming the full Arrow round-trip is lossless.

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

This is a from-scratch implementation, targeting DataFusion 42.2.0 / Arrow 53.4.1.

- `cargo check --all-targets`, `cargo test` (24 unit tests + 10 integration tests against
  a mocked Loki HTTP server via `wiremock`, including regressions for the pagination
  dedup logic and a boundary-tie infinite-loop guard), and `cargo test --doc` all pass
  cleanly with zero warnings. Also verified end-to-end against a real
  `grafana/loki:3.1.0` container (see "Live demo" above), including confirming via
  Loki's own server logs that `IN`, same-column `OR`, and map-mode `labels['x']`
  predicates are pushed down into LogQL regex alternation exactly as designed.
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
