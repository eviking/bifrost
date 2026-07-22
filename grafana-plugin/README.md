# bifrost-datafusion-datasource (Grafana plugin)

A Grafana backend datasource plugin that lets panels query Loki with SQL. It does not talk
to Loki directly — every query is forwarded as plain SQL to the `bifrost-bridge` HTTP
service (see `../bridge/`), which executes it through Apache DataFusion against
`datafusion-loki`.

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

```sh
npm run build                                        # frontend: dist/module.js
GOOS=linux  GOARCH=arm64 go build -o dist/gpx_bifrost_linux_arm64  ./pkg   # for Grafana-in-Docker
GOOS=darwin GOARCH=arm64 go build -o dist/gpx_bifrost_darwin_arm64 ./pkg  # for running Grafana natively
```

Grafana does not hot-reload a backend plugin process on file change — restart the Grafana
container (or process) after rebuilding for changes to take effect:

```sh
docker restart grafana-demo
```
