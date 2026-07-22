use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_schema::SchemaRef;
use chrono::{DateTime, Utc};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use futures::stream::{self, Stream, StreamExt};

use crate::client::{LogEntry, LokiClient, QueryRangeParams};
use crate::config::LokiConfig;
use crate::convert::entries_to_batches;
use crate::schema::LabelSchema;
use crate::time_range::TimeRange;

/// The physical execution plan that streams a single LogQL query's results
/// (with automatic time-window pagination) as Arrow `RecordBatch`es.
#[derive(Debug)]
pub struct LokiExec {
    logql: String,
    config: LokiConfig,
    time_range: TimeRange,
    schema: SchemaRef,
    label_schema: LabelSchema,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl LokiExec {
    pub fn new(
        logql: String,
        config: LokiConfig,
        time_range: TimeRange,
        schema: SchemaRef,
        label_schema: LabelSchema,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            logql,
            config,
            time_range,
            schema,
            label_schema,
            limit,
            properties,
        }
    }
}

impl DisplayAs for LokiExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "LokiExec: query=\"{}\" limit={:?}",
            self.logql, self.limit
        )
    }
}

impl ExecutionPlan for LokiExec {
    fn name(&self) -> &str {
        "LokiExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "LokiExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "LokiExec only has a single partition, got request for partition {partition}"
            )));
        }

        let stream = LokiRowStream::new(
            self.logql.clone(),
            self.config.clone(),
            self.time_range,
            self.schema.clone(),
            self.label_schema.clone(),
            self.limit,
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Drives paginated `query_range` calls against Loki and yields Arrow batches.
///
/// Loki caps each response to `query_limit` entries. When a page returns
/// exactly that many entries, we assume more data may exist and re-issue the
/// query starting just after the last-seen timestamp (in the configured
/// direction), until a short page is returned, `max_pages` is hit, or the
/// scan-level `limit` (from `LIMIT n` pushdown) is satisfied.
struct LokiRowStream {
    inner: Pin<Box<dyn Stream<Item = DfResult<arrow::array::RecordBatch>> + Send>>,
    schema: SchemaRef,
}

impl LokiRowStream {
    fn new(
        logql: String,
        config: LokiConfig,
        time_range: TimeRange,
        schema: SchemaRef,
        label_schema: LabelSchema,
        limit: Option<usize>,
    ) -> Self {
        let schema_for_stream = schema.clone();
        let label_schema_for_stream = label_schema.clone();
        let batch_size = config.batch_size;
        let paginator = paginate(logql, config, time_range, label_schema, limit);

        // Each page can contain up to `query_limit` entries, which may exceed
        // the configured Arrow batch size, so re-chunk each page into
        // `batch_size`-row batches and flatten the resulting per-page Vecs into
        // a single stream of batches.
        let batches = paginator
            .map(move |res| -> Vec<DfResult<arrow::array::RecordBatch>> {
                match res.and_then(|entries| {
                    entries_to_batches(&schema_for_stream, &label_schema_for_stream, &entries, batch_size)
                        .map_err(DataFusionError::from)
                }) {
                    Ok(batches) => batches.into_iter().map(Ok).collect(),
                    Err(e) => vec![Err(e)],
                }
            })
            .flat_map(|batches| stream::iter(batches));

        Self {
            inner: Box::pin(batches),
            schema,
        }
    }
}

impl Stream for LokiRowStream {
    type Item = DfResult<arrow::array::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for LokiRowStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Uniquely identifies a log entry for pagination-boundary deduplication.
/// Loki has no row ID; timestamp + labels + line is the best available proxy,
/// and collisions (two genuinely identical entries at the same nanosecond)
/// are astronomically unlikely for real log data.
type EntryKey = (i64, Vec<(String, String)>, String);

fn entry_key(entry: &LogEntry) -> EntryKey {
    (
        entry.timestamp_ns,
        entry.labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        entry.line.clone(),
    )
}

/// Yields one `Vec<LogEntry>` per Loki page (sorted ascending/descending by
/// timestamp to match `direction`, so DataFusion sees a globally ordered
/// stream without needing an explicit `ORDER BY`), honoring `limit` as a
/// global cap across all pages combined.
///
/// Pagination boundary handling: Loki's `query_range` window is `[start, end)`
/// — `start` inclusive, `end` exclusive (see Loki's HTTP API reference and
/// `pkg/logcli/query/query.go` in grafana/loki, which uses the same
/// re-fetch-and-dedup strategy). Naively nudging the cursor by 1ns past the
/// last-seen timestamp would silently drop any other entries sharing that
/// exact nanosecond whenever a page boundary lands mid-tie — a real risk at
/// high log throughput. Instead, the boundary timestamp is deliberately
/// re-included on the next page (via `start` as-is going forward, since it's
/// already inclusive, or `end + 1ns` going backward, to compensate for `end`
/// being exclusive) and entries already seen at that exact timestamp are
/// tracked and filtered out of the next page, so ties spanning a page
/// boundary are neither dropped nor duplicated.
fn paginate(
    logql: String,
    config: LokiConfig,
    time_range: TimeRange,
    _label_schema: LabelSchema,
    limit: Option<usize>,
) -> impl Stream<Item = DfResult<Vec<LogEntry>>> {
    struct State {
        client: Option<LokiClient>,
        cursor_start: Option<DateTime<Utc>>,
        cursor_end: Option<DateTime<Utc>>,
        /// Timestamp shared by the last page's boundary row(s), and the keys
        /// of every entry already emitted at that exact timestamp — needed
        /// because the next page re-fetches that timestamp inclusively (to
        /// avoid dropping ties) and must filter out what was already sent.
        boundary_ns: Option<i64>,
        seen_at_boundary: std::collections::HashSet<EntryKey>,
        page: u32,
        done: bool,
        emitted: usize,
    }

    let state = State {
        client: None,
        cursor_start: time_range.start,
        cursor_end: time_range.end,
        boundary_ns: None,
        seen_at_boundary: std::collections::HashSet::new(),
        page: 0,
        done: false,
        emitted: 0,
    };

    stream::unfold(state, move |mut st| {
        let logql = logql.clone();
        let config = config.clone();
        let limit = limit;
        async move {
            loop {
                if st.done {
                    return None;
                }
                if st.page >= config.max_pages {
                    return None;
                }
                if let Some(limit) = limit {
                    if st.emitted >= limit {
                        return None;
                    }
                }

                let client = match &st.client {
                    Some(c) => c.clone(),
                    None => match LokiClient::new(&config) {
                        Ok(c) => {
                            st.client = Some(c.clone());
                            c
                        }
                        Err(e) => return Some((Err(DataFusionError::from(e)), st)),
                    },
                };

                let page_limit = match limit {
                    Some(l) => config.query_limit.min((l - st.emitted) as u32),
                    None => config.query_limit,
                };
                if page_limit == 0 {
                    return None;
                }

                let params = QueryRangeParams {
                    logql: &logql,
                    limit: page_limit,
                    direction: config.direction.as_str(),
                    start: st.cursor_start,
                    end: st.cursor_end,
                };

                let resp = match crate::client::with_deadline(
                    client.query_range(&config, &params),
                    config.timeout,
                )
                .await
                {
                    Ok(r) => r,
                    Err(e) => return Some((Err(DataFusionError::from(e)), st)),
                };

                let mut raw_entries = resp.entries;
                sort_entries(&mut raw_entries, config.direction);

                // Loki's own page-size cap (before our dedup below) is the
                // only reliable signal for "there might be more data" — a
                // page smaller than what we asked for means Loki is
                // exhausted, full stop.
                let loki_page_exhausted = raw_entries.len() < page_limit as usize;
                st.page += 1;

                // Boundary bookkeeping is computed from the *raw* page (what
                // Loki actually returned), not the post-dedup result — using
                // post-dedup entries would leave the boundary/seen-set frozen
                // whenever an entire page turns out to be duplicates,
                // re-issuing an identical request forever instead of making
                // progress.
                if let Some(new_boundary) = last_timestamp(&raw_entries, config.direction) {
                    if Some(new_boundary) != st.boundary_ns {
                        st.seen_at_boundary.clear();
                    }
                    st.boundary_ns = Some(new_boundary);
                }

                // Drop entries at the boundary timestamp that were already
                // emitted by a previous page (this page re-fetches that
                // timestamp inclusively so ties spanning the boundary aren't
                // lost).
                let mut entries = raw_entries;
                if let Some(boundary_ns) = st.boundary_ns {
                    entries.retain(|e| {
                        e.timestamp_ns != boundary_ns || !st.seen_at_boundary.contains(&entry_key(e))
                    });
                }
                st.emitted += entries.len();

                // Matching Loki's own logcli client: if every entry at the
                // boundary timestamp came back as an already-seen duplicate
                // AND the page was full (page_limit reached), we cannot tell
                // whether there's genuinely more data past this tie or Loki
                // is stuck re-returning the same saturated timestamp forever
                // — re-issuing the identical request would loop without
                // bound. Surface a clear error instead of hanging.
                if entries.is_empty() && !loki_page_exhausted {
                    return Some((
                        Err(DataFusionError::from(crate::error::LokiError::Unsupported(format!(
                            "more than {page_limit} log entries share timestamp {boundary_ns}ns, exceeding the page limit; \
                             increase LokiConfig::query_limit to make progress past this timestamp",
                            boundary_ns = st.boundary_ns.unwrap_or_default(),
                        )))),
                        st,
                    ));
                }

                if let Some(boundary_ns) = st.boundary_ns {
                    for e in entries.iter().filter(|e| e.timestamp_ns == boundary_ns) {
                        st.seen_at_boundary.insert(entry_key(e));
                    }
                    advance_cursor(&mut st.cursor_start, &mut st.cursor_end, boundary_ns, config.direction);
                }

                if loki_page_exhausted {
                    st.done = true;
                }

                if entries.is_empty() {
                    // Everything in this (non-full) page was a boundary
                    // duplicate but Loki wasn't exhausted — loop around for
                    // the next page instead of yielding an empty batch.
                    if st.done {
                        return None;
                    }
                    continue;
                }

                return Some((Ok(entries), st));
            }
        }
    })
}

fn sort_entries(entries: &mut [LogEntry], direction: crate::config::Direction) {
    use crate::config::Direction;
    match direction {
        Direction::Forward => entries.sort_by_key(|e| e.timestamp_ns),
        Direction::Backward => entries.sort_by_key(|e| std::cmp::Reverse(e.timestamp_ns)),
    }
}

fn last_timestamp(entries: &[LogEntry], direction: crate::config::Direction) -> Option<i64> {
    use crate::config::Direction;
    match direction {
        Direction::Forward => entries.iter().map(|e| e.timestamp_ns).max(),
        Direction::Backward => entries.iter().map(|e| e.timestamp_ns).min(),
    }
}

fn advance_cursor(
    start: &mut Option<DateTime<Utc>>,
    end: &mut Option<DateTime<Utc>>,
    boundary_ns: i64,
    direction: crate::config::Direction,
) {
    use crate::config::Direction;
    let secs = boundary_ns.div_euclid(1_000_000_000);
    let nanos = boundary_ns.rem_euclid(1_000_000_000) as u32;
    let Some(dt) = DateTime::<Utc>::from_timestamp(secs, nanos) else {
        return;
    };
    // Loki's query_range window is [start, end) — start is inclusive
    // (>=), end is exclusive (<). This must stay in sync with how Loki's
    // own logcli client paginates (pkg/logcli/query/query.go): for
    // forward queries the next `start` is set to the boundary timestamp
    // as-is, since `start` is already inclusive; for backward queries
    // `end` must be nudged 1ns past the boundary, since `end` is
    // exclusive and would otherwise drop the boundary row entirely on
    // the next page. Either way, entries already emitted at the exact
    // boundary timestamp are filtered out via `seen_at_boundary` so
    // re-including that timestamp doesn't yield duplicates.
    match direction {
        Direction::Forward => *start = Some(dt),
        Direction::Backward => *end = Some(dt + chrono::Duration::nanoseconds(1)),
    }
}
